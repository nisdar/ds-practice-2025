import sys
import os
import threading
import time
import random
import json

# Set up logging
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("Executor")


# Little price normalization because of funky floating-points
def normalize_price(price):
    return round(price + 1e-9, 2)


# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
executor_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/executor'))
sys.path.insert(0, executor_grpc_path)
import executor_pb2 as executor
import executor_pb2_grpc as executor_grpc

order_queue_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/order_queue'))
sys.path.insert(0, order_queue_grpc_path)
import order_queue_pb2 as order_queue
import order_queue_pb2_grpc as order_queue_grpc

database_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/database'))
sys.path.insert(0, database_grpc_path)
import database_pb2 as database
import database_pb2_grpc as database_grpc

import grpc
from concurrent import futures


class HelloService(executor_grpc.HelloServiceServicer):
    def SayHello(self, request, context):
        response = executor.HelloResponse()
        response.greeting = "Hello, " + request.name
        logger.debug(response.greeting)
        return response


# This was made with the help of Copilot, based on a skeleton of code.
class ExecutorService(executor_grpc.ExecutorServiceServicer):
    def __init__(self, executor_id, peer_ids, queue_stub):
        self.my_id = int(executor_id)
        self.peer_ids = sorted(map(int, peer_ids))
        self.queue_stub = queue_stub
        self.leader_id = None
        self.started = False
        self.lock = threading.Lock()
        self.last_leader_heartbeat = time.time()
        self.alive = True

    def _next_peer(self, id_list):
        idx = id_list.index(self.my_id)
        next_idx = (idx + 1) % len(id_list)
        return id_list[next_idx]

    def _channel_for(self, peer_id):
        host = f"executor-{peer_id}:50055"
        return grpc.insecure_channel(host)

    def _send_to_next_live(self, rpc_fn, request):
        # Try each peer in ring order, skip unreachable ones
        ids = self.peer_ids
        start_idx = (ids.index(self.my_id) + 1) % len(ids)
        for i in range(len(ids) - 1):
            peer_id = ids[(start_idx + i) % len(ids)]
            if peer_id == self.my_id:
                continue
            try:
                channel = grpc.insecure_channel(f"executor-{peer_id}:50055")
                stub = executor_grpc.ExecutorServiceStub(channel)
                rpc_fn(stub, request)
                return peer_id  # success
            except grpc.RpcError:
                logger.warning(f"Peer {peer_id} unreachable, trying next")
        # No live peers — elect self as leader
        logger.warning(f"{self.my_id}: No live peers, electing self as leader")
        with self.lock:
            self.leader_id = self.my_id
        return None

    def _check_alive(self, context):
        if not self.alive:
            context.abort(grpc.StatusCode.UNAVAILABLE, f"Executor {self.my_id} is down")
            return False
        return True

    def _load_title_to_id_map(self, db_stub):
        """
        Fetches all books from database and builds
        { title -> id } mapping.
        """
        try:
            resp = db_stub.GetAll(database.GetAllRequest())
            mapping = {b.title: b.id for b in resp.books}
            logger.info(f"Loaded title→id map: {mapping}")
            return mapping
        except grpc.RpcError as e:
            logger.error(f"Failed to load catalog from database: {e}")
            return {}

    # Starts a new LeaderElection
    # We use election in a ring
    def StartLeaderElection(self, request, context):
        if not self._check_alive(context): return None
        logger.info(f"{self.my_id}: Starting leader election")
        ids = [self.my_id]
        next_id = self._next_peer(self.peer_ids)
        _stub = executor_grpc.ExecutorServiceStub(
            self._channel_for(next_id)
        )
        self._send_to_next_live(
            lambda _stub, req: _stub.ElectLeader(req),
            executor.LeaderElectionRequest(executors_ids=ids, finished=False)
        )
        return executor.LeaderElectionResponse(
            executors_ids=ids,
            finished=False
        )

    # Continues an existing LeaderElection -- Currently not used for bonus
    def ElectLeader(self, request, context):
        if not self._check_alive(context): return None
        ids = list(request.executors_ids)
        if self.my_id not in ids:
            ids.append(self.my_id)
        # If message returns to starter
        if ids[0] == self.my_id and len(ids) > 1:
            leader = max(ids)
            logger.info(f"{self.my_id}: Election complete, leader={leader}")
            # Broadcast announcement
            self._send_to_next_live(
                lambda stub, req: stub.AnnounceLeader(req),
                executor.LeaderAnnouncementRequest(leader_id=leader, finished=False)
            )
            return executor.LeaderElectionResponse(executors_ids=ids, finished=True)

        # Else forward message
        self._send_to_next_live(
            lambda stub, req: stub.ElectLeader(req),
            executor.LeaderElectionRequest(executors_ids=ids, finished=False)
        )
        return executor.LeaderElectionResponse(executors_ids=ids, finished=False)

    # Announces the chosen leader
    def AnnounceLeader(self, request, context):
        if not self._check_alive(context): return None
        leader = request.leader_id
        with self.lock:
            self.leader_id = leader
        logger.info(f"{self.my_id}: leader_id set to {leader}")

        next_id = self._next_peer(self.peer_ids)
        # Stop forwarding once the announcement has gone full circle back to leader
        if next_id == leader:
            return executor.LeaderAnnouncementResponse(leader_id=leader, finished=True)

        # ← use _send_to_next_live instead of direct stub call
        self._send_to_next_live(
            lambda stub, req: stub.AnnounceLeader(req),
            executor.LeaderAnnouncementRequest(leader_id=leader, finished=False)
        )
        return executor.LeaderAnnouncementResponse(leader_id=leader, finished=False)

    def Heartbeat(self, request, context):
        if not self._check_alive(context): return None
        with self.lock:
            if request.leader_id == self.leader_id:
                self.last_leader_heartbeat = time.time()
        return executor.HeartbeatResponse()

    def _broadcast_heartbeat(self):
        logger.info(f"Leader heartbeat: {self.my_id}")
        for peer_id in self.peer_ids:
            if peer_id == self.my_id:
                continue
            try:
                channel = grpc.insecure_channel(f"executor-{peer_id}:50055")
                stub = executor_grpc.ExecutorServiceStub(channel)
                stub.Heartbeat(executor.HeartbeatRequest(leader_id=self.my_id))
            except grpc.RpcError:
                pass  # dead followers don't matter

    # Created with the help of O365 Copilot
    def _parse_order_payload(self, raw_order_json):
        """
        Returns a list of (book_id, quantity) tuples.
        Expects valid JSON payload from orchestrator.
        """
        try:
            parsed = json.loads(raw_order_json)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid order payload JSON: {e}")
            return []

        items = parsed.get("items", [])
        result = []

        for item in items:
            try:
                book_title = item["name"]
                quantity = int(item["quantity"])
                result.append((book_title, quantity))
            except Exception as e:
                logger.error(f"Malformed item in order payload: {e}")

        return result

    def execute_order(self, raw_order):
        primary_id = 1
        channel = grpc.insecure_channel(f"database-{primary_id}:50057")
        db_stub = database_grpc.DatabaseServiceStub(channel)
        title_to_id = self._load_title_to_id_map(db_stub)
        items = self._parse_order_payload(raw_order)
        if not items:
            logger.error("No valid order items parsed")
            return False
        planned_writes = []
        for book_title, quantity in items:
            if book_title not in title_to_id:
                logger.warning(f"Unknown book '{book_title}'")
                return False
            book_id = title_to_id[book_title]
            books = db_stub.GetAll(database.GetAllRequest()).books
            read_book = next((b for b in books if b.id == book_id), None)
            if read_book is None:
                logger.warning(f"Book id {book_id} not found in DB")
                return False
            if read_book.stock < quantity:
                logger.warning(
                    f"Insufficient stock for '{book_title}' "
                    f"(id={book_id}): {read_book.stock} < {quantity}"
                )
                return False
            planned_writes.append((read_book, read_book.stock - quantity))
        for book, new_stock in planned_writes:
            write_resp = db_stub.Write(
                database.WriteRequest(
                    book=database.Book(
                        id=book.id,
                        title=book.title,
                        author=book.author,
                        price=normalize_price(book.price),
                        stock=new_stock
                    )
                )
            )
            if not write_resp.success:
                logger.error(f"Commit failed for book id={book.id}")
                return False
        self.log_database_state(db_stub, prefix="AFTER ORDER")
        return True

    def log_database_state(self, db_stub, prefix="DB STATE"):
        try:
            resp = db_stub.GetAll(database.GetAllRequest())
            books = [
                {
                    "id": b.id,
                    "title": b.title,
                    "author": b.author,
                    "stock": b.stock,
                    "price": normalize_price(b.price),
                }
                for b in resp.books
            ]
            logger.info(f"{prefix}: {json.dumps(books, indent=2)}")
        except Exception as e:
            logger.error(f"Failed to log DB state: {e}")

    def run(self):
        HEARTBEAT_INTERVAL = 2
        LEADER_TIMEOUT = 6
        POLL_INTERVAL = 2

        logger.info(f"{self.my_id}: Executor main loop started")
        time.sleep(2)

        if not self.started:
            self.started = True
            logger.info(f"{self.my_id}: Starting election at startup")
            self._trigger_election()  # ← use _trigger_election, not direct stub

        # Poll until election settles (up to 10 seconds)
        deadline = time.time() + 10
        while time.time() < deadline:
            with self.lock:
                current_leader = self.leader_id
            if current_leader is not None:
                break
            time.sleep(0.5)

        with self.lock:
            current_leader = self.leader_id

        logger.info(f"{self.my_id}: Election settled, leader={current_leader}")

        # Challenge if I have a higher ID than the current leader
        if current_leader is not None and self.my_id > current_leader:
            logger.info(f"{self.my_id}: Higher ID than leader {current_leader}, challenging")
            self._trigger_election()
            time.sleep(5)  # wait for re-election to settle

        while True:
            with self.lock:
                leader = self.leader_id
                alive = self.alive

            if not alive:
                logger.debug(f"{self.my_id}: I am down, pausing...")
                time.sleep(5)
                continue

            if leader == self.my_id:
                logger.info(f"{self.my_id}: I am the leader. Dequeuing...")
                try:
                    response = self.queue_stub.Dequeue(order_queue.DequeueRequest())
                    self._broadcast_heartbeat()
                    logger.info(
                        f"Dequeue response: success={response.success}, "
                        f"order_id='{response.order_id}'"
                    )
                    if not response.success or not response.order_id:
                        logger.debug("Queue empty, backing off...")
                        time.sleep(POLL_INTERVAL)
                        continue
                    logger.info(f"Executing order_id={response.order_id}")
                    success = self.execute_order(response.order_payload_json)
                    if success:
                        logger.info("Order executed successfully")
                    else:
                        logger.warning("Order execution FAILED")
                except Exception as e:
                    logger.error(f"Dequeue error: {e}")
                    time.sleep(POLL_INTERVAL)
                time.sleep(HEARTBEAT_INTERVAL)
            else:
                if leader is not None:
                    elapsed = time.time() - self.last_leader_heartbeat
                    if elapsed > LEADER_TIMEOUT:
                        logger.warning(f"{self.my_id}: Leader {leader} timed out, re-electing")
                        with self.lock:
                            self.leader_id = None
                        self._trigger_election()
                time.sleep(1)

    def _trigger_election(self):
        try:
            self._send_to_next_live(
                lambda stub, req: stub.StartLeaderElection(req),
                executor.LeaderElectionRequest()
            )
            logger.info(f"Triggering reelection")
        except Exception as e:
            logger.error(f"Could not start election: {e}")


# Crahing some replicas in order to show that system is dynamic
def random_crash_simulator(service):
    enabled = os.getenv("CRASH_ENABLED", "false").lower() == "true"
    min_delay = float(os.getenv("CRASH_MIN_DELAY", "15"))
    max_delay = float(os.getenv("CRASH_MAX_DELAY", "45"))
    restart_delay = float(os.getenv("RESTART_DELAY", "10"))

    if not enabled:
        return

    def _crash_and_recover():
        delay = random.uniform(min_delay, max_delay)
        logger.warning(f"Executor {service.my_id}: will crash in {delay:.1f}s")
        time.sleep(delay)

        # Simulate crash
        logger.warning(f"Executor {service.my_id}: CRASHED (simulated)")
        with service.lock:
            service.alive = False
            service.leader_id = None  # forget leader state

        # Simulate downtime
        logger.warning(f"Executor {service.my_id}: will recover in {restart_delay}s")
        time.sleep(restart_delay)

        # Simulate recovery
        logger.warning(f"Executor {service.my_id}: RECOVERED, rejoining cluster")
        with service.lock:
            service.alive = True
            service.last_leader_heartbeat = time.time()

        # Trigger re-election if this node has higher ID
        service._trigger_election()

    threading.Thread(target=_crash_and_recover, daemon=True).start()


# This method was made with the help of Copilot from skeleton code.
def launch_executor(executor_id, peer_ids):
    my_executor_id = int(executor_id)
    peer_ids = list(map(int, peer_ids))
    # Connect to order queue
    channel = grpc.insecure_channel("order_queue:50054")
    queue_stub = order_queue_grpc.OrderQueueServiceStub(channel)
    service = ExecutorService(my_executor_id, peer_ids, queue_stub)

    # gRPC server for receiving election messages
    grpc_server = grpc.server(futures.ThreadPoolExecutor())
    executor_grpc.add_ExecutorServiceServicer_to_server(service, grpc_server)
    executor_grpc.add_HelloServiceServicer_to_server(HelloService(), grpc_server)
    grpc_server.add_insecure_port("[::]:50055")
    grpc_server.start()
    logger.info(f"Executor {my_executor_id} gRPC server running on :50055")
    # run election and processing loop in background
    t = threading.Thread(target=service.run, daemon=True)
    t.start()
    # create a crash in a replica
    random_crash_simulator(service)
    grpc_server.wait_for_termination()


if __name__ == "__main__":
    my_id = int(os.getenv("EXECUTOR_ID", "1"))
    # EXECUTOR_PEERS=1,2,3,4,5 works for any N
    peers = list(map(int, os.getenv("EXECUTOR_PEERS", "1").split(",")))
    launch_executor(my_id, peers)
