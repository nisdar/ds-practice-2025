import sys
import os
import re
import threading
import time
import random
import signal

#Set up logging
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("Executor")

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


import grpc
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor, as_completed

class HelloService(executor_grpc.HelloServiceServicer):
    def SayHello(self, request, context):
        response = executor.HelloResponse()
        response.greeting = "Hello, " + request.name
        logger.debug(response.greeting)
        return response

# This was made with the help of Copilot, based on a skeleton of code.
class ExecutorService(executor_grpc.ExecutorServiceServicer):
    def __init__(self, my_id, peer_ids, queue_stub):
        self.my_id = int(my_id)
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

    # Starts a new LeaderElection
    # We use election in a ring
    def StartLeaderElection(self, request, context):
        if not self._check_alive(context): return None
        logger.info(f"{self.my_id}: Starting leader election")
        ids = [self.my_id]
        next_id = self._next_peer(self.peer_ids)
        stub = executor_grpc.ExecutorServiceStub(
            self._channel_for(next_id)
        )
        self._send_to_next_live(
            lambda stub, req: stub.ElectLeader(req),
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
        next_id = self._next_peer(self.peer_ids)
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

        
        next_id = self._next_peer(self.peer_ids)
        stub = executor_grpc.ExecutorServiceStub(self._channel_for(next_id))
        self._send_to_next_live(
            lambda stub, req: stub.ElectLeader(req),
            executor.LeaderElectionRequest(executors_ids=ids, finished=False)
        )

        return executor.LeaderElectionResponse(
            executors_ids=ids,
            finished=False
        )


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
                    if response.order:
                        logger.info(f"Order is being executed: {response.order}")
                    else:
                        logger.debug("Queue empty, backing off...")
                        time.sleep(POLL_INTERVAL)
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
def launch_executor(my_id, peer_ids):
    my_id = int(my_id)
    peer_ids = list(map(int, peer_ids))
    # Connect to order queue
    channel = grpc.insecure_channel("order_queue:50054")
    queue_stub = order_queue_grpc.OrderQueueServiceStub(channel)
    service = ExecutorService(my_id, peer_ids, queue_stub)

    # gRPC server for receiving election messages
    grpc_server = grpc.server(futures.ThreadPoolExecutor())
    executor_grpc.add_ExecutorServiceServicer_to_server(service, grpc_server)
    executor_grpc.add_HelloServiceServicer_to_server(HelloService(), grpc_server)
    grpc_server.add_insecure_port("[::]:50055")
    grpc_server.start()
    logger.info(f"Executor {my_id} gRPC server running on :50055")
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