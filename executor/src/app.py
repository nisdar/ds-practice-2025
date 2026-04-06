import sys
import os
import threading
import time

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

    def _next_peer(self, id_list):
        idx = id_list.index(self.my_id)
        next_idx = (idx + 1) % len(id_list)
        return id_list[next_idx]

    def _channel_for(self, peer_id):
        host = f"executor-{peer_id}:50055"
        return grpc.insecure_channel(host)

    
    # Starts a new LeaderElection
    # We use election in a ring
    def StartLeaderElection(self, request, context):
        logger.info(f"{self.my_id}: Starting leader election")
        ids = [self.my_id]
        next_id = self._next_peer(self.peer_ids)
        stub = executor_grpc.ExecutorServiceStub(
            self._channel_for(next_id)
        )
        stub.ElectLeader(
                executor.LeaderElectionRequest(
                executors_ids=ids,
                finished=False
            )
        )
        return executor.LeaderElectionResponse(
            executors_ids=ids,
            finished=False
        )
    
    # Continues an existing LeaderElection
    def ElectLeader(self, request, context):
        ids = list(request.executors_ids)
        if self.my_id not in ids:
            ids.append(self.my_id)
        # If message returns to starter
        if ids[0] == self.my_id and len(ids) > 1:
            leader = max(ids)
            logger.info(f"{self.my_id}: Election complete, leader={leader}")
            # Broadcast announcement
            next_id = self._next_peer(self.peer_ids)
            stub = executor_grpc.ExecutorServiceStub(self._channel_for(next_id))
            stub.AnnounceLeader(executor.LeaderAnnouncementRequest(
                leader_id=leader,
                finished=False
            ))
            return executor.LeaderElectionResponse(
                executors_ids=ids,
                finished=True
            )
        # Else forward message
        next_id = self._next_peer(self.peer_ids)
        stub = executor_grpc.ExecutorServiceStub(self._channel_for(next_id))
        stub.ElectLeader(executor.LeaderElectionRequest(
            executors_ids=ids,
            finished=False
        ))

        return executor.LeaderElectionResponse(
            executors_ids=ids,
            finished=False
        )


    # Announces the chosen leader
    def AnnounceLeader(self, request, context):
        leader = request.leader_id
        with self.lock:
            self.leader_id = leader
        logger.info(f"{self.my_id}: Leader announced: {leader}")

        next_id = self._next_peer(self.peer_ids)
        # Stop forwarding only when the message has gone full circle back to leader
        if next_id == leader:
            return executor.LeaderAnnouncementResponse(leader_id=leader, finished=True)

        stub = executor_grpc.ExecutorServiceStub(self._channel_for(next_id))
        stub.AnnounceLeader(executor.LeaderAnnouncementRequest(
            leader_id=leader,
            finished=False
        ))
        return executor.LeaderAnnouncementResponse(leader_id=leader, finished=False)

    
    def run(self):
        logger.info(f"{self.my_id}: Executor main loop started")
        time.sleep(2)
        if not self.started:
            self.started = True
            logger.info(f"{self.my_id}: Starting election at startup")
            stub = executor_grpc.ExecutorServiceStub(
                self._channel_for(self._next_peer(self.peer_ids))
            )
            stub.StartLeaderElection(executor.LeaderElectionRequest())
        
        POLL_INTERVAL = 2  # seconds to wait when queue is empty

        while True:
            if self.leader_id == self.my_id:
                logger.info(f"{self.my_id}: I am the leader. Dequeuing...")
                try:
                    response = self.queue_stub.Dequeue(order_queue.DequeueRequest())
                    if response.order:                        # was response.order_queue
                        logger.info(f"Order is being executed: {response.order}")
                        # process the order here...
                    else:
                        logger.debug("Queue empty, backing off...")
                        time.sleep(POLL_INTERVAL)             # back off when empty
                except Exception as e:
                    logger.error(f"Dequeue error: {e}")
                    time.sleep(POLL_INTERVAL)                 # back off on error too
            else:
                time.sleep(1)

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
    grpc_server.wait_for_termination()

if __name__ == "__main__":
    my_id = int(os.getenv("EXECUTOR_ID", "1"))
    peers = list(map(int, os.getenv("EXECUTOR_PEERS", "1").split(",")))
    launch_executor(my_id, peers)
