import sys
import os
import re

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

class ExecutorService(executor_grpc.ExecutorServiceServicer):
    def __init__(self, executor_id, known_ids, queue_stub):
        self.executor_id = executor_id
        self.known_ids = known_ids
        self.queue_stub = queue_stub
        self.leader_id = None
    
    # Starts a new LeaderElection
    def StartLeaderElection():
        # TODO pick a leader using chosen algorithm
        # TODO i vote for ring election
        ...
    
    # Continues an existing LeaderElection
    def ElectLeader():
        ...

    # Announces the chosen leader
    def AnnounceLeader():
        ...
    
    def run(self):
        # TODO im I'm the leader, repeatedly dequeue and "execute" orders
        # else, watch or wait for changes in leadership
        ...

# TODO this probably replaces serve() ? maybe
def launch_executor(executor_id, known_ids):
    # TODO set up gRPC, connect to order queue
    # queue_stub = ...
    svc = ExecutorService(executor_id, known_ids, queue_stub=None)
    svc.StartLeaderElection()
    svc.run()


def serve():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor())
    executor_grpc.add_HelloServiceServicer_to_server(HelloService(), server)
    # TODO fill with real values
    executor_grpc.add_ExecutorServiceServicer_to_server(ExecutorService(0, [], order_queue_grpc), server)
    # Listen on port 50055
    port = "50055"
    server.add_insecure_port("[::]:" + port)
    # Start the server
    server.start()
    logger.info("Server started. Listening on port 50055.")
    # Keep thread alive
    server.wait_for_termination()


if __name__ == '__main__':
    serve()