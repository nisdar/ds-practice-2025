import sys
import os
import re

#Set up logging
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("Order queue")

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
order_queue_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/order_queue'))
sys.path.insert(0, order_queue_grpc_path)
import order_queue_pb2 as order_queue
import order_queue_pb2_grpc as order_queue_grpc

import grpc
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor, as_completed


# Create a class to define the server functions, derived from
# order_queue_pb2_grpc.HelloServiceServicer
class HelloService(order_queue_grpc.HelloServiceServicer):
    # Create an RPC function to say hello
    def SayHello(self, request, context):
        # Create a HelloResponse object
        response = order_queue.HelloResponse()
        # Set the greeting field of the response object
        response.greeting = "Hello, " + request.name
        # Log the greeting message
        logger.debug(response.greeting)
        # Return the response object
        return response

class OrderQueueService(order_queue_grpc.OrderQueueServiceServicer):
    def __init__(self, svc_idx=0, total_svcs=3):
        self.svc_idx = svc_idx
        self.total_svcs = total_svcs
        self.order_queue = []

    def GetQueue(self, request, context):
        return order_queue.QueueResponse(success=True, order_queue=self.order_queue)

    # Returns (bool, string[])
    def Enqueue(self, request, context):
        # TODO: lock queue, insert request.orderId
        # TODO: return success response
        ...
        return order_queue.QueueResponse(success=True, order_queue=self.order_queue)
    # Returns (bool, string[])
    def Dequeue(self, request, context):
        ...
        # TODO: lock queue, pop an order if available
        # TODO: return the dequeued order on an empty result
        return order_queue.QueueResponse(success=True, order_queue=self.order_queue)

def serve():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor())
    order_queue_grpc.add_HelloServiceServicer_to_server(HelloService(), server)
    order_queue_grpc.add_OrderQueueServiceServicer_to_server(OrderQueueService(), server)
    # Listen on port 50054
    port = "50054"
    server.add_insecure_port("[::]:" + port)
    # Start the server
    server.start()
    logger.info("Server started. Listening on port 50054.")
    # Keep thread alive
    server.wait_for_termination()


if __name__ == '__main__':
    serve()