import sys
import os
import re
import threading

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


# This class was made with the help of Copilot from the skeleton code.
class QueueStorage:
    """
    Thread‑safe queue storage.
    """
    def __init__(self):
        self._queue = []
        self._lock = threading.Lock()

    def enqueue(self, order_id):
        with self._lock:
            self._queue.append(order_id)
            logger.debug(f"Queue after enqueue: {self._queue}")
            return True, list(self._queue)

    def dequeue(self):
        with self._lock:
            if not self._queue:
                logger.debug("Dequeue attempted but queue is empty.")
                return True, None, []

            removed = self._queue.pop(0)
            logger.debug(f"Dequeued: {removed}, queue now: {self._queue}")
            return True, removed, list(self._queue)

    def get_queue(self):
        with self._lock:
            logger.debug(f"Queue read: {self._queue}")
            return True, list(self._queue)

executor = ThreadPoolExecutor(max_workers=6)

def async_enqueue(queue_storage, order_id):
    return executor.submit(queue_storage.enqueue, order_id)

def async_dequeue(queue_storage):
    return executor.submit(queue_storage.dequeue)

def async_get_queue(queue_storage):
    return executor.submit(queue_storage.get_queue)


# This class was made with the help of Copilot from the skeleton code.
class OrderQueueService(order_queue_grpc.OrderQueueServiceServicer):
    def __init__(self, svc_idx=0, total_svcs=3):
        self.svc_idx = svc_idx
        self.total_svcs = total_svcs
        self.queue = QueueStorage()

    def GetQueue(self, request, context):
        ok, order_queue = self.queue.get_queue()
        return order_queue.QueueResponse(
            success=ok,
            order_queue=order_queue
        )

    def Enqueue(self, request, context):
        order_id = request.addable_order
        logger.info(f"Enqueue request: {order_id}")

        ok, order_queue = self.queue.enqueue(order_id)
        return order_queue.QueueResponse(
            success=ok,
            order_queue=order_queue
        )

    def Dequeue(self, request, context):
        logger.info("Dequeue request received")

        ok, removed, order_queue = self.queue.dequeue()
        # We don't expose the removed item directly in QueueResponse,
        # but can log it for debugging
        if removed:
            logger.info(f"Dequeued order: {removed}")

        return order_queue.QueueResponse(
            success=ok,
            order_queue=order_queue
        )


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