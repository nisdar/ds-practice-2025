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
import order_queue_pb2 as oq
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
        response = oq.HelloResponse(
            greeting=f"Hello, {request.name}"
        )
        # Set the greeting field of the response object
        response.greeting = "Hello, " + request.name
        # Log the greeting message
        logger.debug(response.greeting)
        # Return the response object
        return response


# This class was made with the help of Copilot from the skeleton code.
class QueueStorage:
    def __init__(self):
        self._queue = []
        self._lock = threading.Lock()

    def enqueue(self, order_id, payload_json):
        with self._lock:
            self._queue.append((order_id, payload_json))
            logger.debug(f"Queue after enqueue: {self._queue}")
            return True

    def dequeue(self):
        with self._lock:
            if not self._queue:
                return True, None
            return True, self._queue.pop(0)

    def get_queue(self):
        with self._lock:
            # Expose only order_ids for debugging / inspection
            return [order_id for order_id, _ in self._queue]

# This class was made with the help of Copilot from the skeleton code.
class OrderQueueService(order_queue_grpc.OrderQueueServiceServicer):
    def __init__(self, svc_idx=0, total_svcs=3):
        self.svc_idx = svc_idx
        self.total_svcs = total_svcs
        self.queue = QueueStorage()

    def GetQueue(self, request, context):
        queue_contents = self.queue.get_queue()
        return oq.GetQueueResponse(
            orders=queue_contents
        )

    def Enqueue(self, request, context):
        order_id = request.order_id
        payload_json = request.order_payload_json
        logger.info(f"Enqueue request: order_id={order_id}")
        ok = self.queue.enqueue(order_id, payload_json)
        if not ok:
            context.abort(grpc.StatusCode.INTERNAL, "Queue enqueue failed")
        return oq.EnqueueResponse(success=True)
        
    def Dequeue(self, request, context):
        logger.info("Dequeue request received")
        ok, entry = self.queue.dequeue()
        if not ok:
            context.abort(grpc.StatusCode.INTERNAL, "Queue dequeue failed")
        if entry is None:
            logger.debug("Queue empty on dequeue")
            return oq.DequeueResponse(
                success=True,
                order_id="",
                order_payload_json=""
            )
        order_id, payload_json = entry
        logger.info(f"Dequeued order_id={order_id}")
        return oq.DequeueResponse(
            success=True,
            order_id=order_id,
            order_payload_json=payload_json
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