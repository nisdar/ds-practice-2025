import sys
import os
import re

#Set up logging
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("Payment")

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
payment_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/payment'))
sys.path.insert(0, payment_grpc_path)
import payment_pb2 as payment
import payment_pb2_grpc as payment_grpc

import grpc
from concurrent import futures
import asyncio
    
class PaymentService(payment_grpc.PaymentServiceServicer):
    def __init__(self):
        self.order_statuses = dict()

    def PreparePayment(self, request, context):
        if request.orderId in self.order_statuses:
            logger.error(f"Order {request.orderId} cannot be prepared because it is already in state {self.order_statuses[request.orderId]}")
            return payment.PrepareResponse(ready=False)
        logger.info(f"Payment prepared for Order {request.orderId}")
        self.order_statuses[request.orderId] = 'ready'
        return payment.PrepareResponse(ready=True)
        
    def CommitPayment(self, request, context):
        if request.orderId not in self.order_statuses:
            logger.error(f"Order {request.orderId} cannot be commited because it has not been prepared")
            return payment.CommitResponse(success=False)
        elif self.order_statuses[request.orderId] != 'ready':
            logger.error(f"Order {request.orderId} cannot be commited because it is already in state {self.order_statuses[request.orderId]}")
            return payment.CommitResponse(success=False)
        else:
            logger.info(f"Payment committed for order {request.orderId}")
            self.order_statuses[request.orderId] = 'completed'
            return payment.CommitResponse(success=True)

    def AbortPayment(self, request, context):
        if request.orderId not in self.order_statuses:
            logger.error(f"Order {request.orderId} cannot be commited because it has not been prepared")
            return payment.AbortResponse(aborted=False)
        elif self.order_statuses[request.orderId] != 'ready':
            logger.error(f"Order {request.orderId} cannot be commited because it is already in state {self.order_statuses[request.orderId]}")
            return payment.AbortResponse(aborted=False)
        else:
            logger.info(f"Payment aborted for order {request.orderId}")
            self.order_statuses[request.orderId] = 'aborted'
            return payment.AbortResponse(aborted=True)

def serve():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor())
    payment_grpc.add_PaymentServiceServicer_to_server(PaymentService(), server)
    # Listen on port 50056
    port = "50056"
    server.add_insecure_port("[::]:" + port)
    # Start the server
    server.start()
    logger.info(f"Server started. Listening on port {port}.")
    # Keep thread alive
    server.wait_for_termination()


if __name__ == '__main__':
    serve()