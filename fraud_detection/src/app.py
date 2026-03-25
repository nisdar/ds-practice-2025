import sys
import os
import re

#Set up logging
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("Fraud detection")

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
fraud_detection_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/fraud_detection'))
sys.path.insert(0, fraud_detection_grpc_path)
import fraud_detection_pb2 as fraud_detection
import fraud_detection_pb2_grpc as fraud_detection_grpc

import grpc
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor, as_completed


# Create a class to define the server functions, derived from
# fraud_detection_pb2_grpc.HelloServiceServicer
class HelloService(fraud_detection_grpc.HelloServiceServicer):
    # Create an RPC function to say hello
    def SayHello(self, request, context):
        # Create a HelloResponse object
        response = fraud_detection.HelloResponse()
        # Set the greeting field of the response object
        response.greeting = "Hello, " + request.name
        # Log the greeting message
        logger.debug(response.greeting)
        # Return the response object
        return response


class UserDataChecker:
    def __call__(self, card_number):
        """
        Fake rule:
        - If card starts with "999" → suspicious user profile.
        """
        if card_number.startswith("999"):
            return False, "Suspicious card prefix (999*)"
        return True, None


class CreditCardDataChecker:
    def __call__(self, card_number):
        """
        Simple sanity check:
        - Must be digits
        - Length must be 16
        """
        if not re.fullmatch(r'^\d{16}$', card_number):
            return False, "Invalid credit-card format"
        return True, None


class OrderAmountChecker:
    def __call__(self, amount):
        """
        Business rule:
        - > 1000 is considered fraud
        """
        if amount > 1000:
            return False, "Order amount exceeds safe threshold"
        return True, None


from concurrent.futures import ThreadPoolExecutor
executor = ThreadPoolExecutor(max_workers=6)
## asynchronously calling the services

def async_check_user_data(card_number):
    return executor.submit(UserDataChecker(), card_number)

def async_check_credit_card_data(card_number):
    return executor.submit(CreditCardDataChecker(), card_number)

def async_check_order_amount(amount):
    return executor.submit(OrderAmountChecker(), amount)



class FraudDetectionService(fraud_detection_grpc.FraudDetectionServiceServicer):
    def __init__(self, svc_idx=0, total_svcs=3):
        self.svc_idx = svc_idx
        self.total_svcs = total_svcs
        self.orders = {}

    def init_order(self, order_id, data):
        self.orders[order_id] = {"data": data, "vc": [0] * self.total_svcs}

    def CheckFraud(self, request, context):
        card_number = request.card_number
        order_amount = request.order_amount

        logger.info(f"Checking fraud for card {card_number} and amount {order_amount}")

        # Launch concurrent checks
        tasks = {
            async_check_user_data(card_number): "User-data check failed",
            async_check_credit_card_data(card_number): "Credit-card check failed",
            async_check_order_amount(order_amount): "Order-amount check failed"
        }

        fraud_detected = False
        failure_reason = None

        # Aggregate results
        for future in as_completed(tasks):
            ok, error = future.result()
            if not ok:
                fraud_detected = True
                failure_reason = error
                break

        if fraud_detected:
            logger.info(f"Fraud detected: {failure_reason}")
            return fraud_detection.FraudResponse(is_fraud=True)

        return fraud_detection.FraudResponse(is_fraud=False)


def serve():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor())
    fraud_detection_grpc.add_HelloServiceServicer_to_server(HelloService(), server)
    fraud_detection_grpc.add_FraudDetectionServiceServicer_to_server(FraudDetectionService(), server)
    # Listen on port 50051
    port = "50051"
    server.add_insecure_port("[::]:" + port)
    # Start the server
    server.start()
    logger.info("Server started. Listening on port 50051.")
    # Keep thread alive
    server.wait_for_termination()


if __name__ == '__main__':
    serve()