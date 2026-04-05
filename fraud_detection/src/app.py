import sys
import os
import re
from google.protobuf.empty_pb2 import Empty

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

suggestions_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/suggestions'))
sys.path.insert(0, suggestions_grpc_path)
import suggestions_pb2 as suggestions
import suggestions_pb2_grpc as suggestions_grpc

import grpc
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio

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


# The following classes were created with the help of Copilot
#   based on an existing fraud detection function,
#   split into classes for asynchronous threading.
class UserDataChecker:
    def __call__(self, card_number):
        if card_number.startswith("999"):
            return False, "Suspicious card prefix (999*)"
        return True, None


class CreditCardDataChecker:
    def __call__(self, card_number):
        """
        Must be digits, length must be 16
        """
        if not re.fullmatch(r'^\d{16}$', card_number):
            return False, "Invalid credit-card format"
        return True, None


class OrderAmountChecker:
    def __call__(self, amount):
        if amount > 1000:
            return False, "Order amount exceeds safe threshold"
        return True, None


from concurrent.futures import ThreadPoolExecutor
executor = ThreadPoolExecutor(max_workers=6)
## asynchronously calling the services
async def async_check_user_data(card_number):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, UserDataChecker(), card_number)

async def async_check_credit_card_data(card_number):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, CreditCardDataChecker(), card_number)

async def async_check_order_amount(amount):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, OrderAmountChecker(), amount)


# This class was also remade with the help of Copilot to combine the refactored classes.
class FraudDetectionService(fraud_detection_grpc.FraudDetectionServiceServicer):
    def __init__(self, svc_idx=0, total_svcs=3):
        self.svc_idx = svc_idx
        self.total_svcs = total_svcs
        self.orders = {}

    #def init_order(self, order_id, data):
    #    self.orders[order_id] = {"data": data, "vc": [0] * self.total_svcs}

    def merge_and_increment(self, local_vc, incoming_vc):
        for i in range(self.total_svcs):
            local_vc[i] = max(local_vc[i], incoming_vc[i])
        local_vc[self.svc_idx] += 1
    
    def InitFraudDetection(self, request, context):
        order_id = request.orderId
        self.orders[order_id] = {"data" : request, "vc": [0] * self.total_svcs}
        return Empty()

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

    # re-made with the help of Copilot for true asyncio.gather implementation
    def CheckFraudNew(self, request, context):
        order_id = request.id
        incoming_vc = request.vectorClock.timeStamp
        entry = self.orders.get(order_id)
        self.merge_and_increment(entry["vc"], incoming_vc)
        card_number = entry["data"].creditCard.number
        order_amount = sum(int(item.quantity) for item in entry["data"].items)
        logger.info(f"Checking fraud for card {card_number} and amount {order_amount}")
        async def run_checks():
            return await asyncio.gather(
                async_check_user_data(card_number),
                async_check_credit_card_data(card_number),
                async_check_order_amount(order_amount),
                return_exceptions=True
            )
        results = asyncio.run(run_checks())
        error_messages = [
            "User-data check failed",
            "Credit-card check failed",
            "Order-amount check failed"
        ]
        for (ok, err), msg in zip(results, error_messages):
            if isinstance(ok, Exception):
                return fraud_detection.OrderResponse(
                    vectorClock=fraud_detection.VectorClock(timeStamp=entry["vc"]),
                    success=False
                )
            if not ok:
                return fraud_detection.OrderResponse(
                    vectorClock=fraud_detection.VectorClock(timeStamp=entry["vc"]),
                    success=False
                )
            
        with grpc.insecure_channel('suggestions:50053') as channel:
            stub = suggestions_grpc.SuggestionsServiceStub(channel)
            request_obj = suggestions.OrderInfo(
                id=order_id,
                vectorClock=suggestions.VectorClock(timeStamp=entry["vc"])
            )
            response = stub.SuggestBooksNew(request_obj)
        return response



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