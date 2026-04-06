import sys
import os
import re
from google.protobuf.empty_pb2 import Empty

#Set up logging
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("Transaction verification")

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
transaction_verification_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/transaction_verification'))
sys.path.insert(0, transaction_verification_grpc_path)
import transaction_verification_pb2 as transaction_verification
import transaction_verification_pb2_grpc as transaction_verification_grpc
fraud_detection_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/fraud_detection'))
sys.path.insert(0, fraud_detection_grpc_path)
import fraud_detection_pb2 as fraud_detection
import fraud_detection_pb2_grpc as fraud_detection_grpc

import grpc
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor, as_completed

class HelloService(transaction_verification_grpc.HelloServiceServicer):
    def SayHello(self, request, context):
        response = transaction_verification.HelloResponse()
        response.greeting = "Hello, " + request.name
        logger.debug(response.greeting)
        return response

# The following classes were created with the help of Copilot
#   based on an existing transaction verification function,
#   split into classes for asynchronous threading.
class ItemDataChecker:
    def __call__(self, items):
        name_regex = r'^[a-zA-Z0-9 \-\']+$'
        quantity_regex = r'^[1-9]\d*$'
        for item in items:
            if not re.fullmatch(name_regex, item.name):
                return False, "Invalid item name"
            if not re.fullmatch(quantity_regex, item.quantity):
                return False, "Invalid item quantity"
        return True, None

class UserChecker:
    def __call__(self, user):
        name_regex = r'^[a-zA-Z]+ [a-zA-Z]+$'
        email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if re.fullmatch(name_regex, user.name) is None:
            return False, "Invalid user name"
        if re.fullmatch(email_regex, user.contact) is None:
            return False, "Invalid user email"
        return True, None


class CreditCardChecker:
    def __call__(self, card):
        number_regex = r'^[456]\d{15}$'
        exp_date_regex = r'^(0[1-9]|1[0-2])\/\d{2}$'
        cvv_regex = r'^\d{3}$'
        if re.fullmatch(number_regex, card.number) is None:
            return False, "Invalid credit-card number"
        if re.fullmatch(exp_date_regex, card.expirationDate) is None:
            return False, "Invalid expiration date"
        if re.fullmatch(cvv_regex, card.cvv) is None:
            return False, "Invalid CVV"
        return True, None


class CommentChecker:
    def __call__(self, comment):
        comment_regex = r'^[a-zA-Z0-9 \-\'.,!?]*$'
        if re.fullmatch(comment_regex, comment.comment) is None:
            return False, "Invalid comment"
        return True, None


class BillingAddressChecker:
    def __call__(self, address):
        street_regex = r'^[a-zA-Z0-9 \-,\.]+$'
        city_regex = r'^[a-zA-Z \-]+$'
        state_regex = r'^[a-zA-Z ]+$'
        zip_regex = r'^\d{5}(-\d{4})?$'
        country_regex = r'^[a-zA-Z]{2,}$'

        if re.fullmatch(street_regex, address.street) is None:
            return False, "Invalid street"
        if re.fullmatch(city_regex, address.city) is None:
            return False, "Invalid city"
        if re.fullmatch(state_regex, address.state) is None:
            return False, "Invalid state"
        if re.fullmatch(zip_regex, address.zip) is None:
            return False, "Invalid ZIP code"
        if re.fullmatch(country_regex, address.country) is None:
            return False, "Invalid country"
        return True, None


class ShippingMethodChecker:
    def __call__(self, method):
        valid = ["Standard", "Express", "Next-Day"]
        if method not in valid:
            return False, "Invalid shipping method"
        return True, None

import asyncio

async def async_check_item_data(items):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, ItemDataChecker(), items)

async def async_check_user(user):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, UserChecker(), user)

async def async_check_comment(comment):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, CommentChecker(), comment)

async def async_check_billing_address(billing):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, BillingAddressChecker(), billing)

async def async_check_credit_card(card):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, CreditCardChecker(), card)

async def async_check_shipping_method(method):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, ShippingMethodChecker(), method)

# This class was also remade with the help of Copilot to combine the refactored classes.
class TransactionVerificationService(transaction_verification_grpc.TransactionVerificationServiceServicer):
    def __init__(self, svc_idx=0, total_svcs=3):
        self.svc_idx = svc_idx
        self.total_svcs = total_svcs
        self.orders = {}

    #def init_order(self, order_id, data):
    #    self.orders[order_id] = {
    #        "data": data,
    #        "vc": [0] * self.total_svcs
    #    }

    def merge_and_increment(self, local_vc, incoming_vc):
        for i in range(self.total_svcs):
            local_vc[i] = max(local_vc[i], incoming_vc[i])
        local_vc[self.svc_idx] += 1
    
    def increment(self, local_vc):
        local_vc[self.svc_idx] += 1

    def InitTransactionVerification(self, request, context):
        order_id = request.orderId
        self.orders[order_id] = {"data" : request, "vc": [0] * self.total_svcs}
        return Empty()
    
    def VerifyTransaction(self, request, context):
        order_id = request.id
        incoming_vc = list(request.vectorClock.timeStamp)
        entry = self.orders.get(order_id)
        self.merge_and_increment(entry["vc"], incoming_vc)

        data = entry["data"]

        async def run_checks():
            # Round 1: a and b run in parallel
            result_a, result_b = await asyncio.gather(
                async_check_item_data(data.items),   # event a
                async_check_user(data.user),          # event b
            )
            self.increment(entry["vc"])  # tick for event a
            self.increment(entry["vc"])  # tick for event b

            # Check a before proceeding to c
            ok_a, err_a = result_a
            if not ok_a:
                return None, err_a or "Item validation failed"

            # Round 2: c (needs a) and d-trigger (needs b) run in parallel
            # c = card format check, b's result gates fraud_detection's d
            ok_b, err_b = result_b
            
            result_c, result_comment, result_addr, result_ship = await asyncio.gather(
                async_check_credit_card(data.creditCard),      # event c (after a)
                async_check_comment(data.comment),
                async_check_billing_address(data.billingAddress),
                async_check_shipping_method(data.shippingMethod),
            )
            self.increment(entry["vc"])  # tick for event c

            # b must pass before d can run (d is in fraud_detection)
            if not ok_b:
                return None, err_b or "User validation failed"

            for (ok, err), msg in zip(
                [result_c, result_comment, result_addr, result_ship],
                ["Credit card invalid", "Comment invalid", "Address invalid", "Shipping invalid"]
            ):
                if not ok:
                    return None, err or msg

            return True, None

        success, error = asyncio.run(run_checks())

        fail_response = transaction_verification.OrderResponse(
            vectorClock=transaction_verification.VectorClock(timeStamp=entry["vc"]),
            success=False
        )

        if not success:
            return fail_response

        if data.termsAccepted is not True:
            return fail_response

        # Forward to fraud_detection, passing the vector clock
        # fraud_detection will call suggestions, which returns the final response
        self.increment(entry["vc"])
        logger.info(f"VC after final increment: {entry['vc']}")

        with grpc.insecure_channel('fraud_detection:50051') as channel:
            stub = fraud_detection_grpc.FraudDetectionServiceStub(channel)
            response = stub.CheckFraud(fraud_detection.OrderInfo(
                id=order_id,
                vectorClock=fraud_detection.VectorClock(timeStamp=entry["vc"])
            ))
        return response

def serve():
    server = grpc.server(futures.ThreadPoolExecutor())
    transaction_verification_grpc.add_HelloServiceServicer_to_server(HelloService(), server)
    transaction_verification_grpc.add_TransactionVerificationServiceServicer_to_server(
        TransactionVerificationService(), server
    )
    port = "50052"
    server.add_insecure_port("[::]:" + port)
    server.start()
    logger.info("Server started. Listening on port 50052.")
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
