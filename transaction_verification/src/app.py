import sys
import os
import re

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
transaction_verification_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/transaction_verification'))
sys.path.insert(0, transaction_verification_grpc_path)
import transaction_verification_pb2 as transaction_verification
import transaction_verification_pb2_grpc as transaction_verification_grpc

import grpc
from concurrent import futures


# Create a class to define the server functions, derived from
# fraud_detection_pb2_grpc.HelloServiceServicer
class HelloService(transaction_verification_grpc.HelloServiceServicer):
    # Create an RPC function to say hello
    def SayHello(self, request, context):
        # Create a HelloResponse object
        response = transaction_verification.HelloResponse()
        # Set the greeting field of the response object
        response.greeting = "Hello, " + request.name
        # Print the greeting message
        print(response.greeting)
        # Return the response object
        return response

# Create a class to define the server functions
class TransactionVerificationService(transaction_verification_grpc.TransactionVerificationServiceServicer):
    def VerifyTransaction(self, request, context):
        # Nested methods for validating various fields
        def validate_item(item):
            name_regex = r'^[a-zA-Z0-9 \-\']+$'
            quantity_regex = r'^[1-9]\d*$'
            return (
                re.fullmatch(name_regex, item.name) is not None and
                re.fullmatch(quantity_regex, item.quantity) is not None
            )
        def validate_user(user):
            name_regex = r'^[a-zA-Z]+ [a-zA-Z]+$'
            email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            return (
                re.fullmatch(name_regex, user.name) is not None and
                re.fullmatch(email_regex, user.contact) is not None
            )
        def validate_credit_card(card):
            number_regex = r'^[456]\d{15}$'
            exp_date_regex = r'^(0[1-9]|1[0-2])\/\d{2}$'
            cvv_regex = r'^\d{3}$'
            return (
                re.fullmatch(number_regex, card.number) is not None and
                re.fullmatch(exp_date_regex, card.expirationDate) is not None and
                re.fullmatch(cvv_regex, card.cvv) is not None
            )
        def validate_comment(comment):
            comment_regex = r'^[a-zA-Z0-9 \-\'.,!?]*$'
            return re.fullmatch(comment_regex, comment.comment) is not None
        def validate_billing_address(address):
            street_regex = r'^[a-zA-Z0-9 \-,\.]+$'
            city_regex = r'^[a-zA-Z \-]+$'
            state_regex = r'^[a-zA-Z ]+$'
            zip_regex = r'^\d{5}(-\d{4})?$'
            country_regex = r'^[a-zA-Z]{2,}$'
            return (
                re.fullmatch(street_regex, address.street) is not None and
                re.fullmatch(city_regex, address.city) is not None and
                re.fullmatch(state_regex, address.state) is not None and
                re.fullmatch(zip_regex, address.zip) is not None and
                re.fullmatch(country_regex, address.country) is not None
            )
        def validate_shipping_method(method):
            return method in ["Standard", "Express", "Next-day"]
        
        # items
        if not all(validate_item(item) for item in request.items):
            return transaction_verification.VerificationResponse(success=False, comment="Item validation failed")
        # user
        if not validate_user(request.user):
            return transaction_verification.VerificationResponse(success=False, comment="User validation failed")
        # credit card
        if not validate_credit_card(request.creditCard):
            return transaction_verification.VerificationResponse(success=False, comment="Credit card validation failed")
        # comment
        if not validate_comment(request.comment):
            return transaction_verification.VerificationResponse(success=False, comment="Comment validation failed")
        # billing address
        if not validate_billing_address(request.billingAddress):
            return transaction_verification.VerificationResponse(success=False, comment="Address validation failed")
        # shipping method
        if not validate_shipping_method(request.shippingMethod):
            return transaction_verification.VerificationResponse(success=False, comment="Shipping method invalid")
        # gift wrapping doesn't need a check
        # terms of service
        if request.termsAccepted is not True:
            return transaction_verification.VerificationResponse(success=False, comment="Terms of service not accepted")
        # if all checks pass
        return transaction_verification.VerificationResponse(success=True)
    
def serve():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor())
    transaction_verification_grpc.add_HelloServiceServicer_to_server(HelloService(), server)
    transaction_verification_grpc.add_TransactionVerificationServiceServicer_to_server(TransactionVerificationService(), server)
    # Listen on port 50051
    port = "50052"
    server.add_insecure_port("[::]:" + port)
    # Start the server
    server.start()
    print("Server started. Listening on port 50052.")
    # Keep thread alive
    server.wait_for_termination()


if __name__ == '__main__':
    serve()