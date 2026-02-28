import sys
import os

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
fraud_detection_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/fraud_detection'))
transaction_verification_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/transaction_verification'))
""" TODO UNCOMMENT
suggestions_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/suggestions'))"""
sys.path.insert(0, fraud_detection_grpc_path)
sys.path.insert(0, transaction_verification_grpc_path)
""" TODO UNCOMMENT
sys.path.insert(0, suggestions_grpc_path)"""
import fraud_detection_pb2 as fraud_detection
import fraud_detection_pb2_grpc as fraud_detection_grpc
import transaction_verification_pb2 as transaction_verification
import transaction_verification_pb2_grpc as transaction_verification_grpc
""" TODO UNCOMMENT
import suggestions_pb2 as suggestions
import suggestions_pb2_grpc as suggestions_grpc
"""

import grpc

def greet(name='admin'):
    # Establish a connection with the fraud-detection gRPC service.
    with grpc.insecure_channel('fraud_detection:50051') as channel:
        # Create a stub object.
        fraud_stub = fraud_detection_grpc.HelloServiceStub(channel)
        # Call the service through the stub object.
        fraud_response = fraud_stub.SayHello(fraud_detection.HelloRequest(name=name))
    with grpc.insecure_channel('transaction_verification:50052') as channel:
        # Create a stub object.
        transaction_stub = transaction_verification_grpc.HelloServiceStub(channel)
        # Call the service through the stub object.
        transaction_response = transaction_stub.SayHello(transaction_verification.HelloRequest(name=name))
    """ TODO UNCOMMENT
    with grpc.insecure_channel('suggestions:50053') as channel:
        # Create a stub object.
        suggestions_stub = suggestions_grpc.HelloServiceStub(channel)
        # Call the service through the stub object.
        suggestions_response = suggestions_stub.SayHello(suggestions.HelloRequest(name=name))
    """
    # TODO add transaction_response.greeting and suggestions_response.greeting
    return f"Fraud_detection: {fraud_response.greeting} Transaction_verification: {transaction_response.greeting}"

def call_fraud_detection(card_number, order_amount):
    # Establish a connection with the fraud-detection gRPC service.
    with grpc.insecure_channel('fraud_detection:50051') as channel:
        # Create a stub object.
        stub = fraud_detection_grpc.FraudDetectionServiceStub(channel)
        request_obj = fraud_detection.FraudRequest(card_number=card_number, order_amount=order_amount)
        # Call the service through the stub object.
        response = stub.CheckFraud(request_obj)
    return response.is_fraud

def call_transaction_verification(items, card, comment, billingAddress, shippingMethod, giftWrapping, termsAccepted):
    # Establish a connection with the fraud-detection gRPC service.
    with grpc.insecure_channel('transaction_verification:50052') as channel:
        # Create a stub object.
        stub = transaction_verification_grpc.TransactionVerificationServiceStub(channel)
        request_obj = transaction_verification.VerificationRequest(
            items=items, user=user, creditCard=card, 
            comment=comment, billingAddress=billingAddress, 
            shippingMethod=shippingMethod, 
            giftWrapping=giftWrapping, 
            termsAccepted=termsAccepted
        )
        # Call the service through the stub object.
        response = stub.VerifyTransaction(request_obj)
    # TODO response.comment might have not-great formatting
    return response.success + " " + response.comment

# Import Flask.
# Flask is a web framework for Python.
# It allows you to build a web application quickly.
# For more information, see https://flask.palletsprojects.com/en/latest/
from flask import Flask, request
from flask_cors import CORS
import json

# Create a simple Flask app.
app = Flask(__name__)
# Enable CORS for the app.
CORS(app, resources={r'/*': {'origins': '*'}})

# Define a GET endpoint.

@app.route('/', methods=['GET'])
def index():
    """
    #Responds with 'Hello, [name]' when a GET request is made to '/' endpoint.
    """
    # Test the fraud-detection gRPC service.
    response = greet(name='admin;')
    #response = f"card=123, amount=1, is_fraud={call_fraud_detection('123', 1)}" + f"\ncard=999, amount=1, is_fraud={call_fraud_detection('999', 1)}" + f"\ncard=123, amount=2000, is_fraud={call_fraud_detection('123', 2000)}"
    # Return the response.
    return response


@app.route('/checkout', methods=['POST'])
def checkout():
    """
    Responds with a JSON object containing the order ID, status, and suggested books.
    """
    # Get request object data to json
    request_data = json.loads(request.data)
    # Print request object data
    items = request_data.get('items')
    amount = sum([item['quantity'] for item in items])
    card = request_data.get('creditCard')
    comment = request_data.get('comment')
    billingAddress = request_data.get('billingAddress')
    shippingMethod = request_data.get('shippingMethod')
    giftWrapping = request_data.get('giftWrapping')
    termsAccepted = request_data.get('termsAccepted')

    print("Request Data:", request_data)
    
    if not call_transaction_verification(
            items, card, comment, 
            billingAddress, shippingMethod, 
            giftWrapping, termsAccepted
        ):
        status = "Order Rejected"
    else:
        status = "Order Approved"

    """
    fraud = call_fraud_detection(card['number'], amount)
    status = "Order Approved"
    if fraud:
        status = "Order Rejected"
    """

    # Dummy response following the provided YAML specification for the bookstore
    order_status_response = {
        'orderId': '12345',
        'status': status,
        'suggestedBooks': [
            {'bookId': '123', 'title': 'The Best Book', 'author': 'Author 1'},
            {'bookId': '456', 'title': 'The Second Best Book', 'author': 'Author 2'}
        ]
    }

    return order_status_response


if __name__ == '__main__':
    # Run the app in debug mode to enable hot reloading.
    # This is useful for development.
    # The default port is 5000.
    app.run(host='0.0.0.0')
