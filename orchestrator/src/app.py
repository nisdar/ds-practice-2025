import sys
import os

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
fraud_detection_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/fraud_detection'))
transaction_verification_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/transaction_verification'))
suggestions_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/suggestions'))

sys.path.insert(0, fraud_detection_grpc_path)
sys.path.insert(0, transaction_verification_grpc_path)
sys.path.insert(0, suggestions_grpc_path)

import fraud_detection_pb2 as fraud_detection
import fraud_detection_pb2_grpc as fraud_detection_grpc
import transaction_verification_pb2 as transaction_verification
import transaction_verification_pb2_grpc as transaction_verification_grpc
import suggestions_pb2 as suggestions
import suggestions_pb2_grpc as suggestions_grpc

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
    with grpc.insecure_channel('suggestions:50053') as channel:
        # Create a stub object.
        suggestions_stub = suggestions_grpc.HelloServiceStub(channel)
        # Call the service through the stub object.
        suggestions_response = suggestions_stub.SayHello(suggestions.HelloRequest(name=name))
    # TODO add transaction_response.greeting and suggestions_response.greeting
    return f"Fraud_detection: {fraud_response.greeting} Transaction_verification: {transaction_response.greeting} Suggestions: {suggestions_response.greeting}"

def call_fraud_detection(card_number, order_amount):
    # Establish a connection with the fraud-detection gRPC service.
    with grpc.insecure_channel('fraud_detection:50051') as channel:
        # Create a stub object.
        stub = fraud_detection_grpc.FraudDetectionServiceStub(channel)
        request_obj = fraud_detection.FraudRequest(card_number=card_number, order_amount=order_amount)
        # Call the service through the stub object.
        response = stub.CheckFraud(request_obj)
    return response.is_fraud

def call_transaction_verification(items, user, card, comment, billing_address, shipping_method, gift_wrapping, terms_accepted):
    ### due to dictionary usage, transformed to PB2 dictionaries with help from Mistral Le Chat
    # Convert items
    pb_items = []
    for item in items:
        pb_item = transaction_verification.ItemData(name=item['name'], quantity=str(item['quantity']))
        pb_items.append(pb_item)

    # Convert user
    pb_user = transaction_verification.User(name=user['name'], contact=user['contact'])

    # Convert credit card
    pb_card = transaction_verification.CreditCard(
        number=card['number'],
        expirationDate=card['expirationDate'],
        cvv=card['cvv']
    )

    # Convert comment
    pb_comment = transaction_verification.Comment(comment=comment)

    # Convert billing address
    pb_billing_address = transaction_verification.BillingAddress(
        street=billing_address['street'],
        city=billing_address['city'],
        state=billing_address['state'],
        zip=billing_address['zip'],
        country=billing_address['country']
    )

    # Build the request
    request_obj = transaction_verification.VerificationRequest(
        items=pb_items,
        user=pb_user,
        creditCard=pb_card,
        comment=pb_comment,
        billingAddress=pb_billing_address,
        shippingMethod=shipping_method,
        giftWrapping=gift_wrapping,
        termsAccepted=terms_accepted
    )

    with grpc.insecure_channel('transaction_verification:50052') as channel:
        # Create a stub object.
        stub = transaction_verification_grpc.TransactionVerificationServiceStub(channel)
        # Call the service through the stub object.
        response = stub.VerifyTransaction(request_obj)
    # TODO response.comment might have not-great formatting
    return response# + " " + response.comment

def call_suggestions(card_number, order_amount):
    # Establish a connection with the fraud-detection gRPC service.
    with grpc.insecure_channel('suggestions:50053') as channel:
        # Create a stub object.
        stub = suggestions_grpc.SuggestionsServiceStub(channel)
        request_obj = suggestions.SuggestionRequest(card_number=card_number, order_amount=order_amount)
        # Call the service through the stub object.
        response = stub.SuggestBooks(request_obj)

        books_list = []

        result = [
            {
                "id": book.id,
                "title": book.title,
                "author": book.author
            }
            for book in response.books
        ]
    return result

# Threading! with ThreadPoolExecutor
from concurrent.futures import ThreadPoolExecutor
executor = ThreadPoolExecutor(max_workers=5)
## asynchronously calling fraud_detection
def async_fraud_detection(card_number, order_amount):
    return executor.submit(call_fraud_detection, card_number, order_amount)
def async_transaction_verification(items, user, card, comment, billing_address, shipping_method, gift_wrapping, terms_accepted):
    return executor.submit(call_transaction_verification, items, user, card, comment, billing_address, shipping_method, gift_wrapping, terms_accepted)
def async_suggestions(card_number, order_amount):
    return executor.submit(call_suggestions, card_number, order_amount)

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
    ### TODO currently only used for FRAUD, not transaction verify
    amount = sum([item['quantity'] for item in items])
    user = request_data.get('user')
    card = request_data.get('creditCard')
    comment = request_data.get('userComment')
    billing_address = request_data.get('billingAddress')
    shipping_method = request_data.get('shippingMethod')
    gift_wrapping = request_data.get('giftWrapping')
    terms_accepted = request_data.get('termsAccepted')

    print("Request Data:", request_data)
    
    transaction_verification_future = async_transaction_verification(items, user, card, comment, billing_address, shipping_method, gift_wrapping, terms_accepted)
    transaction_verification_response = transaction_verification_future.result()

    if transaction_verification_response.success:
        status = "Order Approved"
    else:
        status = "Order Rejected"
    print(f"Transaction verification: {status}")

    fraud_detection_future = async_fraud_detection(card['number'], amount)
    fraud_detection_response = fraud_detection_future.result()

    print(f"Fraud: {fraud_detection_response}")
    #status = "Order Approved"
    #if fraud:
    #    status = "Order Rejected"

    suggestions_future = async_suggestions('123', 1)
    suggestions_response = suggestions_future.result()
    print(f"Suggestions ID-s: {[item['id'] for item in suggestions_response]}")

    # Dummy response following the provided YAML specification for the bookstore
    order_status_response = {
        'orderId': '12345',
        'status': status,
        'suggestedBooks': suggestions_response
    }

    return order_status_response


if __name__ == '__main__':
    # Run the app in debug mode to enable hot reloading.
    # This is useful for development.
    # The default port is 5000.
    app.run(host='0.0.0.0')
