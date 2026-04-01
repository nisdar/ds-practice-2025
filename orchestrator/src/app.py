import sys
import os
import uuid

#Set up logging
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("Orchestrator")

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
fraud_detection_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/fraud_detection'))
transaction_verification_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/transaction_verification'))
suggestions_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/suggestions'))
order_queue_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/order_queue'))

sys.path.insert(0, order_queue_grpc_path)
sys.path.insert(0, fraud_detection_grpc_path)
sys.path.insert(0, transaction_verification_grpc_path)
sys.path.insert(0, suggestions_grpc_path)

import fraud_detection_pb2 as fraud_detection
import fraud_detection_pb2_grpc as fraud_detection_grpc
import transaction_verification_pb2 as transaction_verification
import transaction_verification_pb2_grpc as transaction_verification_grpc
import suggestions_pb2 as suggestions
import suggestions_pb2_grpc as suggestions_grpc
import order_queue_pb2 as order_queue
import order_queue_pb2_grpc as order_queue_grpc

import grpc

def greet(name='admin'):
    # Testing the services
    with grpc.insecure_channel('fraud_detection:50051') as channel:
        fraud_stub = fraud_detection_grpc.HelloServiceStub(channel)
        fraud_response = fraud_stub.SayHello(fraud_detection.HelloRequest(name=name))
    with grpc.insecure_channel('transaction_verification:50052') as channel:
        transaction_stub = transaction_verification_grpc.HelloServiceStub(channel)
        transaction_response = transaction_stub.SayHello(transaction_verification.HelloRequest(name=name))
    with grpc.insecure_channel('suggestions:50053') as channel:
        suggestions_stub = suggestions_grpc.HelloServiceStub(channel)
        suggestions_response = suggestions_stub.SayHello(suggestions.HelloRequest(name=name))
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

def call_fraud_detection_new(order_id, vc):
    # Establish a connection with the fraud-detection gRPC service.
    with grpc.insecure_channel('fraud_detection:50051') as channel:
        # Create a stub object.
        stub = fraud_detection_grpc.FraudDetectionServiceStub(channel)
        request_obj = fraud_detection.OrderInfo(
            id=order_id,
            vectorClock=fraud_detection.VectorClock(timeStamp=vc)
        )
        # Call the service through the stub object.
        response = stub.CheckFraudNew(request_obj)
    return response

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

    # Establish a connection with the transaction verification gRPC service.
    with grpc.insecure_channel('transaction_verification:50052') as channel:
        # Create a stub object.
        stub = transaction_verification_grpc.TransactionVerificationServiceStub(channel)
        # Call the service through the stub object.
        response = stub.VerifyTransaction(request_obj)
    # TODO response.comment might have not-great formatting
    return response# + " " + response.comment

def call_suggestions(card_number, order_amount):
    # Establish a connection with the suggestions gRPC service.
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

def init_fraud_detection(orderData):
    #Initialize fraud detection
    with grpc.insecure_channel('fraud_detection:50051') as channel:
        stub = fraud_detection_grpc.FraudDetectionServiceStub(channel)
        stub.InitFraudDetection(orderData)
    

def init_transaction_verification(orderData):
    #Initialize transaction verification
    with grpc.insecure_channel('transaction_verification:50052') as channel:
        stub = transaction_verification_grpc.TransactionVerificationServiceStub(channel)
        stub.InitTransactionVerification(orderData)
    

def init_suggestions(orderData):
    #Initialize suggestions
    with grpc.insecure_channel('suggestions:50053') as channel:
        stub = suggestions_grpc.SuggestionsServiceStub(channel)
        stub.InitSuggestions(orderData)

def formatOrderData(service, order_id, request_data):
    items = request_data.get("items")
    user = request_data.get("user")
    card = request_data.get("creditCard")
    comment = request_data.get("userComment")
    billing_address = request_data.get("billingAddress")
    shipping_method = request_data.get("shippingMethod")
    gift_wrapping = request_data.get("giftWrapping")
    terms_accepted = request_data.get("termsAccepted")

    ### due to dictionary usage, transformed to PB2 dictionaries with help from Mistral Le Chat
    # Convert items
    pb_items = []
    for item in items:
        pb_item = service.ItemData(name=item['name'], quantity=str(item['quantity']))
        pb_items.append(pb_item)

    # Convert user
    pb_user = service.User(name=user['name'], contact=user['contact'])

    # Convert credit card
    pb_card = service.CreditCard(
        number=card['number'],
        expirationDate=card['expirationDate'],
        cvv=card['cvv']
    )

    # Convert comment
    pb_comment = service.Comment(comment=comment)

    # Convert billing address
    pb_billing_address = service.BillingAddress(
        street=billing_address['street'],
        city=billing_address['city'],
        state=billing_address['state'],
        zip=billing_address['zip'],
        country=billing_address['country']
    )

    # Build the request
    request_obj = service.OrderData(
        orderId=order_id,
        items=pb_items,
        user=pb_user,
        creditCard=pb_card,
        comment=pb_comment,
        billingAddress=pb_billing_address,
        shippingMethod=shipping_method,
        giftWrapping=gift_wrapping,
        termsAccepted=terms_accepted
    )
    
    return request_obj

# Threading! with ThreadPoolExecutor
from concurrent.futures import ThreadPoolExecutor
executor = ThreadPoolExecutor(max_workers=6)
## asynchronously calling the services
def async_fraud_detection(order_id, vc): #MODIFIED
    return executor.submit(call_fraud_detection_new, order_id, vc)
def async_transaction_verification(items, user, card, comment, billing_address, shipping_method, gift_wrapping, terms_accepted):
    return executor.submit(call_transaction_verification, items, user, card, comment, billing_address, shipping_method, gift_wrapping, terms_accepted)
def async_suggestions(card_number, order_amount):
    return executor.submit(call_suggestions, card_number, order_amount)


# Helper function for enqueueing and deq-ing orders
# Made with the help of Copilot
def call_order_queue_enqueue(order_id):
    with grpc.insecure_channel('order_queue:50054') as channel:
        stub = order_queue_grpc.OrderQueueServiceStub(channel)
        request = order_queue.EnqueueRequest(addable_order=order_id)
        response = stub.Enqueue(request)
    return response

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
    # Test the gRPC services.
    response = greet(name='admin;')
    return response


@app.route('/checkout', methods=['POST'])
def checkout():
    """
    Responds with a JSON object containing the order ID, status, and suggested books.
    """

    order_id = str(uuid.uuid4())
    logger.info(f"Request ID {order_id} received")

    request_data = json.loads(request.data)

    items = request_data.get("items")
    #amount = sum(item["quantity"] for item in items)

    user = request_data.get("user")
    card = request_data.get("creditCard")
    comment = request_data.get("userComment")
    billing_address = request_data.get("billingAddress")
    shipping_method = request_data.get("shippingMethod")
    gift_wrapping = request_data.get("giftWrapping")
    terms_accepted = request_data.get("termsAccepted")

    logger.debug(f"Request Data: {request_data}")

    #Initialize the services
    init_fraud_detection(formatOrderData(fraud_detection, order_id, request_data))
    init_transaction_verification(formatOrderData(transaction_verification, order_id, request_data))
    init_suggestions(formatOrderData(suggestions, order_id, request_data))

    # Start async operations
    logger.info("Creating threads...")

    trx_future = async_transaction_verification(
        items, user, card, comment,
        billing_address, shipping_method,
        gift_wrapping, terms_accepted
    )

    #Fraud detetcion has been changed
    fraud_future = async_fraud_detection(order_id, [0, 0, 0]) #using a dummy vector clock
    
    
    suggestions_future = async_suggestions("123", 1)

    trx_response = trx_future.result()
    fraud_response = fraud_future.result()

    logger.info(f"Transaction verification: {trx_response}")
    logger.info(f"Fraud detection: {fraud_response}")

    status = "Order Approved"
    if not trx_response.success or not fraud_response.success:
        status = "Order Rejected"

    if status == "Order Approved":
        queue_response = call_order_queue_enqueue(order_id)
        logger.info(f"Order {order_id} enqueued: {queue_response.success}")

    suggestions_response = suggestions_future.result()
    logger.info(f"Suggested book IDs: {[s['id'] for s in suggestions_response]}")

    result = {
        "orderId": order_id,
        "status": status,
        "suggestedBooks": suggestions_response
    }

    logger.debug(f"Response Data: {result}")
    return result

if __name__ == '__main__':
    # Run the app in debug mode to enable hot reloading.
    # This is useful for development.
    # The default port is 5000.
    app.run(host='0.0.0.0')
