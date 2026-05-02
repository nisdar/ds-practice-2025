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
database_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/database'))
sys.path.insert(0, database_grpc_path)
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
import database_pb2 as database
import database_pb2_grpc as database_grpc

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

# This currently needs to stay here as transaction_verification gets an actual call
## however, this should be combined into the Init for transaction_verification
def call_transaction_verification(order_id, vc):
    # Establish a connection with the fraud-detection gRPC service.
    with grpc.insecure_channel('transaction_verification:50052') as channel:
        # Create a stub object.
        stub = transaction_verification_grpc.TransactionVerificationServiceStub(channel)
        request_obj = transaction_verification.OrderInfo(
            id=order_id,
            vectorClock=transaction_verification.VectorClock(timeStamp=vc)
        )
        # Call the service through the stub object.
        response = stub.VerifyTransaction(request_obj)
    logger.debug(f"{response}, {type(response)}")
    return {"vc": response.vectorClock,
            "success": response.success,
            "suggestions": [
            {
                "id": book.id,
                "title": book.title,
                "author": book.author
            }
            for book in response.suggestions
        ]}

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

def validate_stock_availability(order_payload):
    """
    Returns (True, None) if stock is sufficient.
    Returns (False, reason) otherwise.
    """
    items = order_payload.get("items", [])
    if not items:
        return False, "No items in order"
    # Connect to PRIMARY database
    with grpc.insecure_channel("database-1:50057") as channel:
        db_stub = database_grpc.DatabaseServiceStub(channel)
        for item in items:
            title = item["name"]
            required_qty = int(item["quantity"])
            all_books = db_stub.GetAll(database.GetAllRequest()).books
            book = next((b for b in all_books if b.title == title), None)
            if book is None:
                return False, f"Unknown book '{title}'"
            if book.stock < required_qty:
                return (
                    False,
                    f"Insufficient stock for '{title}' "
                    f"({book.stock} available, {required_qty} requested)"
                )
    return True, None

def format_order_data(service, order_id, request_data):
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


# Helper function for enqueueing and deq-ing orders
# Made with the help of Copilot
def call_order_queue_enqueue(order_id, order_payload_json):
    with grpc.insecure_channel('order_queue:50054') as channel:
        stub = order_queue_grpc.OrderQueueServiceStub(channel)
        _request = order_queue.EnqueueRequest(
            order_id=order_id,
            order_payload_json=order_payload_json
        )
        response = stub.Enqueue(_request)
    return response

# Threading! with ThreadPoolExecutor
# and asynchronous operation with asyncio
import asyncio
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
async def run_in_thread(func, *args):
    return await loop.run_in_executor(None, func, *args)

## asynchronously calling the services
## Re-done with the help of Copilot for true asynch processing
async def async_checkout_logic(order_id, request_data):
    logger.info(f"[Async] Processing {order_id}")
    # Do a quick stock check before running anything else
    ok, reason = validate_stock_availability(request_data)
    if not ok:
        logger.warning(f"Order {order_id} rejected: {reason}")
        return {
            "orderId": order_id,
            "status": f"Order Rejected: {reason}",
            "suggestedBooks": [],
        }
    # Init services
    await run_in_thread(init_fraud_detection,
                        format_order_data(fraud_detection, order_id, request_data))
    await run_in_thread(init_transaction_verification,
                        format_order_data(transaction_verification, order_id, request_data))
    await run_in_thread(init_suggestions,
                        format_order_data(suggestions, order_id, request_data))
    # Begin the transaction_verification chain
    ## This could be combined with the init_transaction_verification maybe
    resp = await run_in_thread(call_transaction_verification, order_id, [0, 0, 0])
    # Evaluate
    if not resp["success"]:
        return {
            "orderId": order_id,
            "status": "Order Rejected",
            "suggestedBooks": resp["suggestions"],
        }
    ok, reason = validate_stock_availability(request_data)
    if not ok:
        logger.warning(f"Order {order_id} rejected: {reason}")
        return {
            "orderId": order_id,
            "status": f"Order Rejected: {reason}",
            "suggestedBooks": [],
        }
    await run_in_thread(
        call_order_queue_enqueue,
        order_id,
        json.dumps(request_data)
    )
    return {
        "orderId": order_id,
        "status": "Order Approved",
        "suggestedBooks": resp["suggestions"],
    }

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

## Re-done to use asynch checkout with the help of Copilot
@app.route("/checkout", methods=["POST"])
def checkout():
    """
    Responds with a JSON object containing the order ID, status, and suggested books.
    """
    order_id = str(uuid.uuid4())
    payload = json.loads(request.data)
    logger.info(f"[Flask] Received order {order_id}")
    # Synchronously block until async workflow completes
    result = loop.run_until_complete(async_checkout_logic(order_id, payload))
    return result

if __name__ == '__main__':
    # Run the app in debug mode to enable hot reloading.
    # This is useful for development.
    # The default port is 5000.
    app.run(host='0.0.0.0')
