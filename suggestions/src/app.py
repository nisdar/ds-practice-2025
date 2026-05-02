import sys
import os
import random # Currently used for dummy logic, randomly return a book as there is no way to choose inventory
from google.protobuf.empty_pb2 import Empty

#Set up logging
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("Suggestions")

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
suggestions_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/suggestions'))
sys.path.insert(0, suggestions_grpc_path)

import suggestions_pb2 as suggestions
import suggestions_pb2_grpc as suggestions_grpc

import grpc
from concurrent import futures

# Create classes to define the server functions
class HelloService(suggestions_grpc.HelloServiceServicer):
    def SayHello(self, request, context):
        response = suggestions.HelloResponse()
        response.greeting = "Hello, " + request.name
        logger.debug(response.greeting)
        return response

class SuggestionsService(suggestions_grpc.SuggestionsServiceServicer):
    def __init__(self, svc_idx=2, total_svcs=3):
        self.svc_idx = svc_idx
        self.total_svcs = total_svcs
        self.orders = {}

    def merge_and_increment(self, local_vc, incoming_vc):
        for i in range(self.total_svcs):
            local_vc[i] = max(local_vc[i], incoming_vc[i])
        local_vc[self.svc_idx] += 1
        
    def increment(self, local_vc):          # ← must be indented inside class
        local_vc[self.svc_idx] += 1
    
    def InitSuggestions(self, request, context):
        order_id = request.orderId
        self.orders[order_id] = {"data" : request, "vc": [0] * self.total_svcs}
        return Empty()

    def SuggestBooks(self, request, context):
        order_id = request.id
        incoming_vc = list(request.vectorClock.timeStamp)  # ← convert to list
        entry = self.orders.get(order_id)
        
        # Merge fraud_detection's clock (which already carries TV's history)
        self.merge_and_increment(entry["vc"], incoming_vc)
        
        logger.info(f"Generating suggestions for order {order_id}, vc={entry['vc']}")

        books = [
            {"id": "1", "author": "Author 1", "title": "The Best Book"},
            {"id": "2", "author": "Author 2", "title": "The Best Book 2"},
            {"id": "3", "author": "Author 3", "title": "The Best Book 3"},
            {"id": "4", "author": "Author 4", "title": "The Best Book 4"},
            {"id": "5", "author": "Author 5", "title": "The Best Book 5"},
            {"id": "6", "author": "Author 6", "title": "The Best Book 6"},
        ]

        choose_random_number = random.randint(1, 6)  # ← min 1 so you always get results
        selected = books[:choose_random_number]

        # Tick 2: event f completes
        self.increment(entry["vc"])
        
        # Tick 3: send final response
        self.increment(entry["vc"])

        return suggestions.OrderResponse(
            vectorClock=suggestions.VectorClock(timeStamp=entry["vc"]),
            success=True,
            suggestions=[
                suggestions.Book(id=b["id"], title=b["title"], author=b["author"])
                for b in selected
            ]
        )
    
def serve():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor())
    suggestions_grpc.add_HelloServiceServicer_to_server(HelloService(), server)
    suggestions_grpc.add_SuggestionsServiceServicer_to_server(SuggestionsService(), server)
    # Listen on port 50053
    port = "50053"
    server.add_insecure_port("[::]:" + port)
    # Start the server
    server.start()
    logger.info("Server started. Listening on port 50053.")
    # Keep thread alive
    server.wait_for_termination()


if __name__ == '__main__':
    serve()