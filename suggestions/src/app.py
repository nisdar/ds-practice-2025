import sys
import os
import random # Currently used for dummy logic, randomly return a book as there is no way to choose inventory

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
    def SuggestBooks(self, request, context):
        card_number = request.card_number
        order_amount = request.order_amount

        logger.info(f"Generating suggestions for {card_number}, amount {order_amount}")

        books = [
            {"id": "1", "author": "Author 1", "title": "The Best Book"},
            {"id": "2", "author": "Author 2", "title": "The Best Book 2"},
            {"id": "3", "author": "Author 3", "title": "The Best Book 3"},
            {"id": "4", "author": "Author 4", "title": "The Best Book 4"},
            {"id": "5", "author": "Author 5", "title": "The Best Book 5"},
            {"id": "6", "author": "Author 6", "title": "The Best Book 6"},
        ]

        # Temporary dummy logic as there is currently no inventory and no basis for suggestions
        choose_random_number = random.randint(0, 6)

        selected = books[:choose_random_number]

        return suggestions.SuggestionResponse(
            books = [
                suggestions.Book(
                    id=b["id"],
                    title=b["title"],
                    author=b["author"]
                ) for b in selected
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