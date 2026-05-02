import sys
import os
import threading
import time
import random
import signal
import json

#Set up logging
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("Database")

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
database_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/database'))
sys.path.insert(0, database_grpc_path)
import database_pb2 as database
import database_pb2_grpc as database_grpc


import grpc
from concurrent import futures

class HelloService(database_grpc.HelloServiceServicer):
    def SayHello(self, request, context):
        response = database.HelloResponse()
        response.greeting = "Hello, " + request.name
        logger.debug(response.greeting)
        return response

# Key-Value data storage   
class KVStore:
    # Thread-safe in-memory key-value store with optional file persistence.

    def __init__(self, persist_path=None):
        self._store = {}
        self._lock = threading.RLock()
        self._persist_path = persist_path
        if persist_path and os.path.exists(persist_path):
            self._load()

    def read(self, key):
        with self._lock:
            return self._store.get(key)

    def write(self, key, value):
        with self._lock:
            self._store[key] = value
            self._save()
            return True

    def delete(self, key):
        with self._lock:
            if key in self._store:
                del self._store[key]
                self._save()
                return True
            return False
    
    def get_all(self):
        with self._lock:
            return dict(self._store)

    def _save(self):
        if self._persist_path:
            with open(self._persist_path, 'w') as f:
                json.dump(self._store, f)

    def _load(self):
        if not os.path.exists(self._persist_path):
            self._store = {}
            self._save()
            return
        try:
            with open(self._persist_path, 'r') as f:
                content = f.read().strip()
                if not content:
                    # empty file
                    self._store = {}
                    self._save()
                    return
                self._store = json.loads(content)
        except (json.JSONDecodeError, OSError):
            # corrupted or unreadable file
            self._store = {}
            self._save()

class DatabaseService(database_grpc.DatabaseServiceServicer):
    def __init__(self, db_id, peer_ids):
        self.db_id = int(db_id)
        self.peer_ids = sorted(map(int, peer_ids))
        self.store = KVStore(persist_path='/data/books.json')
        self._seed_data()

    def _seed_data(self):
        # Pre-populate with some books if store is empty.
        if self.store.get_all():
            return
        initial_books = [
            {"id": "1", "title": "The Best Book",   "author": "Author 1", "stock": 10, "price": 9.99},
            {"id": "2", "title": "The Best Book 2", "author": "Author 2", "stock": 5,  "price": 14.99},
            {"id": "3", "title": "The Best Book 3", "author": "Author 3", "stock": 8,  "price": 12.99},
            {"id": "4", "title": "The Best Book 4", "author": "Author 4", "stock": 3,  "price": 19.99},
            {"id": "5", "title": "The Best Book 5", "author": "Author 5", "stock": 15, "price": 7.99},
        ]
        for book in initial_books:
            self.store.write(book["id"], book)
        logger.info("Seeded initial book inventory")
    
    def Read(self, request, context):
        logger.info(f"Read: {request.id}")
        data = self.store.read(request.id)
        if data is None:
            return database.ReadResponse(found=False)
        return database.ReadResponse(
            found=True,
            book=database.Book(**data)
        )
    
    def Write(self, request, context):
        logger.info(f"Put: {request.book.id}")
        data = {
            "id":     request.book.id,
            "title":  request.book.title,
            "author": request.book.author,
            "stock":  request.book.stock,
            "price":  request.book.price,
        }
        ok = self.store.write(request.book.id, data)
        return database.WriteResponse(success=ok)

    def Delete(self, request, context):
        logger.info(f"Delete: {request.id}")
        ok = self.store.delete(request.id)
        return database.DeleteResponse(success=ok)
    
    def GetAll(self, request, context):
        logger.info("GetAll")
        all_books = self.store.get_all()
        return database.GetAllResponse(
            books=[database.Book(**b) for b in all_books.values()]
        )
    
def launch_database(db_id, peer_ids):
    db_id = int(db_id)
    peer_ids = list(map(int, peer_ids))

    service = DatabaseService(db_id, peer_ids)

    server = grpc.server(futures.ThreadPoolExecutor())
    database_grpc.add_DatabaseServiceServicer_to_server(service, server)

    port = "50057"
    server.add_insecure_port("[::]:" + port)
    server.start()
    logger.info(f"Database {db_id} started on port {port} with peers {peer_ids}")

    server.wait_for_termination()

if __name__ == '__main__':
    my_id = int(os.getenv("DATABASE_ID", "1"))
    peers = list(map(int, os.getenv("DATABASE_PEERS", "1").split(",")))
    launch_database(my_id, peers)
