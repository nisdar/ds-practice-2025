import sys
import os
import threading
import time
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

from google.protobuf.empty_pb2 import Empty


import grpc
from concurrent import futures

class HelloService(database_grpc.HelloServiceServicer):
    def SayHello(request, context):
        response = database.HelloResponse()
        response.greeting = "Hello, " + request.name
        logger.debug(response.greeting)
        return response

# Little price normalization because of funky floating-points 
def normalize_price(price):
    return round(price + 1e-9, 2)

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
        recovered = self._recover_from_peers()
        if not recovered:
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
        self._log_request("Read", request)
        self._log_store(prefix="BEFORE READ")
        data = self.store.read(request.book_id)
        if data is None:
            logger.warning(
                "[DB %s] Read MISS for key=%r. Available keys=%s",
                self.db_id,
                request.book_id,
                list(self.store.get_all().keys())
            )
            return database.ReadResponse(stock=0)

        logger.info(
            "[DB %s] Read HIT key=%r stock=%d",
            self.db_id,
            request.book_id,
            data["stock"]
        )
        return database.ReadResponse(stock=data["stock"])

    def Write(self, request, context):
        self._log_request("Write", request)
        self._log_store(prefix="BEFORE WRITE")
        book = request.book
        data = self.store.read(book.id)
        if data is None:
            logger.warning(
                "[DB %s] Write FAILED. Book id=%r not found",
                self.db_id,
                book.id
            )
            return database.WriteResponse(success=False)
        logger.info(
            "[DB %s] Write BEFORE id=%r stock=%d -> %d",
            self.db_id,
            book.id,
            data["stock"],
            book.stock
        )
        data.update({
            "id": book.id,
            "title": book.title,
            "author": book.author,
            "stock": book.stock,
            "price": normalize_price(book.price),
        })
        ok = self.store.write(book.id, data)
        logger.info(
            "[DB %s] Write AFTER id=%r ok=%s",
            self.db_id,
            book.id,
            ok
        )
        self._log_store(prefix="AFTER WRITE")
        return database.WriteResponse(success=ok)

    def Delete(self, request, context):
        logger.info(f"Delete: {request.id}")
        ok = self.store.delete(request.id)
        self._log_store(prefix="AFTER DELETE")
        return database.DeleteResponse(success=ok)

    def GetAll(self, request, context):
        logger.info("GetAll")
        all_books = self.store.get_all()
        return database.GetAllResponse(
            books=[database.Book(**b) for b in all_books.values()]
        )
    
    def Sync(self, request, context):
        logger.info("[DB %s] Sync requested", self.db_id)
        all_books = self.store.get_all()
        return database.GetAllResponse(
            books=[database.Book(**b) for b in all_books.values()]
        )

    def _log_store(self, prefix="DB STORE"):
        data = self.store.get_all()
        logger.info(f"{prefix}: {json.dumps(data, indent=2)}")

    def _log_request(self, rpc_name, request):
        logger.info(
            "[DB %s] %s request: %s",
            self.db_id,
            rpc_name,
            request
        )

    def _recover_from_peers(self, max_attempts=10, delay=2):
        if self.store.get_all():
            logger.info("[DB %s] Local store present; skipping recovery", self.db_id)
            return True

        logger.warning("[DB %s] Local store empty; attempting recovery", self.db_id)

        for attempt in range(1, max_attempts + 1):
            for peer_id in self.peer_ids:
                if peer_id == self.db_id:
                    continue

                try:
                    channel = grpc.insecure_channel(f"database-{peer_id}:50057")
                    stub = database_grpc.DatabaseServiceStub(channel)

                    response = stub.Sync(
                        Empty(),  # or Empty()
                        timeout=2
                    )

                    if response.books:
                        logger.info(
                            "[DB %s] Recovered %d books from DB %s on attempt %d",
                            self.db_id,
                            len(response.books),
                            peer_id,
                            attempt
                        )
                        for book in response.books:
                            self.store.write(book.id, {
                                "id": book.id,
                                "title": book.title,
                                "author": book.author,
                                "stock": book.stock,
                                "price": book.price,
                            })
                        return True

                except Exception as e:
                    logger.warning(
                        "[DB %s] Attempt %d: recovery from DB %s failed: %s",
                        self.db_id,
                        attempt,
                        peer_id,
                        e
                    )

            time.sleep(delay)

        logger.warning(
            "[DB %s] Recovery failed after %d attempts",
            self.db_id,
            max_attempts
        )
        return False


# Primary and Backup database code snippets are created from an inconsistent code skeleton
#   with the help of O365 Copilot
class BackupDatabaseService(DatabaseService):
    def Write(self, request, context):
        logger.info(
            "[BACKUP %s] Replicated write id=%s stock=%d",
            self.db_id,
            request.book.id,
            request.book.stock
        )
        data = {
            "id": request.book.id,
            "title": request.book.title,
            "author": request.book.author,
            "stock": request.book.stock,
            "price": normalize_price(request.book.price),
        }
        ok = self.store.write(request.book.id, data)
        return database.WriteResponse(success=ok)
    def Delete(self, request, context):
        logger.info(f"[BACKUP {self.db_id}] Replicated delete: {request.id}")
        ok = self.store.delete(request.id)
        return database.DeleteResponse(success=ok)

class PrimaryDatabaseService(DatabaseService):
    def __init__(self, db_id, peer_ids):
        super().__init__(db_id, peer_ids)

        if not self.store.get_all():
            raise RuntimeError(
                f"Primary DB {db_id} has no state; refusing to start"
            )

        self.backups = []
        for peer_id in peer_ids:
            if peer_id == db_id:
                continue
            channel = grpc.insecure_channel(f"database-{peer_id}:50057")
            stub = database_grpc.DatabaseServiceStub(channel)
            self.backups.append(stub)
        logger.info(f"[PRIMARY {db_id}] Connected to backups")
    def Write(self, request, context):
        logger.info(f"[PRIMARY] Write: {request.book.id}")
        # 1. Local write
        response = super().Write(request, context)
        if not response.success:
            return response
        # 2. Synchronous replication
        for backup in self.backups:
            try:
                backup.Write(request)
            except Exception as e:
                logger.error(f"Replication failed: {e}")
                return database.WriteResponse(success=False)
        return database.WriteResponse(success=True)
    def Delete(self, request, context):
        logger.info(f"[PRIMARY] Delete: {request.id}")
        response = super().Delete(request, context)
        if not response.success:
            return response
        for backup in self.backups:
            try:
                backup.Delete(request)
            except Exception as e:
                logger.error(f"Replication failed: {e}")
                return database.DeleteResponse(success=False)
        return database.DeleteResponse(success=True)

def launch_database(db_id, peer_ids):
    primary_id = min(peer_ids)
    if db_id == primary_id:
        service = PrimaryDatabaseService(db_id, peer_ids)
        logger.info(f"Replica {db_id} is PRIMARY")
    else:
        service = BackupDatabaseService(db_id, peer_ids)
        logger.info(f"Replica {db_id} is BACKUP")

    server = grpc.server(futures.ThreadPoolExecutor())
    database_grpc.add_DatabaseServiceServicer_to_server(service, server)
    server.add_insecure_port("[::]:50057")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    my_id = int(os.getenv("DATABASE_ID", "1"))
    peers = list(map(int, os.getenv("DATABASE_PEERS", "1").split(",")))
    launch_database(my_id, peers)
