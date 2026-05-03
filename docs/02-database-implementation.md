# Database implementation

The database is a simple key-value store. In our application, this is done with JSON objects: those are also used to save the "database" onto device storage.

The database system consists of three replicas. The database IDs are defined in `docker-compose.yaml` and the smallest ID object is chosen as the "Primary". The "Primary" (id: 1) is the database that all operations go through. Other replicas (id: 2 and id: 3) are used purely as backup. 

During regular operation, Primary would have access to its stored `books.json` database.

In case the Primary doesn't have the database (file `books.json`) stored, it first makes 10 attempts to recover the database from the secondary replicas. Assuming this process succeeds, the Primary will make a copy of the secondary's database. The secondaries' databases will then be updated once a `Write` or `Delete` operation is called.
This process may look a bit ugly in the terminal.

In case no replicas have the database stored, the following values will be *seeded* as defaults:
```json
{
    "1": {
        "id": "1",
        "title": "The Best Book",
        "author": "Author 1",
        "stock": 10,
        "price": 9.99
    },
    "2": {
        "id": "2",
        "title": "The Best Book 2",
        "author": "Author 2",
        "stock": 5,
        "price": 14.99
    },
    "3": {
        "id": "3",
        "title": "The Best Book 3",
        "author": "Author 3",
        "stock": 8,
        "price": 12.99
    },
    "4": {
        "id": "4",
        "title": "The Best Book 4",
        "author": "Author 4",
        "stock": 3,
        "price": 19.99
    },
    "5": {
        "id": "5",
        "title": "The Best Book 5",
        "author": "Author 5",
        "stock": 15,
        "price": 7.99
    }
}
```

This seeding process means that to "reset" the database, it suffices to delete all files `database/database_data_{1,2,3}/books.json`. Because we currently have the fixed order quantity of one book ID 1 and two books ID 2, this process may need to be done quite often.

Just for the sake of redundancy, the repository also has `books.json.backup` files in each folder. Therefore, if removing and waiting for the 10 attempts to fail seems tedious, one of the `.backup` files could be copied and then the syncing would magically fill the database once more.

An addition that followed naturally from the database task is that orchestrator now queries the database for stock availability and can return `Order Rejected` based on that.
