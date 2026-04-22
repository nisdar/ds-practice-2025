# Checkpoint 3 plan

## Session 10 tasks
Books Database: Create a new gRPC service - the books database - which will have two atomic operations, Read and Write:

- **Replication**: This service should be replicated, at least 3 times, meaning, there should be at least 3 instances of the database running at every moment. These instances may implement and run the same code, which internally should consist of a basic key-value structure. The gRPC interface, however, should allow for Read and Write operations across the whole distributed database system, abstracting from the client process (the one querying the database) the fact that it’s a distributed database. For that, we need to employ a consistency protocol. 
- **Consistency**: Explore the different strategies for consistency protocols or consensus mechanisms learned in the lectures, choose one that suits some minimum consistency levels, e.g. sequential consistency, and proceed with the implementation. Examples are Primary-based protocols, State-machine Replication, or Chain Replication. There is no right or wrong approach, but be aware of the trade-offs of your design decisions, especially related to trading between consistency and availability, the read/write loads on specific parts of the database system, or the fault-tolerance of the protocol, among other considerations.
- **Order Executor**: The executor services, for each order, should update the database system by performing a series of Reads and Writes, e.g. to read the current value of the stock of a book and calculate and write its new value into the database. 
- **Bonus Points**: How do we deal with concurrent writes by different clients? Think of a solution for the problem of two simultaneous orders trying to update the stocks of the same book. 

## Session 10 decisions

Current idea is to simply have a **Python dictionary as the database** (as discussed with the TAs in session 10). We were asked to create a simple key-value store, so a dictionary (perhaps, if necessary, with JSON backups) would be great.

We would follow a **primary-based** consistency protocol. This would create sequential consistency. From [here](https://csis.pace.edu/~marchese/CS865/Lectures/Chap7/Chapter7fin.htm):
- Each data item is associated with a “primary” replica.
- The primary is responsible for coordinating writes to the data item.
- There are two types of Primary-Based Protocol:
    1. Remote-Write.
        - All writes are performed at a single (remote) server.
        - Read operations can be carried out locally.
        - This model is typically associated with traditional client/server systems.
        - Bad: Performance! *All of those writes can take a long time (especially when a “blocking write protocol” is used).   Using a non-blocking write protocol to handle the updates can lead to fault tolerant problems (which is our next topic).* Good: as the primary is in control, all writes can be sent to each backup replica IN THE SAME ORDER, making it easy to implement sequential consistency.
    2. Local-Write.
        - A single copy of the data item is still maintained.
        - Upon a write, the data item gets transferred to the replica that is writing. The status of *primary* for a data item is *transferrable*.
        - Good: Multiple, successive write operations can be carried out locally, while reading processes can still access their local copy. Difficulty: Can be achieved only if a nonblocking protocol is followed by which updates are propagated to the replicas after the primary has finished with locally performing the updates.


## Session 11 tasks
Extend your system with the following functionality and new services:

- **New Services**: Create one new dummy gRPC service - for instance, the payment system. This service does not need any custom logic apart from the distributed commitment protocol described below. You may implement some dummy logic for their service operations (payment execution). This service doesn’t need to be replicated, meaning, we only need one instance of it.
- **Commitment**: Add the functionality in the executor service, in the database module, and in your new payment system, to establish a commitment protocol of your choice, f.e., 2PC, 3PC, or other. The executor service should act as the coordinator and the other two services should be the participants. Implement and study the trade-offs of your commitment protocol, especially related to the amount of phases, messages exchanged, and probability of blocking in specific phases.
- **Execution**: The goal is to make the participant services to commit to their operations over each order. Find a way to encapsulate these operations within the distributed commitment protocol, meaning, after the last commit message is received from the coordinator, these services should execute their operations, i.e., the payment system should execute the payment (dummy operation) and the database should update the data (operation as in the last practice session).
- **Bonus Points**: How do we deal with failing participants? Think of a solution for the problem of recovering from failing participants, in specific phases of your commitment protocol. Devise and test a mechanism for simple recoveries in one of the services.
- **Bonus Points**: What about failure of the coordinator? Analyse the system and try to understand what are the consequences of a failing coordinator, during the execution of the commitment protocol. Think of a solution for this issue. No implementation is needed, but the points will only be awarded upon good analysis, justification, and solution. 

## Session 11 decisions

About commitment (transaction) schemes: [this article](https://tikv.org/deep-dive/distributed-transaction/distributed-algorithms/) seems nice.

2PC seems perfectly cromulent.