# System model for the bookstore
The online bookstore utilizes a partially asynchronous client-server architecture, where the client (user) interacts with a centralized web server to add books to the cart, input their contact and credit card information and submit the order. 
The communciation between the client and the web server is synchronous. 
Processing inside the various components of the bookstore service, like transaction verification, fraud detection and book suggestions, is modular and asynchronous to allow for flexibility and parallel work. 
Fraud detection, in the fully functional form, would utilise synchronised communication with an external service. This is not yet implemented, as the selection of an external service has proved difficult.
All components utilize threading for parallel processing of requests. 
Furthermore, the order processing is split into queue and executor microservices, ensuring that the queue remains a single source of truth.
Executors rely on an elected leader to ensure consistency and prevent conflicts.
Communication during order processing is assumed to be in a reliable setting: messages are not duplicated nor resent unless the user explicitly resubmits the order.
