# System model for the bookstore
The online bookstore utilizes a partially asynchronous client-server architecture, where the client (user) interacts with a centralized web server to add books to the cart, input their contact and credit card information and submit the order. 
The communciation between the client and the web server is synchronous. 
Processing inside the transaction verification and fraud detection services is asynchronous to allow for flexibility and parallel work; communication between the services themselves is synchronous. 
Fraud detection, in the fully functional form, would utilise synchronised communication with an external service. Suggestions would, in a real implementation, also require synchronous communication with an external service.
All components utilize thread executors for concurrently handling requests. 
Furthermore, the order processing is split into queue and executor microservices, ensuring that the queue remains a single source of truth.
Executors rely on an elected leader to ensure consistency and prevent conflicts.
Communication during order processing is assumed to be in a reliable setting: messages are not duplicated nor re-sent unless the user explicitly re-submits the order.
