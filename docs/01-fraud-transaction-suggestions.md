# `fraud_detection` microservice

The `fraud_detection` microservice is concerned with determining whether an order is fraudulent or not. 

For checkpoint #1, this is limited to some very simple deterministic logic.

`fraud_detection` is mainly implemented and used in three spots.

## <a id="UTILS_PB_FRAUD"></a> `utils/pb/fraud_detection/`
Contains the `protobuf` description for `fraud_detection` and the gRPC code generated from that `protobuf` description.

### `fraud_detection.proto`
Has two `message` objects:
- `FraudRequest` - contains a string `card_number`, initialized as `1`, and a floating point number `order_amount`, initialized as `2`.
- `FraudResponse` - constains a simple boolean value `is_fraud`, determining whether a response is considered fraudulent or not.

Additionally contains service `FraudDetectionService`, what defines an RPC call for `CheckFraud`, which takes a `FraudRequest` as an input and returns a `FraudResponse`.


## `fraud_detection/src/app.py`
Creates a `FraudDetectionService` class that defines all service functions. The server functions are derived from the `protobuf` file in [`utils/pb/fraud_detection/`](#UTILS_PB_FRAUD).

The class `FraudDetectionService` takes the generated `fraud_detection_grpc.FraudDetectionServiceServicer` as an argument and contains the following methods:
- `CheckFraud(self, request, context)` - assumes that a card is not fraudulent, checks the order amount and for an unnatural sequence at the start of the `card_number` and returns a `fraud_detection.FraudResponse(is_fraud)` object.

Additionally, this contains the code required for creating and starting the gRPC server on the specified port.

## `orchestrator/src/app.py`
In the orchestrator, the fraud detection is implemented as follows.

The method `call_fraud_detection(string, float)` takes the card number and order amount as arguments, establishes a connection with the fraud detection gRPC service, creates the stub object based on the generated `FraudDetectionServiceStub`, creates a `FraudRequest` request object, calls the stubs' `CheckFraud` method and returns `response.is_fraud`.



# `transaction_verification` microservice

The `transaction_verification` microservice is concerned with verifying all the information sent by the user. This means checking the validity of the following fields:
- `Items` - do items exist, are quantities valid?
- `Name` - do we have at least two names separated with a space, only alphabetic (English only) characters?
- `Contact` - arguably, this could be both a phone number or an email. We will assume that `contact` contains email information as this is what is provided in the example.
- `Credit Card Number` - we will use real credit card logic for this (TODO).
- `Expiration Date` - we will check whether the date has not yet passed.
- `CVV` - three-number check; we will check, whether there is some logic behind this number.
- `Comment` - mayhaps limit to alphanumeric characters with a few additions; mayhaps not.
- `Billing address` fields will be checked for standard formatting.
    - `Street address`
    - `City/Town`
    - `State`
    - `Postal Code`
    - `Country`
- `Shipping method` - can only be "Standard", "Express" or "Next-day".
- `Gift Wrapping` - can only be represented by a boolean.
- `Accept T&C` - must be checked; can only be represented by a boolean.






# `suggestions` microservice

For checkpoint #1, this is limited to some very simple deterministic logic.