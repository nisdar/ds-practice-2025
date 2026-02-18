from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class HelloRequest(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class HelloResponse(_message.Message):
    __slots__ = ("greeting",)
    GREETING_FIELD_NUMBER: _ClassVar[int]
    greeting: str
    def __init__(self, greeting: _Optional[str] = ...) -> None: ...

class FraudRequest(_message.Message):
    __slots__ = ("card_number", "order_amount")
    CARD_NUMBER_FIELD_NUMBER: _ClassVar[int]
    ORDER_AMOUNT_FIELD_NUMBER: _ClassVar[int]
    card_number: str
    order_amount: float
    def __init__(self, card_number: _Optional[str] = ..., order_amount: _Optional[float] = ...) -> None: ...

class FraudResponse(_message.Message):
    __slots__ = ("is_fraud",)
    IS_FRAUD_FIELD_NUMBER: _ClassVar[int]
    is_fraud: bool
    def __init__(self, is_fraud: bool = ...) -> None: ...
