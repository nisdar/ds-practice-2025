from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class QueueAddRequest(_message.Message):
    __slots__ = ("addable_order",)
    ADDABLE_ORDER_FIELD_NUMBER: _ClassVar[int]
    addable_order: str
    def __init__(self, addable_order: _Optional[str] = ...) -> None: ...

class QueueRemoveRequest(_message.Message):
    __slots__ = ("removable_order",)
    REMOVABLE_ORDER_FIELD_NUMBER: _ClassVar[int]
    removable_order: str
    def __init__(self, removable_order: _Optional[str] = ...) -> None: ...

class QueueResponse(_message.Message):
    __slots__ = ("success", "order_queue")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    ORDER_QUEUE_FIELD_NUMBER: _ClassVar[int]
    success: bool
    order_queue: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, success: bool = ..., order_queue: _Optional[_Iterable[str]] = ...) -> None: ...

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
