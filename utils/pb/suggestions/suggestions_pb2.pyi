from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SuggestionRequest(_message.Message):
    __slots__ = ("card_number", "order_amount")
    CARD_NUMBER_FIELD_NUMBER: _ClassVar[int]
    ORDER_AMOUNT_FIELD_NUMBER: _ClassVar[int]
    card_number: str
    order_amount: float
    def __init__(self, card_number: _Optional[str] = ..., order_amount: _Optional[float] = ...) -> None: ...

class Book(_message.Message):
    __slots__ = ("id", "author", "title")
    ID_FIELD_NUMBER: _ClassVar[int]
    AUTHOR_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    id: str
    author: str
    title: str
    def __init__(self, id: _Optional[str] = ..., author: _Optional[str] = ..., title: _Optional[str] = ...) -> None: ...

class SuggestionResponse(_message.Message):
    __slots__ = ("books",)
    BOOKS_FIELD_NUMBER: _ClassVar[int]
    books: _containers.RepeatedCompositeFieldContainer[Book]
    def __init__(self, books: _Optional[_Iterable[_Union[Book, _Mapping]]] = ...) -> None: ...
