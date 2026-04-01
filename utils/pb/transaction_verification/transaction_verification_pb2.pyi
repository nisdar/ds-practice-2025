from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class VerificationRequest(_message.Message):
    __slots__ = ("items", "user", "creditCard", "comment", "billingAddress", "shippingMethod", "giftWrapping", "termsAccepted")
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    USER_FIELD_NUMBER: _ClassVar[int]
    CREDITCARD_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    BILLINGADDRESS_FIELD_NUMBER: _ClassVar[int]
    SHIPPINGMETHOD_FIELD_NUMBER: _ClassVar[int]
    GIFTWRAPPING_FIELD_NUMBER: _ClassVar[int]
    TERMSACCEPTED_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[ItemData]
    user: User
    creditCard: CreditCard
    comment: Comment
    billingAddress: BillingAddress
    shippingMethod: str
    giftWrapping: bool
    termsAccepted: bool
    def __init__(self, items: _Optional[_Iterable[_Union[ItemData, _Mapping]]] = ..., user: _Optional[_Union[User, _Mapping]] = ..., creditCard: _Optional[_Union[CreditCard, _Mapping]] = ..., comment: _Optional[_Union[Comment, _Mapping]] = ..., billingAddress: _Optional[_Union[BillingAddress, _Mapping]] = ..., shippingMethod: _Optional[str] = ..., giftWrapping: bool = ..., termsAccepted: bool = ...) -> None: ...

class VerificationResponse(_message.Message):
    __slots__ = ("success", "comment")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    success: bool
    comment: str
    def __init__(self, success: bool = ..., comment: _Optional[str] = ...) -> None: ...

class CreditCard(_message.Message):
    __slots__ = ("number", "expirationDate", "cvv")
    NUMBER_FIELD_NUMBER: _ClassVar[int]
    EXPIRATIONDATE_FIELD_NUMBER: _ClassVar[int]
    CVV_FIELD_NUMBER: _ClassVar[int]
    number: str
    expirationDate: str
    cvv: str
    def __init__(self, number: _Optional[str] = ..., expirationDate: _Optional[str] = ..., cvv: _Optional[str] = ...) -> None: ...

class BillingAddress(_message.Message):
    __slots__ = ("street", "city", "state", "zip", "country")
    STREET_FIELD_NUMBER: _ClassVar[int]
    CITY_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    ZIP_FIELD_NUMBER: _ClassVar[int]
    COUNTRY_FIELD_NUMBER: _ClassVar[int]
    street: str
    city: str
    state: str
    zip: str
    country: str
    def __init__(self, street: _Optional[str] = ..., city: _Optional[str] = ..., state: _Optional[str] = ..., zip: _Optional[str] = ..., country: _Optional[str] = ...) -> None: ...

class User(_message.Message):
    __slots__ = ("name", "contact")
    NAME_FIELD_NUMBER: _ClassVar[int]
    CONTACT_FIELD_NUMBER: _ClassVar[int]
    name: str
    contact: str
    def __init__(self, name: _Optional[str] = ..., contact: _Optional[str] = ...) -> None: ...

class OrderInfo(_message.Message):
    __slots__ = ("id", "vectorClock")
    ID_FIELD_NUMBER: _ClassVar[int]
    VECTORCLOCK_FIELD_NUMBER: _ClassVar[int]
    id: str
    vectorClock: VectorClock
    def __init__(self, id: _Optional[str] = ..., vectorClock: _Optional[_Union[VectorClock, _Mapping]] = ...) -> None: ...

class ItemData(_message.Message):
    __slots__ = ("name", "quantity")
    NAME_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    name: str
    quantity: str
    def __init__(self, name: _Optional[str] = ..., quantity: _Optional[str] = ...) -> None: ...

class OrderData(_message.Message):
    __slots__ = ("orderId", "items", "user", "creditCard", "comment", "billingAddress", "shippingMethod", "giftWrapping", "termsAccepted")
    ORDERID_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    USER_FIELD_NUMBER: _ClassVar[int]
    CREDITCARD_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    BILLINGADDRESS_FIELD_NUMBER: _ClassVar[int]
    SHIPPINGMETHOD_FIELD_NUMBER: _ClassVar[int]
    GIFTWRAPPING_FIELD_NUMBER: _ClassVar[int]
    TERMSACCEPTED_FIELD_NUMBER: _ClassVar[int]
    orderId: str
    items: _containers.RepeatedCompositeFieldContainer[ItemData]
    user: User
    creditCard: CreditCard
    comment: Comment
    billingAddress: BillingAddress
    shippingMethod: str
    giftWrapping: bool
    termsAccepted: bool
    def __init__(self, orderId: _Optional[str] = ..., items: _Optional[_Iterable[_Union[ItemData, _Mapping]]] = ..., user: _Optional[_Union[User, _Mapping]] = ..., creditCard: _Optional[_Union[CreditCard, _Mapping]] = ..., comment: _Optional[_Union[Comment, _Mapping]] = ..., billingAddress: _Optional[_Union[BillingAddress, _Mapping]] = ..., shippingMethod: _Optional[str] = ..., giftWrapping: bool = ..., termsAccepted: bool = ...) -> None: ...

class VectorClock(_message.Message):
    __slots__ = ("timeStamp",)
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    timeStamp: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, timeStamp: _Optional[_Iterable[int]] = ...) -> None: ...

class Book(_message.Message):
    __slots__ = ("id", "author", "title")
    ID_FIELD_NUMBER: _ClassVar[int]
    AUTHOR_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    id: str
    author: str
    title: str
    def __init__(self, id: _Optional[str] = ..., author: _Optional[str] = ..., title: _Optional[str] = ...) -> None: ...

class Comment(_message.Message):
    __slots__ = ("comment",)
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    comment: str
    def __init__(self, comment: _Optional[str] = ...) -> None: ...

class ClearDataResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class OrderResponse(_message.Message):
    __slots__ = ("vectorClock", "success", "suggestions")
    VECTORCLOCK_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    SUGGESTIONS_FIELD_NUMBER: _ClassVar[int]
    vectorClock: VectorClock
    success: bool
    suggestions: _containers.RepeatedCompositeFieldContainer[Book]
    def __init__(self, vectorClock: _Optional[_Union[VectorClock, _Mapping]] = ..., success: bool = ..., suggestions: _Optional[_Iterable[_Union[Book, _Mapping]]] = ...) -> None: ...

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
