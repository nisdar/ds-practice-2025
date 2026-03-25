from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class LeaderElectionRequest(_message.Message):
    __slots__ = ("executors_ids", "finished")
    EXECUTORS_IDS_FIELD_NUMBER: _ClassVar[int]
    FINISHED_FIELD_NUMBER: _ClassVar[int]
    executors_ids: _containers.RepeatedScalarFieldContainer[str]
    finished: bool
    def __init__(self, executors_ids: _Optional[_Iterable[str]] = ..., finished: bool = ...) -> None: ...

class LeaderAnnouncementRequest(_message.Message):
    __slots__ = ("leader_id", "finished")
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    FINISHED_FIELD_NUMBER: _ClassVar[int]
    leader_id: str
    finished: bool
    def __init__(self, leader_id: _Optional[str] = ..., finished: bool = ...) -> None: ...

class LeaderElectionResponse(_message.Message):
    __slots__ = ("executors_ids", "finished")
    EXECUTORS_IDS_FIELD_NUMBER: _ClassVar[int]
    FINISHED_FIELD_NUMBER: _ClassVar[int]
    executors_ids: _containers.RepeatedScalarFieldContainer[str]
    finished: bool
    def __init__(self, executors_ids: _Optional[_Iterable[str]] = ..., finished: bool = ...) -> None: ...

class LeaderAnnouncementResponse(_message.Message):
    __slots__ = ("leader_id", "finished")
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    FINISHED_FIELD_NUMBER: _ClassVar[int]
    leader_id: str
    finished: bool
    def __init__(self, leader_id: _Optional[str] = ..., finished: bool = ...) -> None: ...

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
