from enum import StrEnum,IntEnum


class LambdaName(StrEnum):
    notification = 'notification'


class EntityTypeEnum(StrEnum):
    survey = 'SURVEY'
    task = 'ACTIVETASK'


class NoticeTypeEnum(IntEnum):
    NewArrival=10
    Due=20
    Expire=30
    EventClosure=40
    PeriodStart=50

