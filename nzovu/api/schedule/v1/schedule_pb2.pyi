from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import duration_pb2 as _duration_pb2
from nzovu.api.common.v1 import common_pb2 as _common_pb2
from nzovu.api.message.v1 import message_pb2 as _message_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Schedule(_message.Message):
    __slots__ = ("schedule_id", "metadata")
    class Metadata(_message.Message):
        __slots__ = ("payload", "state", "cron_schedule", "calendar_schedule", "queue_name", "message_ids", "next_run", "last_run", "created_at", "updated_at", "state_message", "priority", "has_max_messages", "max_messages", "lease_duration", "timezone", "next_runs", "headers")
        class State(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            SCHEDULED: _ClassVar[Schedule.Metadata.State]
            CANCELED: _ClassVar[Schedule.Metadata.State]
            ERRORED: _ClassVar[Schedule.Metadata.State]
            PAUSED: _ClassVar[Schedule.Metadata.State]
        SCHEDULED: Schedule.Metadata.State
        CANCELED: Schedule.Metadata.State
        ERRORED: Schedule.Metadata.State
        PAUSED: Schedule.Metadata.State
        PAYLOAD_FIELD_NUMBER: _ClassVar[int]
        STATE_FIELD_NUMBER: _ClassVar[int]
        CRON_SCHEDULE_FIELD_NUMBER: _ClassVar[int]
        CALENDAR_SCHEDULE_FIELD_NUMBER: _ClassVar[int]
        QUEUE_NAME_FIELD_NUMBER: _ClassVar[int]
        MESSAGE_IDS_FIELD_NUMBER: _ClassVar[int]
        NEXT_RUN_FIELD_NUMBER: _ClassVar[int]
        LAST_RUN_FIELD_NUMBER: _ClassVar[int]
        CREATED_AT_FIELD_NUMBER: _ClassVar[int]
        UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
        STATE_MESSAGE_FIELD_NUMBER: _ClassVar[int]
        PRIORITY_FIELD_NUMBER: _ClassVar[int]
        HAS_MAX_MESSAGES_FIELD_NUMBER: _ClassVar[int]
        MAX_MESSAGES_FIELD_NUMBER: _ClassVar[int]
        LEASE_DURATION_FIELD_NUMBER: _ClassVar[int]
        TIMEZONE_FIELD_NUMBER: _ClassVar[int]
        NEXT_RUNS_FIELD_NUMBER: _ClassVar[int]
        HEADERS_FIELD_NUMBER: _ClassVar[int]
        payload: _common_pb2.Payload
        state: Schedule.Metadata.State
        cron_schedule: str
        calendar_schedule: CalendarSchedule
        queue_name: str
        message_ids: _containers.RepeatedScalarFieldContainer[str]
        next_run: _timestamp_pb2.Timestamp
        last_run: _timestamp_pb2.Timestamp
        created_at: _timestamp_pb2.Timestamp
        updated_at: _timestamp_pb2.Timestamp
        state_message: str
        priority: int
        has_max_messages: bool
        max_messages: int
        lease_duration: _duration_pb2.Duration
        timezone: str
        next_runs: _containers.RepeatedCompositeFieldContainer[_timestamp_pb2.Timestamp]
        headers: _containers.RepeatedCompositeFieldContainer[_message_pb2.Message.Metadata.Header]
        def __init__(self, payload: _Optional[_Union[_common_pb2.Payload, _Mapping]] = ..., state: _Optional[_Union[Schedule.Metadata.State, str]] = ..., cron_schedule: _Optional[str] = ..., calendar_schedule: _Optional[_Union[CalendarSchedule, _Mapping]] = ..., queue_name: _Optional[str] = ..., message_ids: _Optional[_Iterable[str]] = ..., next_run: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., last_run: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., created_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., state_message: _Optional[str] = ..., priority: _Optional[int] = ..., has_max_messages: bool = ..., max_messages: _Optional[int] = ..., lease_duration: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., timezone: _Optional[str] = ..., next_runs: _Optional[_Iterable[_Union[_timestamp_pb2.Timestamp, _Mapping]]] = ..., headers: _Optional[_Iterable[_Union[_message_pb2.Message.Metadata.Header, _Mapping]]] = ...) -> None: ...
    SCHEDULE_ID_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    schedule_id: str
    metadata: Schedule.Metadata
    def __init__(self, schedule_id: _Optional[str] = ..., metadata: _Optional[_Union[Schedule.Metadata, _Mapping]] = ...) -> None: ...

class ScheduleHistory(_message.Message):
    __slots__ = ("messages", "schedule_id", "next_run", "last_run", "created_at", "updated_at", "executions")
    class Execution(_message.Message):
        __slots__ = ("message_id", "executed_at", "success", "error_message", "message")
        MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
        EXECUTED_AT_FIELD_NUMBER: _ClassVar[int]
        SUCCESS_FIELD_NUMBER: _ClassVar[int]
        ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
        MESSAGE_FIELD_NUMBER: _ClassVar[int]
        message_id: str
        executed_at: _timestamp_pb2.Timestamp
        success: bool
        error_message: str
        message: _message_pb2.Message
        def __init__(self, message_id: _Optional[str] = ..., executed_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., success: bool = ..., error_message: _Optional[str] = ..., message: _Optional[_Union[_message_pb2.Message, _Mapping]] = ...) -> None: ...
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    SCHEDULE_ID_FIELD_NUMBER: _ClassVar[int]
    NEXT_RUN_FIELD_NUMBER: _ClassVar[int]
    LAST_RUN_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    EXECUTIONS_FIELD_NUMBER: _ClassVar[int]
    messages: _containers.RepeatedCompositeFieldContainer[_message_pb2.Message]
    schedule_id: str
    next_run: _timestamp_pb2.Timestamp
    last_run: _timestamp_pb2.Timestamp
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    executions: _containers.RepeatedCompositeFieldContainer[ScheduleHistory.Execution]
    def __init__(self, messages: _Optional[_Iterable[_Union[_message_pb2.Message, _Mapping]]] = ..., schedule_id: _Optional[str] = ..., next_run: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., last_run: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., created_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., executions: _Optional[_Iterable[_Union[ScheduleHistory.Execution, _Mapping]]] = ...) -> None: ...

class CalendarSchedule(_message.Message):
    __slots__ = ("type", "rules", "timezone", "business_calendar", "exceptions")
    class ScheduleType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        MONTHLY: _ClassVar[CalendarSchedule.ScheduleType]
        WEEKLY: _ClassVar[CalendarSchedule.ScheduleType]
        DAILY: _ClassVar[CalendarSchedule.ScheduleType]
        YEARLY: _ClassVar[CalendarSchedule.ScheduleType]
        BUSINESS_DAYS: _ClassVar[CalendarSchedule.ScheduleType]
        CUSTOM: _ClassVar[CalendarSchedule.ScheduleType]
    MONTHLY: CalendarSchedule.ScheduleType
    WEEKLY: CalendarSchedule.ScheduleType
    DAILY: CalendarSchedule.ScheduleType
    YEARLY: CalendarSchedule.ScheduleType
    BUSINESS_DAYS: CalendarSchedule.ScheduleType
    CUSTOM: CalendarSchedule.ScheduleType
    TYPE_FIELD_NUMBER: _ClassVar[int]
    RULES_FIELD_NUMBER: _ClassVar[int]
    TIMEZONE_FIELD_NUMBER: _ClassVar[int]
    BUSINESS_CALENDAR_FIELD_NUMBER: _ClassVar[int]
    EXCEPTIONS_FIELD_NUMBER: _ClassVar[int]
    type: CalendarSchedule.ScheduleType
    rules: _containers.RepeatedCompositeFieldContainer[CalendarRule]
    timezone: str
    business_calendar: BusinessCalendar
    exceptions: _containers.RepeatedCompositeFieldContainer[CalendarException]
    def __init__(self, type: _Optional[_Union[CalendarSchedule.ScheduleType, str]] = ..., rules: _Optional[_Iterable[_Union[CalendarRule, _Mapping]]] = ..., timezone: _Optional[str] = ..., business_calendar: _Optional[_Union[BusinessCalendar, _Mapping]] = ..., exceptions: _Optional[_Iterable[_Union[CalendarException, _Mapping]]] = ...) -> None: ...

class CalendarRule(_message.Message):
    __slots__ = ("monthly", "weekly", "daily", "yearly", "business_days", "custom", "execution_times", "valid_from", "valid_until")
    MONTHLY_FIELD_NUMBER: _ClassVar[int]
    WEEKLY_FIELD_NUMBER: _ClassVar[int]
    DAILY_FIELD_NUMBER: _ClassVar[int]
    YEARLY_FIELD_NUMBER: _ClassVar[int]
    BUSINESS_DAYS_FIELD_NUMBER: _ClassVar[int]
    CUSTOM_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_TIMES_FIELD_NUMBER: _ClassVar[int]
    VALID_FROM_FIELD_NUMBER: _ClassVar[int]
    VALID_UNTIL_FIELD_NUMBER: _ClassVar[int]
    monthly: MonthlyRule
    weekly: WeeklyRule
    daily: DailyRule
    yearly: YearlyRule
    business_days: BusinessDaysRule
    custom: CustomRule
    execution_times: _containers.RepeatedCompositeFieldContainer[TimeOfDay]
    valid_from: _timestamp_pb2.Timestamp
    valid_until: _timestamp_pb2.Timestamp
    def __init__(self, monthly: _Optional[_Union[MonthlyRule, _Mapping]] = ..., weekly: _Optional[_Union[WeeklyRule, _Mapping]] = ..., daily: _Optional[_Union[DailyRule, _Mapping]] = ..., yearly: _Optional[_Union[YearlyRule, _Mapping]] = ..., business_days: _Optional[_Union[BusinessDaysRule, _Mapping]] = ..., custom: _Optional[_Union[CustomRule, _Mapping]] = ..., execution_times: _Optional[_Iterable[_Union[TimeOfDay, _Mapping]]] = ..., valid_from: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., valid_until: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class MonthlyRule(_message.Message):
    __slots__ = ("day_type", "day_value", "occurrence", "months")
    class DayType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        DAY_OF_MONTH: _ClassVar[MonthlyRule.DayType]
        WEEKDAY_OF_MONTH: _ClassVar[MonthlyRule.DayType]
        LAST_WEEKDAY: _ClassVar[MonthlyRule.DayType]
        LAST_DAY: _ClassVar[MonthlyRule.DayType]
    DAY_OF_MONTH: MonthlyRule.DayType
    WEEKDAY_OF_MONTH: MonthlyRule.DayType
    LAST_WEEKDAY: MonthlyRule.DayType
    LAST_DAY: MonthlyRule.DayType
    DAY_TYPE_FIELD_NUMBER: _ClassVar[int]
    DAY_VALUE_FIELD_NUMBER: _ClassVar[int]
    OCCURRENCE_FIELD_NUMBER: _ClassVar[int]
    MONTHS_FIELD_NUMBER: _ClassVar[int]
    day_type: MonthlyRule.DayType
    day_value: int
    occurrence: int
    months: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, day_type: _Optional[_Union[MonthlyRule.DayType, str]] = ..., day_value: _Optional[int] = ..., occurrence: _Optional[int] = ..., months: _Optional[_Iterable[int]] = ...) -> None: ...

class WeeklyRule(_message.Message):
    __slots__ = ("days_of_week", "week_interval", "start_week")
    DAYS_OF_WEEK_FIELD_NUMBER: _ClassVar[int]
    WEEK_INTERVAL_FIELD_NUMBER: _ClassVar[int]
    START_WEEK_FIELD_NUMBER: _ClassVar[int]
    days_of_week: _containers.RepeatedScalarFieldContainer[int]
    week_interval: int
    start_week: _timestamp_pb2.Timestamp
    def __init__(self, days_of_week: _Optional[_Iterable[int]] = ..., week_interval: _Optional[int] = ..., start_week: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class DailyRule(_message.Message):
    __slots__ = ("day_interval", "weekdays_only", "start_date")
    DAY_INTERVAL_FIELD_NUMBER: _ClassVar[int]
    WEEKDAYS_ONLY_FIELD_NUMBER: _ClassVar[int]
    START_DATE_FIELD_NUMBER: _ClassVar[int]
    day_interval: int
    weekdays_only: bool
    start_date: _timestamp_pb2.Timestamp
    def __init__(self, day_interval: _Optional[int] = ..., weekdays_only: bool = ..., start_date: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class YearlyRule(_message.Message):
    __slots__ = ("month", "day", "adjust_for_leap_year")
    MONTH_FIELD_NUMBER: _ClassVar[int]
    DAY_FIELD_NUMBER: _ClassVar[int]
    ADJUST_FOR_LEAP_YEAR_FIELD_NUMBER: _ClassVar[int]
    month: int
    day: int
    adjust_for_leap_year: bool
    def __init__(self, month: _Optional[int] = ..., day: _Optional[int] = ..., adjust_for_leap_year: bool = ...) -> None: ...

class BusinessDaysRule(_message.Message):
    __slots__ = ("business_calendar_id", "day_offset")
    BUSINESS_CALENDAR_ID_FIELD_NUMBER: _ClassVar[int]
    DAY_OFFSET_FIELD_NUMBER: _ClassVar[int]
    business_calendar_id: str
    day_offset: int
    def __init__(self, business_calendar_id: _Optional[str] = ..., day_offset: _Optional[int] = ...) -> None: ...

class CustomRule(_message.Message):
    __slots__ = ("expression", "rule_type", "parameters")
    class ParametersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    EXPRESSION_FIELD_NUMBER: _ClassVar[int]
    RULE_TYPE_FIELD_NUMBER: _ClassVar[int]
    PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    expression: str
    rule_type: str
    parameters: _containers.ScalarMap[str, str]
    def __init__(self, expression: _Optional[str] = ..., rule_type: _Optional[str] = ..., parameters: _Optional[_Mapping[str, str]] = ...) -> None: ...

class TimeOfDay(_message.Message):
    __slots__ = ("hour", "minute", "second")
    HOUR_FIELD_NUMBER: _ClassVar[int]
    MINUTE_FIELD_NUMBER: _ClassVar[int]
    SECOND_FIELD_NUMBER: _ClassVar[int]
    hour: int
    minute: int
    second: int
    def __init__(self, hour: _Optional[int] = ..., minute: _Optional[int] = ..., second: _Optional[int] = ...) -> None: ...

class BusinessCalendar(_message.Message):
    __slots__ = ("calendar_id", "name", "description", "holidays", "weekend_days", "timezone")
    CALENDAR_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    HOLIDAYS_FIELD_NUMBER: _ClassVar[int]
    WEEKEND_DAYS_FIELD_NUMBER: _ClassVar[int]
    TIMEZONE_FIELD_NUMBER: _ClassVar[int]
    calendar_id: str
    name: str
    description: str
    holidays: _containers.RepeatedCompositeFieldContainer[Holiday]
    weekend_days: _containers.RepeatedScalarFieldContainer[int]
    timezone: str
    def __init__(self, calendar_id: _Optional[str] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., holidays: _Optional[_Iterable[_Union[Holiday, _Mapping]]] = ..., weekend_days: _Optional[_Iterable[int]] = ..., timezone: _Optional[str] = ...) -> None: ...

class Holiday(_message.Message):
    __slots__ = ("name", "date", "recurring_yearly", "rule")
    NAME_FIELD_NUMBER: _ClassVar[int]
    DATE_FIELD_NUMBER: _ClassVar[int]
    RECURRING_YEARLY_FIELD_NUMBER: _ClassVar[int]
    RULE_FIELD_NUMBER: _ClassVar[int]
    name: str
    date: _timestamp_pb2.Timestamp
    recurring_yearly: bool
    rule: HolidayRule
    def __init__(self, name: _Optional[str] = ..., date: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., recurring_yearly: bool = ..., rule: _Optional[_Union[HolidayRule, _Mapping]] = ...) -> None: ...

class HolidayRule(_message.Message):
    __slots__ = ("fixed", "relative", "easter_offset")
    FIXED_FIELD_NUMBER: _ClassVar[int]
    RELATIVE_FIELD_NUMBER: _ClassVar[int]
    EASTER_OFFSET_FIELD_NUMBER: _ClassVar[int]
    fixed: FixedDate
    relative: RelativeDate
    easter_offset: EasterOffset
    def __init__(self, fixed: _Optional[_Union[FixedDate, _Mapping]] = ..., relative: _Optional[_Union[RelativeDate, _Mapping]] = ..., easter_offset: _Optional[_Union[EasterOffset, _Mapping]] = ...) -> None: ...

class FixedDate(_message.Message):
    __slots__ = ("month", "day")
    MONTH_FIELD_NUMBER: _ClassVar[int]
    DAY_FIELD_NUMBER: _ClassVar[int]
    month: int
    day: int
    def __init__(self, month: _Optional[int] = ..., day: _Optional[int] = ...) -> None: ...

class RelativeDate(_message.Message):
    __slots__ = ("month", "weekday", "occurrence")
    MONTH_FIELD_NUMBER: _ClassVar[int]
    WEEKDAY_FIELD_NUMBER: _ClassVar[int]
    OCCURRENCE_FIELD_NUMBER: _ClassVar[int]
    month: int
    weekday: int
    occurrence: int
    def __init__(self, month: _Optional[int] = ..., weekday: _Optional[int] = ..., occurrence: _Optional[int] = ...) -> None: ...

class EasterOffset(_message.Message):
    __slots__ = ("days_offset",)
    DAYS_OFFSET_FIELD_NUMBER: _ClassVar[int]
    days_offset: int
    def __init__(self, days_offset: _Optional[int] = ...) -> None: ...

class CalendarException(_message.Message):
    __slots__ = ("date", "type", "reschedule_to", "extra_times", "reason")
    class ExceptionType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        SKIP: _ClassVar[CalendarException.ExceptionType]
        RESCHEDULE: _ClassVar[CalendarException.ExceptionType]
        EXTRA: _ClassVar[CalendarException.ExceptionType]
    SKIP: CalendarException.ExceptionType
    RESCHEDULE: CalendarException.ExceptionType
    EXTRA: CalendarException.ExceptionType
    DATE_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    RESCHEDULE_TO_FIELD_NUMBER: _ClassVar[int]
    EXTRA_TIMES_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    date: _timestamp_pb2.Timestamp
    type: CalendarException.ExceptionType
    reschedule_to: _timestamp_pb2.Timestamp
    extra_times: _containers.RepeatedCompositeFieldContainer[TimeOfDay]
    reason: str
    def __init__(self, date: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., type: _Optional[_Union[CalendarException.ExceptionType, str]] = ..., reschedule_to: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., extra_times: _Optional[_Iterable[_Union[TimeOfDay, _Mapping]]] = ..., reason: _Optional[str] = ...) -> None: ...
