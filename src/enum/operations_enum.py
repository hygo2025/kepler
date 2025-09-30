from enum import Enum


class OperationsEnum(Enum):
    TO_PARQUET = "to_parquet"
    MAKE_USER_SESSIONS = "make_user_sessions"
    ENRICH_EVENTS = "enrich_events"