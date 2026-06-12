"""Databricks-native replacement for Informatica Address Doctor (US scope)."""

from .schemas import AddressSchema, ADDRESS_JSON_SCHEMA
from .scoring import compute_av_status, compute_match_score, AVStatus
from .standardize import standardize_address

__all__ = [
    "AddressSchema",
    "ADDRESS_JSON_SCHEMA",
    "AVStatus",
    "compute_av_status",
    "compute_match_score",
    "standardize_address",
]
