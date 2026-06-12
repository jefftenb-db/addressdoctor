"""End-to-end verification pipeline — callable from batch (Spark) or Model Serving.

Orchestration only. The two Databricks-native steps (LLM parse via ai_query and
Vector Search candidate lookup) are injected as callables so this module stays
unit-testable without a workspace.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Optional

from .schemas import AddressSchema
from .scoring import AVStatus, MatchInputs, compute_av_status, compute_match_score
from .standardize import standardize_address, format_delivery_line, format_last_line


@dataclass
class Candidate:
    """A single reference-data match returned by Vector Search (or exact join)."""
    address: AddressSchema
    vector_distance: float
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    is_exact: bool = False


@dataclass
class VerifiedAddress:
    raw_input: str
    parsed: AddressSchema
    standardized: AddressSchema
    chosen: Optional[AddressSchema]
    delivery_line: str
    last_line: str
    latitude: Optional[float]
    longitude: Optional[float]
    av_status: AVStatus
    match_score: int
    was_corrected: bool
    candidate_count: int
    notes: list[str] = field(default_factory=list)


# Injected function signatures — implemented in Databricks notebooks.
ParseFn = Callable[[str], AddressSchema]
CandidatesFn = Callable[[AddressSchema, int], list[Candidate]]
JudgeFn = Callable[[AddressSchema, list[Candidate]], tuple[Optional[Candidate], float]]


def _differs(a: AddressSchema, b: AddressSchema) -> bool:
    """True if any field present in `a` was changed in `b`.
    Null->value is enrichment, not correction, and does not count."""
    fields = [
        "primary_number", "street_predirection", "street_name", "street_suffix",
        "street_postdirection", "secondary_designator", "secondary_number",
        "city", "state", "zipcode", "zip_plus_4",
    ]
    for f in fields:
        av, bv = getattr(a, f), getattr(b, f)
        if av is not None and av != bv:
            return True
    return False


def verify_address(
    raw_input: str,
    *,
    parse_fn: ParseFn,
    candidates_fn: CandidatesFn,
    judge_fn: JudgeFn,
    top_k: int = 5,
) -> VerifiedAddress:
    """Single-address verification: parse -> standardize -> match -> correct -> geocode."""
    notes: list[str] = []

    parsed = parse_fn(raw_input)
    standardized = standardize_address(parsed)

    candidates = candidates_fn(standardized, top_k)

    exact_hit = next((c for c in candidates if c.is_exact), None)

    if exact_hit is not None:
        chosen = exact_hit
        judge_confidence = 1.0
        notes.append("exact reference hit")
    else:
        chosen, judge_confidence = judge_fn(standardized, candidates)
        if chosen is None:
            notes.append("no candidate accepted by judge")

    final_addr = chosen.address if chosen is not None else standardized
    was_corrected = chosen is not None and _differs(standardized, final_addr)

    inputs = MatchInputs(
        exact_ref_hit=exact_hit is not None,
        vector_distance=chosen.vector_distance if chosen else 2.0,
        llm_judge_confidence=judge_confidence,
        was_corrected=was_corrected,
        has_primary_number=bool(final_addr.primary_number),
        has_street=bool(final_addr.street_name),
        has_city=bool(final_addr.city),
        has_state=bool(final_addr.state),
        has_zip=bool(final_addr.zipcode),
        candidate_count=len(candidates),
    )

    return VerifiedAddress(
        raw_input=raw_input,
        parsed=parsed,
        standardized=standardized,
        chosen=final_addr,
        delivery_line=format_delivery_line(final_addr),
        last_line=format_last_line(final_addr),
        latitude=chosen.latitude if chosen else None,
        longitude=chosen.longitude if chosen else None,
        av_status=compute_av_status(inputs),
        match_score=compute_match_score(inputs),
        was_corrected=was_corrected,
        candidate_count=len(candidates),
        notes=notes,
    )
