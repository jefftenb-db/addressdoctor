"""Address verification status codes and match score.

Mirrors Address Doctor's Vx / Cx / Ix status semantics so downstream
consumers that already branch on those codes work unchanged.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class AVStatus(str, Enum):
    V4 = "V4"  # verified, exact
    V3 = "V3"  # verified with minor standardization (abbreviations, casing)
    V2 = "V2"  # partial — street + ZIP valid, primary number not in reference
    C4 = "C4"  # corrected high-confidence (typo fix, wrong ZIP, etc.)
    C3 = "C3"  # corrected medium-confidence
    I4 = "I4"  # insufficient — could not resolve
    I2 = "I2"  # ambiguous — multiple equally-likely candidates


@dataclass(frozen=True)
class MatchInputs:
    exact_ref_hit: bool
    vector_distance: float           # cosine distance, lower = better (0..2)
    llm_judge_confidence: float      # 0..1
    was_corrected: bool              # did the standardized/LLM output differ from parsed?
    has_primary_number: bool
    has_street: bool
    has_city: bool
    has_state: bool
    has_zip: bool
    candidate_count: int             # top-K candidates from vector search


def compute_match_score(m: MatchInputs) -> int:
    """Composite 0..100 score. Tuned so exact hits land ~95+, fuzzy >80, misses <60."""
    score = 0.0
    score += 40.0 if m.exact_ref_hit else 0.0
    score += max(0.0, (1.0 - m.vector_distance / 2.0)) * 30.0
    score += m.llm_judge_confidence * 20.0
    completeness = sum(
        [m.has_primary_number, m.has_street, m.has_city, m.has_state, m.has_zip]
    ) / 5.0
    score += completeness * 10.0
    return max(0, min(100, int(round(score))))


def compute_av_status(m: MatchInputs) -> AVStatus:
    """Map match inputs to a Vx / Cx / Ix code."""
    if not (m.has_street and m.has_state and m.has_zip):
        return AVStatus.I4

    if m.exact_ref_hit and not m.was_corrected:
        return AVStatus.V4

    if m.exact_ref_hit and m.was_corrected:
        return AVStatus.V3

    if m.llm_judge_confidence >= 0.8 and m.vector_distance < 0.25:
        return AVStatus.C4 if m.was_corrected else AVStatus.V3

    if m.llm_judge_confidence >= 0.6 and m.vector_distance < 0.4:
        return AVStatus.C3

    if m.has_street and m.has_zip and not m.has_primary_number:
        return AVStatus.V2

    if m.candidate_count > 1 and m.llm_judge_confidence < 0.5:
        return AVStatus.I2

    return AVStatus.I4
