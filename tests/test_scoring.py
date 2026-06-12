from address_verify.scoring import (
    AVStatus,
    MatchInputs,
    compute_av_status,
    compute_match_score,
)


def _m(**kwargs) -> MatchInputs:
    base = dict(
        exact_ref_hit=False,
        vector_distance=1.0,
        llm_judge_confidence=0.0,
        was_corrected=False,
        has_primary_number=True,
        has_street=True,
        has_city=True,
        has_state=True,
        has_zip=True,
        candidate_count=1,
    )
    base.update(kwargs)
    return MatchInputs(**base)


def test_v4_exact_no_correction():
    status = compute_av_status(_m(exact_ref_hit=True, vector_distance=0.0, llm_judge_confidence=1.0))
    assert status == AVStatus.V4


def test_v3_exact_with_standardization():
    status = compute_av_status(_m(
        exact_ref_hit=True, vector_distance=0.0, llm_judge_confidence=1.0, was_corrected=True,
    ))
    assert status == AVStatus.V3


def test_c4_high_confidence_correction():
    status = compute_av_status(_m(
        vector_distance=0.15, llm_judge_confidence=0.9, was_corrected=True,
    ))
    assert status == AVStatus.C4


def test_c3_medium_confidence_correction():
    status = compute_av_status(_m(
        vector_distance=0.35, llm_judge_confidence=0.65, was_corrected=True,
    ))
    assert status == AVStatus.C3


def test_v2_missing_primary_number():
    status = compute_av_status(_m(
        has_primary_number=False, vector_distance=0.5, llm_judge_confidence=0.4,
    ))
    assert status == AVStatus.V2


def test_i2_ambiguous():
    status = compute_av_status(_m(
        candidate_count=3, vector_distance=0.6, llm_judge_confidence=0.3,
    ))
    assert status == AVStatus.I2


def test_i4_insufficient():
    status = compute_av_status(_m(has_state=False, has_zip=False))
    assert status == AVStatus.I4


def test_match_score_exact_hit_high():
    score = compute_match_score(_m(
        exact_ref_hit=True, vector_distance=0.0, llm_judge_confidence=1.0,
    ))
    assert score >= 95


def test_match_score_bad_match_low():
    score = compute_match_score(_m(
        vector_distance=1.8, llm_judge_confidence=0.1,
        has_city=False, has_primary_number=False,
    ))
    assert score < 40
