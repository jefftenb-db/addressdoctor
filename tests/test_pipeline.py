from address_verify.pipeline import Candidate, verify_address
from address_verify.schemas import AddressSchema
from address_verify.scoring import AVStatus


REF = AddressSchema(
    primary_number="1600",
    street_predirection="N",
    street_name="PENNSYLVANIA",
    street_suffix="AVE",
    street_postdirection="NW",
    city="WASHINGTON",
    state="DC",
    zipcode="20500",
    zip_plus_4="0003",
    confidence=1.0,
)


def _parse_clean(_raw):
    return AddressSchema(
        primary_number="1600",
        street_name="Pennsylvania",
        street_suffix="Avenue",
        street_postdirection="NW",
        city="Washington",
        state="DC",
        zipcode="20500",
        confidence=0.95,
    )


def _parse_typo(_raw):
    return AddressSchema(
        primary_number="1600",
        street_name="Pensilvania",
        street_suffix="Ave",
        city="Washington",
        state="DC",
        zipcode="20500",
        confidence=0.6,
    )


def _candidates_exact(_parsed, _k):
    return [Candidate(address=REF, vector_distance=0.0, latitude=38.8977, longitude=-77.0365, is_exact=True)]


def _candidates_fuzzy(_parsed, _k):
    return [
        Candidate(address=REF, vector_distance=0.18, latitude=38.8977, longitude=-77.0365),
        Candidate(address=REF.model_copy(update={"primary_number": "1700"}), vector_distance=0.42),
    ]


def _judge_picks_first(_std, cands):
    return (cands[0], 0.9) if cands else (None, 0.0)


def test_verify_exact_hit_returns_v4():
    result = verify_address(
        "1600 Pennsylvania Ave NW, Washington, DC 20500",
        parse_fn=_parse_clean, candidates_fn=_candidates_exact, judge_fn=_judge_picks_first,
    )
    assert result.av_status == AVStatus.V4
    assert result.match_score >= 95
    assert result.latitude == 38.8977
    assert result.delivery_line == "1600 N PENNSYLVANIA AVE NW"


def test_verify_typo_corrected_to_c4():
    result = verify_address(
        "1600 Pensilvania Ave, Washington, DC 20500",
        parse_fn=_parse_typo, candidates_fn=_candidates_fuzzy, judge_fn=_judge_picks_first,
    )
    assert result.av_status == AVStatus.C4
    assert result.was_corrected is True


def test_verify_insufficient_returns_i4():
    def parse_junk(_):
        return AddressSchema(street_name=None, state=None, zipcode=None, confidence=0.1)

    def no_candidates(_s, _k):
        return []

    def no_pick(_s, _c):
        return (None, 0.0)

    result = verify_address(
        "asdf qwerty",
        parse_fn=parse_junk, candidates_fn=no_candidates, judge_fn=no_pick,
    )
    assert result.av_status == AVStatus.I4
