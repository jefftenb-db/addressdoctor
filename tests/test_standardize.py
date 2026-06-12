from address_verify.schemas import AddressSchema
from address_verify.standardize import (
    format_delivery_line,
    format_last_line,
    standardize_address,
)


def _a(**kwargs) -> AddressSchema:
    return AddressSchema(**{"confidence": 1.0, **kwargs})


def test_suffix_street_to_st():
    out = standardize_address(_a(street_name="main", street_suffix="STREET"))
    assert out.street_suffix == "ST"


def test_suffix_avenue_variants():
    for variant in ["AVENUE", "Av", "Aven", "AVNUE"]:
        assert standardize_address(_a(street_suffix=variant)).street_suffix == "AVE"


def test_secondary_apartment_to_apt():
    out = standardize_address(_a(secondary_designator="Apartment", secondary_number="4B"))
    assert out.secondary_designator == "APT"
    assert out.secondary_number == "4B"


def test_directional_full_to_abbrev():
    out = standardize_address(_a(street_predirection="NorthWest", street_postdirection="south"))
    assert out.street_predirection == "NW"
    assert out.street_postdirection == "S"


def test_state_truncated_uppercased():
    assert standardize_address(_a(state="ca")).state == "CA"
    assert standardize_address(_a(state="California")).state == "CA"


def test_zip_extraction():
    out = standardize_address(_a(zipcode="94105-1234"))
    assert out.zipcode == "94105"


def test_zip4_extraction():
    assert standardize_address(_a(zip_plus_4="1234")).zip_plus_4 == "1234"
    assert standardize_address(_a(zip_plus_4="-1234")).zip_plus_4 == "1234"


def test_format_delivery_line_full():
    a = standardize_address(_a(
        primary_number="1600", street_predirection="N",
        street_name="Pennsylvania", street_suffix="Avenue",
        street_postdirection="NW",
    ))
    assert format_delivery_line(a) == "1600 N PENNSYLVANIA AVE NW"


def test_format_last_line_with_plus4():
    a = standardize_address(_a(city="Washington", state="DC", zipcode="20500", zip_plus_4="0003"))
    assert format_last_line(a) == "WASHINGTON, DC 20500-0003"


def test_format_last_line_zip_only():
    a = standardize_address(_a(city="Austin", state="TX", zipcode="78701"))
    assert format_last_line(a) == "AUSTIN, TX 78701"
