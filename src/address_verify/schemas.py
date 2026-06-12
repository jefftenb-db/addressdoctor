"""Address schemas.

`AddressSchema` is the canonical structured representation of a US address.
`ADDRESS_JSON_SCHEMA` is the JSON Schema passed to Databricks `ai_query` via
`responseFormat` so the LLM returns strict, parseable output.
"""

from __future__ import annotations

from typing import Optional
from pydantic import BaseModel, Field


class AddressSchema(BaseModel):
    primary_number: Optional[str] = Field(
        None, description="House or building number, e.g. '1600'"
    )
    street_predirection: Optional[str] = Field(
        None, description="Directional before the street name, e.g. 'N', 'SW'"
    )
    street_name: Optional[str] = Field(
        None, description="Street name without directionals or suffix, e.g. 'Pennsylvania'"
    )
    street_suffix: Optional[str] = Field(
        None, description="USPS Pub 28 street suffix abbreviation, e.g. 'AVE', 'ST', 'BLVD'"
    )
    street_postdirection: Optional[str] = Field(
        None, description="Directional after the street name, e.g. 'NW', 'SE'"
    )
    secondary_designator: Optional[str] = Field(
        None, description="Unit type per Pub 28, e.g. 'APT', 'STE', 'UNIT', 'FL'"
    )
    secondary_number: Optional[str] = Field(
        None, description="Unit number, e.g. '4B', '200'"
    )
    city: Optional[str] = Field(None, description="City / locality")
    state: Optional[str] = Field(None, description="Two-letter USPS state code")
    zipcode: Optional[str] = Field(None, description="5-digit ZIP code")
    zip_plus_4: Optional[str] = Field(None, description="ZIP+4 extension, 4 digits")
    confidence: float = Field(
        0.0,
        ge=0.0,
        le=1.0,
        description="Model self-reported parse confidence, 0.0 to 1.0",
    )


ADDRESS_JSON_SCHEMA: dict = {
    "type": "json_schema",
    "json_schema": {
        "name": "address",
        "schema": {
            "type": "object",
            "properties": {
                "primary_number": {"type": ["string", "null"]},
                "street_predirection": {"type": ["string", "null"]},
                "street_name": {"type": ["string", "null"]},
                "street_suffix": {"type": ["string", "null"]},
                "street_postdirection": {"type": ["string", "null"]},
                "secondary_designator": {"type": ["string", "null"]},
                "secondary_number": {"type": ["string", "null"]},
                "city": {"type": ["string", "null"]},
                "state": {"type": ["string", "null"]},
                "zipcode": {"type": ["string", "null"]},
                "zip_plus_4": {"type": ["string", "null"]},
                "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            },
            "required": [
                "primary_number",
                "street_name",
                "city",
                "state",
                "zipcode",
                "confidence",
            ],
            "additionalProperties": False,
        },
        "strict": True,
    },
}


PARSE_PROMPT = """You are a US postal address parser. Given a free-text address, extract its components
into the provided schema. Use USPS Publication 28 abbreviations for street suffix and
secondary designator (e.g. STREET -> ST, APARTMENT -> APT, SUITE -> STE). State must be
the two-letter USPS code. If a field is missing in the input, return null. Report a
calibrated `confidence` between 0 and 1 reflecting how certain you are of the overall
parse. Return only the JSON object."""
