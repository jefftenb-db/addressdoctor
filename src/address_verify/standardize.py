"""USPS Publication 28 address standardization.

Deterministic rules only — no LLM. Applied *after* the LLM parse step so the
final output conforms to USPS canonical abbreviations regardless of how the
input was phrased.
"""

from __future__ import annotations

import re
from typing import Optional

from .schemas import AddressSchema


# USPS Pub 28, Appendix C1 — standard street suffix abbreviations.
# Key is any accepted variant (uppercase); value is the USPS standard.
STREET_SUFFIX: dict[str, str] = {
    "ALLEY": "ALY", "ALLEE": "ALY", "ALLY": "ALY", "ALY": "ALY",
    "ANEX": "ANX", "ANNEX": "ANX", "ANNX": "ANX", "ANX": "ANX",
    "ARCADE": "ARC", "ARC": "ARC",
    "AVENUE": "AVE", "AV": "AVE", "AVEN": "AVE", "AVENU": "AVE",
    "AVN": "AVE", "AVNUE": "AVE", "AVE": "AVE",
    "BAYOU": "BYU", "BAYOO": "BYU", "BYU": "BYU",
    "BEACH": "BCH", "BCH": "BCH",
    "BEND": "BND", "BND": "BND",
    "BLUFF": "BLF", "BLUF": "BLF", "BLF": "BLF",
    "BLUFFS": "BLFS", "BLFS": "BLFS",
    "BOTTOM": "BTM", "BOT": "BTM", "BOTTM": "BTM", "BTM": "BTM",
    "BOULEVARD": "BLVD", "BOUL": "BLVD", "BOULV": "BLVD", "BLVD": "BLVD",
    "BRANCH": "BR", "BRNCH": "BR", "BR": "BR",
    "BRIDGE": "BRG", "BRDGE": "BRG", "BRG": "BRG",
    "BROOK": "BRK", "BRK": "BRK",
    "BROOKS": "BRKS", "BRKS": "BRKS",
    "BURG": "BG", "BG": "BG",
    "BURGS": "BGS", "BGS": "BGS",
    "BYPASS": "BYP", "BYPA": "BYP", "BYPAS": "BYP", "BYPS": "BYP", "BYP": "BYP",
    "CAMP": "CP", "CMP": "CP", "CP": "CP",
    "CANYON": "CYN", "CANYN": "CYN", "CNYN": "CYN", "CYN": "CYN",
    "CAPE": "CPE", "CPE": "CPE",
    "CAUSEWAY": "CSWY", "CAUSWAY": "CSWY", "CSWY": "CSWY",
    "CENTER": "CTR", "CEN": "CTR", "CENT": "CTR", "CENTR": "CTR",
    "CENTRE": "CTR", "CNTER": "CTR", "CNTR": "CTR", "CTR": "CTR",
    "CENTERS": "CTRS", "CTRS": "CTRS",
    "CIRCLE": "CIR", "CIRC": "CIR", "CIRCL": "CIR", "CRCL": "CIR",
    "CRCLE": "CIR", "CIR": "CIR",
    "CIRCLES": "CIRS", "CIRS": "CIRS",
    "CLIFF": "CLF", "CLF": "CLF",
    "CLIFFS": "CLFS", "CLFS": "CLFS",
    "CLUB": "CLB", "CLB": "CLB",
    "COMMON": "CMN", "CMN": "CMN",
    "CORNER": "COR", "COR": "COR",
    "CORNERS": "CORS", "CORS": "CORS",
    "COURSE": "CRSE", "CRSE": "CRSE",
    "COURT": "CT", "CT": "CT",
    "COURTS": "CTS", "CTS": "CTS",
    "COVE": "CV", "CV": "CV",
    "COVES": "CVS", "CVS": "CVS",
    "CREEK": "CRK", "CRK": "CRK",
    "CRESCENT": "CRES", "CRSENT": "CRES", "CRSNT": "CRES", "CRES": "CRES",
    "CREST": "CRST", "CRST": "CRST",
    "CROSSING": "XING", "CRSSNG": "XING", "XING": "XING",
    "CROSSROAD": "XRD", "XRD": "XRD",
    "CURVE": "CURV", "CURV": "CURV",
    "DALE": "DL", "DL": "DL",
    "DAM": "DM", "DM": "DM",
    "DIVIDE": "DV", "DIV": "DV", "DVD": "DV", "DV": "DV",
    "DRIVE": "DR", "DRIV": "DR", "DRV": "DR", "DR": "DR",
    "DRIVES": "DRS", "DRS": "DRS",
    "ESTATE": "EST", "EST": "EST",
    "ESTATES": "ESTS", "ESTS": "ESTS",
    "EXPRESSWAY": "EXPY", "EXP": "EXPY", "EXPR": "EXPY",
    "EXPRESS": "EXPY", "EXPW": "EXPY", "EXPY": "EXPY",
    "EXTENSION": "EXT", "EXTN": "EXT", "EXTNSN": "EXT", "EXT": "EXT",
    "EXTENSIONS": "EXTS", "EXTS": "EXTS",
    "FALL": "FALL",
    "FALLS": "FLS", "FLS": "FLS",
    "FERRY": "FRY", "FRRY": "FRY", "FRY": "FRY",
    "FIELD": "FLD", "FLD": "FLD",
    "FIELDS": "FLDS", "FLDS": "FLDS",
    "FLAT": "FLT", "FLT": "FLT",
    "FLATS": "FLTS", "FLTS": "FLTS",
    "FORD": "FRD", "FRD": "FRD",
    "FORDS": "FRDS", "FRDS": "FRDS",
    "FOREST": "FRST", "FORESTS": "FRST", "FRST": "FRST",
    "FORGE": "FRG", "FORG": "FRG", "FRG": "FRG",
    "FORGES": "FRGS", "FRGS": "FRGS",
    "FORK": "FRK", "FRK": "FRK",
    "FORKS": "FRKS", "FRKS": "FRKS",
    "FORT": "FT", "FRT": "FT", "FT": "FT",
    "FREEWAY": "FWY", "FREEWY": "FWY", "FRWAY": "FWY",
    "FRWY": "FWY", "FWY": "FWY",
    "GARDEN": "GDN", "GARDN": "GDN", "GRDEN": "GDN", "GRDN": "GDN", "GDN": "GDN",
    "GARDENS": "GDNS", "GRDNS": "GDNS", "GDNS": "GDNS",
    "GATEWAY": "GTWY", "GATEWY": "GTWY", "GATWAY": "GTWY",
    "GTWAY": "GTWY", "GTWY": "GTWY",
    "GLEN": "GLN", "GLN": "GLN",
    "GLENS": "GLNS", "GLNS": "GLNS",
    "GREEN": "GRN", "GRN": "GRN",
    "GREENS": "GRNS", "GRNS": "GRNS",
    "GROVE": "GRV", "GROV": "GRV", "GRV": "GRV",
    "GROVES": "GRVS", "GRVS": "GRVS",
    "HARBOR": "HBR", "HARB": "HBR", "HARBR": "HBR", "HRBOR": "HBR", "HBR": "HBR",
    "HARBORS": "HBRS", "HBRS": "HBRS",
    "HAVEN": "HVN", "HVN": "HVN",
    "HEIGHTS": "HTS", "HT": "HTS", "HTS": "HTS",
    "HIGHWAY": "HWY", "HIGHWY": "HWY", "HIWAY": "HWY",
    "HIWY": "HWY", "HWAY": "HWY", "HWY": "HWY",
    "HILL": "HL", "HL": "HL",
    "HILLS": "HLS", "HLS": "HLS",
    "HOLLOW": "HOLW", "HLLW": "HOLW", "HOLLOWS": "HOLW",
    "HOLWS": "HOLW", "HOLW": "HOLW",
    "INLET": "INLT", "INLT": "INLT",
    "ISLAND": "IS", "ISLND": "IS", "IS": "IS",
    "ISLANDS": "ISS", "ISLNDS": "ISS", "ISS": "ISS",
    "ISLE": "ISLE", "ISLES": "ISLE",
    "JUNCTION": "JCT", "JCTION": "JCT", "JCTN": "JCT",
    "JUNCTN": "JCT", "JUNCTON": "JCT", "JCT": "JCT",
    "JUNCTIONS": "JCTS", "JCTNS": "JCTS", "JCTS": "JCTS",
    "KEY": "KY", "KY": "KY",
    "KEYS": "KYS", "KYS": "KYS",
    "KNOLL": "KNL", "KNOL": "KNL", "KNL": "KNL",
    "KNOLLS": "KNLS", "KNLS": "KNLS",
    "LAKE": "LK", "LK": "LK",
    "LAKES": "LKS", "LKS": "LKS",
    "LAND": "LAND",
    "LANDING": "LNDG", "LNDNG": "LNDG", "LNDG": "LNDG",
    "LANE": "LN", "LANES": "LN", "LN": "LN",
    "LIGHT": "LGT", "LGT": "LGT",
    "LIGHTS": "LGTS", "LGTS": "LGTS",
    "LOAF": "LF", "LF": "LF",
    "LOCK": "LCK", "LCK": "LCK",
    "LOCKS": "LCKS", "LCKS": "LCKS",
    "LODGE": "LDG", "LDGE": "LDG", "LODG": "LDG", "LDG": "LDG",
    "LOOP": "LOOP", "LOOPS": "LOOP",
    "MALL": "MALL",
    "MANOR": "MNR", "MNR": "MNR",
    "MANORS": "MNRS", "MNRS": "MNRS",
    "MEADOW": "MDW",
    "MEADOWS": "MDWS", "MDW": "MDWS", "MEDOWS": "MDWS", "MDWS": "MDWS",
    "MEWS": "MEWS",
    "MILL": "ML", "ML": "ML",
    "MILLS": "MLS", "MLS": "MLS",
    "MISSION": "MSN", "MISSN": "MSN", "MSSN": "MSN", "MSN": "MSN",
    "MOTORWAY": "MTWY", "MTWY": "MTWY",
    "MOUNT": "MT", "MNT": "MT", "MT": "MT",
    "MOUNTAIN": "MTN", "MNTAIN": "MTN", "MNTN": "MTN",
    "MOUNTIN": "MTN", "MTIN": "MTN", "MTN": "MTN",
    "MOUNTAINS": "MTNS", "MNTNS": "MTNS", "MTNS": "MTNS",
    "NECK": "NCK", "NCK": "NCK",
    "ORCHARD": "ORCH", "ORCHRD": "ORCH", "ORCH": "ORCH",
    "OVAL": "OVAL", "OVL": "OVAL",
    "OVERPASS": "OPAS", "OPAS": "OPAS",
    "PARK": "PARK", "PRK": "PARK", "PARKS": "PARK",
    "PARKWAY": "PKWY", "PARKWY": "PKWY", "PKWAY": "PKWY",
    "PKY": "PKWY", "PKWY": "PKWY",
    "PARKWAYS": "PKWY", "PKWYS": "PKWY",
    "PASS": "PASS",
    "PASSAGE": "PSGE", "PSGE": "PSGE",
    "PATH": "PATH", "PATHS": "PATH",
    "PIKE": "PIKE", "PIKES": "PIKE",
    "PINE": "PNE", "PNE": "PNE",
    "PINES": "PNES", "PNES": "PNES",
    "PLACE": "PL", "PL": "PL",
    "PLAIN": "PLN", "PLN": "PLN",
    "PLAINS": "PLNS", "PLNS": "PLNS",
    "PLAZA": "PLZ", "PLZA": "PLZ", "PLZ": "PLZ",
    "POINT": "PT", "PT": "PT",
    "POINTS": "PTS", "PTS": "PTS",
    "PORT": "PRT", "PRT": "PRT",
    "PORTS": "PRTS", "PRTS": "PRTS",
    "PRAIRIE": "PR", "PRR": "PR", "PR": "PR",
    "RADIAL": "RADL", "RAD": "RADL", "RADIEL": "RADL", "RADL": "RADL",
    "RAMP": "RAMP",
    "RANCH": "RNCH", "RANCHES": "RNCH", "RNCHS": "RNCH", "RNCH": "RNCH",
    "RAPID": "RPD", "RPD": "RPD",
    "RAPIDS": "RPDS", "RPDS": "RPDS",
    "REST": "RST", "RST": "RST",
    "RIDGE": "RDG", "RDGE": "RDG", "RDG": "RDG",
    "RIDGES": "RDGS", "RDGS": "RDGS",
    "RIVER": "RIV", "RVR": "RIV", "RIVR": "RIV", "RIV": "RIV",
    "ROAD": "RD", "RD": "RD",
    "ROADS": "RDS", "RDS": "RDS",
    "ROUTE": "RTE", "RTE": "RTE",
    "ROW": "ROW",
    "RUE": "RUE",
    "RUN": "RUN",
    "SHOAL": "SHL", "SHL": "SHL",
    "SHOALS": "SHLS", "SHLS": "SHLS",
    "SHORE": "SHR", "SHOAR": "SHR", "SHR": "SHR",
    "SHORES": "SHRS", "SHOARS": "SHRS", "SHRS": "SHRS",
    "SKYWAY": "SKWY", "SKWY": "SKWY",
    "SPRING": "SPG", "SPNG": "SPG", "SPRNG": "SPG", "SPG": "SPG",
    "SPRINGS": "SPGS", "SPNGS": "SPGS", "SPRNGS": "SPGS", "SPGS": "SPGS",
    "SPUR": "SPUR", "SPURS": "SPUR",
    "SQUARE": "SQ", "SQR": "SQ", "SQRE": "SQ", "SQU": "SQ", "SQ": "SQ",
    "SQUARES": "SQS", "SQRS": "SQS", "SQS": "SQS",
    "STATION": "STA", "STATN": "STA", "STN": "STA", "STA": "STA",
    "STRAVENUE": "STRA", "STRAV": "STRA", "STRAVN": "STRA",
    "STRVN": "STRA", "STRVNUE": "STRA", "STRA": "STRA",
    "STREAM": "STRM", "STREME": "STRM", "STRM": "STRM",
    "STREET": "ST", "STRT": "ST", "STR": "ST", "ST": "ST",
    "STREETS": "STS", "STS": "STS",
    "SUMMIT": "SMT", "SUMIT": "SMT", "SUMITT": "SMT", "SMT": "SMT",
    "TERRACE": "TER", "TERR": "TER", "TER": "TER",
    "THROUGHWAY": "TRWY", "TRWY": "TRWY",
    "TRACE": "TRCE", "TRACES": "TRCE", "TRCE": "TRCE",
    "TRACK": "TRAK", "TRACKS": "TRAK", "TRK": "TRAK",
    "TRKS": "TRAK", "TRAK": "TRAK",
    "TRAFFICWAY": "TRFY", "TRFY": "TRFY",
    "TRAIL": "TRL", "TRAILS": "TRL", "TRLS": "TRL", "TRL": "TRL",
    "TRAILER": "TRLR", "TRLRS": "TRLR", "TRLR": "TRLR",
    "TUNNEL": "TUNL", "TUNEL": "TUNL", "TUNLS": "TUNL",
    "TUNNL": "TUNL", "TUNNELS": "TUNL", "TUNL": "TUNL",
    "TURNPIKE": "TPKE", "TRNPK": "TPKE", "TURNPK": "TPKE", "TPKE": "TPKE",
    "UNDERPASS": "UPAS", "UPAS": "UPAS",
    "UNION": "UN", "UN": "UN",
    "UNIONS": "UNS", "UNS": "UNS",
    "VALLEY": "VLY", "VALLY": "VLY", "VLLY": "VLY", "VLY": "VLY",
    "VALLEYS": "VLYS", "VLYS": "VLYS",
    "VIADUCT": "VIA", "VDCT": "VIA", "VIADCT": "VIA", "VIA": "VIA",
    "VIEW": "VW", "VW": "VW",
    "VIEWS": "VWS", "VWS": "VWS",
    "VILLAGE": "VLG", "VILL": "VLG", "VILLAG": "VLG",
    "VILLG": "VLG", "VILLIAGE": "VLG", "VLG": "VLG",
    "VILLAGES": "VLGS", "VLGS": "VLGS",
    "VILLE": "VL", "VL": "VL",
    "VISTA": "VIS", "VIST": "VIS", "VST": "VIS", "VSTA": "VIS", "VIS": "VIS",
    "WALK": "WALK", "WALKS": "WALK",
    "WALL": "WALL",
    "WAY": "WAY", "WY": "WAY",
    "WAYS": "WAYS",
    "WELL": "WL", "WL": "WL",
    "WELLS": "WLS", "WLS": "WLS",
}

# USPS Pub 28, Appendix C2 — secondary unit designators.
SECONDARY_DESIGNATOR: dict[str, str] = {
    "APARTMENT": "APT", "APT": "APT",
    "BASEMENT": "BSMT", "BSMT": "BSMT",
    "BUILDING": "BLDG", "BLDG": "BLDG",
    "DEPARTMENT": "DEPT", "DEPT": "DEPT",
    "FLOOR": "FL", "FL": "FL",
    "FRONT": "FRNT", "FRNT": "FRNT",
    "HANGAR": "HNGR", "HNGR": "HNGR",
    "LOBBY": "LBBY", "LBBY": "LBBY",
    "LOT": "LOT",
    "LOWER": "LOWR", "LOWR": "LOWR",
    "OFFICE": "OFC", "OFC": "OFC",
    "PENTHOUSE": "PH", "PH": "PH",
    "PIER": "PIER",
    "REAR": "REAR",
    "ROOM": "RM", "RM": "RM",
    "SIDE": "SIDE",
    "SLIP": "SLIP",
    "SPACE": "SPC", "SPC": "SPC",
    "STOP": "STOP",
    "SUITE": "STE", "STE": "STE",
    "TRAILER": "TRLR", "TRLR": "TRLR",
    "UNIT": "UNIT",
    "UPPER": "UPPR", "UPPR": "UPPR",
    "#": "#",
}

DIRECTIONAL: dict[str, str] = {
    "NORTH": "N", "N": "N",
    "SOUTH": "S", "S": "S",
    "EAST": "E", "E": "E",
    "WEST": "W", "W": "W",
    "NORTHEAST": "NE", "NE": "NE",
    "NORTHWEST": "NW", "NW": "NW",
    "SOUTHEAST": "SE", "SE": "SE",
    "SOUTHWEST": "SW", "SW": "SW",
}


def _std(value: Optional[str], table: dict[str, str]) -> Optional[str]:
    if value is None:
        return None
    key = re.sub(r"[^A-Z#]", "", value.upper())
    return table.get(key, value.strip().upper())


def _std_zip(zipcode: Optional[str]) -> Optional[str]:
    if zipcode is None:
        return None
    digits = re.sub(r"\D", "", zipcode)
    return digits[:5] if len(digits) >= 5 else None


def _std_zip4(zip_plus_4: Optional[str]) -> Optional[str]:
    if zip_plus_4 is None:
        return None
    digits = re.sub(r"\D", "", zip_plus_4)
    return digits[-4:] if len(digits) >= 4 else None


def standardize_address(parsed: AddressSchema) -> AddressSchema:
    """Apply USPS Pub 28 abbreviations and formatting to a parsed address."""
    return parsed.model_copy(
        update={
            "street_predirection": _std(parsed.street_predirection, DIRECTIONAL),
            "street_suffix": _std(parsed.street_suffix, STREET_SUFFIX),
            "street_postdirection": _std(parsed.street_postdirection, DIRECTIONAL),
            "secondary_designator": _std(parsed.secondary_designator, SECONDARY_DESIGNATOR),
            "state": (parsed.state or "").strip().upper()[:2] or None,
            "city": (parsed.city or "").strip().upper() or None,
            "street_name": (parsed.street_name or "").strip().upper() or None,
            "zipcode": _std_zip(parsed.zipcode),
            "zip_plus_4": _std_zip4(parsed.zip_plus_4),
        }
    )


def format_delivery_line(a: AddressSchema) -> str:
    """Render the canonical USPS delivery line (line 1)."""
    parts = [
        a.primary_number,
        a.street_predirection,
        a.street_name,
        a.street_suffix,
        a.street_postdirection,
        a.secondary_designator,
        a.secondary_number,
    ]
    return " ".join(p for p in parts if p)


def format_last_line(a: AddressSchema) -> str:
    """Render the canonical USPS last line (city, state, ZIP[+4])."""
    zip_full = a.zipcode or ""
    if a.zip_plus_4:
        zip_full = f"{zip_full}-{a.zip_plus_4}"
    left = ", ".join(p for p in [a.city, a.state] if p)
    return f"{left} {zip_full}".strip()
