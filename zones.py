"""NZ zone detection — priority: city name > postcode > province"""

PROVINCE_ZONE = {
    "canterbury": "outer_cant",
    "otago": "south_island",
    "southland": "south_island",
    "marlborough": "south_island",
    "nelson": "south_island",
    "tasman": "south_island",
    "west coast": "south_island",
    "hawke's bay": "taranaki_wgn_hb",
    "hawkes bay": "taranaki_wgn_hb",
    "manawatu": "ni_lower",
    "manawatu-whanganui": "taranaki_wgn_hb",
    "whanganui": "taranaki_wgn_hb",
    "taranaki": "taranaki_wgn_hb",
    "wellington": "ni_lower",
    "wairarapa": "ni_lower",
    "waikato": "waikato",
    "bay of plenty": "bop_gisborne",
    "gisborne": "bop_gisborne",
    "auckland": "ni_upper",
    "northland": "far_north",
}

CITY_ZONE = {
    # CHCH Local
    "christchurch": "chch_local", "chch": "chch_local",
    "rolleston": "chch_local", "lincoln": "chch_local",
    "hornby": "chch_local", "pegasus": "chch_local",
    "prebbleton": "chch_local", "halswell": "chch_local",
    "wigram": "chch_local", "burnside": "chch_local",
    # Outer Canterbury
    "rangiora": "outer_cant", "kaikoura": "outer_cant",
    "ashburton": "outer_cant", "darfield": "outer_cant",
    "oxford": "outer_cant", "amberley": "outer_cant",
    "methven": "outer_cant", "geraldine": "outer_cant",
    # Regional SI
    "timaru": "regional_si",
    # South Island
    "dunedin": "south_island", "invercargill": "south_island",
    "queenstown": "south_island", "blenheim": "south_island",
    "nelson": "south_island", "greymouth": "south_island",
    "gore": "south_island", "oamaru": "south_island",
    "hokitika": "south_island", "westport": "south_island",
    "cromwell": "south_island", "alexandra": "south_island",
    "richmond": "south_island",
    # NI Lower
    "wellington": "ni_lower", "lower hutt": "ni_lower",
    "upper hutt": "ni_lower", "porirua": "ni_lower",
    "palmerston north": "ni_lower", "masterton": "ni_lower",
    "levin": "ni_lower", "paraparaumu": "ni_lower",
    "kapiti": "ni_lower", "feilding": "ni_lower",
    "dannevirke": "ni_lower", "pahiatua": "ni_lower",
    # Waikato
    "hamilton": "waikato", "cambridge": "waikato",
    "te awamutu": "waikato", "taupo": "waikato",
    "thames": "waikato", "paeroa": "waikato",
    "huntly": "waikato", "morrinsville": "waikato",
    "matamata": "waikato", "tokoroa": "waikato",
    # BoP / Gisborne
    "tauranga": "bop_gisborne", "mount maunganui": "bop_gisborne",
    "rotorua": "bop_gisborne", "whakatane": "bop_gisborne",
    "gisborne": "bop_gisborne", "opotiki": "bop_gisborne",
    "te puke": "bop_gisborne", "katikati": "bop_gisborne",
    # Taranaki / Wanganui / HB
    "new plymouth": "taranaki_wgn_hb", "whanganui": "taranaki_wgn_hb",
    "napier": "taranaki_wgn_hb", "hastings": "taranaki_wgn_hb",
    "hawera": "taranaki_wgn_hb", "stratford": "taranaki_wgn_hb",
    "wairoa": "taranaki_wgn_hb", "waipukurau": "taranaki_wgn_hb",
    # NI Upper (Auckland)
    "auckland": "ni_upper", "manukau": "ni_upper",
    "north shore": "ni_upper", "henderson": "ni_upper",
    "waitakere": "ni_upper", "pukekohe": "ni_upper",
    "papakura": "ni_upper", "albany": "ni_upper",
    "takapuna": "ni_upper", "botany": "ni_upper",
    "henderson valley": "ni_upper", "glen innes": "ni_upper",
    # Far North
    "whangarei": "far_north", "kaitaia": "far_north",
    "kerikeri": "far_north", "paihia": "far_north",
    "dargaville": "far_north",
}

POSTCODE_RANGES = [
    (8011, 8084, "chch_local"),
    (7600, 7699, "chch_local"),
    (7608, 7618, "chch_local"),
    (7300, 7399, "outer_cant"),
    (7400, 7499, "outer_cant"),
    (7500, 7599, "outer_cant"),
    (7700, 7999, "outer_cant"),
    (7900, 7999, "regional_si"),
    (9010, 9099, "south_island"),
    (9300, 9399, "south_island"),
    (9800, 9999, "south_island"),
    (7200, 7299, "south_island"),
    (7000, 7099, "south_island"),
    (7800, 7899, "south_island"),
    (6011, 6099, "ni_lower"),
    (5010, 5099, "ni_lower"),
    (4410, 4499, "ni_lower"),
    (5510, 5599, "ni_lower"),
    (5810, 5899, "ni_lower"),
    (5030, 5049, "ni_lower"),
    (3200, 3299, "waikato"),
    (3400, 3499, "waikato"),
    (3330, 3339, "waikato"),
    (3500, 3599, "waikato"),
    (3110, 3199, "bop_gisborne"),
    (3010, 3099, "bop_gisborne"),
    (3120, 3129, "bop_gisborne"),
    (4010, 4099, "bop_gisborne"),
    (4310, 4399, "taranaki_wgn_hb"),
    (4500, 4599, "taranaki_wgn_hb"),
    (4100, 4199, "taranaki_wgn_hb"),
    (4600, 4699, "taranaki_wgn_hb"),
    (1010, 1099, "ni_upper"),
    (2000, 2199, "ni_upper"),
    (600,  699,  "ni_upper"),
    (900,  999,  "ni_upper"),
    (2120, 2129, "ni_upper"),
    (110,  499,  "far_north"),
]

STD_TO_OZ_ZONE = {
    "chch_local":      "oz_chch",
    "outer_cant":      "oz_chch",
    "regional_si":     "oz_si",
    "south_island":    "oz_si",
    "ni_lower":        "oz_ni_lower",
    "waikato":         "oz_waikato",
    "bop_gisborne":    "oz_bop_gis",
    "taranaki_wgn_hb": "oz_taranaki",
    "ni_upper":        "oz_ni_upper",
    "far_north":       "oz_far_north",
}

def detect_zone(province: str = "", city: str = "", postcode: str = "") -> str:
    city_key = city.lower().strip()
    province_key = province.lower().strip()
    if city_key in CITY_ZONE:
        return CITY_ZONE[city_key]
    if postcode:
        try:
            pc = int(postcode.strip()[:4])
            for low, high, zone in POSTCODE_RANGES:
                if low <= pc <= high:
                    return zone
        except (ValueError, TypeError):
            pass
    for prov_key, zone in PROVINCE_ZONE.items():
        if prov_key in province_key or province_key in prov_key:
            return zone
    return "ni_upper"

def get_oversized_zone(std_zone: str) -> str:
    return STD_TO_OZ_ZONE.get(std_zone, "oz_ni_upper")
