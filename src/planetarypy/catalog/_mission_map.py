"""Mission/instrument mapping for pdr-tests folder names.

Maps folder names like 'cassini_iss', 'diviner', 'gal_ssi' to
(mission, instrument) tuples. Uses a three-tier strategy:
1. Manual map for known ambiguous cases
2. Auto-split on first underscore
3. Ambiguous flag for anything unresolved

For folders that bundle multiple instruments (previously mapped to 'multi'),
MULTI_INSTRUMENT_SPLIT defines how to extract instrument names from product
key prefixes. During ingestion, these folders get split into separate
instrument entries with synthetic folder names like 'mro__ctx'.
"""

# Manual mapping for folder names that don't split cleanly on underscore,
# or where the first segment is an abbreviation that needs expansion.
MANUAL_MISSION_MAP: dict[str, tuple[str, str]] = {
    # Single-word names (instrument-only, mission is implicit)
    "crism": ("mro", "crism"),
    "diviner": ("lro", "diviner"),
    "lroc": ("lro", "lroc"),
    "sharad": ("mro", "sharad"),
    "themis": ("mars_odyssey", "themis"),
    # Multi-instrument bundled definitions (split by MULTI_INSTRUMENT_SPLIT)
    "mro": ("mro", "_split"),
    # Apollo
    "apollo": ("apollo", "_split"),
    # Dawn
    "dawn": ("dawn", "_split"),
    "dawn_certified": ("dawn", "certified"),
    # Deep Impact
    "di_ground": ("deep_impact", "ground"),
    "di_nav": ("deep_impact", "nav"),
    "di_science": ("deep_impact", "science"),
    # Deep Space 1
    "ds1": ("deep_space_1", "_split"),
    # EPOXI (Deep Impact extended mission)
    "epoxi": ("epoxi", "_split"),
    # ESA
    "esa_smart": ("esa", "smart"),
    # Galileo (gal_ prefix)
    "gal_mag": ("galileo", "mag"),
    "gal_misc": ("galileo", "misc"),
    "gal_nims": ("galileo", "nims"),
    "gal_particles": ("galileo", "particles"),
    "gal_plasma": ("galileo", "plasma"),
    "gal_probe": ("galileo", "probe"),
    "gal_ssi": ("galileo", "ssi"),
    # Giotto
    "giotto": ("giotto", "_split"),
    # GRAIL
    "grail": ("grail", "_split"),
    # Ground-based observations
    "ground_based": ("ground_based", "_split"),
    "grsfe": ("grsfe", "_split"),
    # Hayabusa
    "hayabusa": ("hayabusa", "_split"),
    # Hubble
    "hst": ("hst", "_split"),
    # ICE (International Cometary Explorer)
    "ice": ("ice", "_split"),
    # International Halley Watch
    "ihw": ("ihw", "_split"),
    "ihw_irsn": ("ihw", "irsn"),
    "ihw_ppn": ("ihw", "ppn"),
    "ihw_rss": ("ihw", "rss"),
    # IUE (International Ultraviolet Explorer)
    "iue": ("iue", "_split"),
    # Kaguya (SELENE)
    "kaguya": ("kaguya", "_split"),
    "kaguya_jaxa": ("kaguya", "jaxa"),
    # LASP Mars data
    "lasp_mars": ("lasp", "mars"),
    # LOIRP (Lunar Orbiter Image Recovery Project)
    "loirp": ("loirp", "_split"),
    # Lunar observations
    "lunar_earth_based": ("lunar", "earth_based"),
    "lunar_prospector": ("lunar_prospector", "_split"),
    # Clementine
    "clem_GEO": ("clementine", "geo"),
    # Chandrayaan
    "ch1": ("chandrayaan_1", "_split"),
    "ch1_esa": ("chandrayaan_1", "esa"),
    "ch2_isro": ("chandrayaan_2", "isro"),
    "ch3_isro": ("chandrayaan_3", "isro"),
    # Chang'e
    "change1": ("change", "1"),
    "change2": ("change", "2"),
    "change3": ("change", "3"),
    "change4": ("change", "4"),
    "change5": ("change", "5"),
    "change6": ("change", "6"),
    # Magellan (mgn_ prefix)
    "mgn_altimetry": ("magellan", "altimetry"),
    "mgn_bsr": ("magellan", "bsr"),
    "mgn_gravity": ("magellan", "gravity"),
    "mgn_gxdr": ("magellan", "gxdr"),
    "mgn_image": ("magellan", "image"),
    "mgn_occult": ("magellan", "occult"),
    "mgn_post_mission": ("magellan", "post_mission"),
    "mgn_tracking": ("magellan", "tracking"),
    # MGS (Mars Global Surveyor)
    "mgs_mag_er": ("mgs", "mag_er"),
    "mgs_moc": ("mgs", "moc"),
    "mgs_mola": ("mgs", "mola"),
    "mgs_rss_eds": ("mgs", "rss_eds"),
    "mgs_rss_raw": ("mgs", "rss_raw"),
    "mgs_rss_sdp": ("mgs", "rss_sdp"),
    "mgs_rss_sun": ("mgs", "rss_sun"),
    "mgs_sampler": ("mgs", "sampler"),
    "mgs_tes": ("mgs", "tes"),
    "mgs_tes_GEO": ("mgs", "tes_geo"),
    # Mariner
    "mariner": ("mariner", "_split"),
    # Mars
    "mars_model": ("mars", "model"),
    "mars_odyssey": ("mars_odyssey", "_split"),
    "mars_pathfinder": ("mars_pathfinder", "_split"),
    # MRO RSS (separate from main mro folder)
    "mro_rss": ("mro", "rss"),
    # MSX
    "msx": ("msx", "_split"),
    # NEAR (Near Earth Asteroid Rendezvous)
    "near_mag": ("near", "mag"),
    "near_msi": ("near", "msi"),
    "near_nlr": ("near", "nlr"),
    "near_rss": ("near", "rss"),
    "near_spectrometers": ("near", "spectrometers"),
    # New Horizons (nh_ prefix)
    "nh_alice": ("new_horizons", "alice"),
    "nh_derived": ("new_horizons", "derived"),
    "nh_leisa": ("new_horizons", "leisa"),
    "nh_lorri": ("new_horizons", "lorri"),
    "nh_mvic": ("new_horizons", "mvic"),
    "nh_pepssi": ("new_horizons", "pepssi"),
    "nh_rex": ("new_horizons", "rex"),
    "nh_sdc": ("new_horizons", "sdc"),
    "nh_swap": ("new_horizons", "swap"),
    # Phoenix
    "phoenix": ("phoenix", "_split"),
    "phoenix_atm": ("phoenix", "atm"),
    "phoenix_tega": ("phoenix", "tega"),
    # Pioneer
    "pioneer": ("pioneer", "_split"),
    # Pre-Magellan Venus data
    "pre_magellan": ("pre_magellan", "_split"),
    # Pioneer Venus Orbiter
    "pvo": ("pvo", "_split"),
    "pvo_omag": ("pvo", "omag"),
    # Sakigake
    "sakigake": ("sakigake", "_split"),
    # Saturn ring plane crossing
    "saturn_rpx": ("saturn", "rpx"),
    # Shoemaker-Levy 9
    "sl9_jupiter_impact": ("sl9", "jupiter_impact"),
    # SOHO
    "soho": ("soho", "_split"),
    # Stardust
    "stardust": ("stardust", "_split"),
    "stardust_next": ("stardust", "next"),
    # Suisei
    "suisei": ("suisei", "_split"),
    # Ulysses
    "ulysses": ("ulysses", "_split"),
    # Venus Climate Orbiter (Akatsuki)
    "vco": ("vco", "_split"),
    # Vega
    "vega": ("vega", "_split"),
    # Venera
    "venera": ("venera", "_split"),
    # Venus radar
    "venus_radar": ("venus", "radar"),
    # Venus Express (vex_ prefix)
    "vex": ("venus_express", "_split"),
    "vex_aspera": ("venus_express", "aspera"),
    "vex_mag": ("venus_express", "mag"),
    "vex_spicav": ("venus_express", "spicav"),
    "vex_spicav_soir": ("venus_express", "spicav_soir"),
    "vex_vera": ("venus_express", "vera"),
    "vex_virtis": ("venus_express", "virtis"),
    "vex_vmc": ("venus_express", "vmc"),
    # Voyager (vg_ prefix)
    "vg_crs": ("voyager", "crs"),
    "vg_iss": ("voyager", "iss"),
    "vg_lecp": ("voyager", "lecp"),
    "vg_mag": ("voyager", "mag"),
    "vg_pls": ("voyager", "pls"),
    "vg_pos": ("voyager", "pos"),
    "vg_pra": ("voyager", "pra"),
    "vg_pws": ("voyager", "pws"),
    "vg_ring_profiles": ("voyager", "ring_profiles"),
    "vg_rss": ("voyager", "rss"),
    "vg_spectrometers": ("voyager", "spectrometers"),
    # Viking
    "viking": ("viking", "_split"),
    # Wallops Flight Facility atmospheric data
    "wff_atm": ("wff", "atm"),
    # BepiColombo
    "bepi_colombo": ("bepi_colombo", "_split"),
}


# Maps folder_name -> {product_key_prefix: instrument_name}.
# Product keys matching a prefix get that instrument assigned, with the prefix
# stripped from the product_key. Keys not matching any prefix go into a
# "_misc" fallback instrument.
MULTI_INSTRUMENT_SPLIT: dict[str, dict[str, str]] = {
    "mro": {
        "ctx": "ctx",
        "hirise": "hirise",
        "marci": "marci",
        "mcs": "mcs",
    },
    "phoenix": {
        "afm": "afm",
        "elec": "elec",
        "om": "om",
        "rac": "rac",
        "ssi": "ssi",
        "tecp": "tecp",
        "wcl": "wcl",
    },
    "dawn": {
        "fc": "fc",
        "vir": "vir",
    },
    "bepi_colombo": {
        "cam1": "cam1",
        "cam2": "cam2",
        "mag": "mag",
    },
    "epoxi": {
        "hrii": "hrii",
        "hriv": "hriv",
        "mri": "mri",
    },
    "ds1": {
        "micas": "micas",
        "ids": "ids",
        "pepe": "pepe",
    },
    "grail": {
        "lgrs": "lgrs",
        "rss": "rss",
    },
    "mars_pathfinder": {
        "apxs": "apxs",
        "imp": "imp",
        "rvrcam": "rvrcam",
        "eng": "eng",
        "rss": "rss",
        "3d": "3d",
    },
    "mars_odyssey": {
        "edr": "grs",
        "marie": "marie",
    },
    "apollo": {
        "A12": "sws",
        "A14_15": "ccig",
        "A15_16_SMSS": "smss",
        "A15_16_XRFS": "xrfs",
        "A15_SWS": "sws",
        "A17": "tg",
    },
    "ch1": {
        "M3": "m3",
    },
    "giotto": {
        "hmc": "hmc",
        "ims": "ims",
        "jpa": "jpa",
        "mag": "mag",
        "nms": "nms",
        "ope": "ope",
        "pia": "pia",
        "did": "did",
        "gre": "gre",
    },
    "hayabusa": {
        "amica": "amica",
        "lidar": "lidar",
        "nirs": "nirs",
    },
    "ice": {
        "epas": "epas",
        "ici": "ici",
        "mag": "mag",
        "plawav": "plawav",
        "radwav": "radwav",
        "swp": "swp",
        "uleca": "uleca",
    },
    "loirp": {
        "lo": "lo",
    },
    "lunar_prospector": {
        "er": "er",
        "mag": "mag",
        "los": "grs",
    },
    "mariner": {
        "iris": "iris",
        "iss": "iss",
        "mag": "mag",
        "pls": "pls",
    },
    "pvo": {
        "oefd": "oefd",
        "oetp": "oetp",
        "oims": "oims",
        "onms": "onms",
        "orpa": "orpa",
        "orse": "orse",
        "pos": "pos",
    },
    "stardust": {
        "cida": "cida",
        "dfmi": "dfmi",
        "dse": "dse",
        "keck": "keck",
        "nav": "navcam",
        "src": "src",
    },
    "ulysses": {
        "cospin": "cospin",
        "dds": "dds",
        "epac": "epac",
        "gas": "gas",
        "grb": "grb",
        "hiscale": "hiscale",
        "mag": "mag",
        "sce": "sce",
        "swoops": "swoops",
        "urap": "urap",
    },
    "vco": {
        "ir": "ir",
        "lir": "lir",
        "uvi": "uvi",
    },
    "vega": {
        "balloon": "balloon",
        "ducma": "ducma",
        "iks": "iks",
        "mischa": "mischa",
        "pm1": "pm1",
        "puma": "puma",
        "sp1": "sp1",
        "sp2": "sp2",
        "tnm": "tnm",
        "tvs": "tvs",
    },
    "viking": {
        "irtm": "irtm",
        "lcs": "lcs",
        "lr": "lr",
        "seis": "seis",
    },
    "vex": {
        # VEX multi folder is all RSO (Radio Science) data types
    },
    # These folders have products without clear instrument prefixes.
    # Each product_key becomes its own instrument entry.
    "hst": {},
    "ihw": {},
    "iue": {},
    "grsfe": {},
    "ground_based": {},
    "kaguya": {},
    "msx": {},
    "pioneer": {},
    "pre_magellan": {},
    "sakigake": {},
    "soho": {},
    "suisei": {},
    "venera": {},
}


def resolve_mission_instrument(folder_name: str) -> tuple[str, str, str]:
    """Resolve a pdr-tests folder name to (mission, instrument, mapping_status).

    Parameters
    ----------
    folder_name : str
        The folder name from pdr-tests definitions, e.g. 'cassini_iss'

    Returns
    -------
    tuple[str, str, str]
        (mission, instrument, mapping_status) where mapping_status is one of
        'manual', 'auto', '_split', or 'ambiguous'.
        '_split' means the folder contains multiple instruments and product
        keys need to be split via split_product_key().
    """
    if folder_name in MANUAL_MISSION_MAP:
        m, i = MANUAL_MISSION_MAP[folder_name]
        return m, i, "manual"
    if "_" in folder_name:
        parts = folder_name.split("_", 1)
        return parts[0], parts[1], "auto"
    return folder_name, folder_name, "ambiguous"


def split_product_key(folder_name: str, product_key: str) -> tuple[str, str]:
    """Split a product key into (instrument, stripped_key) for multi-instrument folders.

    Parameters
    ----------
    folder_name : str
        The pdr-tests folder name
    product_key : str
        The original product key (e.g. 'ctx_edr')

    Returns
    -------
    tuple[str, str]
        (instrument_name, stripped_product_key). If no split is defined,
        returns (product_key, product_key) — the key itself becomes the instrument.
    """
    if folder_name not in MULTI_INSTRUMENT_SPLIT:
        return product_key, product_key

    prefix_map = MULTI_INSTRUMENT_SPLIT[folder_name]
    if not prefix_map:
        # Empty map: each product_key is its own instrument
        return product_key, product_key

    # Try longest prefix first to avoid partial matches
    for prefix in sorted(prefix_map, key=len, reverse=True):
        if product_key == prefix:
            # Exact match (e.g. 'gas', 'mag', 'swoops')
            return prefix_map[prefix], prefix
        if product_key.startswith(prefix + "_"):
            stripped = product_key[len(prefix) + 1:]
            return prefix_map[prefix], stripped

    # No prefix matched — put in _misc
    return "_misc", product_key
