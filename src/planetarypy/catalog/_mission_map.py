"""Mission/instrument mapping for pdr-tests folder names.

Maps folder names like 'cassini_iss', 'diviner', 'gal_ssi' to
(mission, instrument) tuples. Uses a three-tier strategy:
1. Manual map for known ambiguous cases
2. Auto-split on first underscore
3. Ambiguous flag for anything unresolved
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
    # Multi-instrument bundled definitions
    "mro": ("mro", "multi"),
    # Apollo
    "apollo": ("apollo", "multi"),
    # Dawn
    "dawn": ("dawn", "multi"),
    "dawn_certified": ("dawn", "certified"),
    # Deep Impact
    "di_ground": ("deep_impact", "ground"),
    "di_nav": ("deep_impact", "nav"),
    "di_science": ("deep_impact", "science"),
    # Deep Space 1
    "ds1": ("deep_space_1", "multi"),
    # EPOXI (Deep Impact extended mission)
    "epoxi": ("epoxi", "multi"),
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
    "giotto": ("giotto", "multi"),
    # GRAIL
    "grail": ("grail", "multi"),
    # Ground-based observations
    "ground_based": ("ground_based", "multi"),
    "grsfe": ("grsfe", "multi"),
    # Hayabusa
    "hayabusa": ("hayabusa", "multi"),
    # Hubble
    "hst": ("hst", "multi"),
    # ICE (International Cometary Explorer)
    "ice": ("ice", "multi"),
    # International Halley Watch
    "ihw": ("ihw", "multi"),
    "ihw_irsn": ("ihw", "irsn"),
    "ihw_ppn": ("ihw", "ppn"),
    "ihw_rss": ("ihw", "rss"),
    # IUE (International Ultraviolet Explorer)
    "iue": ("iue", "multi"),
    # Kaguya (SELENE)
    "kaguya": ("kaguya", "multi"),
    "kaguya_jaxa": ("kaguya", "jaxa"),
    # LASP Mars data
    "lasp_mars": ("lasp", "mars"),
    # LOIRP (Lunar Orbiter Image Recovery Project)
    "loirp": ("loirp", "multi"),
    # Lunar observations
    "lunar_earth_based": ("lunar", "earth_based"),
    "lunar_prospector": ("lunar_prospector", "multi"),
    # Clementine
    "clem_GEO": ("clementine", "geo"),
    # Chandrayaan
    "ch1": ("chandrayaan_1", "multi"),
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
    "mariner": ("mariner", "multi"),
    # Mars
    "mars_model": ("mars", "model"),
    "mars_odyssey": ("mars_odyssey", "multi"),
    "mars_pathfinder": ("mars_pathfinder", "multi"),
    # MRO RSS (separate from main mro folder)
    "mro_rss": ("mro", "rss"),
    # MSX
    "msx": ("msx", "multi"),
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
    "phoenix": ("phoenix", "multi"),
    "phoenix_atm": ("phoenix", "atm"),
    "phoenix_tega": ("phoenix", "tega"),
    # Pioneer
    "pioneer": ("pioneer", "multi"),
    # Pre-Magellan Venus data
    "pre_magellan": ("pre_magellan", "multi"),
    # Pioneer Venus Orbiter
    "pvo": ("pvo", "multi"),
    "pvo_omag": ("pvo", "omag"),
    # Sakigake
    "sakigake": ("sakigake", "multi"),
    # Saturn ring plane crossing
    "saturn_rpx": ("saturn", "rpx"),
    # Shoemaker-Levy 9
    "sl9_jupiter_impact": ("sl9", "jupiter_impact"),
    # SOHO
    "soho": ("soho", "multi"),
    # Stardust
    "stardust": ("stardust", "multi"),
    "stardust_next": ("stardust", "next"),
    # Suisei
    "suisei": ("suisei", "multi"),
    # Ulysses
    "ulysses": ("ulysses", "multi"),
    # Venus Climate Orbiter (Akatsuki)
    "vco": ("vco", "multi"),
    # Vega
    "vega": ("vega", "multi"),
    # Venera
    "venera": ("venera", "multi"),
    # Venus radar
    "venus_radar": ("venus", "radar"),
    # Venus Express (vex_ prefix)
    "vex": ("venus_express", "multi"),
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
    "viking": ("viking", "multi"),
    # Wallops Flight Facility atmospheric data
    "wff_atm": ("wff", "atm"),
    # BepiColombo
    "bepi_colombo": ("bepi_colombo", "multi"),
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
        'manual', 'auto', or 'ambiguous'
    """
    if folder_name in MANUAL_MISSION_MAP:
        m, i = MANUAL_MISSION_MAP[folder_name]
        return m, i, "manual"
    if "_" in folder_name:
        parts = folder_name.split("_", 1)
        return parts[0], parts[1], "auto"
    return folder_name, "multi", "ambiguous"
