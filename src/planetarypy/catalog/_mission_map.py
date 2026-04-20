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
    "dawn_certified": ("dawn", "_split"),
    # Deep Impact
    "di_ground": ("deep_impact", "_split"),
    "di_nav": ("deep_impact", "_split"),
    "di_science": ("deep_impact", "_split"),
    # Deep Space 1
    "ds1": ("deep_space_1", "_split"),
    # EPOXI (Deep Impact extended mission)
    "epoxi": ("epoxi", "_split"),
    # ESA
    "esa_smart": ("esa", "smart"),
    # Galileo (gal_ prefix)
    "gal_mag": ("galileo", "mag"),
    "gal_misc": ("galileo", "_split"),
    "gal_nims": ("galileo", "nims"),
    "gal_particles": ("galileo", "_split"),
    "gal_plasma": ("galileo", "_split"),
    "gal_probe": ("galileo", "_split"),
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
    "near_spectrometers": ("near", "_split"),
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
    # Rosetta orbiter (multi-instrument folders)
    "rosetta_dust": ("rosetta", "_split"),
    "rosetta_spectrometers": ("rosetta", "_split"),
    # Rosetta lander (Philae) — separate spacecraft
    "rosetta_lander": ("rosetta_lander", "_split"),
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
    "vg_spectrometers": ("voyager", "_split"),
    # Viking
    "viking": ("viking", "_split"),
    # Wallops Flight Facility atmospheric data
    "wff_atm": ("wff", "atm"),
    # BepiColombo
    "bepi_colombo": ("bepi_colombo", "_split"),
}


# ── Human-readable full names ───────────────────────────────────────
#
# These provide display names for mission shorthands and instruments.
# Used by Mission / Instrument objects and presentation layers.

MISSION_FULL_NAMES: dict[str, str] = {
    "apollo": "Apollo",
    "bepi_colombo": "BepiColombo",
    "cassini": "Cassini-Huygens",
    "chandrayaan_1": "Chandrayaan-1",
    "chandrayaan_2": "Chandrayaan-2",
    "chandrayaan_3": "Chandrayaan-3",
    "change": "Chang'e",
    "clementine": "Clementine",
    "dawn": "Dawn",
    "deep_impact": "Deep Impact",
    "deep_space_1": "Deep Space 1",
    "epoxi": "EPOXI",
    "esa": "European Space Agency",
    "galileo": "Galileo",
    "giotto": "Giotto",
    "grail": "Gravity Recovery and Interior Laboratory",
    "ground_based": "Ground-Based Observations",
    "grsfe": "Geologic Remote Sensing Field Experiment",
    "hayabusa": "Hayabusa",
    "hst": "Hubble Space Telescope",
    "ice": "International Cometary Explorer",
    "ihw": "International Halley Watch",
    "iue": "International Ultraviolet Explorer",
    "juno": "Juno",
    "kaguya": "Kaguya (SELENE)",
    "lasp": "Laboratory for Atmospheric and Space Physics",
    "loirp": "Lunar Orbiter Image Recovery Project",
    "lro": "Lunar Reconnaissance Orbiter",
    "lunar": "Lunar Earth-Based Observations",
    "lunar_prospector": "Lunar Prospector",
    "magellan": "Magellan",
    "mariner": "Mariner",
    "mars": "Mars Derived Data",
    "mars_odyssey": "Mars Odyssey",
    "mars_pathfinder": "Mars Pathfinder",
    "mer": "Mars Exploration Rover",
    "messenger": "MESSENGER",
    "mex": "Mars Express",
    "mgs": "Mars Global Surveyor",
    "mro": "Mars Reconnaissance Orbiter",
    "msl": "Mars Science Laboratory",
    "msx": "Midcourse Space Experiment",
    "near": "NEAR Shoemaker",
    "new_horizons": "New Horizons",
    "phoenix": "Phoenix",
    "pioneer": "Pioneer",
    "pre_magellan": "Pre-Magellan",
    "pvo": "Pioneer Venus Orbiter",
    "rosetta": "Rosetta",
    "rosetta_lander": "Rosetta Lander (Philae)",
    "sakigake": "Sakigake",
    "saturn": "Saturn Ring Plane Crossing",
    "sl9": "Shoemaker-Levy 9",
    "soho": "Solar and Heliospheric Observatory",
    "stardust": "Stardust",
    "suisei": "Suisei",
    "ulysses": "Ulysses",
    "vco": "Venus Climate Orbiter (Akatsuki)",
    "vega": "Vega",
    "venera": "Venera",
    "venus": "Venus Ground-Based Radar",
    "venus_express": "Venus Express",
    "viking": "Viking",
    "voyager": "Voyager",
    "wff": "Wallops Flight Facility",
}

INSTRUMENT_FULL_NAMES: dict[tuple[str, str], str] = {
    # Cassini-Huygens
    ("cassini", "caps"): "Cassini Plasma Spectrometer",
    ("cassini", "cda"): "Cosmic Dust Analyzer",
    ("cassini", "cirs"): "Composite Infrared Spectrometer",
    ("cassini", "hp"): "Huygens Probe",
    ("cassini", "inms"): "Ion and Neutral Mass Spectrometer",
    ("cassini", "iss"): "Imaging Science Subsystem",
    ("cassini", "mag"): "Magnetometer",
    ("cassini", "mimi"): "Magnetospheric Imaging Instrument",
    ("cassini", "occultation"): "Occultation",
    ("cassini", "radar"): "Radar",
    ("cassini", "rpws"): "Radio and Plasma Wave Science",
    ("cassini", "rss"): "Radio Science Subsystem",
    ("cassini", "shape"): "Shape",
    ("cassini", "spice"): "SPICE Kernels",
    ("cassini", "uvis"): "Ultraviolet Imaging Spectrograph",
    ("cassini", "vims"): "Visual and Infrared Mapping Spectrometer",
    # Lunar Reconnaissance Orbiter
    ("lro", "crater"): "Cosmic Ray Telescope for the Effects of Radiation",
    ("lro", "diviner"): "Diviner Lunar Radiometer Experiment",
    ("lro", "lamp"): "Lyman-Alpha Mapping Project",
    ("lro", "lend"): "Lunar Exploration Neutron Detector",
    ("lro", "lola"): "Lunar Orbiter Laser Altimeter",
    ("lro", "lroc"): "Lunar Reconnaissance Orbiter Camera",
    ("lro", "mini_rf"): "Miniature Radio Frequency",
    ("lro", "rss"): "Radio Science Subsystem",
    # Mars Reconnaissance Orbiter
    ("mro", "crism"): "Compact Reconnaissance Imaging Spectrometer for Mars",
    ("mro", "ctx"): "Context Camera",
    ("mro", "hirise"): "High Resolution Imaging Science Experiment",
    ("mro", "marci"): "Mars Color Imager",
    ("mro", "mcs"): "Mars Climate Sounder",
    ("mro", "rss"): "Radio Science Subsystem",
    ("mro", "sharad"): "Shallow Radar",
    # Juno
    ("juno", "fgm"): "Fluxgate Magnetometer",
    ("juno", "gs"): "Gravity Science",
    ("juno", "jade"): "Jovian Auroral Distributions Experiment",
    ("juno", "jedi"): "Jupiter Energetic Particle Detector Instrument",
    ("juno", "jiram"): "Jovian Infrared Auroral Mapper",
    ("juno", "junocam"): "JunoCam",
    ("juno", "mwr"): "Microwave Radiometer",
    ("juno", "waves"): "Waves",
    # New Horizons
    ("new_horizons", "alice"): "Alice Ultraviolet Imaging Spectrometer",
    ("new_horizons", "derived"): "Derived Data",
    ("new_horizons", "leisa"): "Linear Etalon Imaging Spectral Array",
    ("new_horizons", "lorri"): "Long Range Reconnaissance Imager",
    ("new_horizons", "mvic"): "Multispectral Visible Imaging Camera",
    ("new_horizons", "pepssi"): "Pluto Energetic Particle Spectrometer Science Investigation",
    ("new_horizons", "rex"): "Radio Science Experiment",
    ("new_horizons", "sdc"): "Student Dust Counter",
    ("new_horizons", "swap"): "Solar Wind Around Pluto",
    # Mars Express
    ("mex", "aspera"): "Analyzer of Space Plasmas and Energetic Atoms",
    ("mex", "hrsc"): "High Resolution Stereo Camera",
    ("mex", "marsis"): "Mars Advanced Radar for Subsurface and Ionosphere Sounding",
    ("mex", "mrs"): "Mars Radio Science",
    ("mex", "omega"): "Observatoire pour la Mineralogie, l'Eau, les Glaces et l'Activite",
    ("mex", "pfs"): "Planetary Fourier Spectrometer",
    ("mex", "spicam"): "Spectroscopy for Investigation of the Atmosphere of Mars",
    ("mex", "vmc"): "Visual Monitoring Camera",
    # Galileo
    ("galileo", "mag"): "Magnetometer",
    ("galileo", "nims"): "Near-Infrared Mapping Spectrometer",
    ("galileo", "epd"): "Energetic Particles Detector",
    ("galileo", "gdds"): "Galileo Dust Detection System",
    ("galileo", "ssd"): "Star Scanner Detector",
    ("galileo", "pls"): "Plasma Science",
    ("galileo", "pws"): "Plasma Wave Subsystem",
    ("galileo", "ppr"): "Photopolarimeter-Radiometer",
    ("galileo", "trajectory"): "Spacecraft Trajectory",
    ("galileo", "asi"): "Atmospheric Structure Instrument",
    ("galileo", "dwe"): "Doppler Wind Experiment",
    ("galileo", "epi"): "Energetic Particles Investigation",
    ("galileo", "had"): "Helium Abundance Detector",
    ("galileo", "nep"): "Nephelometer",
    ("galileo", "nfr"): "Net Flux Radiometer",
    ("galileo", "nms"): "Neutral Mass Spectrometer",
    ("galileo", "ssi"): "Solid State Imaging",
    # Voyager
    ("voyager", "crs"): "Cosmic Ray Subsystem",
    ("voyager", "iss"): "Imaging Science Subsystem",
    ("voyager", "lecp"): "Low Energy Charged Particle",
    ("voyager", "mag"): "Magnetometer",
    ("voyager", "pls"): "Plasma Science",
    ("voyager", "pos"): "Position (Ephemeris) Data",
    ("voyager", "pra"): "Planetary Radio Astronomy",
    ("voyager", "pws"): "Plasma Wave System",
    ("voyager", "ring_profiles"): "Ring Profiles",
    ("voyager", "rss"): "Radio Science Subsystem",
    ("voyager", "iris"): "Infrared Interferometer Spectrometer",
    ("voyager", "uvs"): "Ultraviolet Spectrometer",
    # Rosetta
    ("rosetta", "consert"): "Comet Nucleus Sounding Experiment by Radiowave Transmission",
    ("rosetta", "cosima"): "Cometary Secondary Ion Mass Analyzer",
    ("rosetta", "giada"): "Grain Impact Analyser and Dust Accumulator",
    ("rosetta", "midas"): "Micro-Imaging Dust Analysis System",
    # Rosetta Lander (Philae) — separate mission
    ("rosetta_lander", "apxs"): "Alpha Particle X-Ray Spectrometer",
    ("rosetta_lander", "cosac"): "Cometary Sampling and Composition",
    ("rosetta_lander", "mupus"): "Multi-Purpose Sensors for Surface and Subsurface Science",
    ("rosetta_lander", "ptolemy"): "Ptolemy",
    ("rosetta_lander", "rolis"): "Rosetta Lander Imaging System",
    ("rosetta_lander", "romap"): "Rosetta Magnetometer and Plasma Monitor",
    ("rosetta_lander", "sd2"): "Sample, Drill and Distribution",
    ("rosetta_lander", "sesame"): "Surface Electrical, Seismic and Acoustic Monitoring Experiments",
    ("rosetta", "navcam"): "Navigation Camera",
    ("rosetta", "osiris"): "Optical, Spectroscopic, and Infrared Remote Imaging System",
    ("rosetta", "rpc"): "Rosetta Plasma Consortium",
    ("rosetta", "rsi"): "Radio Science Investigation",
    ("rosetta", "alice"): "Alice Ultraviolet Spectrometer",
    ("rosetta", "miro"): "Microwave Instrument for the Rosetta Orbiter",
    ("rosetta", "rosina"): "Rosetta Orbiter Spectrometer for Ion and Neutral Analysis",
    ("rosetta", "virtis"): "Visible and Infrared Thermal Imaging Spectrometer",
    ("rosetta", "srem"): "Standard Radiation Environment Monitor",
    # Mars Science Laboratory (Curiosity)
    ("msl", "apxs"): "Alpha Particle X-Ray Spectrometer",
    ("msl", "ccam"): "Chemistry and Camera",
    ("msl", "cmn"): "Chemistry and Mineralogy",
    ("msl", "dan"): "Dynamic Albedo of Neutrons",
    ("msl", "haz_edr"): "Hazard Avoidance Camera EDR",
    ("msl", "haz_rdr"): "Hazard Avoidance Camera RDR",
    ("msl", "nav_mosaic"): "Navigation Camera Mosaic",
    ("msl", "nav_rdr"): "Navigation Camera RDR",
    ("msl", "places"): "Places",
    ("msl", "rems"): "Rover Environmental Monitoring Station",
    ("msl", "sam"): "Sample Analysis at Mars",
    # Mars Exploration Rover (Spirit/Opportunity)
    ("mer", "descam"): "Descent Camera",
    ("mer", "haz_rdr"): "Hazard Avoidance Camera RDR",
    ("mer", "mi"): "Microscopic Imager",
    ("mer", "mosaic"): "Mosaic",
    ("mer", "mw"): "Mini-TES",
    ("mer", "nav_rdr"): "Navigation Camera RDR",
    ("mer", "pan_rdr"): "Panoramic Camera RDR",
    ("mer", "pan_science"): "Panoramic Camera Science",
    ("mer", "rss"): "Radio Science Subsystem",
    # Mars Global Surveyor
    ("mgs", "mag_er"): "Magnetometer and Electron Reflectometer",
    ("mgs", "moc"): "Mars Orbiter Camera",
    ("mgs", "mola"): "Mars Orbiter Laser Altimeter",
    ("mgs", "rss_eds"): "Radio Science EDS",
    ("mgs", "rss_raw"): "Radio Science Raw",
    ("mgs", "rss_sdp"): "Radio Science SDP",
    ("mgs", "rss_sun"): "Radio Science Sun",
    ("mgs", "sampler"): "Sampler",
    ("mgs", "tes"): "Thermal Emission Spectrometer",
    ("mgs", "tes_geo"): "Thermal Emission Spectrometer Geometries",
    # MESSENGER
    ("messenger", "grnd_cal"): "Ground Calibration",
    ("messenger", "grns"): "Gamma-Ray and Neutron Spectrometer",
    ("messenger", "mascs"): "Mercury Atmospheric and Surface Composition Spectrometer",
    ("messenger", "meap"): "Mercury Energetic Atoms and Particles",
    # Magellan
    ("magellan", "altimetry"): "Altimetry",
    ("magellan", "bsr"): "Bistatic Radar",
    ("magellan", "gravity"): "Gravity",
    ("magellan", "gxdr"): "Global Experiment Data Record",
    ("magellan", "image"): "Imaging Radar",
    ("magellan", "occult"): "Occultation",
    ("magellan", "post_mission"): "Post Mission",
    ("magellan", "tracking"): "Tracking",
    # Dawn
    # Dawn certified data uses same instruments as main Dawn (fc, vir)
    ("dawn", "fc"): "Framing Camera",
    ("dawn", "vir"): "Visible and Infrared Spectrometer",
    # Venus Express
    ("venus_express", "aspera"): "Analyzer of Space Plasmas and Energetic Atoms",
    ("venus_express", "mag"): "Magnetometer",
    ("venus_express", "spicav"): "Spectroscopy for Investigation of the Atmosphere of Venus",
    ("venus_express", "spicav_soir"): "SPICAV Solar Occultation in the Infrared",
    ("venus_express", "vera"): "Venus Express Radio Science",
    ("venus_express", "virtis"): "Visible and Infrared Thermal Imaging Spectrometer",
    ("venus_express", "vmc"): "Venus Monitoring Camera",
    # Viking
    ("viking", "irtm"): "Infrared Thermal Mapper",
    ("viking", "lcs"): "Lander Camera System",
    ("viking", "lr"): "Labeled Release",
    ("viking", "seis"): "Seismometer",
    # Mars Odyssey
    ("mars_odyssey", "grs"): "Gamma Ray Spectrometer",
    ("mars_odyssey", "marie"): "Martian Radiation Environment Experiment",
    ("mars_odyssey", "themis"): "Thermal Emission Imaging System",
    # Mars Pathfinder
    ("mars_pathfinder", "3d"): "3D Data",
    ("mars_pathfinder", "apxs"): "Alpha Proton X-Ray Spectrometer",
    ("mars_pathfinder", "eng"): "Engineering",
    ("mars_pathfinder", "imp"): "Imager for Mars Pathfinder",
    ("mars_pathfinder", "rss"): "Radio Science Subsystem",
    ("mars_pathfinder", "rvrcam"): "Rover Camera",
    # Phoenix
    ("phoenix", "afm"): "Atomic Force Microscope",
    ("phoenix", "atm"): "Atmospheric Science",
    ("phoenix", "elec"): "Electrical Properties",
    ("phoenix", "om"): "Optical Microscope",
    ("phoenix", "rac"): "Robotic Arm Camera",
    ("phoenix", "ssi"): "Surface Stereo Imager",
    ("phoenix", "tecp"): "Thermal and Electrical Conductivity Probe",
    ("phoenix", "tega"): "Thermal and Evolved Gas Analyzer",
    ("phoenix", "wcl"): "Wet Chemistry Laboratory",
    # NEAR Shoemaker
    ("near", "mag"): "Magnetometer",
    ("near", "msi"): "Multi-Spectral Imager",
    ("near", "nlr"): "NEAR Laser Rangefinder",
    ("near", "rss"): "Radio Science Subsystem",
    ("near", "grs"): "Gamma-Ray Spectrometer",
    ("near", "nis"): "Near-Infrared Spectrometer",
    ("near", "xrs"): "X-Ray Spectrometer",
    # Hayabusa
    ("hayabusa", "amica"): "Asteroid Multiband Imaging Camera",
    ("hayabusa", "lidar"): "Light Detection and Ranging",
    ("hayabusa", "nirs"): "Near-Infrared Spectrometer",
    # Stardust
    ("stardust", "cida"): "Cometary and Interstellar Dust Analyzer",
    ("stardust", "dfmi"): "Dust Flux Monitor Instrument",
    ("stardust", "dse"): "Deep Space Experiment",
    ("stardust", "keck"): "Keck",
    ("stardust", "navcam"): "Navigation Camera",
    ("stardust", "next"): "New Exploration of Tempel 1",
    ("stardust", "src"): "Sample Return Capsule",
    # Ulysses
    ("ulysses", "cospin"): "Cosmic and Solar Particle Investigation",
    ("ulysses", "dds"): "Dust Detection System",
    ("ulysses", "epac"): "Energetic Particle Composition",
    ("ulysses", "gas"): "Interstellar Neutral Gas",
    ("ulysses", "grb"): "Gamma-Ray Burst",
    ("ulysses", "hiscale"): "Heliosphere Instrument for Spectra, Composition, and Anisotropy at Low Energies",
    ("ulysses", "mag"): "Magnetometer",
    ("ulysses", "sce"): "Solar Corona Experiment",
    ("ulysses", "swoops"): "Solar Wind Observations Over the Poles of the Sun",
    ("ulysses", "urap"): "Unified Radio and Plasma Wave",
    # Lunar Prospector
    ("lunar_prospector", "er"): "Electron Reflectometer",
    ("lunar_prospector", "grs"): "Gamma Ray Spectrometer",
    ("lunar_prospector", "mag"): "Magnetometer",
    # GRAIL
    ("grail", "lgrs"): "Lunar Gravity Ranging System",
    ("grail", "rss"): "Radio Science Subsystem",
    # International Cometary Explorer
    ("ice", "epas"): "Energetic Proton and Alpha Sensor",
    ("ice", "ici"): "Ion Composition Instrument",
    ("ice", "mag"): "Magnetometer",
    ("ice", "plawav"): "Plasma Wave",
    ("ice", "radwav"): "Radio Wave",
    ("ice", "swp"): "Solar Wind Plasma",
    ("ice", "uleca"): "Ultra Low Energy Charge Analyzer",
    # Giotto
    ("giotto", "did"): "Dust Impact Detector",
    ("giotto", "gre"): "Giotto Radio-science Experiment",
    ("giotto", "hmc"): "Halley Multicolour Camera",
    ("giotto", "ims"): "Ion Mass Spectrometer",
    ("giotto", "jpa"): "Johnstone Plasma Analyzer",
    ("giotto", "mag"): "Magnetometer",
    ("giotto", "nms"): "Neutral Mass Spectrometer",
    ("giotto", "ope"): "Optical Probe Experiment",
    ("giotto", "pia"): "Particulate Impact Analyzer",
    # Deep Impact
    ("deep_impact", "ground"): "Ground-Based Data",
    ("deep_impact", "nav"): "Navigation",
    ("deep_impact", "science"): "Science Instruments",
    # EPOXI
    ("epoxi", "hrii"): "High Resolution Instrument Infrared",
    ("epoxi", "hriv"): "High Resolution Instrument Visible",
    ("epoxi", "mri"): "Medium Resolution Instrument",
    # Deep Space 1
    ("deep_space_1", "ids"): "Ion Propulsion Diagnostic Subsystem",
    ("deep_space_1", "micas"): "Miniature Integrated Camera and Spectrometer",
    ("deep_space_1", "pepe"): "Plasma Experiment for Planetary Exploration",
    # BepiColombo
    ("bepi_colombo", "cam1"): "Monitoring Camera 1",
    ("bepi_colombo", "cam2"): "Monitoring Camera 2",
    ("bepi_colombo", "mag"): "Magnetometer",
    # Venus Climate Orbiter (Akatsuki)
    ("vco", "ir"): "Infrared Camera",
    ("vco", "lir"): "Longwave Infrared Camera",
    ("vco", "uvi"): "Ultraviolet Imager",
    # Pioneer Venus Orbiter
    ("pvo", "oefd"): "Orbiter Electric Field Detector",
    ("pvo", "oetp"): "Orbiter Electron Temperature Probe",
    ("pvo", "oims"): "Orbiter Ion Mass Spectrometer",
    ("pvo", "omag"): "Orbiter Magnetometer",
    ("pvo", "onms"): "Orbiter Neutral Mass Spectrometer",
    ("pvo", "orpa"): "Orbiter Retarding Potential Analyzer",
    ("pvo", "orse"): "Orbiter Radio Science Experiment",
    ("pvo", "pos"): "Position (Ephemeris) Data",
    # Vega
    ("vega", "balloon"): "Balloon",
    ("vega", "ducma"): "Dust Counter and Mass Analyzer",
    ("vega", "iks"): "Infrared Spectrometer",
    ("vega", "mischa"): "Magnetometer",
    ("vega", "pm1"): "Plasmag-1",
    ("vega", "puma"): "Dust Mass Spectrometer",
    ("vega", "sp1"): "Energetic Particle Analyzer SP-1",
    ("vega", "sp2"): "Energetic Particle Analyzer SP-2",
    ("vega", "tnm"): "Tunde-M Energetic Particle Detector",
    ("vega", "tvs"): "Television System",
    # Chandrayaan-1
    ("chandrayaan_1", "esa"): "ESA Instruments",
    ("chandrayaan_1", "m3"): "Moon Mineralogy Mapper",
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
    # Galileo multi-instrument folders
    "gal_misc": {
        "ppr": "ppr",
        "sc_traj": "trajectory",
    },
    "gal_particles": {
        "epd": "epd",
        "gdds": "gdds",
        "ssd": "ssd",
    },
    "gal_plasma": {
        "pls": "pls",
        "pws": "pws",
    },
    "gal_probe": {
        "asi": "asi",
        "dwe": "dwe",
        "epi": "epi",
        "had": "had",
        "nep": "nep",
        "nfr": "nfr",
        "nms": "nms",
    },
    # NEAR spectrometers
    "near_spectrometers": {
        "grs": "grs",
        "nis": "nis",
        "xrs": "xrs",
    },
    # Rosetta folders use infix matching: product keys are like
    # EDR_alice, RDR_midas_img, DDR_giada — instrument name comes
    # after a data level prefix.
    "rosetta_dust": {
        "giada": "giada",
        "midas": "midas",
    },
    "rosetta_lander": {
        "apxs": "apxs",
        "cosac": "cosac",
        "mupus": "mupus",
        "ptolemy": "ptolemy",
        "rolis": "rolis",
        "romap": "romap",
        "sd2": "sd2",
        "sesame": "sesame",
    },
    "rosetta_spectrometers": {
        "alice": "alice",
        "miro": "miro",
        "rosina": "rosina",
        "virtis": "virtis",
    },
    # Dawn certified (FC + VIR)
    "dawn_certified": {
        "fc": "fc",
        "vir": "vir",
    },
    # Voyager spectrometers
    "vg_spectrometers": {
        "iris": "iris",
        "uvs": "uvs",
    },
    # Deep Impact multi-instrument folders
    "di_ground": {
        "hawaii": "hawaii",
        "irtf": "irtf",
        "keck": "keck",
        "kpno": "kpno",
        "lowell": "lowell",
        "martir": "martir",
        "mt_bigelow": "mt_bigelow",
    },
    "di_nav": {
        "hriv": "hriv",
        "its": "its",
        "mri": "mri",
    },
    "di_science": {
        "hrii": "hrii",
        "hriv": "hriv",
        "iras": "iras",
        "its": "its",
        "mri": "mri",
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


# ── Product key normalization ────────────────────────────────────────
#
# pdr-tests product keys encode up to three dimensions:
#   1. Data type (edr, rdr, ddr, summ, ...)
#   2. Target body / mission phase (saturn, jupiter, neptune, ...)
#   3. Data format (ascii, binary)
#
# We decompose them into (normalized_type, phase, format) so the catalog
# is queryable on each axis independently.

# Known data format / representation suffixes.
# Includes both serialization formats (ascii, binary) and coordinate
# system representations (heliographic, U1, L1, NLS) — all describe
# how the same underlying data is represented.
FORMAT_SUFFIXES: dict[str, str] = {
    "_asc": "ascii",
    "_bin": "binary",
    "_ascii": "ascii",
    "_hg": "heliographic",
    "_u1": "u1_coords",
    "_l1": "l1_coords",
    "_nls": "nls_coords",
}

# Known target body / mission phase identifiers.
# Used both as suffixes (_sat, _jup) and prefixes (sat_, jup_).
BODY_MAP: dict[str, str] = {
    "sat": "saturn",
    "saturn": "saturn",
    "jup": "jupiter",
    "jupiter": "jupiter",
    "nep": "neptune",
    "neptune": "neptune",
    "ura": "uranus",
    "ur": "uranus",
    "uranus": "uranus",
    "evj": "earth_venus_jupiter",
    "cru": "cruise",
    "cruise": "cruise",
    "early": "early_mission",
    "late": "late_mission",
    "pre_jup": "pre_jupiter",
    # New Horizons mission phases
    "pluto": "pluto",
    "arrokoth": "arrokoth",
    "launch": "launch",
    "kem_cruise": "kem_cruise",
    # Target bodies (flyby targets, moons, minor bodies)
    "ceres": "ceres",
    "vesta": "vesta",
    "gaspra": "gaspra",
    "ida": "ida",
    "halley": "halley",
    "phobos": "phobos",
    "earth": "earth",
}

# Product keys where the ENTIRE key is a body/phase name.
# For these, the body becomes the phase and we need context to determine
# the normalized type.  The default is to keep the original key as
# normalized_type with phase set — callers with instrument context
# (e.g. Voyager POS = ephemeris data) can override.
STANDALONE_BODY_KEYS: set[str] = {
    "jupiter", "neptune", "saturn", "uranus", "cruise",
    "gaspra", "ida",
}

# Product keys that should NOT be decomposed despite matching patterns.
# These are legitimate compound names where the body-like substring is
# part of the actual product type semantics.
NORMALIZATION_EXCEPTIONS: set[str] = {
    # Pre-Magellan: "mars" is part of instrument/dataset name, not a phase
    "vi_mars",
    # Ground-based: "comet" is the target class, not a mission phase
    "lowell_comet",
    # Solar wind is a physical phenomenon, not a phase
    "solar_wind",
    # Processing levels: _l1 here means "Level 1", not L1 coordinate system
    "cla_l1",
    "70cm_l1",
}


# Instruments where standalone body keys represent ephemeris (position) data,
# not science data.  For these, normalized_type becomes "ephemeris" instead
# of keeping the body name.
EPHEMERIS_INSTRUMENTS: set[tuple[str, str]] = {
    ("voyager", "pos"),
}


def normalize_product_key(product_key: str) -> tuple[str, str, str]:
    """Decompose a product key into (normalized_type, phase, format).

    Multi-pass normalization:
    1. Strip format suffixes (_asc, _bin) → format column
    2. Strip body/phase suffixes (_sat, _jup, _nep, ...) → phase column
    3. Strip body/phase prefixes (sat_, jup_, nep_, ...) → phase column
    4. Handle standalone body keys (jupiter, saturn, ...) → keep as type

    Parameters
    ----------
    product_key : str
        Raw product key from pdr-tests (e.g. 'sat_rdr_asc', 'edr_sat')

    Returns
    -------
    tuple[str, str, str]
        (normalized_type, phase, format) where phase/format are '' if
        not detected.

    Examples
    --------
    >>> normalize_product_key("edr_sat")
    ('edr', 'saturn', '')
    >>> normalize_product_key("sat_rdr_asc")
    ('rdr', 'saturn', 'ascii')
    >>> normalize_product_key("jup_ddr")
    ('ddr', 'jupiter', '')
    >>> normalize_product_key("summ_neptune")
    ('summ', 'neptune', '')
    >>> normalize_product_key("calib")
    ('calib', '', '')
    """
    if product_key in NORMALIZATION_EXCEPTIONS:
        return product_key, "", ""

    if product_key in STANDALONE_BODY_KEYS:
        return product_key, BODY_MAP[product_key], ""

    remaining = product_key
    phase = ""
    fmt = ""

    # Pass 1: Strip format suffix
    for suffix in sorted(FORMAT_SUFFIXES, key=len, reverse=True):
        if remaining.endswith(suffix):
            candidate = remaining[: -len(suffix)]
            if candidate:
                fmt = FORMAT_SUFFIXES[suffix]
                remaining = candidate
                break

    # Pass 1b: After format stripping, check if remainder is a body name
    # (e.g. nep_ascii → strip _ascii → "nep" is a body name)
    if remaining in BODY_MAP:
        return remaining, BODY_MAP[remaining], fmt

    # Pass 2: Strip body/phase suffix
    # Try compound suffixes first (e.g. _pre_jup before _jup)
    all_body_keys = list(BODY_MAP.keys())
    for body_key in sorted(all_body_keys, key=len, reverse=True):
        suffix = f"_{body_key}"
        if remaining.endswith(suffix):
            candidate = remaining[: -len(suffix)]
            if candidate:
                phase = BODY_MAP[body_key]
                remaining = candidate
                break

    # Pass 3: Strip body/phase prefix (only if suffix didn't match)
    if not phase:
        for body_key in sorted(all_body_keys, key=len, reverse=True):
            prefix = f"{body_key}_"
            if remaining.startswith(prefix):
                candidate = remaining[len(prefix):]
                if candidate:
                    phase = BODY_MAP[body_key]
                    remaining = candidate
                    break

    # Pass 3b: Check if remainder is a short body key after prefix/suffix strip
    if remaining in BODY_MAP and not phase:
        phase = BODY_MAP[remaining]

    return remaining, phase, fmt


def apply_instrument_overrides(
    normalized_type: str,
    phase: str,
    fmt: str,
    mission: str,
    instrument: str,
) -> tuple[str, str, str]:
    """Apply instrument-context overrides to normalization results.

    Some instruments have product keys that are standalone body names
    (e.g. 'jupiter', 'saturn') where the normalized_type should be
    renamed based on what the instrument actually measures.

    For example, Voyager POS stores pre-SPICE spacecraft position data —
    standalone body keys there should be called 'ephemeris', not kept as
    the body name.

    Parameters
    ----------
    normalized_type, phase, fmt : str
        Output from normalize_product_key()
    mission, instrument : str
        Instrument context for the override lookup

    Returns
    -------
    tuple[str, str, str]
        (normalized_type, phase, format), potentially with overrides applied
    """
    if (mission, instrument) in EPHEMERIS_INSTRUMENTS:
        if normalized_type in STANDALONE_BODY_KEYS:
            return "ephemeris", phase, fmt
    return normalized_type, phase, fmt


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


# Folders where instrument names appear as infixes in product keys
# (e.g. EDR_alice, RDR_midas_img) rather than as prefixes.
# For these, we search for the instrument name anywhere in the key.
INFIX_SPLIT_FOLDERS: set[str] = {
    "rosetta_dust",
    "rosetta_lander",
    "rosetta_spectrometers",
}


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

    # Infix matching: instrument name can appear anywhere in the key
    # e.g. EDR_alice → alice, RDR_midas_img → midas
    if folder_name in INFIX_SPLIT_FOLDERS:
        key_lower = product_key.lower()
        for name in sorted(prefix_map, key=len, reverse=True):
            name_lower = name.lower()
            if key_lower == name_lower:
                return prefix_map[name], product_key
            # Check as infix: _name_ or _name at end or name_ at start
            if (f"_{name_lower}_" in key_lower
                    or key_lower.endswith(f"_{name_lower}")
                    or key_lower.startswith(f"{name_lower}_")
                    or f"_{name_lower}" in key_lower
                    or f"{name_lower}_" in key_lower):
                return prefix_map[name], product_key
        return "_misc", product_key

    # Standard prefix matching
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
