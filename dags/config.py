# Updated country_dict with English labels
country_dict = {
    # ==========================================
    # 1. SOUTHEAST ASIA (ASEAN)
    # ==========================================
    "VN": "Viet Nam",                      # Chọn phiên bản chuẩn ISO chính thức
    "TH": "Thailand",
    "SG": "Singapore", 
    "ID": "Indonesia",
    "MY": "Malaysia",
    "PH": "Philippines",
    "BN": "Brunei",
    "KH": "Cambodia",
    "LA": "Laos",
    "MM": "Myanmar",
    "TL": "Timor-Leste",

    # ==========================================
    # 2. EUROPE (Eurozone & Major Economies)
    # ==========================================
    "DE": "Germany",
    "FR": "France",
    "IT": "Italy",
    "ES": "Spain",
    "NL": "Netherlands",
    "GB": "United Kingdom",
    "BE": "Belgium",
    "AT": "Austria",
    "PT": "Portugal",
    "GR": "Greece",
    "FI": "Finland",
    "IE": "Ireland",
    "DK": "Denmark",
    "SE": "Sweden",
    "PL": "Poland",
    "CZ": "Czechia",
    "RO": "Romania",
    "HU": "Hungary",
    "VA": "Holy See (Vatican City State)",  # Tên đầy đủ hơn
    "UA": "Ukraine",
    "TR": "Türkiye",                       # Tên chính thức mới
    "XK": "Kosovo, Republic of",

    # ==========================================
    # 3. AMERICAS (North & South America)
    # ==========================================
    "US": "United States",
    "CA": "Canada",
    "MX": "Mexico",
    "BR": "Brazil",
    "AR": "Argentina",
    "CO": "Colombia",
    "SR": "Suriname",
    "SV": "El Salvador",
    "SX": "Sint Maarten (Dutch part)",
    "TT": "Trinidad and Tobago",
    "UY": "Uruguay",
    "VC": "Saint Vincent and the Grenadines",
    "VE": "Venezuela, Bolivarian Republic", # Tên đầy đủ hơn

    # ==========================================
    # 4. MIDDLE EAST & CENTRAL ASIA
    # ==========================================
    "SA": "Saudi Arabia", 
    "AE": "UAE",          
    "QA": "Qatar",
    "KW": "Kuwait",
    "OM": "Oman",
    "BH": "Bahrain",
    "IQ": "Iraq",
    "SY": "Syrian Arab Republic",           # Tên đầy đủ hơn
    "TJ": "Tajikistan",
    "TM": "Turkmenistan",
    "UZ": "Uzbekistan",
    "YE": "Yemen",

    # ==========================================
    # 5. AFRICA
    # ==========================================
    "SN": "Senegal",
    "SO": "Somalia",
    "SS": "South Sudan",
    "SZ": "Eswatini",
    "TD": "Chad",
    "TG": "Togo",
    "TN": "Tunisia",
    "TZ": "Tanzania, United Republic of",  # Tên đầy đủ hơn
    "UG": "Uganda",
    "ZA": "South Africa",
    "ZM": "Zambia",
    "ZW": "Zimbabwe",

    # ==========================================
    # 6. OCEANIA & OTHERS / REGIONAL
    # ==========================================
    "TO": "Tonga",
    "TV": "Tuvalu",
    "VU": "Vanuatu",
    "WS": "Samoa",
    "ST": "Sao Tome and Principe",
    "SUH": "Former U.S.S.R.",
    "U2": "Euro Area (Member States and Institutions of the Euro Area) changing composition"
}


country_dict_3_char = {
    # ==========================================
    # 1. SOUTHEAST ASIA (ASEAN)
    # ==========================================
    "VNM": "Vietnam",
    "THA": "Thailand",
    "SGP": "Singapore",
    "IDN": "Indonesia",
    "MYS": "Malaysia",
    "PHL": "Philippines",
    "BRN": "Brunei",
    "KHM": "Cambodia",
    "LAO": "Laos",
    "MMR": "Myanmar",
    "TLS": "Timor-Leste",

    # ==========================================
    # 2. EUROPE (Eurozone & Major Economies)
    # ==========================================
    "DEU": "Germany",
    "FRA": "France",
    "ITA": "Italy",
    "ESP": "Spain",
    "NLD": "Netherlands",
    "GBR": "United Kingdom",
    "BEL": "Belgium",
    "AUT": "Austria",
    "PRT": "Portugal",
    "GRC": "Greece",
    "FIN": "Finland",
    "IRL": "Ireland",
    "DNK": "Denmark",
    "SWE": "Sweden",
    "POL": "Poland",
    "CZE": "Czechia",
    "ROU": "Romania",
    "HUN": "Hungary",

    # ==========================================
    # 3. AMERICAS (North & South America)
    # ==========================================
    "USA": "United States",
    "CAN": "Canada",
    "MEX": "Mexico",
    "BRA": "Brazil",
    "ARG": "Argentina",
    "COL": "Colombia",

    # ==========================================
    # 4. MIDDLE EAST (Strait of Hormuz Region)
    # ==========================================
    "SAU": "Saudi Arabia",
    "ARE": "UAE",
    "QAT": "Qatar",
    "KWT": "Kuwait",
    "OMN": "Oman",
    "BHR": "Bahrain",
    "IRQ": "Iraq"
}




us_economy_metrics = {
    'CPI_All_Items': 'CPIAUCSL',         # Monthly
    'Unemployment_Rate': 'UNRATE',       # Monthly
    'Total_Nonfarm_Payrolls': 'PAYEMS',  # Monthly
    'Real_GDP': 'GDPC1',                 # Quarterly
    'Manufacturing_Investment': 'C307RX1Q020SBEA' # Quarterly
}

dict_3_char = {
    # SOUTHEAST ASIA
    "VNM": "Vietnam", "THA": "Thailand", "SGP": "Singapore", "IDN": "Indonesia",
    "MYS": "Malaysia", "PHL": "Philippines", "BRN": "Brunei", "KHM": "Cambodia",
    "LAO": "Laos", "MMR": "Myanmar", "TLS": "Timor-Leste",
    # EUROPE
    "DEU": "Germany", "FRA": "France", "ITA": "Italy", "ESP": "Spain",
    "NLD": "Netherlands", "GBR": "United Kingdom", "BEL": "Belgium", "AUT": "Austria",
    "PRT": "Portugal", "GRC": "Greece", "FIN": "Finland", "IRL": "Ireland",
    "DNK": "Denmark", "SWE": "Sweden", "POL": "Poland", "CZE": "Czechia",
    "ROU": "Romania", "HUN": "Hungary", "VAT": "Holy See (Vatican City State)",
    "UKR": "Ukraine", "TUR": "Türkiye", "XKX": "Kosovo, Republic of",
    # AMERICAS
    "USA": "United States", "CAN": "Canada", "MEX": "Mexico", "BRA": "Brazil",
    "ARG": "Argentina", "COL": "Colombia", "SUR": "Suriname", "SLV": "El Salvador",
    "SXM": "Sint Maarten (Dutch part)", "TTO": "Trinidad and Tobago", "URY": "Uruguay",
    "VCT": "Saint Vincent and the Grenadines", "VEN": "Venezuela, Bolivarian Republic",
    # MIDDLE EAST & CENTRAL ASIA
    "SAU": "Saudi Arabia", "ARE": "UAE", "QAT": "Qatar", "KWT": "Kuwait",
    "OMN": "Oman", "BHR": "Bahrain", "IRQ": "Iraq", "SYR": "Syrian Arab Republic",
    "TJK": "Tajikistan", "TKM": "Turkmenistan", "UZB": "Uzbekistan", "YEM": "Yemen",
    # AFRICA
    "SEN": "Senegal", "SOM": "Somalia", "SSD": "South Sudan", "SWZ": "Eswatini",
    "TCD": "Chad", "TGO": "Togo", "TUN": "Tunisia", "TZA": "Tanzania, United Republic of",
    "UGA": "Uganda", "ZAF": "South Africa", "ZMB": "Zambia", "ZWE": "Zimbabwe",
    # OCEANIA & OTHERS / REGIONAL
    "TON": "Tonga", "TUV": "Tuvalu", "VUT": "Vanuatu", "WSM": "Samoa",
    "STP": "Sao Tome and Principe", "SUN": "Former U.S.S.R.",
    # ADDITIONAL COUNTRIES (IMF PI / Industrial Production dataset coverage)
    "AUS": "Australia", "BGR": "Bulgaria", "BIH": "Bosnia and Herzegovina", "BRB": "Barbados",
    "CHL": "Chile", "CHN": "China", "CIV": "Côte d'Ivoire", "CRI": "Costa Rica",
    "CYP": "Cyprus", "ECU": "Ecuador", "EGY": "Egypt", "EST": "Estonia",
    "HRV": "Croatia", "IND": "India", "ISL": "Iceland", "JOR": "Jordan",
    "JPN": "Japan", "KAZ": "Kazakhstan", "KGZ": "Kyrgyz Republic", "KOR": "Korea, Republic of",
    "LTU": "Lithuania", "LUX": "Luxembourg", "MKD": "North Macedonia", "MNE": "Montenegro",
    "MWI": "Malawi", "NOR": "Norway", "PRY": "Paraguay", "RUS": "Russian Federation",
    "SRB": "Serbia", "SVK": "Slovak Republic", "SVN": "Slovenia", "TWN": "Taiwan Province of China"
}

# 2. Dictionary 2-character (Alpha-2)
dict_2_char = {
    # SOUTHEAST ASIA
    "VN": "Viet Nam", "TH": "Thailand", "SG": "Singapore", "ID": "Indonesia",
    "MY": "Malaysia", "PH": "Philippines", "BN": "Brunei", "KH": "Cambodia",
    "LA": "Laos", "MM": "Myanmar", "TL": "Timor-Leste",
    # EUROPE
    "DE": "Germany", "FR": "France", "IT": "Italy", "ES": "Spain",
    "NL": "Netherlands", "GB": "United Kingdom", "BE": "Belgium", "AT": "Austria",
    "PT": "Portugal", "GR": "Greece", "FI": "Finland", "IE": "Ireland",
    "DK": "Denmark", "SE": "Sweden", "PL": "Poland", "CZ": "Czechia",
    "RO": "Romania", "HU": "Hungary", "VA": "Holy See (Vatican City State)",
    "UA": "Ukraine", "TR": "Türkiye", "XK": "Kosovo, Republic of",
    # AMERICAS
    "US": "United States", "CA": "Canada", "MX": "Mexico", "BR": "Brazil",
    "AR": "Argentina", "CO": "Colombia", "SR": "Suriname", "SV": "El Salvador",
    "SX": "Sint Maarten (Dutch part)", "TT": "Trinidad and Tobago", "UY": "Uruguay",
    "VC": "Saint Vincent and the Grenadines", "VE": "Venezuela, Bolivarian Republic",
    # MIDDLE EAST & CENTRAL ASIA
    "SA": "Saudi Arabia", "AE": "UAE", "QA": "Qatar", "KW": "Kuwait",
    "OM": "Oman", "BH": "Bahrain", "IQ": "Iraq", "SY": "Syrian Arab Republic",
    "TJ": "Tajikistan", "TM": "Turkmenistan", "UZ": "Uzbekistan", "YE": "Yemen",
    # AFRICA
    "SN": "Senegal", "SO": "Somalia", "SS": "South Sudan", "SZ": "Eswatini",
    "TD": "Chad", "TG": "Togo", "TN": "Tunisia", "TZ": "Tanzania, United Republic of",
    "UG": "Uganda", "ZA": "South Africa", "ZM": "Zambia", "ZW": "Zimbabwe",
    # OCEANIA & OTHERS / REGIONAL
    "TO": "Tonga", "TV": "Tuvalu", "VU": "Vanuatu", "WS": "Samoa",
    "ST": "Sao Tome and Principe", "SUH": "Former U.S.S.R.",
    # ADDITIONAL COUNTRIES (IMF PI / Industrial Production dataset coverage)
    "AU": "Australia", "BG": "Bulgaria", "BA": "Bosnia and Herzegovina", "BB": "Barbados",
    "CL": "Chile", "CN": "China", "CI": "Côte d'Ivoire", "CR": "Costa Rica",
    "CY": "Cyprus", "EC": "Ecuador", "EG": "Egypt", "EE": "Estonia",
    "HR": "Croatia", "IN": "India", "IS": "Iceland", "JO": "Jordan",
    "JP": "Japan", "KZ": "Kazakhstan", "KG": "Kyrgyz Republic", "KR": "Korea, Republic of",
    "LT": "Lithuania", "LU": "Luxembourg", "MK": "North Macedonia", "ME": "Montenegro",
    "MW": "Malawi", "NO": "Norway", "PY": "Paraguay", "RU": "Russian Federation",
    "RS": "Serbia", "SK": "Slovak Republic", "SI": "Slovenia", "TW": "Taiwan Province of China"
}