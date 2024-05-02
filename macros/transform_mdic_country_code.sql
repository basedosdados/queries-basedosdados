{% macro transform_mdic_country_code(column_name) %}
    -- mdic stands for: Ministério do Desenvolvimento, Indústria, Comércio e Serviços
    -- it has its own country ids that can be found here:
    -- https://balanca.economia.gov.br/balanca/bd/tabelas/PAIS.csv
    -- NOTE:
    -- '' values were set beacause the original matches were ZZZ which
    -- indicates the inexistance of the code
    case
        {% for old_code, new_code in [
            (0, null),
            (13, "AFG"),
            (15, "ALA"),
            (17, "ALB"),
            (20, "ESP"),
            (23, "DEU"),
            (25, "DEU"),
            (31, "BFA"),
            (37, "AND"),
            (40, "AGO"),
            (41, "AIA"),
            (42, "ATA"),
            (43, "ATG"),
            (47, "ANT"),
            (53, "SAU"),
            (59, "DZA"),
            (63, "ARG"),
            (64, "ARM"),
            (65, "ABW"),
            (69, "AUS"),
            (72, "AUT"),
            (73, "AZE"),
            (77, "BHS"),
            (80, "BHR"),
            (81, "BGD"),
            (83, "BRB"),
            (85, "BLR"),
            (87, "BEL"),
            (88, "BLZ"),
            (90, "BMU"),
            (93, "MMR"),
            (97, "BOL"),
            (98, "BIH"),
            (99, "BES"),
            (100, "BRA"),
            (101, "BWA"),
            (102, "BVT"),
            (105, "BRA"),
            (108, "BRN"),
            (111, "BGR"),
            (115, "BDI"),
            (119, "BTN"),
            (127, "CPV"),
            (137, "CYM"),
            (141, "KHM"),
            (145, "CMR"),
            (149, "CAN"),
            (150, "GGY"),
            (151, "ESP"),
            (152, "GBR"),
            (153, "KAZ"),
            (154, "QAT"),
            (158, "CHL"),
            (160, "CHN"),
            (161, "TWN"),
            (163, "CYP"),
            (165, "CCK"),
            (169, "COL"),
            (173, "COM"),
            (177, "COG"),
            (183, "COK"),
            (187, "PRK"),
            (190, "KOR"),
            (193, "CIV"),
            (195, "HRV"),
            (196, "CRI"),
            (198, "KWT"),
            (199, "CUB"),
            (200, "CUW"),
            (229, "BEN"),
            (232, "DNK"),
            (235, "DMA"),
            (237, "ARE"),
            (239, "ECU"),
            (240, "EGY"),
            (243, "ERI"),
            (244, "ARE"),
            (245, "ESP"),
            (246, "SVN"),
            (247, "SVK"),
            (249, "USA"),
            (251, "EST"),
            (253, "ETH"),
            (255, "FLK"),
            (259, "FRO"),
            (267, "PHL"),
            (271, "FIN"),
            (275, "FRA"),
            (281, "GAB"),
            (285, "GMB"),
            (289, "GHA"),
            (291, "GEO"),
            (292, "SGS"),
            (293, "GIB"),
            (297, "GRD"),
            (301, "GRC"),
            (305, "GRL"),
            (309, "GLP"),
            (313, "GUM"),
            (317, "GTM"),
            (321, "GGY"),
            (325, "GUF"),
            (329, "GIN"),
            (331, "GNQ"),
            (334, "GNB"),
            (337, "GUY"),
            (341, "HTI"),
            (343, "HMD"),
            (345, "HND"),
            (351, "HKG"),
            (355, "HUN"),
            (357, "YEM"),
            (358, "YMD"),
            (359, "IMN"),
            (361, "IND"),
            (365, "IDN"),
            (367, "GBR"),
            (369, "IRQ"),
            (372, "IRN"),
            (375, "IRL"),
            (379, "ISL"),
            (383, "ISR"),
            (386, "ITA"),
            (388, "SCG"),
            (391, "JAM"),
            (393, null),
            (396, "USA"),
            (399, "JPN"),
            (403, "JOR"),
            (411, "KIR"),
            (420, "LAO"),
            (423, "MYS"),
            (426, "LSO"),
            (427, "LVA"),
            (431, "LBN"),
            (434, "LBR"),
            (438, "LBY"),
            (440, "LIE"),
            (442, "LTU"),
            (445, "LUX"),
            (447, "MAC"),
            (449, "MKD"),
            (450, "MDG"),
            (452, "PRT"),
            (455, "MYS"),
            (458, "MWI"),
            (461, "MDV"),
            (464, "MLI"),
            (467, "MLT"),
            (472, "MNP"),
            (474, "MAR"),
            (476, "MHL"),
            (477, "MTQ"),
            (485, "MUS"),
            (488, "MRT"),
            (489, null),
            (490, "UMI"),
            (493, "MEX"),
            (494, "MDA"),
            (495, "MCO"),
            (497, "MNG"),
            (498, "MNE"),
            (499, "FSM"),
            (501, "MSR"),
            (505, "MOZ"),
            (507, "NAM"),
            (508, "NRU"),
            (511, "CXR"),
            (517, "NPL"),
            (521, "NIC"),
            (525, "NER"),
            (528, "NGA"),
            (531, "NIU"),
            (535, "NFK"),
            (538, "NOR"),
            (542, "NCL"),
            (545, "PNG"),
            (548, "NZL"),
            (551, "VUT"),
            (556, "OMN"),
            (563, "UMI"),
            (566, "UMI"),
            (569, "UMI"),
            (573, "NLD"),
            (575, "PLW"),
            (576, "PAK"),
            (578, "PSE"),
            (580, "PAN"),
            (583, "PNG"),
            (586, "PRY"),
            (589, "PER"),
            (593, "PCN"),
            (599, "PYF"),
            (603, "POL"),
            (607, "PRT"),
            (611, "PRI"),
            (623, "KEN"),
            (625, "KGZ"),
            (628, "GBR"),
            (640, "CAF"),
            (647, "DOM"),
            (660, "REU"),
            (665, "ZWE"),
            (670, "ROU"),
            (675, "RWA"),
            (676, "RUS"),
            (677, "SLB"),
            (678, "KNA"),
            (685, "ESH"),
            (687, "SLV"),
            (690, "WSM"),
            (691, "ASM"),
            (693, "BLM"),
            (695, "KNA"),
            (697, "SMR"),
            (698, null),
            (699, "SXM"),
            (700, "SPM"),
            (705, "VCT"),
            (710, "SHN"),
            (715, "LCA"),
            (720, "STP"),
            (728, "SEN"),
            (731, "SYC"),
            (735, "SLE"),
            (737, "SRB"),
            (741, "SGP"),
            (744, "SYR"),
            (748, "SOM"),
            (750, "LKA"),
            (754, "SWZ"),
            (755, "SJM"),
            (756, "ZAF"),
            (759, "SDN"),
            (760, "SSD"),
            (764, "SWE"),
            (767, "CHE"),
            (770, "SUR"),
            (772, "TJK"),
            (776, "THA"),
            (780, "TZA"),
            (781, "ATF"),
            (782, "IOT"),
            (783, "DJI"),
            (785, null),
            (786, "GBR"),
            (788, "TCD"),
            (790, "CSK"),
            (791, "CZE"),
            (795, "TLS"),
            (800, "TGO"),
            (805, "TKL"),
            (810, "TON"),
            (815, "TTO"),
            (820, "TUN"),
            (823, "TCA"),
            (824, "TKM"),
            (827, "TUR"),
            (828, "TUV"),
            (831, "UKR"),
            (833, "UGA"),
            (840, null),
            (845, "URY"),
            (847, "UZB"),
            (848, "VAT"),
            (850, "VEN"),
            (858, "VNM"),
            (863, "VGB"),
            (866, "VIR"),
            (870, "FJI"),
            (873, "USA"),
            (875, "WLF"),
            (888, "COD"),
            (890, "ZMB"),
            (895, "PCZ"),
            (990, null),
            (994, null),
            (995, null),
            (997, null),
            (998, null),
            (999, null),
        ] %}
            when {{ column_name }} = '{{ old_code }}' then '{{ new_code }}'
        {% endfor %}
        else {{ column_name }}
    end

{% endmacro %}