import requests
import xmltodict
import json
import os
import re
from datetime import datetime
from unidecode import unidecode
from typing import Dict, List, Any, Optional, Union, Tuple
from abc import ABC, abstractmethod

# =================== CONFIGURAÇÕES GLOBAIS =======================

JSON_FILE = "data.json"

# =================== MAPEAMENTOS DE VEÍCULOS =======================

MAPEAMENTO_CATEGORIAS = {}
OPCIONAL_CHAVE_HATCH = "limpador traseiro"

# --- Listas de Modelos por Categoria ---

hatch_models = ["308", "IX35", "A 200", "a200", "joy", "gol", "uno", "palio", "celta", "march", "sandero", "i30", "golf", "fox", "up", "fit", "etios", "bravo", "punto", "208", "argo", "mobi", "c3", "picanto", "stilo", "c4 vtr", "kwid", "soul", "agile", "fusca", "a1", "new beetle", "116i", "118i", "120i", "125i", "m135i", "m140i"]
for model in hatch_models: MAPEAMENTO_CATEGORIAS[model] = "Hatch"

sedan_models = ["A5", "430i", "civic", "a6", "sentra", "jetta", "voyage", "siena", "grand siena", "cobalt", "logan", "fluence", "cerato", "elantra", "virtus", "accord", "altima", "fusion", "passat", "vectra sedan", "classic", "cronos", "linea", "408", "c4 pallas", "bora", "hb20s", "lancer", "camry", "onix plus", "azera", "malibu", "318i", "320d", "320i", "328i", "330d", "330i", "335i", "520d", "528i", "530d", "530i", "535i", "540i", "550i", "740i", "750i", "c180", "c200", "c250", "c300", "e250", "e350", "m3", "m5", "s4", "classe c", "classe e", "classe s", "eqe", "eqs"]
for model in sedan_models: MAPEAMENTO_CATEGORIAS[model] = "Sedan"

hatch_sedan_models = ["320iA", "onix", "hb20", "yaris", "city", "a3", "corolla", "focus", "fiesta", "corsa", "astra", "vectra", "cruze", "clio", "megane", "206", "207", "307", "tiida", "ka", "versa", "prisma", "polo", "c4", "sonic", "série 1", "série 2", "série 3", "série 4", "série 5", "série 6", "série 7", "classe a", "cla"]
for model in hatch_sedan_models: MAPEAMENTO_CATEGORIAS[model] = "hatch,sedan"

suv_models = ["Captiva", "Discovery", "Macan", "JOURNEY", "XC90", "xc60", "tiggo", "edge", "outlander", "range rover evoque", "song plus", "duster", "ecosport", "hrv", "hr-v", "COMPASS", "compass", "renegade", "tracker", "kicks", "captur", "creta", "tucson", "santa fe", "sorento", "sportage", "pajero", "tr4", "aircross", "tiguan", "t-cross", "tcross", "rav4", "land cruiser", "cherokee", "grand cherokee", "trailblazer", "pulse", "fastback", "territory", "bronco sport", "2008", "3008", "5008", "c4 cactus", "taos", "crv", "cr-v", "corolla cross", "hilux sw4", "sw4", "pajero sport", "commander", "nivus", "equinox", "x1", "x2", "x3", "x4", "x5", "x6", "x7", "ix", "ix1", "ix2", "ix3", "gla", "glb", "glc", "gle", "gls", "classe g", "eqa", "eqb", "eqc", "q2", "q3", "q5", "q7", "q8", "q6 e-tron", "e-tron", "q4 e-tron", "q4etron", "wrx", "xv"]
for model in suv_models: MAPEAMENTO_CATEGORIAS[model] = "SUV"

caminhonete_models = ["Silverado", "F-1000", "F1000", "duster oroch", "d20", "hilux", "ranger", "s10", "s-10", "L200 Triton", "l200", "triton", "toro", "frontier", "amarok", "maverick", "montana", "ram 1500", "rampage", "f-250", "f250", "courier", "dakota", "gladiator", "hoggar"]
for model in caminhonete_models: MAPEAMENTO_CATEGORIAS[model] = "Caminhonete"

utilitario_models = ["saveiro", "strada", "oroch", "kangoo", "partner", "doblo", "fiorino", "kombi", "doblo cargo", "berlingo", "combo", "express", "hr"]
for model in utilitario_models: MAPEAMENTO_CATEGORIAS[model] = "Utilitário"

furgao_models = ["boxer", "daily", "ducato", "expert", "jumper", "jumpy", "master", "scudo", "sprinter", "trafic", "transit", "vito"]
for model in furgao_models: MAPEAMENTO_CATEGORIAS[model] = "Furgão"

coupe_models = ["911", "718", "370z", "brz", "camaro", "challenger", "corvette", "gt86", "mustang", "r8", "rcz", "rx8", "supra", "tt", "tts", "veloster", "m2", "m4", "m8", "s5", "amg gt"]
for model in coupe_models: MAPEAMENTO_CATEGORIAS[model] = "Coupe"

conversivel_models = ["911 cabrio", "beetle cabriolet", "boxster", "eos", "miata", "mini cabrio", "slk", "z4", "série 8", "slc", "sl"]
for model in conversivel_models: MAPEAMENTO_CATEGORIAS[model] = "Conversível"

station_wagon_models = ["a4 avant", "fielder", "golf variant", "palio weekend", "parati", "quantum", "spacefox", "rs2", "rs4", "rs6"]
for model in station_wagon_models: MAPEAMENTO_CATEGORIAS[model] = "Station Wagon"

minivan_models = ["caravan", "carnival", "grand c4", "idea", "livina", "meriva", "picasso", "scenic", "sharan", "spin", "touran", "xsara picasso", "zafira", "série 2 active tourer", "classe b", "classe t", "classe r", "classe v"]
for model in minivan_models: MAPEAMENTO_CATEGORIAS[model] = "Minivan"

offroad_models = ["T4", "bandeirante", "bronco", "defender", "grand vitara", "jimny", "samurai", "troller", "wrangler"]
for model in offroad_models: MAPEAMENTO_CATEGORIAS[model] = "Off-road"

# =================== MAPEAMENTOS DE MOTOCICLETAS =======================

# Mapeamento combinado: cilindrada e categoria
MAPEAMENTO_MOTOS = {
    # Street/Urbanas (commuter básicas e econômicas)
    
    "DK 150": (150, "street"),
    "SPEED 400": (400, "street"),
    "FAN Flex": (160, "street"),    
    "FZ15 150": (150, "street"),
    "xy 150": (150, "street"),
    "cg 150 sport": (150, "street"),
    "YS 150 FAZER": (150, "street"),
    "dk 160": (160, "street"),
    "cg 150 titan": (150, "street"),
    "cg150 titan": (150, "street"),  # Variação sem espaço
    "cg 160 titan": (160, "street"),
    "cg160 titan": (160, "street"),  # Variação sem espaço
    "cg 125": (125, "street"),
    "cg125": (125, "street"),  # Variação sem espaço
    "cg 160": (160, "street"),
    "cg160": (160, "street"),  # Variação sem espaço
    "cg 160 fan": (160, "street"),
    "cg160 fan": (160, "street"),  # Variação sem espaço
    "cg 160 start": (160, "street"),
    "cg160 start": (160, "street"),  # Variação sem espaço
    "cg 160 titan s": (160, "street"),
    "cg160 titan s": (160, "street"),  # Variação sem espaço
    "cg 125 fan ks": (125, "street"),
    "cg125 fan ks": (125, "street"),  # Variação sem espaço
    "cg150 fan": (150, "street"),
    "cg 150 fan": (150, "street"),
    "cg 150 fan esdi": (150, "street"),
    "cg150 titan": (150, "street"),
    "ybr 150": (150, "street"),
    "ybr150": (150, "street"),  # Variação sem espaço
    "ybr 125": (125, "street"),
    "ybr125": (125, "street"),  # Variação sem espaço
    "factor 125": (125, "street"),
    "factor125": (125, "street"),  # Variação sem espaço
    "factor 150": (150, "street"),
    "factor150": (150, "street"),  # Variação sem espaço
    "fz25": (250, "street"),
    "fz 25": (250, "street"),
    "fz25 fazer": (250, "street"),
    "fz 25 fazer": (250, "street"),
    "fz15 fazer": (150, "street"),
    "fz 15 fazer": (150, "street"),
    "fazer 150": (150, "street"),
    "fazer150": (150, "street"),  # Variação sem espaço
    "fazer 250": (250, "street"),
    "fazer250": (250, "street"),  # Variação sem espaço
    "ys 250": (250, "street"),
    "ys250": (250, "street"),  # Variação sem espaço
    "cb 300": (300, "street"),
    "cb300": (300, "street"),  # Variação sem espaço
    "cb twister": (300, "street"),
    "twister": (300, "street"),
    "fz6": (150, "street"),
    
    # Scooter (transmissão automática, design step-through)
    
    "SH 300": (300, "scooter"),
    "lead 110": (110, "scooter"),
    "biz 125": (125, "scooter"),
    "jet 50": (50, "scooter"),
    "jl 50": (50, "scooter"),
    "xy 125": (125, "scooter"),
    "adv 150": (150, "scooter"),
    "biz125": (125, "scooter"),  # Variação sem espaço
    "biz 125 es": (125, "scooter"),
    "biz125 es": (125, "scooter"),  # Variação sem espaço
    "biz 110": (110, "scooter"),
    "biz110": (110, "scooter"),  # Variação sem espaço
    "biz es": (125, "scooter"),
    "biz ex": (125, "scooter"),    
    "pop 110": (110, "scooter"),
    "pop110": (110, "scooter"),  # Variação sem espaço
    "pop 110i": (110, "scooter"),
    "pop110i": (110, "scooter"),  # Variação sem espaço
    "pcx 150": (150, "scooter"),
    "pcx150": (150, "scooter"),  # Variação sem espaço
    "pcx 160": (160, "scooter"),
    "pcx160": (160, "scooter"),  # Variação sem espaço
    "elite 125": (125, "scooter"),
    "elite125": (125, "scooter"),  # Variação sem espaço
    "nmax 160": (160, "scooter"),
    "nmax160": (160, "scooter"),  # Variação sem espaço
    "xmax 250": (250, "scooter"),
    "xmax250": (250, "scooter"),  # Variação sem espaço
    "burgman 125": (125, "scooter"),
    "burgman125": (125, "scooter"),  # Variação sem espaço
    "dafra citycom 300": (300, "scooter"),
    "citycom": (300, "scooter"),
    
    # Trail/Offroad (dual-sport, suspensão robusta)
    "nxr 150 bros": (150, "trail"),
    "shi 175": (150, "trail"),
    "nxr150 bros": (150, "trail"),  # Variação sem espaço
    "nxr 160": (160, "trail"),
    "nxr160": (160, "trail"),  # Variação sem espaço
    "bros 160": (160, "trail"),
    "bros160": (160, "trail"),  # Variação sem espaço
    "nxr 160 bros": (160, "trail"),
    "nxr160 bros": (160, "trail"),  # Variação sem espaço
    "xre 190": (190, "trail"),
    "xre190": (190, "trail"),  # Variação sem espaço
    "xre 300": (300, "trail"),
    "xre300": (300, "trail"),  # Variação sem espaço
    "xre 300 sahara": (300, "trail"),
    "xre300 sahara": (300, "trail"),  # Variação sem espaço
    "sahara 300": (300, "trail"),
    "sahara300": (300, "trail"),  # Variação sem espaço
    "sahara 300 rally": (300, "trail"),
    "sahara300 rally": (300, "trail"),  # Variação sem espaço
    "xr300l tornado": (300, "trail"),
    "xr 300l tornado": (300, "trail"),
    "crf 230f": (230, "offroad"),
    "crf230f": (230, "offroad"),  # Variação sem espaço
    "dr 160": (160, "trail"),
    "dr160": (160, "trail"),  # Variação sem espaço
    "dr 160 s": (160, "trail"),
    "dr160 s": (160, "trail"),  # Variação sem espaço
    "xtz 150": (150, "trail"),
    "xtz150": (150, "trail"),  # Variação sem espaço
    "xtz 250": (250, "trail"),
    "xtz250": (250, "trail"),  # Variação sem espaço
    "xtz 250 tenere": (250, "trail"),
    "xtz250 tenere": (250, "trail"),  # Variação sem espaço
    "tenere 250": (250, "trail"),
    "tenere250": (250, "trail"),  # Variação sem espaço
    "lander 250": (250, "trail"),
    "lander250": (250, "trail"),  # Variação sem espaço
    "falcon": (400, "trail"),
    "dl160": (160, "trail"),
    
    # BigTrail/Adventure (alta cilindrada, touring)
    "cb 500x": (500, "bigtrail"),   
    "tiger 660": (660, "trail"),
    "DL 650 ": (650, "bigtrail"),
    "DL 650 XT": (650, "bigtrail"),
    "R 1200 GS": (1200, "bigtrail"),
    "DL 1000": (1000, "bigtrail"),
    "PAN AMERICA 1250": (1250, "bigtrail"),
    "crf 1100l": (1100, "bigtrail"),
    "crf 1100l": (1100, "bigtrail"),
    "NC 750": (750, "bigtrail"),
    "crf1100l": (1100, "bigtrail"),
    "g 310": (300, "bigtrail"),
    "g310": (300, "bigtrail"),  # Variação sem espaço
    "g 310 gs": (300, "bigtrail"),
    "g310 gs": (300, "bigtrail"),  # Variação sem espaço
    "f 750 gs": (850, "bigtrail"),
    "f750 gs": (850, "bigtrail"),  # Variação sem espaço
    "f 850 gs": (850, "bigtrail"),
    "f850 gs": (850, "bigtrail"),  # Variação sem espaço
    "f 900": (900, "bigtrail"),
    "f900": (900, "bigtrail"),  # Variação sem espaço
    "f 900 gs": (900, "bigtrail"),
    "f900 gs": (900, "bigtrail"),  # Variação sem espaço
    "r 1250": (1250, "bigtrail"),
    "r1250": (1250, "bigtrail"),  # Variação sem espaço
    "r 1250 gs": (1250, "bigtrail"),
    "r1250 gs": (1250, "bigtrail"),  # Variação sem espaço
    "r 1300": (1300, "bigtrail"),
    "r1300": (1300, "bigtrail"),  # Variação sem espaço
    "r 1300 gs": (1300, "bigtrail"),
    "r1300 gs": (1300, "bigtrail"),  # Variação sem espaço
    "g 650 gs": (650, "bigtrail"),
    "g650 gs": (650, "bigtrail"),  # Variação sem espaço
    "versys 300": (300, "bigtrail"),
    "versys300": (300, "bigtrail"),  # Variação sem espaço
    "versys 650": (650, "bigtrail"),
    "versys650": (650, "bigtrail"),  # Variação sem espaço
    "versys-x 300": (300, "bigtrail"),
    "versysx 300": (300, "bigtrail"),  # Variação sem hífen
    "tiger 800": (800, "bigtrail"),
    "tiger800": (800, "bigtrail"),  # Variação sem espaço
    "tiger 900": (900, "bigtrail"),
    "tiger900": (900, "bigtrail"),  # Variação sem espaço
    "himalayan": (400, "bigtrail"),
    "700 x": (700, "bigtrail"),
    "TIGER 1200": (1200, "bigtrail"),
    
    # Esportiva Carenada (supersport, carenagem completa)
    "GSX-R 1000": (1000, "esportiva carenada"),
    "s 1000 rr": (1000, "esportiva carenada"),
    "cbr 250": (250, "esportiva carenada"),
    "cbr250": (250, "esportiva carenada"),  # Variação sem espaço
    "cbr 300": (300, "esportiva carenada"),
    "cbr300": (300, "esportiva carenada"),  # Variação sem espaço
    "cbr 500": (500, "esportiva carenada"),
    "cbr500": (500, "esportiva carenada"),  # Variação sem espaço
    "cbr 600": (600, "esportiva carenada"),
    "cbr600": (600, "esportiva carenada"),  # Variação sem espaço
    "cbr 650": (650, "esportiva carenada"),
    "cbr650": (650, "esportiva carenada"),  # Variação sem espaço
    "cbr 1000": (1000, "esportiva carenada"),
    "cbr1000": (1000, "esportiva carenada"),  # Variação sem espaço
    "cbr 1000r": (1000, "esportiva carenada"),
    "cbr1000r": (1000, "esportiva carenada"),  # Variação sem espaço
    "yzf r3": (300, "esportiva carenada"),
    "yzf r-3": (300, "esportiva carenada"),
    "yzf r-6": (600, "esportiva carenada"),
    "r15": (150, "esportiva carenada"),
    "r1": (1000, "esportiva carenada"),
    "ninja 300": (300, "esportiva carenada"),
    "ninja300": (300, "esportiva carenada"),  # Variação sem espaço
    "ninja 400": (400, "esportiva carenada"),
    "ninja400": (400, "esportiva carenada"),  # Variação sem espaço
    "ninja 650": (650, "esportiva carenada"),
    "ninja650": (650, "esportiva carenada"),  # Variação sem espaço
    "ninja 1000": (1050, "esportiva carenada"),
    "ninja1000": (1050, "esportiva carenada"),  # Variação sem espaço
    "ninja zx-10r": (1000, "esportiva carenada"),
    "ninja zx10r": (1000, "esportiva carenada"),  # Variação sem hífen
    "s 1000": (1000, "esportiva carenada"),
    "s1000": (1000, "esportiva carenada"),  # Variação sem espaço
    "s 1000 rr": (1000, "esportiva carenada"),
    "s1000 rr": (1000, "esportiva carenada"),  # Variação sem espaço
    "panigale v2": (950, "esportiva carenada"),
    "panigale v4": (1100, "esportiva carenada"),
    "hayabusa": (1350, "esportiva carenada"),
    
    # Esportiva Naked (naked sport, sem carenagem)
    "Z 400": (1000, "esportiva naked"),    
    "310 R": (1000, "esportiva naked"),
    "Z 1000": (1000, "esportiva naked"),
    "mt 03": (300, "esportiva naked"),
    "mt-03": (300, "esportiva naked"),
    "mt03": (300, "esportiva naked"),
    "mt 07": (690, "esportiva naked"),
    "mt-07": (690, "esportiva naked"),
    "mt07": (690, "esportiva naked"),  # Variação sem hífen
    "mt 09": (890, "esportiva naked"),
    "mt-09": (890, "esportiva naked"),
    "mt09": (890, "esportiva naked"),  # Variação sem hífen
    "cb 500": (500, "esportiva naked"),
    "cb500": (500, "esportiva naked"),  # Variação sem espaço
    "cb 650": (650, "esportiva naked"),
    "cb650": (650, "esportiva naked"),  # Variação sem espaço
    "cb 1000r": (1000, "esportiva naked"),
    "cb1000r": (1000, "esportiva naked"),  # Variação sem espaço
    "hornet 600": (600, "esportiva naked"),
    "hornet600": (600, "esportiva naked"),  # Variação sem espaço
    "cb 600f": (600, "esportiva naked"),
    "cb600f": (600, "esportiva naked"),  # Variação sem espaço
    "xj6": (600, "esportiva naked"),
    "z300": (300, "esportiva naked"),
    "z400": (400, "esportiva naked"),
    "z650": (650, "esportiva naked"),
    "z750": (750, "esportiva naked"),
    "z800": (800, "esportiva naked"),
    "z900": (950, "esportiva naked"),
    "z1000": (1000, "esportiva naked"),
    "er6n": (650, "esportiva naked"),
    "er-6n": (650, "esportiva naked"),
    "bandit 600": (600, "esportiva naked"),
    "bandit600": (600, "esportiva naked"),  # Variação sem espaço
    "bandit 650": (650, "esportiva naked"),
    "bandit650": (650, "esportiva naked"),  # Variação sem espaço
    "bandit 1250": (1250, "esportiva naked"),
    "bandit1250": (1250, "esportiva naked"),  # Variação sem espaço
    "gsx 650f": (650, "esportiva naked"),
    "gsx650f": (650, "esportiva naked"),  # Variação sem espaço
    "gsx-s 750": (750, "esportiva naked"),
    "gsxs 750": (750, "esportiva naked"),  # Variação sem hífen
    "gsx-s 1000": (1000, "esportiva naked"),
    "gsxs 1000": (1000, "esportiva naked"),  # Variação sem hífen
    "gixxer 250": (250, "esportiva naked"),
    "gixxer250": (250, "esportiva naked"),  # Variação sem espaço
    "gs500": (500, "esportiva naked"),
    "monster 797": (800, "esportiva naked"),
    "monster797": (800, "esportiva naked"),  # Variação sem espaço
    "monster 821": (820, "esportiva naked"),
    "monster821": (820, "esportiva naked"),  # Variação sem espaço
    "monster 937": (940, "esportiva naked"),
    "monster937": (940, "esportiva naked"),  # Variação sem espaço
    "street triple": (750, "esportiva naked"),
    "speed triple": (1050, "esportiva naked"),
    "trident 660": (660, "esportiva naked"),
    "trident660": (660, "esportiva naked"),  # Variação sem espaço
    
    # Custom/Cruiser (posição relaxada, estética clássica)
    "FAT BOY": (1690, "custom"),
    "NIGHTSTER SPECIAL": (975, "custom"),
    "iron 883": (883, "custom"),
    "v-rod": (1250, "custom"),
    "iron883": (883, "custom"),  # Variação sem espaço
    "forty eight": (1200, "custom"),
    "sportster s": (1250, "custom"),
    "fat bob": (1140, "custom"),
    "meteor 350": (350, "custom"),
    "meteor350": (350, "custom"),  # Variação sem espaço
    "classic 350": (350, "custom"),
    "classic350": (350, "custom"),  # Variação sem espaço
    "hunter 350": (350, "custom"),
    "hunter350": (350, "custom"),  # Variação sem espaço
    "interceptor 650": (650, "custom"),
    "interceptor650": (650, "custom"),  # Variação sem espaço
    "continental gt 650": (650, "custom"),
    "continental gt650": (650, "custom"),  # Variação sem espaço
    "diavel 1260": (1260, "custom"),
    "diavel1260": (1260, "custom"),  # Variação sem espaço
    "r 18": (1800, "custom"),
    "r18": (1800, "custom"),  # Variação sem espaço
    "bonneville": (900, "custom"),
    "mt 01": (1700, "custom"),
    "mt01": (1700, "custom"),
    "Meteor Supernova": (350, "custom"),
    "VT 600 ": (600, "custom"),# Variação sem espaço
    
    # Touring (longas distâncias, conforto)
    "ELECTRA GLIDE ULTRA": (1700, "touring"),
    "GOLD WING 1500": (1500, "touring"),
    "road glide": (2150, "touring"),
    "street glide": (1750, "touring"),
    "k 1300": (1300, "touring"),
    "k1300": (1300, "touring"),  # Variação sem espaço
    "k 1600": (1650, "touring"),
    "k1600": (1650, "touring"),  # Variação sem espaço
    "xt 660": (660, "touring"),
    "xt660": (660, "touring"),  # Variação sem espaço
    "xt 600": (600, "touring"),
    "xt600": (600, "touring"),  # Variação sem espaço
    "HERITAGE": (1690, "touring"),
    
    # ATV/Quadriciclo
    "cforce 1000": (1000, "custom"),
    "cforce1000": (1000, "custom"),  # Variação sem espaço
    "trx 420": (420, "custom"),
    "trx420": (420, "custom"),  # Variação sem espaço
    "t350 x": (350, "custom"),
    "t350x": (350, "custom"),  # Variação sem espaço
    
    # Modelos especiais
    "commander 250": (250, "street"),
    "commander250": (250, "street"),  # Variação sem espaço
    "gk350": (350, "street"),
}

# Mapeamento legado apenas para cilindrada (compatibilidade)
MAPEAMENTO_CILINDRADAS = {modelo: cilindrada for modelo, (cilindrada, _) in MAPEAMENTO_MOTOS.items()}

# =================== UTILS =======================

def normalizar_texto(texto: str) -> str:
    if not texto: return ""
    texto_norm = unidecode(str(texto)).lower()
    texto_norm = re.sub(r'[^a-z0-9\s]', '', texto_norm)
    texto_norm = re.sub(r'\s+', ' ', texto_norm).strip()
    return texto_norm

def definir_categoria_veiculo(modelo: str, opcionais: str = "") -> Optional[str]:
    """
    Define a categoria de um veículo usando busca EXATA no mapeamento.
    Para modelos ambíguos ("hatch,sedan"), usa os opcionais para decidir.
    """
    if not modelo: return None
    
    # Normaliza o modelo do feed para uma busca exata
    modelo_norm = normalizar_texto(modelo)
    
    # Busca pela chave exata no mapeamento
    categoria_result = MAPEAMENTO_CATEGORIAS.get(modelo_norm)
    
    # Se encontrou uma correspondência exata
    if categoria_result:
        if categoria_result == "hatch,sedan":
            opcionais_norm = normalizar_texto(opcionais)
            opcional_chave_norm = normalizar_texto(OPCIONAL_CHAVE_HATCH)
            if opcional_chave_norm in opcionais_norm:
                return "Hatch"
            else:
                return "Sedan"
        else:
            # Para todos os outros casos (SUV, Caminhonete, etc.)
            return categoria_result
            
    # Se não encontrou correspondência exata, verifica os modelos ambíguos
    # Isso é útil para casos como "Onix LTZ" corresponder a "onix"
    for modelo_ambiguo, categoria_ambigua in MAPEAMENTO_CATEGORIAS.items():
        if categoria_ambigua == "hatch,sedan":
            if normalizar_texto(modelo_ambiguo) in modelo_norm:
                opcionais_norm = normalizar_texto(opcionais)
                opcional_chave_norm = normalizar_texto(OPCIONAL_CHAVE_HATCH)
                if opcional_chave_norm in opcionais_norm:
                    return "Hatch"
                else:
                    return "Sedan"
    
    # ADICIONE ESTE BLOCO AQUI:
    # Busca parcial para categorias não ambíguas
    for modelo_mapeado, categoria in MAPEAMENTO_CATEGORIAS.items():
        if categoria != "hatch,sedan":  # Pula os ambíguos que já foram tratados acima
            if normalizar_texto(modelo_mapeado) in modelo_norm:
                return categoria
    
    return None # Nenhuma correspondência encontrada

def inferir_cilindrada_e_categoria_moto(modelo: str, versao: str = "") -> Tuple[Optional[int], Optional[str]]:
   """
   Infere cilindrada e categoria para motocicletas baseado no modelo e versão.
   Busca primeiro no modelo, depois na versão se não encontrar.
   Retorna uma tupla (cilindrada, categoria).
   """
   def buscar_no_texto(texto: str) -> Tuple[Optional[int], Optional[str]]:
       if not texto: 
           return None, None
       
       texto_norm = normalizar_texto(texto)
       
       # Busca exata primeiro
       if texto_norm in MAPEAMENTO_MOTOS:
           cilindrada, categoria = MAPEAMENTO_MOTOS[texto_norm]
           return cilindrada, categoria
       
       # Busca por correspondência parcial - ordena por comprimento (mais específico primeiro)
       matches = []
       for modelo_mapeado, (cilindrada, categoria) in MAPEAMENTO_MOTOS.items():
           modelo_mapeado_norm = normalizar_texto(modelo_mapeado)
           
           # Verifica se o modelo mapeado está contido no texto
           if modelo_mapeado_norm in texto_norm:
               matches.append((modelo_mapeado_norm, cilindrada, categoria, len(modelo_mapeado_norm)))
           
           # Verifica também variações sem espaço (ybr150 vs ybr 150)
           modelo_sem_espaco = modelo_mapeado_norm.replace(' ', '')
           if modelo_sem_espaco in texto_norm:
               matches.append((modelo_sem_espaco, cilindrada, categoria, len(modelo_sem_espaco)))
       
       # Se encontrou correspondências, retorna a mais específica (maior comprimento)
       if matches:
           # Ordena por comprimento decrescente para pegar a correspondência mais específica
           matches.sort(key=lambda x: x[3], reverse=True)
           _, cilindrada, categoria, _ = matches[0]
           return cilindrada, categoria
       
       return None, None
   
   # Busca primeiro no modelo
   cilindrada, categoria = buscar_no_texto(modelo)
   
   # Se não encontrou e tem versão, busca na versão
   if not cilindrada and versao:
       cilindrada, categoria = buscar_no_texto(versao)
   
   # TERCEIRA TENTATIVA: modelo + versao como frase completa
   if not cilindrada and versao:
       cilindrada, categoria = buscar_no_texto(f"{modelo} {versao}")
   
   return cilindrada, categoria

def inferir_cilindrada(modelo: str, versao: str = "") -> Optional[int]:
    """Função legada para compatibilidade - retorna apenas cilindrada"""
    cilindrada, _ = inferir_cilindrada_e_categoria_moto(modelo, versao)
    return cilindrada

def converter_preco(valor: Any) -> float:
    if not valor: return 0.0
    try:
        if isinstance(valor, (int, float)): return float(valor)
        valor_str = str(valor)
        valor_str = re.sub(r'[^\d,.]', '', valor_str).replace(',', '.')
        parts = valor_str.split('.')
        if len(parts) > 2: valor_str = ''.join(parts[:-1]) + '.' + parts[-1]
        return float(valor_str) if valor_str else 0.0
    except (ValueError, TypeError): return 0.0

def safe_get(data: Dict, keys: Union[str, List[str]], default: Any = None) -> Any:
    if isinstance(keys, str): keys = [keys]
    for key in keys:
        if isinstance(data, dict) and key in data and data[key] is not None:
            return data[key]
    return default

def flatten_list(data: Any) -> List[Dict]:
    if not data: return []
    if isinstance(data, list):
        result = []
        for item in data:
            if isinstance(item, dict): result.append(item)
            elif isinstance(item, list): result.extend(flatten_list(item))
        return result
    elif isinstance(data, dict): return [data]
    return []

def normalize_fotos(fotos_data: Any) -> List[str]:
    """
    Normaliza diferentes estruturas de fotos para uma lista simples de URLs.
    
    Entrada aceitas:
    - Lista simples de URLs: ["url1", "url2"]  
    - Lista aninhada: [["url1", "url2"], ["url3"]]
    - Lista de objetos: [{"url": "url1"}, {"IMAGE_URL": "url2"}]
    - Objeto único: {"url": "url1"}
    - String única: "url1"
    
    Retorna sempre: ["url1", "url2", "url3"]
    """
    if not fotos_data:
        return []
    
    result = []
    
    def extract_url_from_item(item):
        """Extrai URL de um item que pode ser string, dict ou outro tipo"""
        if isinstance(item, str):
            return item.strip()
        elif isinstance(item, dict):
            # Tenta várias chaves possíveis para URL
            for key in ["url", "URL", "src", "IMAGE_URL", "path", "link", "href"]:
                if key in item and item[key]:
                    url = str(item[key]).strip()
                    # Remove parâmetros de query se houver
                    return url.split("?")[0] if "?" in url else url
        return None
    
    def process_item(item):
        """Processa um item que pode ser string, lista ou dict"""
        if isinstance(item, str):
            url = extract_url_from_item(item)
            if url:
                result.append(url)
        elif isinstance(item, list):
            # Lista aninhada - processa cada subitem
            for subitem in item:
                process_item(subitem)
        elif isinstance(item, dict):
            url = extract_url_from_item(item)
            if url:
                result.append(url)
    
    # Processa a estrutura principal
    if isinstance(fotos_data, list):
        for item in fotos_data:
            process_item(item)
    else:
        process_item(fotos_data)
    
    # Remove duplicatas e URLs vazias, mantém a ordem
    seen = set()
    normalized = []
    for url in result:
        if url and url not in seen:
            seen.add(url)
            normalized.append(url)
    
    return normalized

# =================== PARSERS =======================

class BaseParser(ABC):
    @abstractmethod
    def can_parse(self, data: Any, url: str) -> bool: pass
    
    @abstractmethod
    def parse(self, data: Any, url: str) -> List[Dict]: pass
    
    def normalize_vehicle(self, vehicle: Dict) -> Dict:
        # Aplica normalização nas fotos antes de retornar
        fotos = vehicle.get("fotos", [])
        vehicle["fotos"] = normalize_fotos(fotos)
        
        return {
            "id": vehicle.get("id"), "tipo": vehicle.get("tipo"), "titulo": vehicle.get("titulo"),
            "versao": vehicle.get("versao"), "marca": vehicle.get("marca"), "modelo": vehicle.get("modelo"),
            "ano": vehicle.get("ano"), "ano_fabricacao": vehicle.get("ano_fabricacao"), "km": vehicle.get("km"),
            "cor": vehicle.get("cor"), "combustivel": vehicle.get("combustivel"), "cambio": vehicle.get("cambio"),
            "motor": vehicle.get("motor"), "portas": vehicle.get("portas"), "categoria": vehicle.get("categoria"),
            "cilindrada": vehicle.get("cilindrada"), "preco": vehicle.get("preco", 0.0),
            "opcionais": vehicle.get("opcionais", ""), "fotos": vehicle.get("fotos", [])
        }

class AltimusParser(BaseParser):
    def can_parse(self, data: Any, url: str) -> bool: 
        return "altimus.com.br" in url.lower()
    
    def parse(self, data: Any, url: str) -> List[Dict]: 
        veiculos = data.get("veiculos", []) 
        if isinstance(veiculos, dict): veiculos = [veiculos]
        
        parsed_vehicles = [] 
        for v in veiculos: 
            modelo_veiculo = v.get("modelo") 
            versao_veiculo = v.get("versao") 
            opcionais_veiculo = self._parse_opcionais(v.get("opcionais"))
            
            # Determina se é moto ou carro 
            tipo_veiculo = v.get("tipo", "").lower() 
            is_moto = "moto" in tipo_veiculo or "motocicleta" in tipo_veiculo
            
            if is_moto: 
                # Para motos: usa o novo sistema com modelo E versão 
                cilindrada_final, categoria_final = inferir_cilindrada_e_categoria_moto(modelo_veiculo, versao_veiculo) 
            else: 
                # Para carros: usa o sistema existente 
                categoria_final = definir_categoria_veiculo(modelo_veiculo, opcionais_veiculo) 
                cilindrada_final = v.get("cilindrada") or inferir_cilindrada(modelo_veiculo, versao_veiculo)
            
            parsed = self.normalize_vehicle({ 
                "id": v.get("id"), 
                "tipo": "eletrico" if v.get("tipo") in ["Bicicleta", "Patinete Elétrico"] else ("moto" if is_moto else ("carro" if v.get("tipo") == "Carro/Camioneta" else v.get("tipo"))), 
                "titulo": None, "versao": versao_veiculo, 
                "marca": v.get("marca"), "modelo": modelo_veiculo, "ano": v.get("anoModelo") or v.get("ano"), 
                "ano_fabricacao": v.get("anoFabricacao") or v.get("ano_fabricacao"), "km": v.get("km"), 
                "cor": v.get("cor"), "combustivel": v.get("combustivel"), 
                "cambio": "manual" if "manual" in str(v.get("cambio", "")).lower() else ("automatico" if "automático" in str(v.get("cambio", "")).lower() else v.get("cambio")), 
                "motor": re.search(r'\b(\d+\.\d+)\b', str(versao_veiculo or "")).group(1) if re.search(r'\b(\d+\.\d+)\b', str(versao_veiculo or "")) else None, 
                "portas": v.get("portas"), "categoria": categoria_final or v.get("categoria"), 
                "cilindrada": cilindrada_final, 
                "preco": converter_preco(v.get("valorVenda") or v.get("preco")), 
                "opcionais": opcionais_veiculo, "fotos": v.get("fotos", []) 
            }) 
            parsed_vehicles.append(parsed) 
        return parsed_vehicles
    
    def _parse_opcionais(self, opcionais: Any) -> str: 
        if isinstance(opcionais, list): return ", ".join(str(item) for item in opcionais if item) 
        return str(opcionais) if opcionais else ""

class AutocertoParser(BaseParser):
    def can_parse(self, data: Any, url: str) -> bool: 
        return "autocerto.com" in url.lower()
    
    def parse(self, data: Any, url: str) -> List[Dict]:
        veiculos = data["estoque"]["veiculo"]
        if isinstance(veiculos, dict): veiculos = [veiculos]
        
        parsed_vehicles = []
        for v in veiculos:
            modelo_veiculo = v.get("modelo")
            versao_veiculo = v.get("versao")
            opcionais_veiculo = self._parse_opcionais(v.get("opcionais"))
            
            # Determina se é moto ou carro
            tipo_veiculo = v.get("tipoveiculo", "").lower()
            is_moto = "moto" in tipo_veiculo or "motocicleta" in tipo_veiculo
            
            if is_moto:
                cilindrada_final, categoria_final = inferir_cilindrada_e_categoria_moto(modelo_veiculo, versao_veiculo)
            else:
                categoria_final = definir_categoria_veiculo(modelo_veiculo, opcionais_veiculo)
                cilindrada_final = inferir_cilindrada(modelo_veiculo, versao_veiculo)

            parsed = self.normalize_vehicle({
                "id": v.get("idveiculo"), 
                "tipo": "moto" if is_moto else v.get("tipoveiculo"), 
                "titulo": None, 
                "versao": ((v.get('modelo', '').strip() + ' ' + ' '.join(re.sub(r'\b(\d\.\d|4x[0-4]|\d+v|diesel|flex|gasolina|manual|automático|4p)\b', '', v.get('versao', ''), flags=re.IGNORECASE).split())).strip()) if v.get("versao") else (v.get("modelo", "").strip() or None),
                "marca": v.get("marca"), "modelo": modelo_veiculo, "ano": v.get("anomodelo"), "ano_fabricacao": None,
                "km": v.get("quilometragem"), "cor": v.get("cor"), "combustivel": v.get("combustivel"),
                "cambio": v.get("cambio"), 
                "motor": v.get("versao", "").strip().split()[0] if v.get("versao") else None, 
                "portas": v.get("numeroportas"), "categoria": categoria_final,
                "cilindrada": cilindrada_final, "preco": converter_preco(v.get("preco")),
                "opcionais": opcionais_veiculo, "fotos": self.extract_photos(v)
            })
            parsed_vehicles.append(parsed)
        return parsed_vehicles

    def _parse_opcionais(self, opcionais: Any) -> str:
        if isinstance(opcionais, dict) and "opcional" in opcionais:
            opcional = opcionais["opcional"]
            if isinstance(opcional, list): return ", ".join(str(item) for item in opcional if item)
            return str(opcional) if opcional else ""
        return ""
    
    def extract_photos(self, v: Dict) -> List[str]:
        fotos = v.get("fotos")
        if not fotos or not (fotos_foto := fotos.get("foto")): return []
        if isinstance(fotos_foto, dict): fotos_foto = [fotos_foto]
        return [img["url"].split("?")[0] for img in fotos_foto if isinstance(img, dict) and "url" in img]

class AutoconfParser(BaseParser):
    # Mapeamento de categorias específico do Autoconf
    CATEGORIA_MAPPING = {
        "conversivel/cupe": "Conversível",
        "conversível/cupê": "Conversível", 
        "picapes": "Caminhonete",
        "suv / utilitario esportivo": "SUV",
        "suv / utilitário esportivo": "SUV",
        "suv": "SUV",
        "van/utilitario": "Utilitário",
        "van/utilitário": "Utilitário",
        "wagon/perua": "Minivan",
        "perua": "Minivan"
    }
    
    def can_parse(self, data: Any, url: str) -> bool:
        return "autoconf" in url.lower()
    
    def parse(self, data: Any, url: str) -> List[Dict]:
        ads = data["ADS"]["AD"]
        if isinstance(ads, dict): ads = [ads]
        
        parsed_vehicles = []
        for v in ads:
            modelo_veiculo = v.get("MODEL")
            versao_veiculo = v.get("VERSION")
            opcionais_veiculo = self._parse_features(v.get("FEATURES"))
            
            # Determina se é moto ou carro
            categoria_veiculo = v.get("CATEGORY", "").lower()
            is_moto = categoria_veiculo == "motos" or "moto" in categoria_veiculo
            
            if is_moto:
                cilindrada_final, categoria_final = inferir_cilindrada_e_categoria_moto(modelo_veiculo, versao_veiculo)
                tipo_final = "moto"
            else:
                # Para carros, usa SEMPRE o campo BODY e aplica o mapeamento específico
                body_category = v.get("BODY", "").lower().strip()
                categoria_final = self.CATEGORIA_MAPPING.get(body_category, v.get("BODY"))
                
                cilindrada_final = inferir_cilindrada(modelo_veiculo, versao_veiculo)
                tipo_final = "carro" if categoria_veiculo == "carros" else categoria_veiculo

            parsed = self.normalize_vehicle({
                "id": v.get("ID"), 
                "tipo": tipo_final,
                "titulo": None, 
                "versao": (' '.join(re.sub(r'\b(\d\.\d|4x[0-4]|\d+v|diesel|flex|aut|aut.|dies|dies.|mec.|mec|gasolina|manual|automático|4p)\b', '', versao_veiculo or '', flags=re.IGNORECASE).split()).strip()) if versao_veiculo else None,
                "marca": v.get("MAKE"), "modelo": modelo_veiculo, "ano": v.get("YEAR"), "ano_fabricacao": v.get("FABRIC_YEAR"),
                "km": v.get("MILEAGE"), "cor": v.get("COLOR"), "combustivel": v.get("FUEL"),
                "cambio": v.get("gear") or v.get("GEAR"), "motor": v.get("MOTOR"), "portas": v.get("DOORS"),
                "categoria": categoria_final, 
                "cilindrada": cilindrada_final,
                "preco": converter_preco(v.get("PRICE")), "opcionais": opcionais_veiculo, "fotos": self.extract_photos(v)
            })
            parsed_vehicles.append(parsed)
        return parsed_vehicles
    
    def _parse_features(self, features: Any) -> str:
        if not features: return ""
        if isinstance(features, list):
            return ", ".join(feat.get("FEATURE", "") if isinstance(feat, dict) else str(feat) for feat in features)
        return str(features)
    
    def extract_photos(self, v: Dict) -> List[str]:
        images = v.get("IMAGES", [])
        if not images: return []
    
        # Se é uma lista (múltiplos IMAGES)
        if isinstance(images, list):
            return [img.get("IMAGE_URL") for img in images if isinstance(img, dict) and img.get("IMAGE_URL")]
    
        # Se é um dict único
        elif isinstance(images, dict) and images.get("IMAGE_URL"):
            return [images["IMAGE_URL"]]
        
        return []

class RevendamaisParser(BaseParser):
    def can_parse(self, data: Any, url: str) -> bool:
        return "revendamais.com.br" in url.lower()

    def parse(self, data: Any, url: str) -> List[Dict]:
        ads = data["ADS"]["AD"]
        if isinstance(ads, dict): ads = [ads]
        
        parsed_vehicles = []
        for v in ads:
            modelo_veiculo = v.get("MODEL")
            versao_veiculo = v.get("VERSION")
            opcionais_veiculo = v.get("ACCESSORIES") or ""
            
            # Determina se é moto ou carro
            categoria_veiculo = v.get("CATEGORY", "").lower()
            is_moto = categoria_veiculo == "motocicleta" or "moto" in categoria_veiculo
            
            if is_moto:
                cilindrada_final, categoria_final = inferir_cilindrada_e_categoria_moto(modelo_veiculo, versao_veiculo)
                tipo_final = "moto"
            else:
                categoria_final = definir_categoria_veiculo(modelo_veiculo, opcionais_veiculo)
                cilindrada_final = inferir_cilindrada(modelo_veiculo, versao_veiculo)
                tipo_final = v.get("CATEGORY")

            parsed = self.normalize_vehicle({
                "id": v.get("ID"), "tipo": tipo_final, "titulo": v.get("TITLE"), "versao": versao_veiculo,
                "marca": v.get("MAKE"), "modelo": modelo_veiculo, "ano": v.get("YEAR"),
                "ano_fabricacao": v.get("FABRIC_YEAR"), "km": v.get("MILEAGE"), "cor": v.get("COLOR"),
                "combustivel": v.get("FUEL"), "cambio": v.get("GEAR"), "motor": v.get("MOTOR"),
                "portas": v.get("DOORS"), "categoria": categoria_final or v.get("BODY_TYPE"),
                "cilindrada": cilindrada_final, "preco": converter_preco(v.get("PRICE")),
                "opcionais": opcionais_veiculo, "fotos": self.extract_photos(v)
            })
            parsed_vehicles.append(parsed)
        return parsed_vehicles
    
    def extract_photos(self, v: Dict) -> List[str]:
        images = v.get("IMAGES", [])
        if not images: return []
        
        if isinstance(images, list):
            return [img.get("IMAGE_URL") for img in images if isinstance(img, dict) and img.get("IMAGE_URL")]
        elif isinstance(images, dict) and images.get("IMAGE_URL"):
            return [images["IMAGE_URL"]]
        
        return []

class FronteiraParser(BaseParser):
    def can_parse(self, data: Any, url: str) -> bool:
        return "fronteiraveiculos.com" in url.lower()

    def parse(self, data: Any, url: str) -> List[Dict]:
        # Pega direto do nó <estoque><veiculo>
        ads = data["estoque"]["veiculo"]

        # Garante que seja lista
        if isinstance(ads, dict):
            ads = [ads]
        
        parsed_vehicles = []
        for v in ads:
            modelo_veiculo = v.get("modelo")
            versao_veiculo = v.get("titulo")
            opcionais_veiculo = v.get("opcionais") or ""
            
            # Determina se é moto ou carro
            categoria_veiculo = v.get("CATEGORY", "").lower()
            is_moto = categoria_veiculo == "motocicleta" or "moto" in categoria_veiculo
            
            if is_moto:
                cilindrada_final, categoria_final = inferir_cilindrada_e_categoria_moto(modelo_veiculo, versao_veiculo)
                tipo_final = "moto"
            else:
                categoria_final = definir_categoria_veiculo(modelo_veiculo, opcionais_veiculo)
                cilindrada_final = inferir_cilindrada(modelo_veiculo, versao_veiculo)
                tipo_final = v.get("CATEGORY")

            parsed = self.normalize_vehicle({
                "id": v.get("id"), "tipo": 'carro', "titulo": v.get("titulo"), "versao": versao_veiculo,
                "marca": v.get("marca"), "modelo": modelo_veiculo, "ano": v.get("ano"),
                "ano_fabricacao": v.get("FABRIC_YEAR"), "km": v.get("km"), "cor": v.get("cor"),
                "combustivel": v.get("combustivel"), "cambio": v.get("cambio"), "motor": v.get("motor"),
                "portas": v.get("DOORS"), "categoria": categoria_final or v.get("BODY_TYPE"),
                "cilindrada": cilindrada_final, "preco": converter_preco(v.get("preco")),
                "opcionais": opcionais_veiculo, "fotos": self.extract_photos(v)
            })
            parsed_vehicles.append(parsed)
        return parsed_vehicles
    
    def extract_photos(self, v: Dict) -> List[str]:
        fotos = v.get("fotos", {})
        if not fotos:
            return []

        images = fotos.get("foto")
        if not images:
            return []

        # Se só uma foto (string)
        if isinstance(images, str):
            return [images]

        # Se várias fotos (lista de strings)
        if isinstance(images, list):
            return [img for img in images if isinstance(img, str)]

        return []

class RevendaproParser(BaseParser):
    def can_parse(self, data: Any, url: str) -> bool:
        return "revendapro.com.br" in url.lower()

    def parse(self, data: Any, url: str) -> List[Dict]:
        # Pega direto do nó <estoque><veiculo>
        ads = data["CargaVeiculos"]["Veiculo"]

        # Garante que seja lista
        if isinstance(ads, dict):
            ads = [ads]
        
        parsed_vehicles = []
        for v in ads:
            modelo_veiculo = v.get("Modelo")
            versao_veiculo = v.get("Versao")
            opcionais_veiculo = v.get("Equipamentos") or ""
            
            # Determina se é moto ou carro
            categoria_veiculo = v.get("Tipo", "").lower()
            is_moto = categoria_veiculo == "motocicleta" or "moto" in categoria_veiculo
            
            if is_moto:
                cilindrada_final, categoria_final = inferir_cilindrada_e_categoria_moto(modelo_veiculo, versao_veiculo)
            else:
                categoria_final = definir_categoria_veiculo(modelo_veiculo, opcionais_veiculo)
                cilindrada_final = inferir_cilindrada(modelo_veiculo, versao_veiculo)

            parsed = self.normalize_vehicle({
                "id": v.get("Codigo"), "tipo": v.get("Tipo"), "titulo": v.get(""), "versao": v.get("Versao"),
                "marca": v.get("Marca"), "modelo": v.get("Modelo"), "ano": v.get("AnoModelo"),
                "ano_fabricacao": v.get("AnoFabr"), "km": v.get("km"), "cor": v.get("Cor"),
                "combustivel": v.get("Combustivel"), "cambio": v.get("Cambio"), "motor": (v.get("Versao") or "").split()[0] if v.get("Versao") else ""
,
                "portas": v.get("Portas"), "categoria": categoria_final,
                "cilindrada": cilindrada_final, "preco": converter_preco(v.get("Preco")),
                "opcionais": opcionais_veiculo, "fotos": self.extract_photos(v)
            })
            parsed_vehicles.append(parsed)
        return parsed_vehicles
    
    def extract_photos(self, v: Dict[str, Any]) -> List[str]:
        fotos = v.get("Fotos")
        if not fotos:
            return []

        # Caso 1: Fotos vem como dict {"foto": "..."} ou {"foto": ["...", "..."]}
        if isinstance(fotos, dict):
            images = fotos.get("foto")
            if isinstance(images, str):
                return [images]
            if isinstance(images, list):
                return [img for img in images if isinstance(img, str)]
            return []

        # Caso 2: Fotos vem como string única "<Fotos> url1 ; url2 ... </Fotos>"
        if isinstance(fotos, str):
            s = re.sub(r"</?\s*fotos?\s*>", "", fotos, flags=re.IGNORECASE).strip()
            urls = [u.strip() for u in re.split(r"[;\n]+", s) if u.strip()]
            return urls

        return []

        images = fotos.get("foto")
        if not images:
            return []

        # Se só uma foto (string)
        if isinstance(images, str):
            return [images]

        # Se várias fotos (lista de strings)
        if isinstance(images, list):
            return [img for img in images if isinstance(img, str)]

        return []

class ClickGarageParser(BaseParser):
    def can_parse(self, data: Any, url: str) -> bool:
        return "clickgarage.com.br" in url.lower()
    
    def parse(self, data: Any, url: str) -> List[Dict]:
        """
        Processa dados do ClickGarage
        """
        estoque = data.get("estoque", {})
        veiculos = estoque.get("veiculo", [])
        
        # Normaliza para lista se for um único veículo
        if isinstance(veiculos, dict):
            veiculos = [veiculos]
        
        parsed_vehicles = []
        
        for v in veiculos:
            if not isinstance(v, dict):
                continue
            
            # Extrai marca e modelo do campo composto
            marca_modelo = v.get("marca", "")
            modelo_completo = v.get("modelo", "")
            
            # Separa marca do modelo
            marca_final, modelo_final = self._extract_marca_modelo(marca_modelo, modelo_completo)
            
            # Processa opcionais
            opcionais_processados = self._parse_opcionais_clickgarage(v.get("opcionais", {}))
            
            # Determina se é moto ou carro
            tipo_veiculo = v.get("tipo", "").lower()
            is_moto = "moto" in tipo_veiculo or "motocicleta" in tipo_veiculo
            
            if is_moto:
                # Para motos: usa o novo sistema com modelo E versão
                cilindrada_final, categoria_final = inferir_cilindrada_e_categoria_moto(modelo_final, "")
                tipo_final = "moto"
            else:
                # Para carros: usa o sistema existente
                categoria_final = definir_categoria_veiculo(modelo_final, opcionais_processados)
                cilindrada_final = inferir_cilindrada(modelo_final, "")
                tipo_final = "carro"
            
            # Extrai informações do motor da versão/modelo
            motor_info = self._extract_motor_info(modelo_completo)
            
            parsed = self.normalize_vehicle({
                "id": v.get("placa")[::-1] if v.get("placa") else v.get("id"),
                "tipo": tipo_final,
                "titulo": v.get("titulo"),
                "versao": self._clean_version(modelo_completo),
                "marca": marca_final,
                "modelo": modelo_final,
                "ano": v.get("anomod") or v.get("ano"),
                "ano_fabricacao": v.get("anofab"),
                "km": v.get("km"),
                "cor": v.get("cor"),
                "combustivel": v.get("combustivel"),
                "cambio": self._extract_cambio_info(modelo_completo),
                "motor": motor_info,
                "portas": None,  # ClickGarage não fornece esse campo explicitamente
                "categoria": categoria_final,
                "cilindrada": cilindrada_final,
                "preco": converter_preco(v.get("preco")),
                "opcionais": opcionais_processados,
                "fotos": self._extract_photos_clickgarage(v)
            })
            
            parsed_vehicles.append(parsed)
        
        return parsed_vehicles
    
    def _extract_marca_modelo(self, marca_campo: str, modelo_completo: str) -> Tuple[str, str]:
        """
        Extrai marca e modelo dos campos compostos do ClickGarage
        Exemplo: marca="GM - Chevrolet", modelo="CRUZE Premier 1.4 16V TB Flex Aut."
        """
        # Limpa e processa o campo marca
        if marca_campo:
            # Remove prefixos como "GM - " e pega a marca principal
            marca_parts = marca_campo.split(" - ")
            marca_final = marca_parts[-1].strip() if marca_parts else marca_campo.strip()
        else:
            marca_final = ""
        
        # Extrai o modelo base (primeira palavra geralmente)
        if modelo_completo:
            modelo_words = modelo_completo.strip().split()
            modelo_final = modelo_words[0] if modelo_words else modelo_completo
        else:
            modelo_final = ""
        
        return marca_final, modelo_final
    
    def _parse_opcionais_clickgarage(self, opcionais: Dict) -> str:
        """
        Processa os opcionais do ClickGarage convertendo as chaves em texto legível
        Exemplo: <Farol-de-led>sim</Farol-de-led> -> "Farol de led"
        """
        if not isinstance(opcionais, dict):
            return ""
        
        opcionais_list = []
        
        for chave, valor in opcionais.items():
            # Só inclui se o valor for "sim"
            if str(valor).lower() == "sim":
                # Converte a chave: remove hífens, capitaliza primeira letra
                opcional_nome = chave.replace("-", " ").lower()
                # Capitaliza a primeira letra
                opcional_nome = opcional_nome.capitalize()
                opcionais_list.append(opcional_nome)
        
        return ", ".join(opcionais_list)
    
    def _extract_photos_clickgarage(self, veiculo: Dict) -> List[str]:
        """
        Extrai todas as fotos do veículo ClickGarage
        Campos: imagem_principal, foto2, foto3, ..., foto9
        """
        fotos = []
        
        # Imagem principal
        if img_principal := veiculo.get("imagem_principal"):
            fotos.append(img_principal.strip())
        
        # Fotos numeradas (foto2 até foto9, ou mais se houver)
        for i in range(2, 20):  # Verifica até foto19 por segurança
            foto_key = f"foto{i}"
            if foto_url := veiculo.get(foto_key):
                fotos.append(foto_url.strip())
        
        return fotos
    
    def _clean_version(self, modelo_completo: str) -> str:
        """
        Limpa a versão removendo informações técnicas redundantes
        """
        if not modelo_completo:
            return ""
        
        # Remove padrões técnicos comuns
        versao_limpa = re.sub(r'\b(\d+\.\d+|16V|TB|Flex|Aut\.|Manual|4p|2p)\b', '', modelo_completo, flags=re.IGNORECASE)
        # Remove espaços extras
        versao_limpa = re.sub(r'\s+', ' ', versao_limpa).strip()
        
        return versao_limpa
    
    def _extract_motor_info(self, modelo_completo: str) -> Optional[str]:
        """
        Extrai informações do motor do modelo completo
        Exemplo: "CRUZE Premier 1.4 16V TB Flex Aut." -> "1.4"
        """
        if not modelo_completo:
            return None
        
        # Busca padrão de cilindrada (ex: 1.4, 2.0, 1.6)
        motor_match = re.search(r'\b(\d+\.\d+)\b', modelo_completo)
        return motor_match.group(1) if motor_match else None
    
    def _extract_cambio_info(self, modelo_completo: str) -> Optional[str]:
        """
        Extrai informações do câmbio do modelo completo
        """
        if not modelo_completo:
            return None
        
        modelo_lower = modelo_completo.lower()
        
        if "aut" in modelo_lower:
            return "automatico"
        elif "manual" in modelo_lower:
            return "manual"
        
        return None

class SimplesVeiculoParser(BaseParser):
    def can_parse(self, data: Any, url: str) -> bool:
        return "simplesveiculo.com.br" in url.lower()
    
    def parse(self, data: Any, url: str) -> List[Dict]:
        """
        Processa dados do SimplesVeiculo
        """
        listings = data.get("listings", {})
        veiculos = listings.get("listing", [])
        
        # Normaliza para lista se for um único veículo
        if isinstance(veiculos, dict):
            veiculos = [veiculos]
        
        parsed_vehicles = []
        
        for v in veiculos:
            if not isinstance(v, dict):
                continue
            
            # Extrai informações básicas
            titulo = v.get("title", "")
            modelo_completo = v.get("model", "")
            marca = v.get("make", "")
            
            # Extrai modelo base da string completa
            modelo_final = self._extract_modelo_base(modelo_completo, marca)
            
            # Processa quilometragem
            km_final = self._extract_mileage(v.get("mileage", {}))
            
            # Determina se é moto ou carro
            vehicle_type = v.get("vehicle_type", "").lower()
            body_style = v.get("body_style", "").lower()
            
            # SimplesVeiculo usa 'car_truck' para carros e 'motorcycle' para motos
            is_moto = vehicle_type == "motorcycle" or "moto" in vehicle_type
            
            if is_moto:
                # Para motos: usa o novo sistema com modelo E versão
                cilindrada_final, categoria_final = inferir_cilindrada_e_categoria_moto(modelo_final, modelo_completo)
                tipo_final = "moto"
            else:
                # Para carros: usa o sistema existente
                categoria_final = self._map_body_style_to_categoria(body_style) or definir_categoria_veiculo(modelo_final, "")
                cilindrada_final = inferir_cilindrada(modelo_final, modelo_completo)
                tipo_final = "carro"
            
            # Extrai informações do motor da descrição/modelo
            motor_info = self._extract_motor_info(modelo_completo)
            
            # Processa combustível
            combustivel_final = self._map_fuel_type(v.get("fuel_type", ""))
            
            # Processa câmbio
            cambio_final = self._map_transmission(v.get("transmission", ""))
            
            parsed = self.normalize_vehicle({
                "id": v.get("vehicle_id"),
                "tipo": tipo_final,
                "titulo": titulo,
                "versao": self._clean_version(modelo_completo, marca),
                "marca": marca,
                "modelo": modelo_final,
                "ano": self._safe_int(v.get("year")),
                "ano_fabricacao": None,  # SimplesVeiculo não fornece separadamente
                "km": km_final,
                "cor": self._normalize_color(v.get("exterior_color", "")),
                "combustivel": combustivel_final,
                "cambio": cambio_final,
                "motor": motor_info,
                "portas": None,  # Não fornecido explicitamente
                "categoria": categoria_final,
                "cilindrada": cilindrada_final,
                "preco": converter_preco(v.get("price")),
                "opcionais": "",  # SimplesVeiculo não fornece opcionais neste formato
                "fotos": self._extract_photos_simples(v)
            })
            
            parsed_vehicles.append(parsed)
        
        return parsed_vehicles
    
    def _extract_modelo_base(self, modelo_completo: str, marca: str) -> str:
        """
        Extrai o modelo base da string completa
        Exemplo: "QQ 1.0 ACT 12V 69cv 5p" -> "QQ"
        """
        if not modelo_completo:
            return ""
        
        # Remove a marca se estiver no início
        modelo_sem_marca = modelo_completo
        if marca and modelo_completo.upper().startswith(marca.upper()):
            modelo_sem_marca = modelo_completo[len(marca):].strip()
        
        # Pega a primeira palavra que geralmente é o modelo
        palavras = modelo_sem_marca.strip().split()
        if palavras:
            return palavras[0]
        
        return modelo_completo.strip()
    
    def _extract_mileage(self, mileage_data: Dict) -> Optional[int]:
        """
        Extrai quilometragem do objeto mileage
        Exemplo: {"value": "95528", "unit": "KM"} -> 95528
        """
        if not isinstance(mileage_data, dict):
            return None
        
        value = mileage_data.get("value")
        if value:
            try:
                return int(float(str(value).replace(",", "").replace(".", "")))
            except (ValueError, TypeError):
                return None
        
        return None
    
    def _map_body_style_to_categoria(self, body_style: str) -> Optional[str]:
        """
        Mapeia body_style do SimplesVeiculo para nossas categorias
        """
        if not body_style:
            return None
        
        body_style_lower = body_style.lower()
        
        mapping = {
            "sedan": "Sedan",
            "hatchback": "Hatch", 
            "suv": "SUV",
            "pickup": "Caminhonete",
            "truck": "Caminhonete",
            "van": "Utilitário",
            "wagon": "Station Wagon",
            "coupe": "Coupe",
            "convertible": "Conversível",
            "other": None  # Deixa None para usar a lógica padrão
        }
        
        return mapping.get(body_style_lower)
    
    def _map_fuel_type(self, fuel_type: str) -> Optional[str]:
        """
        Mapeia fuel_type do SimplesVeiculo para nosso padrão
        """
        if not fuel_type:
            return None
        
        fuel_lower = fuel_type.lower()
        
        mapping = {
            "gasoline": "gasolina",
            "ethanol": "etanol", 
            "flex": "flex",
            "diesel": "diesel",
            "electric": "elétrico",
            "hybrid": "híbrido"
        }
        
        return mapping.get(fuel_lower, fuel_type.lower())
    
    def _map_transmission(self, transmission: str) -> Optional[str]:
        """
        Mapeia transmission do SimplesVeiculo para nosso padrão
        """
        if not transmission:
            return None
        
        trans_lower = transmission.lower()
        
        if "manual" in trans_lower:
            return "manual"
        elif "automatic" in trans_lower or "auto" in trans_lower:
            return "automatico"
        
        return transmission.lower()
    
    def _extract_photos_simples(self, veiculo: Dict) -> List[str]:
        """
        Extrai todas as fotos do veículo SimplesVeiculo
        Cada foto está em um elemento <image><url>...</url></image>
        Quando há múltiplas tags image, o xmltodict cria uma lista
        """
        fotos = []
        
        # Verifica se há um campo 'image' 
        image_data = veiculo.get("image")
        
        if not image_data:
            return fotos
        
        # Se é uma lista de imagens (caso mais comum com múltiplas tags <image>)
        if isinstance(image_data, list):
            for img in image_data:
                if isinstance(img, dict) and "url" in img:
                    url = str(img["url"]).strip()
                    if url and url != "https://app.simplesveiculo.com.br/":  # Ignora URLs vazias/placeholder
                        fotos.append(url)
                elif isinstance(img, str) and img.strip():
                    if img.strip() != "https://app.simplesveiculo.com.br/":
                        fotos.append(img.strip())
        
        # Se é um objeto único de imagem
        elif isinstance(image_data, dict):
            if "url" in image_data:
                url = str(image_data["url"]).strip()
                if url and url != "https://app.simplesveiculo.com.br/":
                    fotos.append(url)
        
        # Se é uma string única
        elif isinstance(image_data, str) and image_data.strip():
            if image_data.strip() != "https://app.simplesveiculo.com.br/":
                fotos.append(image_data.strip())
        
        return fotos
    
    def _clean_version(self, modelo_completo: str, marca: str) -> Optional[str]:
        """
        Limpa a versão removendo a marca e mantendo informações relevantes
        Exemplo: "QQ 1.0 ACT 12V 69cv 5p" (com marca "CHERY") -> "1.0 ACT 12V 69cv 5p"
        """
        if not modelo_completo:
            return None
        
        versao = modelo_completo
        
        # Remove a marca se estiver no início
        if marca and versao.upper().startswith(marca.upper()):
            versao = versao[len(marca):].strip()
        
        # Remove o modelo base (primeira palavra)
        palavras = versao.split()
        if len(palavras) > 1:
            versao = " ".join(palavras[1:])
        else:
            return None  # Se só sobrou uma palavra, não há versão
        
        return versao.strip() if versao.strip() else None
    
    def _extract_motor_info(self, modelo_completo: str) -> Optional[str]:
        """
        Extrai informações do motor do modelo completo
        Exemplo: "QQ 1.0 ACT 12V 69cv 5p" -> "1.0"
        """
        if not modelo_completo:
            return None
        
        # Busca padrão de cilindrada (ex: 1.0, 1.4, 2.0, 1.6)
        motor_match = re.search(r'\b(\d+\.\d+)\b', modelo_completo)
        return motor_match.group(1) if motor_match else None
    
    def _normalize_color(self, color: str) -> Optional[str]:
        """
        Normaliza a cor removendo formatação estranha
        """
        if not color:
            return None
        
        return color.strip().lower().capitalize()
    
    def _safe_int(self, value: Any) -> Optional[int]:
        """
        Converte valor para int de forma segura
        """
        if value is None:
            return None
        
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

class BoomParser(BaseParser):
    def can_parse(self, data: Any, url: str) -> bool: 
        return "boomsistemas.com.br" in url.lower()
    
    def parse(self, data: Any, url: str) -> List[Dict]:
        veiculos = []
        if isinstance(data, list): veiculos = flatten_list(data)
        elif isinstance(data, dict):
            for key in ['veiculos', 'vehicles', 'data', 'items', 'results', 'content']:
                if key in data: veiculos = flatten_list(data[key]); break
            if not veiculos and self._looks_like_vehicle(data): veiculos = [data]
        
        parsed_vehicles = []
        for v in veiculos:
            if not isinstance(v, dict): continue
            
            modelo_veiculo = safe_get(v, ["modelo", "model", "nome", "MODEL"])
            versao_veiculo = safe_get(v, ["versao", "version", "variant", "VERSION"])
            opcionais_veiculo = self._parse_opcionais(safe_get(v, ["opcionais", "options", "extras", "features", "FEATURES"]))
            
            # Determina se é moto ou carro baseado em campos disponíveis
            tipo_veiculo = safe_get(v, ["tipo", "type", "categoria_veiculo", "CATEGORY", "vehicle_type"]) or ""
            is_moto = any(termo in str(tipo_veiculo).lower() for termo in ["moto", "motocicleta", "motorcycle", "bike"])
            
            if is_moto:
                cilindrada_final, categoria_final = inferir_cilindrada_e_categoria_moto(modelo_veiculo, versao_veiculo)
                tipo_final = "moto"
            else:
                categoria_final = definir_categoria_veiculo(modelo_veiculo, opcionais_veiculo)
                cilindrada_final = safe_get(v, ["cilindrada", "displacement", "engine_size"]) or inferir_cilindrada(modelo_veiculo, versao_veiculo)
                tipo_final = tipo_veiculo or "carro"

            parsed = self.normalize_vehicle({
                "id": safe_get(v, ["id", "ID", "codigo", "cod"]), 
                "tipo": tipo_final,
                "titulo": safe_get(v, ["titulo", "title", "TITLE"]), 
                "versao": versao_veiculo,
                "marca": safe_get(v, ["marca", "brand", "fabricante", "MAKE"]), 
                "modelo": modelo_veiculo,
                "ano": safe_get(v, ["ano_mod", "anoModelo", "ano", "year_model", "ano_modelo", "YEAR"]),
                "ano_fabricacao": safe_get(v, ["ano_fab", "anoFabricacao", "ano_fabricacao", "year_manufacture", "FABRIC_YEAR"]),
                "km": safe_get(v, ["km", "quilometragem", "mileage", "kilometers", "MILEAGE"]), 
                "cor": safe_get(v, ["cor", "color", "colour", "COLOR"]),
                "combustivel": safe_get(v, ["combustivel", "fuel", "fuel_type", "FUEL"]), 
                "cambio": safe_get(v, ["cambio", "transmission", "gear", "GEAR"]),
                "motor": safe_get(v, ["motor", "engine", "motorization", "MOTOR"]), 
                "portas": safe_get(v, ["portas", "doors", "num_doors", "DOORS"]),
                "categoria": categoria_final,
                "cilindrada": cilindrada_final,
                "preco": converter_preco(safe_get(v, ["valor", "valorVenda", "preco", "price", "value", "PRICE"])),
                "opcionais": opcionais_veiculo, "fotos": self._parse_fotos(v)
            })
            parsed_vehicles.append(parsed)
        return parsed_vehicles
    
    def _looks_like_vehicle(self, data: Dict) -> bool: 
        return any(field in data for field in ['modelo', 'model', 'marca', 'brand', 'preco', 'price', 'ano', 'year'])
    
    def _parse_opcionais(self, opcionais: Any) -> str:
        if not opcionais: return ""
        if isinstance(opcionais, list):
            if all(isinstance(i, dict) for i in opcionais):
                return ", ".join(name for item in opcionais if (name := safe_get(item, ["nome", "name", "descricao", "description", "FEATURE"])))
            return ", ".join(str(item) for item in opcionais if item)
        return str(opcionais)
    
    def _parse_fotos(self, v: Dict) -> List[str]:
        fotos_data = safe_get(v, ["galeria", "fotos", "photos", "images", "gallery", "IMAGES"], [])
        if not isinstance(fotos_data, list): fotos_data = [fotos_data] if fotos_data else []
        
        result = []
        for foto in fotos_data:
            if isinstance(foto, str): result.append(foto)
            elif isinstance(foto, dict):
                if url := safe_get(foto, ["url", "URL", "src", "IMAGE_URL", "path"]):
                    result.append(url)
        return result

# =================== SISTEMA PRINCIPAL =======================

class UnifiedVehicleFetcher:
    def __init__(self):
        self.parsers = [
            AltimusParser(),
            FronteiraParser(),
            ClickGarageParser(), 
            AutocertoParser(), 
            RevendamaisParser(), 
            AutoconfParser(), 
            SimplesVeiculoParser(),
            RevendaproParser(),
            BoomParser()
        ]
        print("[INFO] Sistema unificado iniciado - seleção de parser baseada na URL")
    
    def get_urls(self) -> List[str]: 
        return list({val for var, val in os.environ.items() if var.startswith("XML_URL") and val})
    
    def detect_format(self, content: bytes, url: str) -> tuple[Any, str]:
        content_str = content.decode('utf-8', errors='ignore')
        try: return json.loads(content_str), "json"
        except json.JSONDecodeError:
            try: return xmltodict.parse(content_str), "xml"
            except Exception: raise ValueError(f"Formato não reconhecido para URL: {url}")
    
    def select_parser(self, data: Any, url: str) -> Optional['BaseParser']:
        """
        Seleciona o parser baseado na URL primeiro, depois na estrutura dos dados.
        """
        # Primeira prioridade: seleção baseada na URL
        for parser in self.parsers:
            if parser.can_parse(data, url):
                print(f"[INFO] Parser selecionado por URL: {parser.__class__.__name__}")
                return parser
        
        # Se nenhum parser foi encontrado baseado na URL, tenta fallback
        print(f"[AVISO] Nenhum parser específico encontrado para URL: {url}")
        print(f"[INFO] Tentando parser genérico BoomParser como fallback...")
        
        # Usa BoomParser como fallback se a estrutura for compatível
        boom_parser = BoomParser()
        if boom_parser.can_parse(data, url):
            print(f"[INFO] Usando BoomParser como fallback")
            return boom_parser
        
        return None
    
    def process_url(self, url: str) -> List[Dict]:
        print(f"[INFO] Processando URL: {url}")
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data, format_type = self.detect_format(response.content, url)
            print(f"[INFO] Formato detectado: {format_type}")
            
            parser = self.select_parser(data, url)
            if parser:
                return parser.parse(data, url)
            else:
                print(f"[ERRO] Nenhum parser adequado encontrado para URL: {url}")
                return []
                
        except requests.RequestException as e: 
            print(f"[ERRO] Erro de requisição para URL {url}: {e}")
            return []
        except Exception as e: 
            print(f"[ERRO] Erro crítico ao processar URL {url}: {e}")
            return []
    
    def fetch_all(self) -> Dict:
        urls = self.get_urls()
        if not urls:
            print("[AVISO] Nenhuma variável de ambiente 'XML_URL' foi encontrada.")
            return {}
        
        print(f"[INFO] {len(urls)} URL(s) encontrada(s) para processar")
        all_vehicles = [vehicle for url in urls for vehicle in self.process_url(url)]
        
        # Estatísticas por tipo e categoria
        stats = self._generate_stats(all_vehicles)
        
        result = {
            "veiculos": all_vehicles, 
            "_updated_at": datetime.now().isoformat(), 
            "_total_count": len(all_vehicles), 
            "_sources_processed": len(urls),
            "_statistics": stats
        }
        
        try:
            with open(JSON_FILE, "w", encoding="utf-8") as f: 
                json.dump(result, f, ensure_ascii=False, indent=2)
            print(f"\n[OK] Arquivo {JSON_FILE} salvo com sucesso!")
        except Exception as e: 
            print(f"[ERRO] Erro ao salvar arquivo JSON: {e}")
        
        print(f"[OK] Total de veículos processados: {len(all_vehicles)}")
        self._print_stats(stats)
        return result
    
    def _generate_stats(self, vehicles: List[Dict]) -> Dict:
        """Gera estatísticas dos veículos processados"""
        stats = {
            "por_tipo": {},
            "motos_por_categoria": {},
            "carros_por_categoria": {},
            "top_marcas": {},
            "cilindradas_motos": {},
            "parsers_utilizados": {}
        }
        
        for vehicle in vehicles:
            # Estatísticas por tipo
            tipo = vehicle.get("tipo", "indefinido")
            stats["por_tipo"][tipo] = stats["por_tipo"].get(tipo, 0) + 1
            
            # Estatísticas por categoria
            categoria = vehicle.get("categoria", "indefinido")
            if tipo and "moto" in str(tipo).lower():
                stats["motos_por_categoria"][categoria] = stats["motos_por_categoria"].get(categoria, 0) + 1
                
                # Cilindradas das motos
                cilindrada = vehicle.get("cilindrada")
                if cilindrada:
                    range_key = self._get_cilindrada_range(cilindrada)
                    stats["cilindradas_motos"][range_key] = stats["cilindradas_motos"].get(range_key, 0) + 1
            else:
                stats["carros_por_categoria"][categoria] = stats["carros_por_categoria"].get(categoria, 0) + 1
            
            # Top marcas
            marca = vehicle.get("marca", "indefinido")
            stats["top_marcas"][marca] = stats["top_marcas"].get(marca, 0) + 1
        
        return stats
    
    def _get_cilindrada_range(self, cilindrada: int) -> str:
        """Categoriza cilindradas em faixas"""
        if cilindrada <= 125:
            return "até 125cc"
        elif cilindrada <= 250:
            return "126cc - 250cc"
        elif cilindrada <= 500:
            return "251cc - 500cc"
        elif cilindrada <= 1000:
            return "501cc - 1000cc"
        else:
            return "acima de 1000cc"
    
    def _print_stats(self, stats: Dict):
        """Imprime estatísticas formatadas"""
        print(f"\n{'='*60}\nESTATÍSTICAS DO PROCESSAMENTO\n{'='*60}")
        
        print(f"\n📊 Distribuição por Tipo:")
        for tipo, count in sorted(stats["por_tipo"].items(), key=lambda x: x[1], reverse=True):
            print(f"  • {tipo}: {count}")
        
        if stats["motos_por_categoria"]:
            print(f"\n🏍️  Motos por Categoria:")
            for categoria, count in sorted(stats["motos_por_categoria"].items(), key=lambda x: x[1], reverse=True):
                print(f"  • {categoria}: {count}")
        
        if stats["carros_por_categoria"]:
            print(f"\n🚗 Carros por Categoria:")
            for categoria, count in sorted(stats["carros_por_categoria"].items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"  • {categoria}: {count}")
        
        if stats["cilindradas_motos"]:
            print(f"\n🔧 Cilindradas das Motos:")
            for faixa, count in sorted(stats["cilindradas_motos"].items(), key=lambda x: x[1], reverse=True):
                print(f"  • {faixa}: {count}")
        
        print(f"\n🏭 Top 5 Marcas:")
        for marca, count in sorted(stats["top_marcas"].items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"  • {marca}: {count}")

# =================== FUNÇÃO PARA IMPORTAÇÃO =======================

def fetch_and_convert_xml():
    """Função de alto nível para ser importada por outros módulos."""
    fetcher = UnifiedVehicleFetcher()
    return fetcher.fetch_all()

# =================== EXECUÇÃO PRINCIPAL (SE RODADO DIRETAMENTE) =======================

if __name__ == "__main__":
    result = fetch_and_convert_xml()
    
    if result and 'veiculos' in result:
        total = result.get('_total_count', 0)
        print(f"\n{'='*50}\nRESUMO DO PROCESSAMENTO\n{'='*50}")
        print(f"Total de veículos: {total}")
        print(f"Atualizado em: {result.get('_updated_at', 'N/A')}")
        print(f"Fontes processadas: {result.get('_sources_processed', 0)}")
        
        if total > 0:
            print(f"\nExemplo dos primeiros 5 veículos:")
            for i, v in enumerate(result['veiculos'][:5], 1):
                tipo = v.get('tipo', 'N/A')
                categoria = v.get('categoria', 'N/A')
                cilindrada = v.get('cilindrada', '')
                cilindrada_str = f" - {cilindrada}cc" if cilindrada else ""
                print(f"{i}. {v.get('marca', 'N/A')} {v.get('modelo', 'N/A')} ({tipo}/{categoria}{cilindrada_str}) {v.get('ano', 'N/A')} - R$ {v.get('preco', 0.0):,.2f}")
            
            # Exemplos específicos de motos categorizadas
            motos = [v for v in result['veiculos'] if v.get('tipo') and 'moto' in str(v.get('tipo')).lower()]
            if motos:
                print(f"\nExemplos de motos categorizadas:")
                for i, moto in enumerate(motos[:3], 1):
                    print(f"{i}. {moto.get('marca', 'N/A')} {moto.get('modelo', 'N/A')} - {moto.get('categoria', 'N/A')} - {moto.get('cilindrada', 'N/A')}cc")
            
            # Demonstração da normalização de fotos
            print(f"\nExemplos de fotos normalizadas:")
            vehicles_with_photos = [v for v in result['veiculos'] if v.get('fotos')][:3]
            for i, vehicle in enumerate(vehicles_with_photos, 1):
                fotos = vehicle.get('fotos', [])
                print(f"{i}. {vehicle.get('marca', 'N/A')} {vehicle.get('modelo', 'N/A')} - {len(fotos)} foto(s)")
                if fotos:
                    print(f"   Primeira foto: {fotos[0]}")
                    if len(fotos) > 1:
                        print(f"   Tipo da estrutura: Lista simples com {len(fotos)} URLs")
