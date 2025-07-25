import requests
import xmltodict
import json
import os
import re
from datetime import datetime
from unidecode import unidecode
from typing import Dict, List, Any, Optional, Union
from abc import ABC, abstractmethod

# =================== CONFIGURAÇÕES GLOBAIS =======================

JSON_FILE = "data.json"

# =================== MAPEAMENTOS DE VEÍCULOS =======================

MAPEAMENTO_CATEGORIAS = {}
OPCIONAL_CHAVE_HATCH = "limpador traseiro"

# --- Listas de Modelos por Categoria ---

hatch_models = ["gol", "uno", "palio", "celta", "march", "sandero", "i30", "golf", "fox", "up", "fit", "etios", "bravo", "punto", "208", "argo", "mobi", "c3", "picanto", "stilo", "c4 vtr", "kwid", "soul", "agile", "fusca", "a1", "new beetle", "116i", "118i", "120i", "125i", "m135i", "m140i"]
for model in hatch_models: MAPEAMENTO_CATEGORIAS[model] = "Hatch"

sedan_models = ["a6", "sentra", "jetta", "voyage", "siena", "grand siena", "cobalt", "logan", "fluence", "cerato", "elantra", "virtus", "accord", "altima", "fusion", "passat", "vectra sedan", "classic", "cronos", "linea", "408", "c4 pallas", "bora", "hb20s", "lancer", "camry", "onix plus", "azera", "malibu", "318i", "320d", "320i", "328i", "330d", "330i", "335i", "520d", "528i", "530d", "530i", "535i", "540i", "550i", "740i", "750i", "c180", "c200", "c250", "c300", "e250", "e350", "m3", "m5", "s4", "classe c", "classe e", "classe s", "eqe", "eqs"]
for model in sedan_models: MAPEAMENTO_CATEGORIAS[model] = "Sedan"

hatch_sedan_models = ["onix", "hb20", "yaris", "city", "a3", "corolla", "civic", "focus", "fiesta", "corsa", "astra", "vectra", "cruze", "clio", "megane", "206", "207", "307", "tiida", "ka", "versa", "prisma", "polo", "c4", "sonic", "série 1", "série 2", "série 3", "série 4", "série 5", "série 6", "série 7", "classe a", "cla"]
for model in hatch_sedan_models: MAPEAMENTO_CATEGORIAS[model] = "hatch,sedan"

suv_models = ["xc60", "edge", "outlander", "range rover evoque", "song plus", "duster", "ecosport", "hrv", "hr-v", "compass", "renegade", "tracker", "kicks", "captur", "creta", "tucson", "santa fe", "sorento", "sportage", "pajero", "tr4", "aircross", "tiguan", "t-cross", "tcross", "rav4", "land cruiser", "cherokee", "grand cherokee", "trailblazer", "pulse", "fastback", "territory", "bronco sport", "2008", "3008", "5008", "c4 cactus", "taos", "crv", "cr-v", "corolla cross", "sw4", "pajero sport", "commander", "nivus", "equinox", "x1", "x2", "x3", "x4", "x5", "x6", "x7", "ix", "ix1", "ix2", "ix3", "gla", "glb", "glc", "gle", "gls", "classe g", "eqa", "eqb", "eqc", "q2", "q3", "q5", "q7", "q8", "q6 e-tron", "e-tron", "q4 e-tron", "q4etron", "wrx", "xv"]
for model in suv_models: MAPEAMENTO_CATEGORIAS[model] = "SUV"

caminhonete_models = ["duster oroch", "hilux", "ranger", "s10", "l200", "triton", "toro", "frontier", "amarok", "maverick", "montana", "ram 1500", "rampage", "f-250", "f250", "courier", "dakota", "gladiator", "hoggar"]
for model in caminhonete_models: MAPEAMENTO_CATEGORIAS[model] = "Caminhonete"

utilitario_models = ["saveiro", "strada", "oroch", "kangoo", "partner", "doblo", "fiorino", "kombi", "doblo cargo", "berlingo", "combo", "express", "hr"]
for model in utilitario_models: MAPEAMENTO_CATEGORIAS[model] = "Utilitário"

furgao_models = ["boxer", "daily", "ducato", "expert", "jumper", "jumpy", "master", "scudo", "sprinter", "trafic", "transit", "vito"]
for model in furgao_models: MAPEAMENTO_CATEGORIAS[model] = "Furgão"

coupe_models = ["370z", "brz", "camaro", "challenger", "corvette", "gt86", "mustang", "r8", "rcz", "rx8", "supra", "tt", "tts", "veloster", "m2", "m4", "m8", "s5", "amg gt"]
for model in coupe_models: MAPEAMENTO_CATEGORIAS[model] = "Coupe"

conversivel_models = ["911 cabrio", "beetle cabriolet", "boxster", "eos", "miata", "mini cabrio", "slk", "z4", "série 8", "slc", "sl"]
for model in conversivel_models: MAPEAMENTO_CATEGORIAS[model] = "Conversível"

station_wagon_models = ["a4 avant", "fielder", "golf variant", "palio weekend", "parati", "quantum", "spacefox", "rs2", "rs4", "rs6"]
for model in station_wagon_models: MAPEAMENTO_CATEGORIAS[model] = "Station Wagon"

minivan_models = ["caravan", "carnival", "grand c4", "idea", "livina", "meriva", "picasso", "scenic", "sharan", "spin", "touran", "xsara picasso", "zafira", "série 2 active tourer", "classe b", "classe t", "classe r", "classe v"]
for model in minivan_models: MAPEAMENTO_CATEGORIAS[model] = "Minivan"

offroad_models = ["bandeirante", "bronco", "defender", "grand vitara", "jimny", "samurai", "troller", "wrangler"]
for model in offroad_models: MAPEAMENTO_CATEGORIAS[model] = "Off-road"

MAPEAMENTO_CILINDRADAS = {
    "g 310": 300, "f 750 gs": 850, "f 850 gs": 850, "f 900": 900, "r 1250": 1250, "r 1300": 1300, "r 18": 1800, "k 1300": 1300, "k 1600": 1650, "s 1000": 1000, "g 650 gs": 650, "cb 300": 300, "cb 500": 500, "cb 650": 650, "cb 1000r": 1000, "cb twister": 300, "twister": 300, "cbr 250": 250, "cbr 500": 500, "cbr 600": 600, "cbr 650": 650, "cbr 1000": 1000, "hornet 600": 600, "cb 600f": 600, "xre 190": 190, "xre 300": 300, "xre 300 sahara": 300, "sahara 300": 300, "sahara 300 rally": 300, "nxr 160": 160, "bros 160": 160, "cg 160": 160, "cg 160 titan": 160, "cg 160 fan": 160, "cg 160 start": 160, "cg 160 titan s": 160, "cg 125": 125, "cg 125 fan ks": 125, "biz 125": 125, "biz 125 es": 125, "biz 110": 110, "pop 110": 110, "pop 110i": 110, "pcx 150": 150, "pcx 160": 160, "xj6": 600, "mt 03": 300, "mt 07": 690, "mt 09": 890, "mt 01": 1700, "fazer 150": 150, "fazer 250": 250, "ys 250": 250, "factor 125": 125, "factor 150": 150, "xtz 150": 150, "xtz 250": 250, "xtz 250 tenere": 250, "tenere 250": 250, "lander 250": 250, "yzf r3": 300, "yzf r-3": 300, "r15": 150, "r1": 1000, "nmax 160": 160, "xmax 250": 250, "gs500": 500, "bandit 600": 600, "bandit 650": 650, "bandit 1250": 1250, "gsx 650f": 650, "gsx-s 750": 750, "gsx-s 1000": 1000, "hayabusa": 1350, "gixxer 250": 250, "burgman 125": 125, "z300": 300, "z400": 400, "z650": 650, "z750": 750, "z800": 800, "z900": 950, "z1000": 1000, "ninja 300": 300, "ninja 400": 400, "ninja 650": 650, "ninja 1000": 1050, "ninja zx-10r": 1000, "er6n": 650, "versys 300": 300, "versys 650": 650, "xt 660": 660, "meteor 350": 350, "classic 350": 350, "hunter 350": 350, "himalayan": 400, "interceptor 650": 650, "continental gt 650": 650, "tiger 800": 800, "tiger 900": 900, "street triple": 750, "speed triple": 1050, "bonneville": 900, "trident 660": 660, "monster 797": 800, "monster 821": 820, "monster 937": 940, "panigale v2": 950, "panigale v4": 1100, "iron 883": 883, "forty eight": 1200, "sportster s": 1250, "fat bob": 1140, "road glide": 2150, "street glide": 1750, "next 300": 300, "commander 250": 250, "dafra citycom 300": 300, "dr 160": 160, "dr 160 s": 160, "cforce 1000": 1000, "trx 420": 420, "t350 x": 350, "xr300l tornado": 300, "fz25 fazer": 250, "fz15 fazer": 150, "biz es": 125, "elite 125": 125, "crf 230f": 230, "cg150 fan": 150, "cg150 titan": 150, "diavel 1260": 1260, "YZF R-6": 600, "MT-03": 300, "MT03": 300, "ER-6N": 650, "xt 600": 600, "cg 125": 125
}

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

    return None # Nenhuma correspondência encontrada

def inferir_cilindrada(modelo: str) -> Optional[int]:
    if not modelo: return None
    modelo_norm = normalizar_texto(modelo)
    for mapeado, cilindrada in MAPEAMENTO_CILINDRADAS.items():
        if normalizar_texto(mapeado) in modelo_norm:
            return cilindrada
    return None

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

# =================== PARSERS =======================
# (O conteúdo das classes BaseParser e dos parsers específicos permanece o mesmo)
class BaseParser(ABC):
    @abstractmethod
    def can_parse(self, data: Any, url: str) -> bool: pass
    
    @abstractmethod
    def parse(self, data: Any, url: str) -> List[Dict]: pass
    
    def normalize_vehicle(self, vehicle: Dict) -> Dict:
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
    def can_parse(self, data: Any, url: str) -> bool: return isinstance(data, dict) and "veiculos" in data
    
    def parse(self, data: Any, url: str) -> List[Dict]:
        veiculos = data.get("veiculos", [])
        if isinstance(veiculos, dict): veiculos = [veiculos]
        
        parsed_vehicles = []
        for v in veiculos:
            modelo_veiculo = v.get("modelo")
            opcionais_veiculo = self._parse_opcionais(v.get("opcionais"))
            categoria_final = definir_categoria_veiculo(modelo_veiculo, opcionais_veiculo)
            
            parsed = self.normalize_vehicle({
                "id": v.get("id"), "tipo": "carro" if v.get("tipo") == "Carro/Camioneta" else v.get("tipo"), "titulo": None, "versao": v.get("versao"),
                "marca": v.get("marca"), "modelo": modelo_veiculo, "ano": v.get("anoModelo") or v.get("ano"),
                "ano_fabricacao": v.get("anoFabricacao") or v.get("ano_fabricacao"), "km": v.get("km"),
                "cor": v.get("cor"), "combustivel": v.get("combustivel"), "cambio": "manual" if "manual" in str(v.get("cambio", "")).lower() else ("automatico" if "automático" in str(v.get("cambio", "")).lower() else v.get("cambio")),
                "motor": re.search(r'\b(\d+\.\d+)\b', str(v.get("versao", ""))).group(1) if re.search(r'\b(\d+\.\d+)\b', str(v.get("versao", ""))) else None, "portas": v.get("portas"), "categoria": categoria_final or v.get("categoria"),
                "cilindrada": v.get("cilindrada") or inferir_cilindrada(modelo_veiculo),
                "preco": converter_preco(v.get("valorVenda") or v.get("preco")),
                "opcionais": opcionais_veiculo, "fotos": v.get("fotos") or []
            })
            parsed_vehicles.append(parsed)
        return parsed_vehicles
    
    def _parse_opcionais(self, opcionais: Any) -> str:
        if isinstance(opcionais, list): return ", ".join(str(item) for item in opcionais if item)
        return str(opcionais) if opcionais else ""

class AutocertoParser(BaseParser):
    def can_parse(self, data: Any, url: str) -> bool: return isinstance(data, dict) and "estoque" in data and "veiculo" in data.get("estoque", {})
    
    def parse(self, data: Any, url: str) -> List[Dict]:
        veiculos = data["estoque"]["veiculo"]
        if isinstance(veiculos, dict): veiculos = [veiculos]
        
        parsed_vehicles = []
        for v in veiculos:
            modelo_veiculo = v.get("modelo")
            opcionais_veiculo = self._parse_opcionais(v.get("opcionais"))
            categoria_final = definir_categoria_veiculo(modelo_veiculo, opcionais_veiculo)

            parsed = self.normalize_vehicle({
                "id": v.get("idveiculo"), "tipo": v.get("tipoveiculo"), "titulo": None, "versao": ((v.get('modelo', '').strip() + ' ' + ' '.join(re.sub(r'\b(\d\.\d|4x[0-4]|\d+v|diesel|flex|gasolina|manual|automático|4p)\b', '', v.get('versao', ''), flags=re.IGNORECASE).split())).strip()) if v.get("versao") else (v.get("modelo", "").strip() or None),
                "marca": v.get("marca"), "modelo": modelo_veiculo, "ano": v.get("anomodelo"), "ano_fabricacao": None,
                "km": v.get("quilometragem"), "cor": v.get("cor"), "combustivel": v.get("combustivel"),
                "cambio": v.get("cambio"), "motor": v.get("versao", "").strip().split()[0] if v.get("versao") else None, "portas": v.get("numeroportas"), "portas": v.get("numeroportas"), "categoria": categoria_final,
                "cilindrada": inferir_cilindrada(modelo_veiculo), "preco": converter_preco(v.get("preco")),
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
    def can_parse(self, data: Any, url: str) -> bool:
        base_check = isinstance(data, dict) and "ADS" in data and "AD" in data.get("ADS", {})
        if not base_check: return False
        return "autoconf" in url
    
    def parse(self, data: Any, url: str) -> List[Dict]:
        ads = data["ADS"]["AD"]
        if isinstance(ads, dict): ads = [ads]
        
        parsed_vehicles = []
        for v in ads:
            modelo_veiculo = v.get("MODEL")
            opcionais_veiculo = self._parse_features(v.get("FEATURES"))
            categoria_final = definir_categoria_veiculo(modelo_veiculo, opcionais_veiculo)

            parsed = self.normalize_vehicle({
                "id": v.get("ID"), "tipo": ("carro" if v.get("CATEGORY") == "carros" else "moto" if v.get("CATEGORY") == "motos" else v.get("CATEGORY")), "titulo": None, 
                "titulo": None, "versao": (' '.join(re.sub(r'\b(\d\.\d|4x[0-4]|\d+v|diesel|flex|aut|aut.|dies|dies.|mec.|mec|gasolina|manual|automático|4p)\b', '', v.get('VERSION', ''), flags=re.IGNORECASE).split()).strip()) if v.get("VERSION") else None,
                "marca": v.get("MAKE"), "modelo": modelo_veiculo, "ano": v.get("YEAR"), "ano_fabricacao": v.get("FABRIC_YEAR"),
                "km": v.get("MILEAGE"), "cor": v.get("COLOR"), "combustivel": v.get("FUEL"),
                "cambio": v.get("gear") or v.get("GEAR"), "motor": v.get("MOTOR"), "portas": v.get("DOORS"),
                "categoria": categoria_final or v.get("BODY"), "cilindrada": inferir_cilindrada(v.get("VERSION") or modelo_veiculo),
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
    

class RevendamaisParser(BaseParser):
    def can_parse(self, data: Any, url: str) -> bool:
       base_check = isinstance(data, dict) and "ADS" in data and "AD" in data.get("ADS", {})
       if not base_check: return False
       return "revendamais" in url

    def parse(self, data: Any, url: str) -> List[Dict]:
        ads = data["ADS"]["AD"]
        if isinstance(ads, dict): ads = [ads]
        
        parsed_vehicles = []
        for v in ads:
            modelo_veiculo = v.get("MODEL")
            opcionais_veiculo = v.get("ACCESSORIES") or ""
            categoria_final = definir_categoria_veiculo(modelo_veiculo, opcionais_veiculo)
            tipo_veiculo = "MOTO" if v.get("CATEGORY", "").lower() == "motocicleta" else v.get("CATEGORY")

            parsed = self.normalize_vehicle({
                "id": v.get("ID"), "tipo": tipo_veiculo, "titulo": v.get("TITLE"), "versao": None,
                "marca": v.get("MAKE"), "modelo": modelo_veiculo, "ano": v.get("YEAR"),
                "ano_fabricacao": v.get("FABRIC_YEAR"), "km": v.get("MILEAGE"), "cor": v.get("COLOR"),
                "combustivel": v.get("FUEL"), "cambio": v.get("GEAR"), "motor": v.get("MOTOR"),
                "portas": v.get("DOORS"), "categoria": categoria_final or v.get("BODY_TYPE"),
                "cilindrada": inferir_cilindrada(modelo_veiculo), "preco": converter_preco(v.get("PRICE")),
                "opcionais": opcionais_veiculo, "fotos": self.extract_photos(v)
            })
            parsed_vehicles.append(parsed)
        return parsed_vehicles

class BoomParser(BaseParser):
    def can_parse(self, data: Any, url: str) -> bool: return isinstance(data, (dict, list))
    
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
            opcionais_veiculo = self._parse_opcionais(safe_get(v, ["opcionais", "options", "extras", "features", "FEATURES"]))
            categoria_final = definir_categoria_veiculo(modelo_veiculo, opcionais_veiculo)

            parsed = self.normalize_vehicle({
                "id": safe_get(v, ["id", "ID", "codigo", "cod"]), "tipo": safe_get(v, ["tipo", "type", "categoria_veiculo", "CATEGORY"]),
                "titulo": safe_get(v, ["titulo", "title", "TITLE"]), "versao": safe_get(v, ["versao", "version", "variant", "VERSION"]),
                "marca": safe_get(v, ["marca", "brand", "fabricante", "MAKE"]), "modelo": modelo_veiculo,
                "ano": safe_get(v, ["ano_mod", "anoModelo", "ano", "year_model", "ano_modelo", "YEAR"]),
                "ano_fabricacao": safe_get(v, ["ano_fab", "anoFabricacao", "ano_fabricacao", "year_manufacture", "FABRIC_YEAR"]),
                "km": safe_get(v, ["km", "quilometragem", "mileage", "kilometers", "MILEAGE"]), "cor": safe_get(v, ["cor", "color", "colour", "COLOR"]),
                "combustivel": safe_get(v, ["combustivel", "fuel", "fuel_type", "FUEL"]), "cambio": safe_get(v, ["cambio", "transmission", "gear", "GEAR"]),
                "motor": safe_get(v, ["motor", "engine", "motorization", "MOTOR"]), "portas": safe_get(v, ["portas", "doors", "num_doors", "DOORS"]),
                "categoria": categoria_final or safe_get(v, ["categoria", "category", "class", "BODY"]),
                "cilindrada": safe_get(v, ["cilindrada", "displacement", "engine_size"]) or inferir_cilindrada(modelo_veiculo),
                "preco": converter_preco(safe_get(v, ["valor", "valorVenda", "preco", "price", "value", "PRICE"])),
                "opcionais": opcionais_veiculo, "fotos": self._parse_fotos(v)
            })
            parsed_vehicles.append(parsed)
        return parsed_vehicles
    
    def _looks_like_vehicle(self, data: Dict) -> bool: return any(field in data for field in ['modelo', 'model', 'marca', 'brand', 'preco', 'price', 'ano', 'year'])
    
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
        self.parsers = [AltimusParser(), AutocertoParser(), RevendamaisParser(), AutoconfParser(), BoomParser()]
        print("[INFO] Sistema unificado iniciado - detecção automática ativada")
    
    def get_urls(self) -> List[str]: return list({val for var, val in os.environ.items() if var.startswith("XML_URL") and val})
    
    def detect_format(self, content: bytes, url: str) -> tuple[Any, str]:
        content_str = content.decode('utf-8', errors='ignore')
        try: return json.loads(content_str), "json"
        except json.JSONDecodeError:
            try: return xmltodict.parse(content_str), "xml"
            except Exception: raise ValueError(f"Formato não reconhecido para URL: {url}")
    
    def process_url(self, url: str) -> List[Dict]:
        print(f"[INFO] Processando URL: {url}")
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data, format_type = self.detect_format(response.content, url)
            print(f"[INFO] Formato detectado: {format_type}")
            for parser in self.parsers:
                if parser.can_parse(data, url):
                    print(f"[INFO] Usando parser: {parser.__class__.__name__}")
                    return parser.parse(data, url)
            print(f"[AVISO] Nenhum parser adequado encontrado para URL: {url}")
            return []
        except requests.RequestException as e: print(f"[ERRO] Erro de requisição para URL {url}: {e}"); return []
        except Exception as e: print(f"[ERRO] Erro crítico ao processar URL {url}: {e}"); return []
    
    def fetch_all(self) -> Dict:
        urls = self.get_urls()
        if not urls:
            print("[AVISO] Nenhuma variável de ambiente 'XML_URL' foi encontrada.")
            return {}
        
        print(f"[INFO] {len(urls)} URL(s) encontrada(s) para processar")
        all_vehicles = [vehicle for url in urls for vehicle in self.process_url(url)]
        
        result = {"veiculos": all_vehicles, "_updated_at": datetime.now().isoformat(), "_total_count": len(all_vehicles), "_sources_processed": len(urls)}
        
        try:
            with open(JSON_FILE, "w", encoding="utf-8") as f: json.dump(result, f, ensure_ascii=False, indent=2)
            print(f"\n[OK] Arquivo {JSON_FILE} salvo com sucesso!")
        except Exception as e: print(f"[ERRO] Erro ao salvar arquivo JSON: {e}")
        
        print(f"[OK] Total de veículos processados: {len(all_vehicles)}")
        return result

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
              print(f"{i}. {v.get('marca', 'N/A')} {v.get('modelo', 'N/A')} ({v.get('categoria', 'N/A')}) {v.get('ano', 'N/A')} - R$ {v.get('preco', 0.0):,.2f}")
