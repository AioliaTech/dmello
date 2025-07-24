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

# Mapeamento de cilindradas para motos
MAPEAMENTO_CILINDRADAS = {
    "g 310": 300, "f 750 gs": 850, "f 850 gs": 850, "f 900": 900, "r 1250": 1250,
    "r 1300": 1300, "r 18": 1800, "k 1300": 1300, "k 1600": 1650, "s 1000": 1000,
    "g 650 gs": 650, "cb 300": 300, "cb 500": 500, "cb 650": 650, "cb 1000r": 1000,
    "cb twister": 300, "twister": 300, "cbr 250": 250, "cbr 500": 500, "cbr 600": 600,
    "cbr 650": 650, "cbr 1000": 1000, "hornet 600": 600, "cb 600f": 600, "xre 190": 190,
    "xre 300": 300, "xre 300 sahara": 300, "sahara 300": 300, "sahara 300 rally": 300,
    "nxr 160": 160, "bros 160": 160, "cg 160": 160, "cg 160 titan": 160, "cg 160 fan": 160,
    "cg 160 start": 160, "cg 160 titan s": 160, "cg 125": 125, "cg 125 fan ks": 125,
    "biz 125": 125, "biz 125 es": 125, "biz 110": 110, "pop 110": 110, "pop 110i": 110,
    "pcx 150": 150, "pcx 160": 160, "xj6": 600, "mt 03": 300, "mt 07": 690, "mt 09": 890,
    "mt 01": 1700, "fazer 150": 150, "fazer 250": 250, "ys 250": 250, "factor 125": 125,
    "factor 150": 150, "xtz 150": 150, "xtz 250": 250, "xtz 250 tenere": 250, "tenere 250": 250,
    "lander 250": 250, "yzf r3": 300, "yzf r-3": 300, "r15": 150, "r1": 1000,
    "nmax 160": 160, "xmax 250": 250, "gs500": 500, "bandit 600": 600, "bandit 650": 650,
    "bandit 1250": 1250, "gsx 650f": 650, "gsx-s 750": 750, "gsx-s 1000": 1000,
    "hayabusa": 1350, "gixxer 250": 250, "burgman 125": 125, "z300": 300, "z400": 400,
    "z650": 650, "z750": 750, "z800": 800, "z900": 950, "z1000": 1000, "ninja 300": 300,
    "ninja 400": 400, "ninja 650": 650, "ninja 1000": 1050, "ninja zx-10r": 1000,
    "er6n": 650, "versys 300": 300, "versys 650": 650, "xt 660": 660, "meteor 350": 350,
    "classic 350": 350, "hunter 350": 350, "himalayan": 400, "interceptor 650": 650,
    "continental gt 650": 650, "tiger 800": 800, "tiger 900": 900, "street triple": 750,
    "speed triple": 1050, "bonneville": 900, "trident 660": 660, "monster 797": 800,
    "monster 821": 820, "monster 937": 940, "panigale v2": 950, "panigale v4": 1100,
    "iron 883": 883, "forty eight": 1200, "sportster s": 1250, "fat bob": 1140,
    "road glide": 2150, "street glide": 1750, "next 300": 300, "commander 250": 250,
    "dafra citycom 300": 300, "dr 160": 160, "dr 160 s": 160, "cforce 1000": 1000,
    "trx 420": 420, "t350 x": 350, "xr300l tornado": 300, "fz25 fazer": 250, "fz15 fazer": 150,
    "biz es": 125, "elite 125": 125, "crf 230f": 230, "cg150 fan": 150, "cg150 titan": 150,
    "diavel 1260": 1260, "YZF R-6": 600, "MT-03": 300, "MT03": 300, "ER-6N": 650,
    "xt 600": 600, "cg 125": 125
}


# Mapeamento de categorias para carros
MAPEAMENTO_CATEGORIAS = {
    # Hatch
    "gol": "Hatch", "uno": "Hatch", "palio": "Hatch", "celta": "Hatch", "ka": "Hatch",
    "fiesta": "Hatch", "march": "Hatch", "sandero": "Hatch", "onix": "Hatch", "hb20": "Hatch",
    "i30": "Hatch", "golf": "Hatch", "polo": "Hatch", "fox": "Hatch", "up": "Hatch",
    "fit": "Hatch", "city": "Hatch", "yaris": "Hatch", "etios": "Hatch", "clio": "Hatch",
    "corsa": "Hatch", "bravo": "Hatch", "punto": "Hatch", "208": "Hatch", "argo": "Hatch",
    "mobi": "Hatch", "c3": "Hatch", "picanto": "Hatch", "kwid": "Hatch", "soul": "Hatch",
    
    # Sedan
    "civic": "Sedan", "corolla": "Sedan", "sentra": "Sedan", "versa": "Sedan", "jetta": "Sedan",
    "prisma": "Sedan", "voyage": "Sedan", "siena": "Sedan", "cruze": "Sedan", "cobalt": "Sedan",
    "logan": "Sedan", "fluence": "Sedan", "cerato": "Sedan", "elantra": "Sedan", "virtus": "Sedan",
    "accord": "Sedan", "altima": "Sedan", "fusion": "Sedan", "passat": "Sedan", "city sedan": "Sedan",
    "cronos": "Sedan", "linea": "Sedan", "ka sedan": "Sedan", "polo sedan": "Sedan", "bora": "Sedan",
    "hb20s": "Sedan", "lancer": "Sedan", "camry": "Sedan", "onix plus": "Sedan",
    
    # SUV
    "duster": "SUV", "ecosport": "SUV", "hrv": "SUV", "compass": "SUV", "renegade": "SUV",
    "tracker": "SUV", "kicks": "SUV", "captur": "SUV", "creta": "SUV", "tucson": "SUV",
    "santa fe": "SUV", "sorento": "SUV", "sportage": "SUV", "tiguan": "SUV", "t-cross": "SUV",
    "rav4": "SUV", "cr-v": "SUV", "corolla cross": "SUV", "pulse": "SUV", "nivus": "SUV",
    
    # Caminhonete
    "hilux": "Caminhonete", "ranger": "Caminhonete", "s10": "Caminhonete", "l200": "Caminhonete",
    "triton": "Caminhonete", "toro": "Caminhonete", "frontier": "Caminhonete", "amarok": "Caminhonete",
    "maverick": "Caminhonete", "ram 1500": "Caminhonete",
    
    # Utilitário
    "saveiro": "Utilitário", "strada": "Utilitário", "montana": "Utilitário", "oroch": "Utilitário",
    "kangoo": "Utilitário", "partner": "Utilitário", "doblo": "Utilitário", "fiorino": "Utilitário",
    
    # Furgão
    "master": "Furgão", "sprinter": "Furgão", "ducato": "Furgão", "daily": "Furgão",
    "jumper": "Furgão", "boxer": "Furgão", "transit": "Furgão",
}

# =================== UTILS =======================

def normalizar_texto(texto: str) -> str:
    """Normaliza texto removendo acentos e caracteres especiais"""
    if not texto:
        return ""
    texto_norm = unidecode(texto).lower()
    texto_norm = re.sub(r'[^a-z0-9]', '', texto_norm)
    return texto_norm

def inferir_categoria(modelo: str) -> Optional[str]:
    """Infere a categoria do veículo baseado no modelo"""
    if not modelo:
        return None
    modelo_norm = normalizar_texto(modelo)
    for mapeado, categoria in MAPEAMENTO_CATEGORIAS.items():
        mapeado_norm = normalizar_texto(mapeado)
        if mapeado_norm in modelo_norm:
            return categoria
    return None

def inferir_cilindrada(modelo: str) -> Optional[int]:
    """Infere a cilindrada da moto baseado no modelo"""
    if not modelo:
        return None
    modelo_norm = normalizar_texto(modelo)
    for mapeado, cilindrada in MAPEAMENTO_CILINDRADAS.items():
        mapeado_norm = normalizar_texto(mapeado)
        if mapeado_norm in modelo_norm:
            return cilindrada
    return None

def converter_preco(valor: Any) -> float:
    """Converte valor para float, lidando com diferentes formatos"""
    if not valor:
        return 0.0
    
    try:
        if isinstance(valor, (int, float)):
            return float(valor)
        
        # String - remove caracteres não numéricos
        valor_str = str(valor)
        valor_str = re.sub(r'[^\d,.]', '', valor_str)
        valor_str = valor_str.replace(',', '.')
        
        # Remove pontos extras (milhares)
        parts = valor_str.split('.')
        if len(parts) > 2:
            valor_str = ''.join(parts[:-1]) + '.' + parts[-1]
        
        return float(valor_str) if valor_str else 0.0
    except:
        return 0.0

def safe_get(data: Dict, keys: Union[str, List[str]], default: Any = None) -> Any:
    """Extrai valor de dicionário tentando múltiplas chaves"""
    if isinstance(keys, str):
        keys = [keys]
    
    for key in keys:
        if isinstance(data, dict) and key in data and data[key] is not None:
            return data[key]
    
    return default

def flatten_list(data: Any) -> List[Dict]:
    """Achata estruturas de dados para sempre retornar lista de dicionários"""
    if not data:
        return []
    
    if isinstance(data, list):
        result = []
        for item in data:
            if isinstance(item, dict):
                result.append(item)
            elif isinstance(item, list):
                result.extend(flatten_list(item))
        return result
    elif isinstance(data, dict):
        return [data]
    
    return []

# =================== PARSERS =======================

class BaseParser(ABC):
    """Classe base para todos os parsers"""
    
    @abstractmethod
    def can_parse(self, data: Any, url: str) -> bool:
        """Verifica se este parser pode processar os dados"""
        pass
    
    @abstractmethod
    def parse(self, data: Any, url: str) -> List[Dict]:
        """Processa os dados e retorna lista de veículos"""
        pass
    
    def extract_photos(self, vehicle_data: Dict) -> List[str]:
        """Extrai fotos do veículo - pode ser sobrescrito"""
        return []
    
    def normalize_vehicle(self, vehicle: Dict) -> Dict:
        """Garante que todos os campos existam, mesmo que nulos"""
        normalized = {
            "id": vehicle.get("id"),
            "tipo": vehicle.get("tipo"),
            "titulo": vehicle.get("titulo"),
            "versao": vehicle.get("versao"),
            "marca": vehicle.get("marca"),
            "modelo": vehicle.get("modelo"),
            "ano": vehicle.get("ano"),
            "ano_fabricacao": vehicle.get("ano_fabricacao"),
            "km": vehicle.get("km"),
            "cor": vehicle.get("cor"),
            "combustivel": vehicle.get("combustivel"),
            "cambio": vehicle.get("cambio"),
            "motor": vehicle.get("motor"),
            "portas": vehicle.get("portas"),
            "categoria": vehicle.get("categoria"),
            "cilindrada": vehicle.get("cilindrada"),
            "preco": vehicle.get("preco", 0.0),
            "opcionais": vehicle.get("opcionais", ""),
            "fotos": vehicle.get("fotos", [])
        }
        return normalized

class AltimusParser(BaseParser):
    """Parser para formato Altimus (JSON direto)"""
    
    def can_parse(self, data: Any, url: str) -> bool:
        return isinstance(data, dict) and "veiculos" in data
    
    def parse(self, data: Any, url: str) -> List[Dict]:
        veiculos = data.get("veiculos", [])
        if isinstance(veiculos, dict):
            veiculos = [veiculos]
        
        parsed_vehicles = []
        for v in veiculos:
            parsed = {
                "id": v.get("id"),
                "tipo": v.get("tipo"),
                "titulo": None,  # Altimus não tem título
                "versao": v.get("versao"),
                "marca": v.get("marca"),
                "modelo": v.get("modelo"),
                "ano": v.get("anoModelo") or v.get("ano"),
                "ano_fabricacao": v.get("anoFabricacao") or v.get("ano_fabricacao"),
                "km": v.get("km"),
                "cor": v.get("cor"),
                "combustivel": v.get("combustivel"),
                "cambio": v.get("cambio"),
                "motor": v.get("motor"),
                "portas": v.get("portas"),
                "categoria": v.get("categoria"),
                "cilindrada": v.get("cilindrada") or inferir_cilindrada(v.get("modelo")),
                "preco": converter_preco(v.get("valorVenda") or v.get("preco")),
                "opcionais": self._parse_opcionais(v.get("opcionais")),
                "fotos": v.get("fotos") or []
            }
            parsed_vehicles.append(self.normalize_vehicle(parsed))
        
        return parsed_vehicles
    
    def _parse_opcionais(self, opcionais: Any) -> str:
        if isinstance(opcionais, list):
            return ", ".join(str(item) for item in opcionais if item)
        return str(opcionais) if opcionais else ""

class AutocertoParser(BaseParser):
    """Parser para formato Autocerto (XML estoque/veiculo)"""
    
    def can_parse(self, data: Any, url: str) -> bool:
        return isinstance(data, dict) and "estoque" in data and "veiculo" in data.get("estoque", {})
    
    def parse(self, data: Any, url: str) -> List[Dict]:
        veiculos = data["estoque"]["veiculo"]
        if isinstance(veiculos, dict):
            veiculos = [veiculos]
        
        parsed_vehicles = []
        for v in veiculos:
            parsed = {
                "id": v.get("idveiculo"),
                "tipo": v.get("tipoveiculo"),
                "titulo": None,  # Autocerto não tem título
                "versao": None,  # Autocerto não tem versão
                "marca": v.get("marca"),
                "modelo": v.get("modelo"),
                "ano": v.get("anomodelo"),
                "ano_fabricacao": None,  # Autocerto não tem ano fabricação
                "km": v.get("quilometragem"),
                "cor": v.get("cor"),
                "combustivel": v.get("combustivel"),
                "cambio": v.get("cambio"),
                "motor": None,  # Autocerto não tem motor
                "portas": v.get("numeroportas"),
                "categoria": inferir_categoria(v.get("modelo")),
                "cilindrada": inferir_cilindrada(v.get("modelo")),
                "preco": converter_preco(v.get("preco")),
                "opcionais": self._parse_opcionais(v.get("opcionais")),
                "fotos": self.extract_photos(v)
            }
            parsed_vehicles.append(self.normalize_vehicle(parsed))
        
        return parsed_vehicles
    
    def _parse_opcionais(self, opcionais: Any) -> str:
        if isinstance(opcionais, dict) and "opcional" in opcionais:
            opcional = opcionais["opcional"]
            if isinstance(opcional, list):
                return ", ".join(str(item) for item in opcional if item)
            return str(opcional) if opcional else ""
        return ""
    
    def extract_photos(self, v: Dict) -> List[str]:
        fotos = v.get("fotos")
        if not fotos:
            return []
        
        fotos_foto = fotos.get("foto")
        if not fotos_foto:
            return []
        
        if isinstance(fotos_foto, dict):
            fotos_foto = [fotos_foto]
        
        return [
            img["url"].split("?")[0]
            for img in fotos_foto
            if isinstance(img, dict) and "url" in img
        ]

class AutoconfParser(BaseParser):
    """Parser para formato Autoconf (XML ADS/AD)"""
    
    def can_parse(self, data: Any, url: str) -> bool:
        return isinstance(data, dict) and "ADS" in data and "AD" in data.get("ADS", {})
    
    def parse(self, data: Any, url: str) -> List[Dict]:
        ads = data["ADS"]["AD"]
        if isinstance(ads, dict):
            ads = [ads]
        
        parsed_vehicles = []
        for v in ads:
            parsed = {
                "id": v.get("ID"),
                "tipo": v.get("CATEGORY"),
                "titulo": None,  # Autoconf não tem título
                "versao": v.get("VERSION"),
                "marca": v.get("MAKE"),
                "modelo": v.get("MODEL"),
                "ano": v.get("YEAR"),
                "ano_fabricacao": v.get("FABRIC_YEAR"),
                "km": v.get("MILEAGE"),
                "cor": v.get("COLOR"),
                "combustivel": v.get("FUEL"),
                "cambio": v.get("gear") or v.get("GEAR"),
                "motor": v.get("MOTOR"),
                "portas": v.get("DOORS"),
                "categoria": v.get("BODY") or inferir_categoria(v.get("MODEL")),
                "cilindrada": inferir_cilindrada(v.get("VERSION") or v.get("MODEL")),
                "preco": converter_preco(v.get("PRICE")),
                "opcionais": self._parse_features(v.get("FEATURES")),
                "fotos": self.extract_photos(v)
            }
            parsed_vehicles.append(self.normalize_vehicle(parsed))
        
        return parsed_vehicles
    
    def _parse_features(self, features: Any) -> str:
        if not features:
            return ""
        
        if isinstance(features, list):
            result = []
            for feat in features:
                if isinstance(feat, dict) and "FEATURE" in feat:
                    result.append(feat["FEATURE"])
                elif isinstance(feat, str):
                    result.append(feat)
            return ", ".join(result)
        
        return str(features)
    
    def extract_photos(self, v: Dict) -> List[str]:
        images = v.get("IMAGES")
        if not images:
            return []
        
        fotos = []
        if isinstance(images, list):
            for img in images:
                if isinstance(img, dict) and "IMAGE_URL" in img:
                    fotos.append(img["IMAGE_URL"])
        elif isinstance(images, dict) and "IMAGE_URL" in images:
            fotos.append(images["IMAGE_URL"])
        
        return fotos

# =================== CLASSE ALTERADA =======================
class RevendamaisParser(AutoconfParser):
    """Parser para formato Revendamais (similar ao Autoconf mas com campos extras)"""
    
    def can_parse(self, data: Any, url: str) -> bool:
        # Primeiro, verifica se a estrutura básica do Autoconf é válida.
        if not super().can_parse(data, url):
            return False
        
        # Agora, verifica uma característica específica do Revendamais.
        # Por exemplo, a presença da tag <TITLE> no primeiro veículo.
        try:
            ads = data.get("ADS", {}).get("AD", [])
            if isinstance(ads, dict):
                ads = [ads]
            
            # Se houver veículos, verifica se o primeiro tem a tag 'TITLE'
            if ads and 'TITLE' in ads[0]:
                return True
        except (IndexError, KeyError):
            return False
            
        return False

    def parse(self, data: Any, url: str) -> List[Dict]:
        # O método parse() continua o mesmo, pois já estava correto.
        ads = data["ADS"]["AD"]
        if isinstance(ads, dict):
            ads = [ads]
        
        parsed_vehicles = []
        for v in ads:
            parsed = {
                "id": v.get("ID"),
                "tipo": "moto" if v.get("CATEGORY", "").lower() == "motocicleta" else v.get("CATEGORY"),
                "titulo": v.get("TITLE"),  # Revendamais tem título!
                "versao": v.get("MODEL"),
                "marca": v.get("MAKE"),
                "modelo": v.get("BASE_MODEL"),
                "ano": v.get("YEAR"),
                "ano_fabricacao": v.get("FABRIC_YEAR"),
                "km": v.get("MILEAGE"),
                "cor": v.get("COLOR"),
                "combustivel": v.get("FUEL"),
                "cambio": v.get("GEAR"),
                "motor": v.get("MOTOR"),
                "portas": v.get("DOORS"),
                "categoria": v.get("BODY_TYPE") or inferir_categoria(v.get("MODEL")),
                "cilindrada": inferir_cilindrada(v.get("MODEL")),
                "preco": converter_preco(v.get("PRICE")),
                "opcionais": v.get("ACCESSORIES") or "",
                "fotos": self.extract_photos(v)
            }
            parsed_vehicles.append(self.normalize_vehicle(parsed))
        
        return parsed_vehicles

class BoomParser(BaseParser):
    """Parser para formato Boom (JSON genérico)"""
    
    def can_parse(self, data: Any, url: str) -> bool:
        # Este parser é usado como fallback para JSONs genéricos
        return isinstance(data, (dict, list))
    
    def parse(self, data: Any, url: str) -> List[Dict]:
        # Extrai veículos de diferentes estruturas
        veiculos = []
        
        if isinstance(data, list):
            veiculos = flatten_list(data)
        elif isinstance(data, dict):
            # Tenta encontrar lista de veículos em chaves comuns
            for key in ['veiculos', 'vehicles', 'data', 'items', 'results', 'content']:
                if key in data:
                    veiculos = flatten_list(data[key])
                    break
            
            # Se não encontrou, trata o próprio dict como veículo
            if not veiculos and self._looks_like_vehicle(data):
                veiculos = [data]
        
        parsed_vehicles = []
        for v in veiculos:
            if not isinstance(v, dict):
                continue
            
            parsed = {
                "id": safe_get(v, ["id", "ID", "codigo", "cod"]),
                "tipo": safe_get(v, ["tipo", "type", "categoria_veiculo", "CATEGORY"]),
                "titulo": safe_get(v, ["titulo", "title", "TITLE"]),
                "versao": safe_get(v, ["versao", "version", "variant", "VERSION"]),
                "marca": safe_get(v, ["marca", "brand", "fabricante", "MAKE"]),
                "modelo": safe_get(v, ["modelo", "model", "nome", "MODEL"]),
                "ano": safe_get(v, ["ano_mod", "anoModelo", "ano", "year_model", "ano_modelo", "YEAR"]),
                "ano_fabricacao": safe_get(v, ["ano_fab", "anoFabricacao", "ano_fabricacao", "year_manufacture", "FABRIC_YEAR"]),
                "km": safe_get(v, ["km", "quilometragem", "mileage", "kilometers", "MILEAGE"]),
                "cor": safe_get(v, ["cor", "color", "colour", "COLOR"]),
                "combustivel": safe_get(v, ["combustivel", "fuel", "fuel_type", "FUEL"]),
                "cambio": safe_get(v, ["cambio", "transmission", "gear", "GEAR"]),
                "motor": safe_get(v, ["motor", "engine", "motorization", "MOTOR"]),
                "portas": safe_get(v, ["portas", "doors", "num_doors", "DOORS"]),
                "categoria": safe_get(v, ["categoria", "category", "class", "BODY"]),
                "cilindrada": safe_get(v, ["cilindrada", "displacement", "engine_size"]),
                "preco": converter_preco(safe_get(v, ["valor", "valorVenda", "preco", "price", "value", "PRICE"])),
                "opcionais": self._parse_opcionais(safe_get(v, ["opcionais", "options", "extras", "features", "FEATURES"])),
                "fotos": self._parse_fotos(v)
            }
            
            # Infere categoria se não encontrada
            if not parsed["categoria"] and parsed["tipo"] != "MOTO":
                parsed["categoria"] = inferir_categoria(parsed["modelo"])
                
            # Infere cilindrada se não encontrada
            if not parsed["cilindrada"] and parsed["tipo"] == "MOTO":
                parsed["cilindrada"] = inferir_cilindrada(parsed["modelo"])
            
            # Normaliza para garantir todos os campos
            parsed_vehicles.append(self.normalize_vehicle(parsed))
        
        return parsed_vehicles
    
    def _looks_like_vehicle(self, data: Dict) -> bool:
        """Verifica se um dicionário parece ser um veículo"""
        vehicle_fields = ['modelo', 'model', 'marca', 'brand', 'preco', 'price', 'ano', 'year']
        return any(field in data for field in vehicle_fields)
    
    def _parse_opcionais(self, opcionais: Any) -> str:
        if isinstance(opcionais, list):
            # Se for uma lista de dicionários com uma chave 'nome' ou similar
            if all(isinstance(i, dict) for i in opcionais):
                processed_items = []
                for item in opcionais:
                    # Tenta extrair um valor de chaves comuns para nome do opcional
                    name = safe_get(item, ["nome", "name", "descricao", "description", "FEATURE"])
                    if name:
                        processed_items.append(str(name))
                if processed_items:
                    return ", ".join(processed_items)
            
            # Se for uma lista de strings
            return ", ".join(str(item) for item in opcionais if item)
            
        return str(opcionais) if opcionais else ""
    
    def _parse_fotos(self, v: Dict) -> List[str]:
        fotos = safe_get(v, ["galeria", "fotos", "photos", "images", "gallery", "IMAGES"], [])
        
        if not isinstance(fotos, list):
            if fotos:
                fotos = [fotos]
            else:
                fotos = []
        
        # Processa lista de fotos
        result = []
        for foto in fotos:
            if isinstance(foto, str):
                result.append(foto)
            elif isinstance(foto, dict):
                # Tenta extrair URL de diferentes formatos
                url = safe_get(foto, ["url", "URL", "src", "IMAGE_URL", "path"])
                if url:
                    result.append(url)
        
        return result

# =================== SISTEMA PRINCIPAL (ALTERADO) =======================

class UnifiedVehicleFetcher:
    """Sistema unificado para buscar e processar veículos de múltiplas fontes"""
    
    def __init__(self):
        """Inicializa o fetcher com todos os parsers disponíveis"""
        # A ORDEM IMPORTA: coloque os parsers mais específicos primeiro.
        self.parsers = [
            AltimusParser(),
            AutocertoParser(),
            RevendamaisParser(),  # <-- ESPECÍFICO PRIMEIRO
            AutoconfParser(),     # <-- GENÉRICO DEPOIS
            BoomParser(),         # Deve ser o último pois é o mais genérico de todos
        ]
        print("[INFO] Sistema unificado iniciado - detecção automática ativada")
    
    def get_urls(self) -> List[str]:
        """Obtém todas as URLs configuradas nas variáveis de ambiente"""
        urls = set() # Usar set para evitar duplicatas
        
        for var, val in os.environ.items():
            if var.startswith("XML_URL") and val:
                urls.add(val)
        
        return list(urls)
    
    def detect_format(self, content: bytes, url: str) -> tuple[Any, str]:
        """Detecta o formato (XML/JSON) e faz o parse inicial"""
        content_str = content.decode('utf-8', errors='ignore')
        
        # Tenta JSON primeiro
        try:
            data = json.loads(content_str)
            return data, "json"
        except json.JSONDecodeError:
            pass
        
        # Tenta XML
        try:
            data = xmltodict.parse(content)
            return data, "xml"
        except Exception as e:
            # Erro de parsing de XML
            pass
        
        raise ValueError(f"Formato não reconhecido para URL: {url}")
    
    def process_url(self, url: str) -> List[Dict]:
        """Processa uma URL e retorna lista de veículos"""
        print(f"[INFO] Processando URL: {url}")
        
        try:
            # Faz download
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # Detecta formato
            data, format_type = self.detect_format(response.content, url)
            print(f"[INFO] Formato detectado: {format_type}")
            
            # Encontra parser adequado
            for parser in self.parsers:
                if parser.can_parse(data, url):
                    print(f"[INFO] Usando parser: {parser.__class__.__name__}")
                    vehicles = parser.parse(data, url)
                    print(f"[INFO] {len(vehicles)} veículos processados")
                    return vehicles
            
            print(f"[AVISO] Nenhum parser adequado encontrado para URL: {url}")
            return []
            
        except requests.RequestException as e:
            print(f"[ERRO] Erro de requisição para URL {url}: {e}")
            return []
        except Exception as e:
            print(f"[ERRO] Erro ao processar URL {url}: {e}")
            return []
    
    def fetch_all(self) -> Dict:
        """Processa todas as URLs e salva resultado unificado"""
        urls = self.get_urls()
        
        if not urls:
            print("[AVISO] Nenhuma variável de ambiente 'XML_URL' foi encontrada.")
            return {} # Retorna um dicionário vazio se não houver URLs
        
        print(f"[INFO] {len(urls)} URL(s) encontrada(s) para processar")
        
        all_vehicles = []
        
        for url in urls:
            vehicles = self.process_url(url)
            all_vehicles.extend(vehicles)
        
        result = {
            "veiculos": all_vehicles,
            "_updated_at": datetime.now().isoformat(),
            "_total_count": len(all_vehicles),
            "_sources_processed": len(urls)
        }
        
        try:
            with open(JSON_FILE, "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            print(f"[OK] Arquivo {JSON_FILE} salvo com sucesso!")
        except Exception as e:
            print(f"[ERRO] Erro ao salvar arquivo: {e}")
        
        print(f"[OK] Total de veículos processados: {len(all_vehicles)}")
        return result

# =================== FUNÇÃO DE COMPATIBILIDADE =======================

def fetch_and_convert_xml():
    """Função de compatibilidade com código antigo"""
    fetcher = UnifiedVehicleFetcher()
    return fetcher.fetch_all()

# =================== EXECUÇÃO PRINCIPAL =======================

if __name__ == "__main__":
    # O sistema sempre aceita qualquer combinação de URLs
    # Exemplos de uso no terminal:
    # export XML_URL="https://autocerto.com/estoque.xml"
    # export XML_URL_LOJA1="https://altimus.com/veiculos.json"
    # export XML_URL_LOJA2="https://boom.com/inventory.json"
    
    fetcher = UnifiedVehicleFetcher()
    result = fetcher.fetch_all()
    
    if result:
      # Mostra resumo
      total = result.get('_total_count', 0)
      print(f"\n{'='*50}")
      print(f"RESUMO DO PROCESSAMENTO")
      print(f"{'='*50}")
      print(f"Total de veículos: {total}")
      print(f"Atualizado em: {result.get('_updated_at', 'N/A')}")
      print(f"Fontes processadas: {result.get('_sources_processed', 0)}")
      
      if total > 0:
          print(f"\nPrimeiros 5 veículos:")
          for i, v in enumerate(result.get('veiculos', [])[:5], 1):
              marca = v.get('marca', 'N/A')
              modelo = v.get('modelo', 'N/A')
              ano = v.get('ano', 'N/A')
              preco = v.get('preco', 0.0)
              print(f"{i}. {marca} {modelo} {ano} - R$ {preco:,.2f}")
