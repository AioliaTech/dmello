from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from unidecode import unidecode
from rapidfuzz import fuzz
from apscheduler.schedulers.background import BackgroundScheduler
from xml_fetcher import fetch_and_convert_xml
import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass

app = FastAPI()

# Arquivo para armazenar status da última atualização
STATUS_FILE = "last_update_status.json"

# Configuração de prioridades para fallback (do menos importante para o mais importante)
FALLBACK_PRIORITY = [
    "motor",         # Primeiro a ser removido
    "portas",
    "cor",           
    "combustivel",
    "opcionais",
    "cambio",
    "KmMax",
    "AnoMax",
    "modelo",
    "marca",
    "categoria"         # Mais importante (nunca remove sozinho)
]

# Mapeamento de categorias por modelo - Versão atualizada e melhorada
MAPEAMENTO_CATEGORIAS = {}

# Mapeamento combinado: cilindrada e categoria para motos (você vai colar depois)
MAPEAMENTO_MOTOS = {
    # Street/Urbanas (commuter básicas e econômicas)
    "cg 150 titan": (150, "street"),
    "cg 160 titan": (160, "street"),
    "cg 125": (125, "street"),
    "cg 160": (160, "street"),
    "cg 160 fan": (160, "street"),
    "cg 160 start": (160, "street"),
    "cg 160 titan s": (160, "street"),
    "cg 125 fan ks": (125, "street"),
    "cg150 fan": (150, "street"),
    "cg150 titan": (150, "street"),
    "ybr 150": (150, "street"),
    "ybr 125": (125, "street"),
    "factor 125": (125, "street"),
    "factor 150": (150, "street"),
    "fz25": (250, "street"),
    "fz25 fazer": (250, "street"),
    "fz15 fazer": (150, "street"),
    "fazer 150": (150, "street"),
    "fazer 250": (250, "street"),
    "ys 250": (250, "street"),
    "cb 300": (300, "street"),
    "cb twister": (300, "street"),
    "twister": (300, "street"),
    "next 300": (300, "street"),

    # Scooter (transmissão automática, design step-through)
    "biz 125": (125, "scooter"),
    "biz 125 es": (125, "scooter"),
    "biz 110": (110, "scooter"),
    "biz es": (125, "scooter"),
    "pop 110": (110, "scooter"),
    "pop 110i": (110, "scooter"),
    "pcx 150": (150, "scooter"),
    "pcx 160": (160, "scooter"),
    "elite 125": (125, "scooter"),
    "nmax 160": (160, "scooter"),
    "xmax 250": (250, "scooter"),
    "burgman 125": (125, "scooter"),
    "dafra citycom 300": (300, "scooter"),
    "citycom": (300, "scooter"),

    # Trail/Offroad (dual-sport, suspensão robusta)
    "nxr 150 bros": (150, "trail"),
    "nxr 160": (160, "trail"),
    "bros 160": (160, "trail"),
    "nxr 160 bros": (160, "trail"),
    "xre 190": (190, "trail"),
    "xre 300": (300, "trail"),
    "xre 300 sahara": (300, "trail"),
    "sahara 300": (300, "trail"),
    "sahara 300 rally": (300, "trail"),
    "xr300l tornado": (300, "trail"),
    "crf 230f": (230, "offroad"),
    "dr 160": (160, "trail"),
    "dr 160 s": (160, "trail"),
    "xtz 150": (150, "trail"),
    "xtz 250": (250, "trail"),
    "xtz 250 tenere": (250, "trail"),
    "tenere 250": (250, "trail"),
    "lander 250": (250, "trail"),

    # BigTrail/Adventure (alta cilindrada, touring)
    "g 310": (300, "bigtrail"),
    "g 310 gs": (300, "bigtrail"),
    "f 750 gs": (850, "bigtrail"),
    "f 850 gs": (850, "bigtrail"),
    "f 900": (900, "bigtrail"),
    "f 900 gs": (900, "bigtrail"),
    "r 1250": (1250, "bigtrail"),
    "r 1250 gs": (1250, "bigtrail"),
    "r 1300": (1300, "bigtrail"),
    "r 1300 gs": (1300, "bigtrail"),
    "g 650 gs": (650, "bigtrail"),
    "versys 300": (300, "bigtrail"),
    "versys 650": (650, "bigtrail"),
    "versys-x 300": (300, "bigtrail"),
    "tiger 800": (800, "bigtrail"),
    "tiger 900": (900, "bigtrail"),
    "himalayan": (400, "bigtrail"),

    # Esportiva Carenada (supersport, carenagem completa)
    "cbr 250": (250, "esportiva carenada"),
    "cbr 300": (300, "esportiva carenada"),
    "cbr 500": (500, "esportiva carenada"),
    "cbr 600": (600, "esportiva carenada"),
    "cbr 650": (650, "esportiva carenada"),
    "cbr 1000": (1000, "esportiva carenada"),
    "cbr 1000r": (1000, "esportiva carenada"),
    "yzf r3": (300, "esportiva carenada"),
    "yzf r-3": (300, "esportiva carenada"),
    "yzf r-6": (600, "esportiva carenada"),
    "r15": (150, "esportiva carenada"),
    "r1": (1000, "esportiva carenada"),
    "ninja 300": (300, "esportiva carenada"),
    "ninja 400": (400, "esportiva carenada"),
    "ninja 650": (650, "esportiva carenada"),
    "ninja 1000": (1050, "esportiva carenada"),
    "ninja zx-10r": (1000, "esportiva carenada"),
    "s 1000": (1000, "esportiva carenada"),
    "s 1000 rr": (1000, "esportiva carenada"),
    "panigale v2": (950, "esportiva carenada"),
    "panigale v4": (1100, "esportiva carenada"),
    "hayabusa": (1350, "esportiva carenada"),

    # Esportiva Naked (naked sport, sem carenagem)
    "mt 03": (300, "esportiva naked"),
    "mt-03": (300, "esportiva naked"),
    "mt03": (300, "esportiva naked"),
    "mt 07": (690, "esportiva naked"),
    "mt-07": (690, "esportiva naked"),
    "mt 09": (890, "esportiva naked"),
    "mt-09": (890, "esportiva naked"),
    "cb 500": (500, "esportiva naked"),
    "cb 650": (650, "esportiva naked"),
    "cb 1000r": (1000, "esportiva naked"),
    "hornet 600": (600, "esportiva naked"),
    "cb 600f": (600, "esportiva naked"),
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
    "bandit 650": (650, "esportiva naked"),
    "bandit 1250": (1250, "esportiva naked"),
    "gsx 650f": (650, "esportiva naked"),
    "gsx-s 750": (750, "esportiva naked"),
    "gsx-s 1000": (1000, "esportiva naked"),
    "gixxer 250": (250, "esportiva naked"),
    "gs500": (500, "esportiva naked"),
    "monster 797": (800, "esportiva naked"),
    "monster 821": (820, "esportiva naked"),
    "monster 937": (940, "esportiva naked"),
    "street triple": (750, "esportiva naked"),
    "speed triple": (1050, "esportiva naked"),
    "trident 660": (660, "esportiva naked"),

    # Custom/Cruiser (posição relaxada, estética clássica)
    "iron 883": (883, "custom"),
    "forty eight": (1200, "custom"),
    "sportster s": (1250, "custom"),
    "fat bob": (1140, "custom"),
    "meteor 350": (350, "custom"),
    "classic 350": (350, "custom"),
    "hunter 350": (350, "custom"),
    "interceptor 650": (650, "custom"),
    "continental gt 650": (650, "custom"),
    "diavel 1260": (1260, "custom"),
    "r 18": (1800, "custom"),
    "bonneville": (900, "custom"),
    "mt 01": (1700, "custom"),

    # Touring (longas distâncias, conforto)
    "road glide": (2150, "touring"),
    "street glide": (1750, "touring"),
    "k 1300": (1300, "touring"),
    "k 1600": (1650, "touring"),
    "xt 660": (660, "touring"),
    "xt 600": (600, "touring"),

    # ATV/Quadriciclo
    "cforce 1000": (1000, "custom"),
    "trx 420": (420, "custom"),
    "t350 x": (350, "custom"),

    # Modelos especiais
    "commander 250": (250, "street"),
    "gk350": (350, "street"),
}

# --- Listas de Modelos por Categoria ---

hatch_models = ["gol", "uno", "palio", "celta", "march", "sandero", "i30", "golf", "fox", "up", "fit", "etios", "bravo", "punto", "208", "argo", "mobi", "c3", "picanto", "stilo", "c4 vtr", "kwid", "soul", "agile", "fusca", "a1", "new beetle", "116i", "118i", "120i", "125i", "m135i", "m140i"]
for model in hatch_models: 
    MAPEAMENTO_CATEGORIAS[model] = "hatch"

sedan_models = ["a6", "sentra", "jetta", "voyage", "siena", "grand siena", "cobalt", "logan", "fluence", "cerato", "elantra", "virtus", "accord", "altima", "fusion", "passat", "vectra sedan", "classic", "cronos", "linea", "408", "c4 pallas", "bora", "hb20s", "lancer", "camry", "onix plus", "azera", "malibu", "318i", "320d", "320i", "328i", "330d", "330i", "335i", "520d", "528i", "530d", "530i", "535i", "540i", "550i", "740i", "750i", "c180", "c200", "c250", "c300", "e250", "e350", "m3", "m5", "s4", "classe c", "classe e", "classe s", "eqe", "eqs"]
for model in sedan_models: 
    MAPEAMENTO_CATEGORIAS[model] = "sedan"

hatch_sedan_models = ["onix", "hb20", "yaris", "city", "a3", "corolla", "civic", "focus", "fiesta", "corsa", "astra", "vectra", "cruze", "clio", "megane", "206", "207", "307", "tiida", "ka", "versa", "prisma", "polo", "c4", "sonic", "série 1", "série 2", "série 3", "série 4", "série 5", "série 6", "série 7", "classe a", "cla"]
for model in hatch_sedan_models: 
    MAPEAMENTO_CATEGORIAS[model] = "hatch,sedan"

suv_models = ["xc60", "edge", "outlander", "range rover evoque", "song plus", "duster", "ecosport", "hrv", "hr-v", "compass", "renegade", "tracker", "kicks", "captur", "creta", "tucson", "santa fe", "sorento", "sportage", "pajero", "tr4", "aircross", "tiguan", "t-cross", "tcross", "rav4", "land cruiser", "cherokee", "grand cherokee", "trailblazer", "pulse", "fastback", "territory", "bronco sport", "2008", "3008", "5008", "c4 cactus", "taos", "crv", "cr-v", "corolla cross", "sw4", "pajero sport", "commander", "nivus", "equinox", "x1", "x2", "x3", "x4", "x5", "x6", "x7", "ix", "ix1", "ix2", "ix3", "gla", "glb", "glc", "gle", "gls", "classe g", "eqa", "eqb", "eqc", "q2", "q3", "q5", "q7", "q8", "q6 e-tron", "e-tron", "q4 e-tron", "q4etron", "wrx", "xv"]
for model in suv_models: 
    MAPEAMENTO_CATEGORIAS[model] = "suv"

caminhonete_models = ["duster oroch", "hilux", "ranger", "s10", "l200", "triton", "toro", "frontier", "amarok", "maverick", "montana", "ram 1500", "rampage", "f-250", "f250", "courier", "dakota", "gladiator", "hoggar"]
for model in caminhonete_models: 
    MAPEAMENTO_CATEGORIAS[model] = "caminhonete"

utilitario_models = ["saveiro", "strada", "oroch", "kangoo", "partner", "doblo", "fiorino", "kombi", "doblo cargo", "berlingo", "combo", "express", "hr"]
for model in utilitario_models: 
    MAPEAMENTO_CATEGORIAS[model] = "utilitario"

furgao_models = ["boxer", "daily", "ducato", "expert", "jumper", "jumpy", "master", "scudo", "sprinter", "trafic", "transit", "vito"]
for model in furgao_models: 
    MAPEAMENTO_CATEGORIAS[model] = "furgao"

coupe_models = ["370z", "brz", "camaro", "challenger", "corvette", "gt86", "mustang", "r8", "rcz", "rx8", "supra", "tt", "tts", "veloster", "m2", "m4", "m8", "s5", "amg gt"]
for model in coupe_models: 
    MAPEAMENTO_CATEGORIAS[model] = "coupe"

conversivel_models = ["911 cabrio", "beetle cabriolet", "boxster", "eos", "miata", "mini cabrio", "slk", "z4", "série 8", "slc", "sl"]
for model in conversivel_models: 
    MAPEAMENTO_CATEGORIAS[model] = "conversivel"

station_wagon_models = ["a4 avant", "fielder", "golf variant", "palio weekend", "parati", "quantum", "spacefox", "rs2", "rs4", "rs6"]
for model in station_wagon_models: 
    MAPEAMENTO_CATEGORIAS[model] = "station wagon"

minivan_models = ["caravan", "carnival", "grand c4", "idea", "livina", "meriva", "picasso", "scenic", "sharan", "spin", "touran", "xsara picasso", "zafira", "série 2 active tourer", "classe b", "classe t", "classe r", "classe v"]
for model in minivan_models: 
    MAPEAMENTO_CATEGORIAS[model] = "minivan"

offroad_models = ["bandeirante", "bronco", "defender", "grand vitara", "jimny", "samurai", "troller", "wrangler"]
for model in offroad_models: 
    MAPEAMENTO_CATEGORIAS[model] = "off-road"

@dataclass
class SearchResult:
    """Resultado de uma busca com informações de fallback"""
    vehicles: List[Dict[str, Any]]
    total_found: int
    fallback_info: Dict[str, Any]
    removed_filters: List[str]

class VehicleSearchEngine:
    """Engine de busca de veículos com sistema de fallback inteligente"""
    
    def __init__(self):
        self.exact_fields = ["tipo", "marca", "cambio", "motor", "portas"]
        
    def normalize_text(self, text: str) -> str:
        """Normaliza texto para comparação"""
        if not text:
            return ""
        return unidecode(str(text)).lower().replace("-", "").replace(" ", "").strip()
    
    def convert_price(self, price_str: Any) -> Optional[float]:
        """Converte string de preço para float"""
        if not price_str:
            return None
        try:
            # Se já é um número (float/int), retorna diretamente
            if isinstance(price_str, (int, float)):
                return float(price_str)
            
            # Se é string, limpa e converte
            cleaned = str(price_str).replace(",", "").replace("R$", "").replace(".", "").strip()
            return float(cleaned) / 100 if len(cleaned) > 2 else float(cleaned)
        except (ValueError, TypeError):
            return None
    
    def convert_year(self, year_str: Any) -> Optional[int]:
        """Converte string de ano para int"""
        if not year_str:
            return None
        try:
            cleaned = str(year_str).strip().replace('\n', '').replace('\r', '').replace(' ', '')
            return int(cleaned)
        except (ValueError, TypeError):
            return None
    
    def convert_km(self, km_str: Any) -> Optional[int]:
        """Converte string de km para int"""
        if not km_str:
            return None
        try:
            cleaned = str(km_str).replace(".", "").replace(",", "").strip()
            return int(cleaned)
        except (ValueError, TypeError):
            return None
    
    def convert_cc(self, cc_str: Any) -> Optional[float]:
        """Converte string de cilindrada para float"""
        if not cc_str:
            return None
        try:
            # Se já é um número (float/int), retorna diretamente
            if isinstance(cc_str, (int, float)):
                return float(cc_str)
            
            # Se é string, limpa e converte
            cleaned = str(cc_str).replace(",", ".").replace("L", "").replace("l", "").strip()
            # Se o valor for menor que 10, provavelmente está em litros (ex: 1.0, 2.0)
            # Converte para CC multiplicando por 1000
            value = float(cleaned)
            if value < 10:
                return value * 1000
            return value
        except (ValueError, TypeError):
            return None
    
    def get_max_value_from_range_param(self, param_value: str) -> str:
        """Extrai o maior valor de parâmetros de range que podem ter múltiplos valores"""
        if not param_value:
            return param_value
        
        # Se tem vírgula, pega o maior valor
        if ',' in param_value:
            try:
                values = [float(v.strip()) for v in param_value.split(',') if v.strip()]
                if values:
                    return str(max(values))
            except (ValueError, TypeError):
                pass
        
        return param_value
    
    def find_category_by_model(self, model: str) -> Optional[str]:
        """Encontra categoria baseada no modelo usando mapeamento"""
        if not model:
            return None
            
        # Normaliza o modelo para busca
        normalized_model = self.normalize_text(model)
        
        # Busca em motos primeiro (mapeamento mais específico)
        if normalized_model in MAPEAMENTO_MOTOS:
            _, category = MAPEAMENTO_MOTOS[normalized_model]
            return category
        
        # Busca parcial em motos - verifica se alguma palavra do modelo está no mapeamento
        model_words = normalized_model.split()
        for word in model_words:
            if len(word) >= 3 and word in MAPEAMENTO_MOTOS:
                _, category = MAPEAMENTO_MOTOS[word]
                return category
        
        # Busca por substring em motos - verifica se o modelo contém alguma chave do mapeamento
        for key, (_, category) in MAPEAMENTO_MOTOS.items():
            if key in normalized_model or normalized_model in key:
                return category
        
        # Se não encontrou em motos, busca em carros
        # Busca exata primeiro
        if normalized_model in MAPEAMENTO_CATEGORIAS:
            return MAPEAMENTO_CATEGORIAS[normalized_model]
        
        # Busca parcial - verifica se alguma palavra do modelo está no mapeamento
        for word in model_words:
            if len(word) >= 3 and word in MAPEAMENTO_CATEGORIAS:
                return MAPEAMENTO_CATEGORIAS[word]
        
        # Busca por substring - verifica se o modelo contém alguma chave do mapeamento
        for key, category in MAPEAMENTO_CATEGORIAS.items():
            if key in normalized_model or normalized_model in key:
                return category
        
        return None
    
    def exact_match(self, query_words: List[str], field_content: str) -> Tuple[bool, str]:
        """Busca exata: todas as palavras devem estar presentes (substring)"""
        if not query_words or not field_content:
            return False, "empty_input"
            
        normalized_content = self.normalize_text(field_content)
        
        for word in query_words:
            normalized_word = self.normalize_text(word)
            if len(normalized_word) < 2:
                continue
                
            # Deve estar presente como substring
            if normalized_word not in normalized_content:
                return False, f"exact_miss: '{normalized_word}' não encontrado"
        
        return True, f"exact_match: todas as palavras encontradas"
    
    def _fuzzy_match_all_words(self, query_words: List[str], field_content: str, fuzzy_threshold: int) -> Tuple[bool, str]:
        """Para motos: TODAS as palavras da query devem ter match"""
        normalized_content = self.normalize_text(field_content)
        matched_words = []
        match_details = []
        
        for word in query_words:
            normalized_word = self.normalize_text(word)
            if len(normalized_word) < 2:
                continue
                
            word_matched = False
            
            # Match exato (substring)
            if normalized_word in normalized_content:
                matched_words.append(normalized_word)
                match_details.append(f"exact:{normalized_word}")
                word_matched = True
            
            # Match no início da palavra
            elif not word_matched:
                content_words = normalized_content.split()
                for content_word in content_words:
                    if content_word.startswith(normalized_word):
                        matched_words.append(normalized_word)
                        match_details.append(f"starts_with:{normalized_word}")
                        word_matched = True
                        break
            
            # Substring match
            elif not word_matched and len(normalized_word) >= 3:
                content_words = normalized_content.split()
                for content_word in content_words:
                    if normalized_word in content_word:
                        matched_words.append(normalized_word)
                        match_details.append(f"substring:{normalized_word}>{content_word}")
                        word_matched = True
                        break
            
            # Fuzzy match como último recurso
            elif not word_matched and len(normalized_word) >= 3:
                partial_score = fuzz.partial_ratio(normalized_content, normalized_word)
                ratio_score = fuzz.ratio(normalized_content, normalized_word)
                max_score = max(partial_score, ratio_score)
                
                if max_score >= fuzzy_threshold:
                    matched_words.append(normalized_word)
                    match_details.append(f"fuzzy:{normalized_word}({max_score})")
                    word_matched = True
            
            # Se alguma palavra não teve match, falha para motos
            if not word_matched:
                return False, f"moto_strict: palavra '{normalized_word}' não encontrada"
        
        # Todas as palavras tiveram match
        if len(matched_words) >= len([w for w in query_words if len(self.normalize_text(w)) >= 2]):
            return True, f"moto_all_match: {', '.join(match_details)}"
        
        return False, "moto_strict: nem todas as palavras encontradas"
    
    def _fuzzy_match_any_word(self, query_words: List[str], field_content: str, fuzzy_threshold: int) -> Tuple[bool, str]:
        """Para carros: mantém a lógica original (qualquer palavra basta)"""
        normalized_content = self.normalize_text(field_content)
        
        for word in query_words:
            normalized_word = self.normalize_text(word)
            if len(normalized_word) < 2:
                continue
                
            # Match exato (substring)
            if normalized_word in normalized_content:
                return True, f"exact_match: {normalized_word}"
            
            # Match no início da palavra
            content_words = normalized_content.split()
            for content_word in content_words:
                if content_word.startswith(normalized_word):
                    return True, f"starts_with_match: {normalized_word}"
                    
            # Match fuzzy para palavras com 3+ caracteres
            if len(normalized_word) >= 3:
                # Substring match
                for content_word in content_words:
                    if normalized_word in content_word:
                        return True, f"substring_match: {normalized_word} in {content_word}"
                
                # Fuzzy matching tradicional
                partial_score = fuzz.partial_ratio(normalized_content, normalized_word)
                ratio_score = fuzz.ratio(normalized_content, normalized_word)
                max_score = max(partial_score, ratio_score)
                
                if max_score >= fuzzy_threshold:
                    return True, f"fuzzy_match: {max_score} (threshold: {fuzzy_threshold})"
        
        return False, "no_match"
    
    def fuzzy_match(self, query_words: List[str], field_content: str, vehicle_type: str = None) -> Tuple[bool, str]:
        """Verifica se há match fuzzy entre as palavras da query e o conteúdo do campo"""
        if not query_words or not field_content:
            return False, "empty_input"
        
        # Define o threshold baseado no tipo do veículo
        fuzzy_threshold = 98 if vehicle_type == "moto" else 87
        
        # Para motos: exige que TODAS as palavras façam match
        # Para outros veículos: mantém a lógica original (qualquer palavra)
        if vehicle_type == "moto":
            return self._fuzzy_match_all_words(query_words, field_content, fuzzy_threshold)
        else:
            return self._fuzzy_match_any_word(query_words, field_content, fuzzy_threshold)
    
    def model_match(self, query_words: List[str], field_content: str, vehicle_type: str = None) -> Tuple[bool, str]:
        """Busca em três níveis: Exato → Fuzzy → Falha"""
        
        # NÍVEL 1: Busca exata
        exact_result, exact_reason = self.exact_match(query_words, field_content)
        if exact_result:
            return True, f"EXACT: {exact_reason}"
        
        # NÍVEL 2: Busca fuzzy
        fuzzy_result, fuzzy_reason = self.fuzzy_match(query_words, field_content, vehicle_type)
        if fuzzy_result:
            return True, f"FUZZY: {fuzzy_reason}"
        
        # NÍVEL 3: Falha (vai para fallback)
        return False, f"NO_MATCH: exact({exact_reason}) + fuzzy({fuzzy_reason})"
    
    def model_exists_in_database(self, vehicles: List[Dict], model_query: str) -> bool:
        """Verifica se um modelo existe no banco usando busca em três níveis"""
        if not model_query:
            return False
            
        query_words = model_query.split()
        
        for vehicle in vehicles:
            vehicle_type = vehicle.get("tipo", "")
            
            # Verifica nos campos de modelo, titulo e versao
            for field in ["modelo", "titulo", "versao"]:
                field_value = str(vehicle.get(field, ""))
                if field_value:
                    is_match, _ = self.model_match(query_words, field_value, vehicle_type)
                    if is_match:
                        return True
        return False
    
    def split_multi_value(self, value: str) -> List[str]:
        """Divide valores múltiplos separados por vírgula"""
        if not value:
            return []
        return [v.strip() for v in str(value).split(',') if v.strip()]
    
    def apply_filters(self, vehicles: List[Dict], filters: Dict[str, str]) -> List[Dict]:
        """Aplica filtros aos veículos"""
        if not filters:
            return vehicles
            
        filtered_vehicles = list(vehicles)
        
        for filter_key, filter_value in filters.items():
            if not filter_value or not filtered_vehicles:
                continue
            
            if filter_key == "modelo":
                # Filtro de modelo: busca em três níveis (exato → fuzzy → falha)
                multi_values = self.split_multi_value(filter_value)
                all_words = []
                for val in multi_values:
                    all_words.extend(val.split())
                
                filtered_vehicles = [
                    v for v in filtered_vehicles
                    if (self.model_match(all_words, str(v.get("modelo", "")), v.get("tipo", ""))[0] or 
                        self.model_match(all_words, str(v.get("titulo", "")), v.get("tipo", ""))[0] or
                        self.model_match(all_words, str(v.get("versao", "")), v.get("tipo", ""))[0])
                ]
                
            elif filter_key == "cor":
                # Outros filtros continuam usando apenas fuzzy
                multi_values = self.split_multi_value(filter_value)
                all_words = []
                for val in multi_values:
                    all_words.extend(val.split())
                
                filtered_vehicles = [
                    v for v in filtered_vehicles
                    if self.fuzzy_match(all_words, str(v.get("cor", "")), v.get("tipo", ""))[0]
                ]
                
            elif filter_key == "categoria":
                multi_values = self.split_multi_value(filter_value)
                all_words = []
                for val in multi_values:
                    all_words.extend(val.split())
                
                filtered_vehicles = [
                    v for v in filtered_vehicles
                    if self.fuzzy_match(all_words, str(v.get("categoria", "")), v.get("tipo", ""))[0]
                ]
                
            elif filter_key == "opcionais":
                multi_values = self.split_multi_value(filter_value)
                all_words = []
                for val in multi_values:
                    all_words.extend(val.split())
                
                filtered_vehicles = [
                    v for v in filtered_vehicles
                    if self.fuzzy_match(all_words, str(v.get("opcionais", "")), v.get("tipo", ""))[0]
                ]
                
            elif filter_key == "combustivel":
                multi_values = self.split_multi_value(filter_value)
                all_words = []
                for val in multi_values:
                    all_words.extend(val.split())
                
                filtered_vehicles = [
                    v for v in filtered_vehicles
                    if self.fuzzy_match(all_words, str(v.get("combustivel", "")), v.get("tipo", ""))[0]
                ]
                
            elif filter_key in self.exact_fields:
                normalized_values = [
                    self.normalize_text(v) for v in self.split_multi_value(filter_value)
                ]
                
                filtered_vehicles = [
                    v for v in filtered_vehicles
                    if self.normalize_text(str(v.get(filter_key, ""))) in normalized_values
                ]
        
        return filtered_vehicles
    
    def apply_range_filters(self, vehicles: List[Dict], valormax: Optional[str], 
                          anomax: Optional[str], kmmax: Optional[str], ccmax: Optional[str]) -> List[Dict]:
        """Aplica filtros de faixa"""
        filtered_vehicles = list(vehicles)
        
        # Filtro de valor máximo - sem expansão, ranqueia por proximidade
        if valormax:
            try:
                target_price = float(valormax)
                # Não aplica filtro de teto aqui, apenas prepara para ranqueamento
            except ValueError:
                pass
        
        # Filtro de ano máximo - como teto (não ranqueia)
        if anomax:
            try:
                max_year = int(anomax)
                filtered_vehicles = [
                    v for v in filtered_vehicles
                    if self.convert_year(v.get("ano")) is not None and
                    self.convert_year(v.get("ano")) <= max_year
                ]
            except ValueError:
                pass
        
        # Filtro de km máximo - como teto (não ranqueia)
        if kmmax:
            try:
                max_km = int(kmmax)
                filtered_vehicles = [
                    v for v in filtered_vehicles
                    if self.convert_km(v.get("km")) is not None and
                    self.convert_km(v.get("km")) <= max_km
                ]
            except ValueError:
                pass
        
        # Filtro de cilindrada - sem teto, ranqueia por proximidade
        if ccmax:
            try:
                target_cc = float(ccmax)
                # Converte para CC se necessário (valores < 10 são assumidos como litros)
                if target_cc < 10:
                    target_cc *= 1000
                # Não aplica filtro de teto aqui, apenas prepara para ranqueamento
            except ValueError:
                pass
        
        return filtered_vehicles
    
    def sort_vehicles(self, vehicles: List[Dict], valormax: Optional[str], 
                     anomax: Optional[str], kmmax: Optional[str], ccmax: Optional[str]) -> List[Dict]:
        """Ordena veículos baseado nos filtros aplicados"""
        if not vehicles:
            return vehicles
        
        # Prioridade 1: Se tem CcMax, ordena por proximidade da cilindrada
        if ccmax:
            try:
                target_cc = float(ccmax)
                # Converte para CC se necessário (valores < 10 são assumidos como litros)
                if target_cc < 10:
                    target_cc *= 1000
                    
                return sorted(vehicles, key=lambda v: 
                    abs((self.convert_cc(v.get("cilindrada")) or 0) - target_cc))
            except ValueError:
                pass
        
        # Prioridade 2: Se tem ValorMax, ordena por proximidade do valor
        if valormax:
            try:
                target_price = float(valormax)
                return sorted(vehicles, key=lambda v: 
                    abs((self.convert_price(v.get("preco")) or 0) - target_price))
            except ValueError:
                pass
        
        # Prioridade 3: Se tem KmMax, ordena por KM crescente
        if kmmax:
            return sorted(vehicles, key=lambda v: self.convert_km(v.get("km")) or float('inf'))
        
        # Prioridade 4: Se tem AnoMax, ordena por ano decrescente (mais novos primeiro)
        if anomax:
            return sorted(vehicles, key=lambda v: self.convert_year(v.get("ano")) or 0, reverse=True)
        
        # Ordenação padrão: por preço decrescente
        return sorted(vehicles, key=lambda v: self.convert_price(v.get("preco")) or 0, reverse=True)
    
    def search_with_fallback(self, vehicles: List[Dict], filters: Dict[str, str],
                            valormax: Optional[str], anomax: Optional[str], kmmax: Optional[str],
                            ccmax: Optional[str], excluded_ids: set) -> SearchResult:
        """Executa busca com fallback progressivo seguindo FALLBACK_PRIORITY"""
        
        # Primeira tentativa: busca normal (já inclui exato → fuzzy para modelo)
        filtered_vehicles = self.apply_filters(vehicles, filters)
        filtered_vehicles = self.apply_range_filters(filtered_vehicles, valormax, anomax, kmmax, ccmax)
        
        if excluded_ids:
            filtered_vehicles = [
                v for v in filtered_vehicles
                if str(v.get("id")) not in excluded_ids
            ]
        
        if filtered_vehicles:
            sorted_vehicles = self.sort_vehicles(filtered_vehicles, valormax, anomax, kmmax, ccmax)
            
            return SearchResult(
                vehicles=sorted_vehicles[:6],
                total_found=len(sorted_vehicles),
                fallback_info={},
                removed_filters=[]
            )
        
        # REGRA ESPECIAL: Se só tem 1 filtro e NÃO é 'modelo', não faz fallback
        if len(filters) == 1 and "modelo" not in filters:
            return SearchResult(
                vehicles=[],
                total_found=0,
                fallback_info={"no_fallback_reason": "single_filter_not_model"},
                removed_filters=[]
            )
        
        # Fallback normal: segue a ordem FALLBACK_PRIORITY
        current_filters = dict(filters)
        removed_filters = []
        current_valormax = valormax
        current_anomax = anomax
        current_kmmax = kmmax
        current_ccmax = ccmax
        
        for filter_to_remove in FALLBACK_PRIORITY:
            if filter_to_remove == "KmMax" and current_kmmax:
                # Verifica se existem veículos que atendem ao KmMax antes de remover
                test_vehicles = self.apply
