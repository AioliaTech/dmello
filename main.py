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
    "cor",           # Menos importante
    "combustivel",
    "opcionais",
    "cambio",
    "modelo",
    "marca",
    "categoria"         # Mais importante (nunca remove sozinho)
]

# Prioridade para parâmetros de range
RANGE_FALLBACK = ["CcMax", "KmMax", "AnoMax", "ValorMax"]

# Mapeamento de categorias por modelo
# Mapeamento de categorias por modelo
MAPEAMENTO_CATEGORIAS = {
    # Hatchbacks
    "gol": "hatch", "uno": "hatch", "palio": "hatch", "celta": "hatch", "fiesta": "hatch", "march": "hatch", "sandero": "hatch", "onix": "hatch", "hb20": "hatch", "i30": "hatch", "golf": "hatch", "polo": "hatch", "fox": "hatch", "up": "hatch", "fit": "hatch", "city": "hatch", "yaris": "hatch", "etios": "hatch", "clio": "hatch", "corsa": "hatch", "bravo": "hatch", "punto": "hatch", "208": "hatch", "argo": "hatch", "mobi": "hatch", "c3": "hatch", "picanto": "hatch", "stilo": "hatch", "206": "hatch", "207": "hatch", "c4 vtr": "hatch", "kwid": "hatch", "soul": "hatch", "agile": "hatch", "fusca": "hatch",
    
    # Sedans
    "civic": "sedan", "corolla": "sedan", "sentra": "sedan", "jetta": "sedan", "voyage": "sedan", "siena": "sedan", "grand siena": "sedan", "cruze": "sedan", "cobalt": "sedan", "logan": "sedan", "fluence": "sedan", "cerato": "sedan", "elantra": "sedan", "virtus": "sedan", "accord": "sedan", "altima": "sedan", "fusion": "sedan", "mazda3": "sedan", "mazda6": "sedan", "passat": "sedan", "vectra sedan": "sedan", "classic": "sedan", "cronos": "sedan", "linea": "sedan", "408": "sedan", "508": "sedan", "c4 pallas": "sedan", "bora": "sedan", "hb20s": "sedan", "lancer": "sedan", "camry": "sedan", "onix plus": "sedan", "megane": "sedan", "azera": "sedan", "mondeo": "sedan",
    
    # Modelos com multiplas categorias (hatch e sedan)
    "focus": "hatch,sedan", "astra": "hatch,sedan", "sonic": "hatch,sedan", "c4": "hatch,sedan", "ka": "hatch,sedan", "versa": "hatch,sedan", "prisma": "hatch,sedan", "polo": "hatch,sedan", "city": "hatch,sedan",
    
    # SUVs
    "duster": "suv", "ecosport": "suv", "hrv": "suv", "hr-v": "suv", "compass": "suv", "renegade": "suv", "tracker": "suv", "kicks": "suv", "captur": "suv", "creta": "suv", "tucson": "suv", "santa fe": "suv", "santa": "suv", "sorento": "suv", "sportage": "suv", "outlander": "suv", "asx": "suv", "pajero": "suv", "tr4": "suv", "aircross": "suv", "tiguan": "suv", "t-cross": "suv", "tcross": "suv", "touareg": "suv", "rav4": "suv", "cx5": "suv", "forester": "suv", "wrv": "suv", "land cruiser": "suv", "cherokee": "suv", "grand cherokee": "suv", "xtrail": "suv", "x-trail": "suv", "murano": "suv", "cx9": "suv", "edge": "suv", "trailblazer": "suv", "pulse": "suv", "fastback": "suv", "territory": "suv", "bronco sport": "suv", "2008": "suv", "3008": "suv", "5008": "suv", "c4 cactus": "suv", "taos": "suv", "crv": "suv", "cr-v": "suv", "corolla cross": "suv", "sw4": "suv", "pajero sport": "suv", "commander": "suv", "xv": "suv", "xc60": "suv", "tiggo 5x": "suv", "haval h6": "suv", "nivus": "suv", "pilot": "suv", "highlander": "suv", "equinox": "suv", "tahoe": "suv", "explorer": "suv", "pathfinder": "suv", "frontier suv": "suv",
    
    # Caminhonetes
    "hilux": "caminhonete", "ranger": "caminhonete", "s10": "caminhonete", "l200": "caminhonete", "triton": "caminhonete", "toro": "caminhonete", "frontier": "caminhonete", "amarok": "caminhonete", "gladiator": "caminhonete", "maverick": "caminhonete", "colorado": "caminhonete", "dakota": "caminhonete", "montana (nova)": "caminhonete", "f-250": "caminhonete", "f250": "caminhonete", "courier (pickup)": "caminhonete", "hoggar": "caminhonete", "ram 1500": "caminhonete",
    
    # Utilitarios
    "saveiro": "utilitario", "strada": "utilitario", "montana": "utilitario", "oroch": "utilitario", "kangoo": "utilitario", "partner": "utilitario", "doblo": "utilitario", "fiorino": "utilitario", "berlingo": "utilitario", "express": "utilitario", "combo": "utilitario", "kombi": "utilitario", "doblo cargo": "utilitario", "kangoo express": "utilitario",
    
    # Furgoes
    "master": "furgao", "sprinter": "furgao", "ducato": "furgao", "daily": "furgao", "jumper": "furgao", "boxer": "furgao", "trafic": "furgao", "transit": "furgao", "vito": "furgao", "expert (furgao)": "furgao", "jumpy (furgao)": "furgao", "scudo (furgao)": "furgao",
    
    # Coupes
    "camaro": "coupe", "mustang": "coupe", "tt": "coupe", "supra": "coupe", "370z": "coupe", "rx8": "coupe", "challenger": "coupe", "corvette": "coupe", "veloster": "coupe", "cerato koup": "coupe", "clk coupe": "coupe", "a5 coupe": "coupe", "gt86": "coupe", "rcz": "coupe", "brz": "coupe",
    
    # Conversiveis
    "z4": "conversivel", "boxster": "conversivel", "miata": "conversivel", "beetle cabriolet": "conversivel", "slk": "conversivel", "911 cabrio": "conversivel", "tt roadster": "conversivel", "a5 cabrio": "conversivel", "mini cabrio": "conversivel", "206 cc": "conversivel", "eos": "conversivel",
    
    # Minivans
    "spin": "minivan", "livina": "minivan", "caravan": "minivan", "touran": "minivan", "sharan": "minivan", "zafira": "minivan", "picasso": "minivan", "grand c4": "minivan", "meriva": "minivan", "scenic": "minivan", "xsara picasso": "minivan", "carnival": "minivan", "idea": "minivan",
    
    # Station wagons
    "parati": "station wagon", "quantum": "station wagon", "spacefox": "station wagon", "golf variant": "station wagon", "palio weekend": "station wagon", "astra sw": "station wagon", "206 sw": "station wagon", "a4 avant": "station wagon", "fielder": "station wagon",
    
    # Off-road
    "wrangler": "off-road", "troller": "off-road", "defender": "off-road", "bronco": "off-road", "samurai": "off-road", "jimny": "off-road", "land cruiser": "off-road", "grand vitara": "off-road", "jimny sierra": "off-road", "bandeirante (ate 2001)": "off-road",
}

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
        self.fuzzy_fields = ["modelo", "titulo", "cor", "opcionais"]
        self.exact_fields = ["tipo", "marca", "categoria", "cambio", "combustivel"]
        
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
    
    def find_category_by_model(self, model: str) -> Optional[str]:
        """Encontra categoria baseada no modelo usando mapeamento"""
        if not model:
            return None
            
        # Normaliza o modelo para busca
        normalized_model = self.normalize_text(model)
        
        # Busca exata primeiro
        if normalized_model in MAPEAMENTO_CATEGORIAS:
            return MAPEAMENTO_CATEGORIAS[normalized_model]
        
        # Busca parcial - verifica se alguma palavra do modelo está no mapeamento
        model_words = normalized_model.split()
        for word in model_words:
            if len(word) >= 3 and word in MAPEAMENTO_CATEGORIAS:
                return MAPEAMENTO_CATEGORIAS[word]
        
        # Busca por substring - verifica se o modelo contém alguma chave do mapeamento
        for key, category in MAPEAMENTO_CATEGORIAS.items():
            if key in normalized_model or normalized_model in key:
                return category
        
        return None
    
    def model_exists_in_database(self, vehicles: List[Dict], model_query: str) -> bool:
        """Verifica se um modelo existe no banco de dados usando fuzzy matching"""
        if not model_query:
            return False
            
        query_words = model_query.split()
        
        for vehicle in vehicles:
            # Verifica em todos os campos fuzzy
            for field in self.fuzzy_fields:
                field_value = str(vehicle.get(field, ""))
                if field_value:
                    is_match, _ = self.fuzzy_match(query_words, field_value)
                    if is_match:
                        return True
        return False
    
    def split_multi_value(self, value: str) -> List[str]:
        """Divide valores múltiplos separados por vírgula"""
        if not value:
            return []
        return [v.strip() for v in str(value).split(',') if v.strip()]
    
    def fuzzy_match(self, query_words: List[str], field_content: str) -> Tuple[bool, str]:
        """Verifica se há match fuzzy entre as palavras da query e o conteúdo do campo"""
        if not query_words or not field_content:
            return False, "empty_input"
            
        normalized_content = self.normalize_text(field_content)
        
        for word in query_words:
            normalized_word = self.normalize_text(word)
            if len(normalized_word) < 2:
                continue
                
            # Match exato
            if normalized_word in normalized_content:
                return True, f"exact_match: {normalized_word}"
                
            # Match fuzzy para palavras com 3+ caracteres
            if len(normalized_word) >= 3:
                partial_score = fuzz.partial_ratio(normalized_content, normalized_word)
                ratio_score = fuzz.ratio(normalized_content, normalized_word)
                max_score = max(partial_score, ratio_score)
                
                if max_score >= 80:
                    return True, f"fuzzy_match: {max_score}"
        
        return False, "no_match"
    
    def apply_filters(self, vehicles: List[Dict], filters: Dict[str, str]) -> List[Dict]:
        """Aplica filtros aos veículos"""
        if not filters:
            return vehicles
            
        filtered_vehicles = list(vehicles)
        
        for filter_key, filter_value in filters.items():
            if not filter_value or not filtered_vehicles:
                continue
            
            if filter_key in self.fuzzy_fields:
                # Filtro fuzzy
                multi_values = self.split_multi_value(filter_value)
                all_words = []
                for val in multi_values:
                    all_words.extend(val.split())
                
                filtered_vehicles = [
                    v for v in filtered_vehicles
                    if any(
                        self.fuzzy_match(all_words, str(v.get(field, "")))[0]
                        for field in self.fuzzy_fields
                    )
                ]
                
            elif filter_key in self.exact_fields:
                # Filtro exato
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
        """Aplica filtros de faixa com expansão automática"""
        filtered_vehicles = list(vehicles)
        
        # Filtro de valor máximo - expande automaticamente até 25k acima
        if valormax:
            try:
                max_price = float(valormax) + 25000  # Adiciona 25k automaticamente
                filtered_vehicles = [
                    v for v in filtered_vehicles
                    if self.convert_price(v.get("preco")) is not None and
                    self.convert_price(v.get("preco")) <= max_price
                ]
            except ValueError:
                pass
        
        # Filtro de ano - interpreta como base e expande 3 anos para baixo, sem limite superior
        if anomax:
            try:
                target_year = int(anomax)
                min_year = target_year - 3  # Vai 3 anos para baixo
                
                filtered_vehicles = [
                    v for v in filtered_vehicles
                    if self.convert_year(v.get("ano")) is not None and
                    self.convert_year(v.get("ano")) >= min_year
                ]
                
            except ValueError:
                pass
        
        # Filtro de km máximo - busca do menor até o teto com margem
        if kmmax:
            try:
                target_km = int(kmmax)
                max_km_with_margin = target_km + 30000  # Adiciona 30k de margem
                
                # Filtra veículos que têm informação de KM
                vehicles_with_km = [
                    v for v in filtered_vehicles
                    if self.convert_km(v.get("km")) is not None
                ]
                
                if vehicles_with_km:
                    # Encontra o menor KM disponível
                    min_km_available = min(self.convert_km(v.get("km")) for v in vehicles_with_km)
                    
                    # Se o menor KM disponível é maior que o target, ancora no menor disponível
                    if min_km_available > target_km:
                        min_km_filter = min_km_available
                    else:
                        min_km_filter = 0  # Busca desde 0 se há KMs menores que o target
                    
                    # Aplica o filtro: do menor (ou âncora) até o máximo com margem
                    filtered_vehicles = [
                        v for v in filtered_vehicles
                        if self.convert_km(v.get("km")) is not None and
                        min_km_filter <= self.convert_km(v.get("km")) <= max_km_with_margin
                    ]
            except ValueError:
                pass
        
        # Filtro de cilindrada - não expande, busca próximos do valor
        if ccmax:
            try:
                target_cc = float(ccmax)
                # Converte para CC se necessário (valores < 10 são assumidos como litros)
                if target_cc < 10:
                    target_cc *= 1000
                
                filtered_vehicles = [
                    v for v in filtered_vehicles
                    if self.convert_cc(v.get("cilindrada")) is not None
                ]
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
        
        # Prioridade 2: Se tem KmMax, ordena por KM crescente
        if kmmax:
            return sorted(vehicles, key=lambda v: self.convert_km(v.get("km")) or float('inf'))
        
        # Prioridade 3: Se tem ValorMax, ordena por proximidade do valor
        if valormax:
            try:
                target_price = float(valormax)
                return sorted(vehicles, key=lambda v: 
                    abs((self.convert_price(v.get("preco")) or 0) - target_price))
            except ValueError:
                pass
        
        # Prioridade 4: Se tem AnoMax, ordena por proximidade do ano
        if anomax:
            try:
                target_year = int(anomax)
                return sorted(vehicles, key=lambda v: 
                    abs((self.convert_year(v.get("ano")) or 0) - target_year))
            except ValueError:
                pass
        
        # Ordenação padrão: por preço decrescente
        return sorted(vehicles, key=lambda v: self.convert_price(v.get("preco")) or 0, reverse=True)
    
    def search_with_fallback(self, vehicles: List[Dict], filters: Dict[str, str],
                            valormax: Optional[str], anomax: Optional[str], kmmax: Optional[str],
                            ccmax: Optional[str], excluded_ids: set) -> SearchResult:
        """Executa busca com fallback progressivo simplificado"""
        
        # Primeira tentativa: busca normal com expansão automática
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
                vehicles=sorted_vehicles[:6],  # Limita a 6 resultados
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
        
        # VERIFICAÇÃO PRÉVIA: Se tem 'modelo', verifica se ele existe no banco
        current_filters = dict(filters)
        removed_filters = []
        
        if "modelo" in current_filters:
            model_value = current_filters["modelo"]
            model_exists = self.model_exists_in_database(vehicles, model_value)
            
            if not model_exists:
                # Se não tem categoria, tenta mapear modelo→categoria
                if "categoria" not in current_filters:
                    mapped_category = self.find_category_by_model(model_value)
                    if mapped_category:
                        current_filters["categoria"] = mapped_category
                        removed_filters.append(f"modelo({model_value})->categoria({mapped_category})")
                    else:
                        removed_filters.append(f"modelo({model_value})")
                else:
                    # Se já tem categoria, só remove o modelo
                    removed_filters.append(f"modelo({model_value})")
                
                # Remove o modelo dos filtros
                current_filters = {k: v for k, v in current_filters.items() if k != "modelo"}
                
                # Tenta busca sem o modelo inexistente
                if current_filters:  # Se ainda sobrou algum filtro
                    filtered_vehicles = self.apply_filters(vehicles, current_filters)
                    filtered_vehicles = self.apply_range_filters(filtered_vehicles, valormax, anomax, kmmax, ccmax)
                    
                    if excluded_ids:
                        filtered_vehicles = [v for v in filtered_vehicles if str(v.get("id")) not in excluded_ids]
                    
                    if filtered_vehicles:
                        sorted_vehicles = self.sort_vehicles(filtered_vehicles, valormax, anomax, kmmax, ccmax)
                        fallback_info = {
                            "fallback": {
                                "removed_filters": removed_filters,
                                "reason": "model_not_found_in_database"
                            }
                        }
                        
                        return SearchResult(
                            vehicles=sorted_vehicles[:6],  # Limita a 6 resultados
                            total_found=len(sorted_vehicles),
                            fallback_info=fallback_info,
                            removed_filters=removed_filters
                        )
        
        # Fallback normal: tentar removendo parâmetros progressivamente
        current_valormax = valormax
        current_anomax = anomax
        current_kmmax = kmmax
        current_ccmax = ccmax
        
        # Primeiro remove parâmetros de range
        for range_param in RANGE_FALLBACK:
            if range_param == "CcMax" and current_ccmax:
                current_ccmax = None
                removed_filters.append(range_param)
            elif range_param == "ValorMax" and current_valormax:
                current_valormax = None
                removed_filters.append(range_param)
            elif range_param == "AnoMax" and current_anomax:
                current_anomax = None
                removed_filters.append(range_param)
            elif range_param == "KmMax" and current_kmmax:
                current_kmmax = None
                removed_filters.append(range_param)
            else:
                continue
            
            # Tenta busca sem este parâmetro de range
            filtered_vehicles = self.apply_filters(vehicles, current_filters)
            filtered_vehicles = self.apply_range_filters(filtered_vehicles, current_valormax, current_anomax, current_kmmax, current_ccmax)
            
            if excluded_ids:
                filtered_vehicles = [
                    v for v in filtered_vehicles
                    if str(v.get("id")) not in excluded_ids
                ]
            
            if filtered_vehicles:
                sorted_vehicles = self.sort_vehicles(filtered_vehicles, current_valormax, current_anomax, current_kmmax, current_ccmax)
                fallback_info = {"fallback": {"removed_filters": removed_filters}}
                
                return SearchResult(
                    vehicles=sorted_vehicles[:6],  # Limita a 6 resultados
                    total_found=len(sorted_vehicles),
                    fallback_info=fallback_info,
                    removed_filters=removed_filters
                )
        
        # Depois remove filtros normais (só se tiver 2+ filtros)
        for filter_to_remove in FALLBACK_PRIORITY:
            if filter_to_remove not in current_filters:
                continue
            
            # REGRA: Não faz fallback se sobrar apenas 1 filtro
            remaining_filters = [k for k, v in current_filters.items() if v]
            if len(remaining_filters) <= 1:
                break
            
            # Remove o filtro atual
            current_filters = {k: v for k, v in current_filters.items() if k != filter_to_remove}
            removed_filters.append(filter_to_remove)
            
            # Tenta busca sem o filtro removido
            filtered_vehicles = self.apply_filters(vehicles, current_filters)
            filtered_vehicles = self.apply_range_filters(filtered_vehicles, current_valormax, current_anomax, current_kmmax, current_ccmax)
            
            if excluded_ids:
                filtered_vehicles = [
                    v for v in filtered_vehicles
                    if str(v.get("id")) not in excluded_ids
                ]
            
            if filtered_vehicles:
                sorted_vehicles = self.sort_vehicles(filtered_vehicles, current_valormax, current_anomax, current_kmmax, current_ccmax)
                fallback_info = {"fallback": {"removed_filters": removed_filters}}
                
                return SearchResult(
                    vehicles=sorted_vehicles[:6],  # Limita a 6 resultados
                    total_found=len(sorted_vehicles),
                    fallback_info=fallback_info,
                    removed_filters=removed_filters
                )
        
        # Nenhum resultado encontrado
        return SearchResult(
            vehicles=[],
            total_found=0,
            fallback_info={},
            removed_filters=removed_filters
        )

# Instância global do motor de busca
search_engine = VehicleSearchEngine()

def save_update_status(success: bool, message: str = "", vehicle_count: int = 0):
    """Salva o status da última atualização"""
    status = {
        "timestamp": datetime.now().isoformat(),
        "success": success,
        "message": message,
        "vehicle_count": vehicle_count
    }
    
    try:
        with open(STATUS_FILE, "w", encoding="utf-8") as f:
            json.dump(status, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Erro ao salvar status: {e}")

def get_update_status() -> Dict:
    """Recupera o status da última atualização"""
    try:
        if os.path.exists(STATUS_FILE):
            with open(STATUS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        print(f"Erro ao ler status: {e}")
    
    return {
        "timestamp": None,
        "success": False,
        "message": "Nenhuma atualização registrada",
        "vehicle_count": 0
    }

def wrapped_fetch_and_convert_xml():
    """Wrapper para fetch_and_convert_xml com logging de status"""
    try:
        print("Iniciando atualização dos dados...")
        fetch_and_convert_xml()
        
        # Verifica quantos veículos foram carregados
        vehicle_count = 0
        if os.path.exists("data.json"):
            try:
                with open("data.json", "r", encoding="utf-8") as f:
                    data = json.load(f)
                    vehicle_count = len(data.get("veiculos", []))
            except:
                pass
        
        save_update_status(True, "Dados atualizados com sucesso", vehicle_count)
        print(f"Atualização concluída: {vehicle_count} veículos carregados")
        
    except Exception as e:
        error_message = f"Erro na atualização: {str(e)}"
        save_update_status(False, error_message)
        print(error_message)

@app.on_event("startup")
def schedule_tasks():
    """Agenda tarefas de atualização de dados"""
    scheduler = BackgroundScheduler(timezone="America/Sao_Paulo")
    scheduler.add_job(wrapped_fetch_and_convert_xml, "cron", hour="0,12")
    scheduler.start()
    wrapped_fetch_and_convert_xml()  # Executa uma vez na inicialização

@app.get("/api/data")
def get_data(request: Request):
    """Endpoint principal para busca de veículos"""
    
    # Verifica se o arquivo de dados existe
    if not os.path.exists("data.json"):
        return JSONResponse(
            content={
                "error": "Nenhum dado disponível",
                "resultados": [],
                "total_encontrado": 0
            },
            status_code=404
        )
    
    # Carrega os dados
    try:
        with open("data.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        
        vehicles = data.get("veiculos", [])
        if not isinstance(vehicles, list):
            raise ValueError("Formato inválido: 'veiculos' deve ser uma lista")
            
    except (json.JSONDecodeError, ValueError, KeyError) as e:
        return JSONResponse(
            content={
                "error": f"Erro ao carregar dados: {str(e)}",
                "resultados": [],
                "total_encontrado": 0
            },
            status_code=500
        )
    
    # Extrai parâmetros da query
    query_params = dict(request.query_params)
    
    # Parâmetros especiais
    valormax = query_params.pop("ValorMax", None)
    anomax = query_params.pop("AnoMax", None)
    kmmax = query_params.pop("KmMax", None)
    ccmax = query_params.pop("CcMax", None)
    simples = query_params.pop("simples", None)
    excluir = query_params.pop("excluir", None)
    
    # Filtros principais
    filters = {
        "tipo": query_params.get("tipo"),
        "modelo": query_params.get("modelo"),
        "categoria": query_params.get("categoria"),
        "cambio": query_params.get("cambio"),
        "opcionais": query_params.get("opcionais"),
        "marca": query_params.get("marca"),
        "cor": query_params.get("cor"),
        "combustivel": query_params.get("combustivel")
    }
    
    # Remove filtros vazios
    filters = {k: v for k, v in filters.items() if v}
    
    # Verifica se há filtros de busca reais (exclui parâmetros especiais)
    has_search_filters = bool(filters) or valormax or anomax or kmmax or ccmax
    
    # Processa IDs a excluir
    excluded_ids = set()
    if excluir:
        excluded_ids = set(e.strip() for e in excluir.split(",") if e.strip())
    
    # Se não há filtros de busca, retorna todo o estoque
    if not has_search_filters:
        all_vehicles = list(vehicles)
        
        # Remove IDs excluídos se especificado
        if excluded_ids:
            all_vehicles = [
                v for v in all_vehicles
                if str(v.get("id")) not in excluded_ids
            ]
        
        # Ordena por preço decrescente (padrão)
        sorted_vehicles = sorted(all_vehicles, key=lambda v: search_engine.convert_price(v.get("preco")) or 0, reverse=True)
        
        # Limita a 6 resultados
        limited_vehicles = sorted_vehicles[:6]
        
        # Aplica modo simples se solicitado
        if simples == "1":
            for vehicle in limited_vehicles:
                # Mantém apenas a primeira foto
                fotos = vehicle.get("fotos")
                if isinstance(fotos, list):
                    vehicle["fotos"] = fotos[:1] if fotos else []
                # Remove opcionais
        
        return JSONResponse(content={
            "resultados": limited_vehicles,
            "total_encontrado": len(sorted_vehicles),
            "info": "Exibindo todo o estoque disponível"
        })
    
    # Executa a busca com fallback
    result = search_engine.search_with_fallback(
        vehicles, filters, valormax, anomax, kmmax, ccmax, excluded_ids
    )
    
    # Aplica modo simples se solicitado
    if simples == "1" and result.vehicles:
        for vehicle in result.vehicles:
            # Mantém apenas a primeira foto
            fotos = vehicle.get("fotos")
            if isinstance(fotos, list):
                vehicle["fotos"] = fotos[:1] if fotos else []
            # Remove opcionais
            vehicle.pop("opcionais", None)
    
    # Monta resposta
    response_data = {
        "resultados": result.vehicles,
        "total_encontrado": result.total_found
    }
    
    # Adiciona informações de fallback apenas se houver filtros removidos
    if result.fallback_info:
        response_data.update(result.fallback_info)
    
    # Mensagem especial se não encontrou nada
    if result.total_found == 0:
        response_data["instrucao_ia"] = (
            "Não encontramos veículos com os parâmetros informados "
            "e também não encontramos opções próximas."
        )
    
    return JSONResponse(content=response_data)

@app.get("/api/health")
def health_check():
    """Endpoint de verificação de saúde"""
    return {"status": "healthy", "timestamp": "2025-07-13"}

@app.get("/api/status")
def get_status():
    """Endpoint para verificar status da última atualização dos dados"""
    status = get_update_status()
    
    # Informações adicionais sobre os arquivos
    data_file_exists = os.path.exists("data.json")
    data_file_size = 0
    data_file_modified = None
    
    if data_file_exists:
        try:
            stat = os.stat("data.json")
            data_file_size = stat.st_size
            data_file_modified = datetime.fromtimestamp(stat.st_mtime).isoformat()
        except:
            pass
    
    return {
        "last_update": status,
        "data_file": {
            "exists": data_file_exists,
            "size_bytes": data_file_size,
            "modified_at": data_file_modified
        },
        "current_time": datetime.now().isoformat()
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
