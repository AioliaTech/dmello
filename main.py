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

# Mapeamento de categorias por modelo - Organizado por categoria
MAPEAMENTO_CATEGORIAS = {}

# Hatch apenas
hatch_models = ["gol", "uno", "palio", "celta", "march", "sandero", "i30", "golf", "fox", "up", "fit", "etios", "bravo", "punto", "208", "argo", "mobi", "c3", "picanto", "stilo", "c4 vtr", "kwid", "soul", "agile", "fusca"]
for model in hatch_models:
    MAPEAMENTO_CATEGORIAS[model] = "hatch"

# Sedan apenas
sedan_models = ["sentra", "jetta", "voyage", "siena", "grand siena", "cobalt", "logan", "fluence", "cerato", "elantra", "virtus", "accord", "altima", "fusion", "mazda6", "passat", "vectra sedan", "classic", "cronos", "linea", "408", "508", "c4 pallas", "bora", "hb20s", "lancer", "camry", "onix plus", "azera", "mondeo"]
for model in sedan_models:
    MAPEAMENTO_CATEGORIAS[model] = "sedan"

# Hatch e Sedan (modelos que existem nas duas versoes)
hatch_sedan_models = ["onix", "hb20", "yaris", "city", "audi_a3", "audi a3", "mercedes_a_class", "mercedes a class", "mazda3", "corolla", "civic", "impreza", "focus", "fiesta", "escort", "corsa", "astra", "vectra", "chevette", "monza", "sonic", "cruze", "clio", "megane", "206", "207", "307", "tiida", "accent", "rio", "swift", "baleno", "ka", "versa", "prisma", "polo", "c4"]
for model in hatch_sedan_models:
    MAPEAMENTO_CATEGORIAS[model] = "hatch,sedan"

# SUV
suv_models = ["duster", "ecosport", "hrv", "hr-v", "compass", "renegade", "tracker", "kicks", "captur", "creta", "tucson", "santa fe", "santa", "sorento", "sportage", "outlander", "asx", "pajero", "tr4", "aircross", "tiguan", "t-cross", "tcross", "touareg", "rav4", "cx5", "forester", "wrv", "land cruiser", "cherokee", "grand cherokee", "xtrail", "x-trail", "murano", "cx9", "edge", "trailblazer", "pulse", "fastback", "territory", "bronco sport", "2008", "3008", "5008", "c4 cactus", "taos", "crv", "cr-v", "corolla cross", "sw4", "pajero sport", "commander", "xv", "xc60", "tiggo 5x", "haval h6", "nivus", "pilot", "highlander", "equinox", "tahoe", "explorer", "pathfinder", "frontier suv"]
for model in suv_models:
    MAPEAMENTO_CATEGORIAS[model] = "suv"

# Caminhonete
caminhonete_models = ["hilux", "ranger", "s10", "l200", "triton", "toro", "frontier", "amarok", "gladiator", "maverick", "colorado", "dakota", "montana (nova)", "f-250", "f250", "courier (pickup)", "hoggar", "ram 1500", "rampage"]
for model in caminhonete_models:
    MAPEAMENTO_CATEGORIAS[model] = "caminhonete"

# Utilitario
utilitario_models = ["saveiro", "strada", "montana", "oroch", "kangoo", "partner", "doblo", "fiorino", "berlingo", "express", "combo", "kombi", "doblo cargo", "kangoo express"]
for model in utilitario_models:
    MAPEAMENTO_CATEGORIAS[model] = "utilitario"

# Furgao
furgao_models = ["master", "sprinter", "ducato", "daily", "jumper", "boxer", "trafic", "transit", "vito", "expert (furgao)", "jumpy (furgao)", "scudo (furgao)"]
for model in furgao_models:
    MAPEAMENTO_CATEGORIAS[model] = "furgao"

# Coupe
coupe_models = ["camaro", "mustang", "tt", "supra", "370z", "rx8", "challenger", "corvette", "veloster", "cerato koup", "clk coupe", "a5 coupe", "gt86", "rcz", "brz"]
for model in coupe_models:
    MAPEAMENTO_CATEGORIAS[model] = "coupe"

# Conversivel
conversivel_models = ["z4", "boxster", "miata", "beetle cabriolet", "slk", "911 cabrio", "tt roadster", "a5 cabrio", "mini cabrio", "206 cc", "eos"]
for model in conversivel_models:
    MAPEAMENTO_CATEGORIAS[model] = "conversivel"

# Minivan
minivan_models = ["spin", "livina", "caravan", "touran", "sharan", "zafira", "picasso", "grand c4", "meriva", "scenic", "xsara picasso", "carnival", "idea"]
for model in minivan_models:
    MAPEAMENTO_CATEGORIAS[model] = "minivan"

# Station Wagon
station_wagon_models = ["parati", "quantum", "spacefox", "golf variant", "palio weekend", "astra sw", "206 sw", "a4 avant", "fielder"]
for model in station_wagon_models:
    MAPEAMENTO_CATEGORIAS[model] = "station wagon"

# Off-road
offroad_models = ["wrangler", "troller", "defender", "bronco", "samurai", "jimny", "land cruiser", "grand vitara", "jimny sierra", "bandeirante (ate 2001)"]
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
            # Verifica nos campos de modelo e titulo (onde modelo é buscado)
            for field in ["modelo", "titulo"]:
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
                
            # Match exato (substring)
            if normalized_word in normalized_content:
                return True, f"exact_match: {normalized_word}"
            
            # Match no início da palavra (para casos como "ram" em "rampage")
            content_words = normalized_content.split()
            for content_word in content_words:
                if content_word.startswith(normalized_word):
                    return True, f"starts_with_match: {normalized_word}"
                    
            # Match fuzzy para palavras com 3+ caracteres
            if len(normalized_word) >= 3:
                # Verifica se a palavra da query está contida em alguma palavra do conteúdo
                for content_word in content_words:
                    if normalized_word in content_word:
                        return True, f"substring_match: {normalized_word} in {content_word}"
                
                # Fuzzy matching tradicional
                partial_score = fuzz.partial_ratio(normalized_content, normalized_word)
                ratio_score = fuzz.ratio(normalized_content, normalized_word)
                max_score = max(partial_score, ratio_score)
                
                if max_score >= 87:
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
            
            if filter_key == "modelo":
                # Filtro de modelo: busca em 'modelo' e 'titulo' com fuzzy
                multi_values = self.split_multi_value(filter_value)
                all_words = []
                for val in multi_values:
                    all_words.extend(val.split())
                
                filtered_vehicles = [
                    v for v in filtered_vehicles
                    if (self.fuzzy_match(all_words, str(v.get("modelo", "")))[0] or 
                        self.fuzzy_match(all_words, str(v.get("titulo", "")))[0])
                ]
                
            elif filter_key == "cor":
                # Filtro de cor: busca apenas no campo 'cor' com fuzzy
                multi_values = self.split_multi_value(filter_value)
                all_words = []
                for val in multi_values:
                    all_words.extend(val.split())
                
                filtered_vehicles = [
                    v for v in filtered_vehicles
                    if self.fuzzy_match(all_words, str(v.get("cor", "")))[0]
                ]
                
            elif filter_key == "opcionais":
                # Filtro de opcionais: busca apenas no campo 'opcionais' com fuzzy
                multi_values = self.split_multi_value(filter_value)
                all_words = []
                for val in multi_values:
                    all_words.extend(val.split())
                
                filtered_vehicles = [
                    v for v in filtered_vehicles
                    if self.fuzzy_match(all_words, str(v.get("opcionais", "")))[0]
                ]
                
            elif filter_key in self.exact_fields:
                # Filtros exatos (tipo, marca, categoria, cambio, combustivel)
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
    
    # Parâmetro especial para busca por ID
    id_param = query_params.pop("id", None)
    
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
    
    # BUSCA POR ID ESPECÍFICO - tem prioridade sobre tudo
    if id_param:
        vehicle_found = None
        for vehicle in vehicles:
            if str(vehicle.get("id")) == str(id_param):
                vehicle_found = vehicle
                break
        
        if vehicle_found:
            # Aplica modo simples se solicitado
            if simples == "1":
                fotos = vehicle_found.get("fotos")
                if isinstance(fotos, list):
                    vehicle_found["fotos"] = fotos[:1] if fotos else []
                vehicle_found.pop("opcionais", None)
            
            return JSONResponse(content={
                "resultados": [vehicle_found],
                "total_encontrado": 1,
                "info": f"Veículo encontrado por ID: {id_param}"
            })
        else:
            return JSONResponse(content={
                "resultados": [],
                "total_encontrado": 0,
                "error": f"Veículo com ID {id_param} não encontrado"
            })
    
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
        
        # Aplica modo simples se solicitado
        if simples == "1":
            for vehicle in sorted_vehicles:
                # Mantém apenas a primeira foto
                fotos = vehicle.get("fotos")
                if isinstance(fotos, list):
                    vehicle["fotos"] = fotos[:1] if fotos else []
                # Remove opcionais
                vehicle.pop("opcionais", None)
        
        return JSONResponse(content={
            "resultados": sorted_vehicles,  # AQUI ESTAVA O PROBLEMA - retorna todos, não limita a 6
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
