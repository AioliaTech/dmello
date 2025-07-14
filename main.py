from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from unidecode import unidecode
from rapidfuzz import fuzz
from apscheduler.schedulers.background import BackgroundScheduler
from xml_fetcher import fetch_and_convert_xml
import json
import os
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass

app = FastAPI()

# Configuração de prioridades para fallback (do menos importante para o mais importante)
FALLBACK_PRIORITY = [
    "cor",           # Menos importante
    "combustivel",
    "opcionais",
    "cambio",
    "categoria",
    "marca",
    "modelo"         # Mais importante (nunca remove sozinho)
]

# Prioridade para parâmetros de range
RANGE_FALLBACK = ["CcMax", "KmMax", "AnoMax", "ValorMax"]

# Mapeamento de categorias por modelo
MAPEAMENTO_CATEGORIAS = {
    # Hatchbacks
    "gol": "hatch",
    "polo": "hatch", 
    "up": "hatch",
    "fox": "hatch",
    "fit": "hatch",
    "city": "hatch",
    "hb20": "hatch",
    "i30": "hatch",
    "march": "hatch",
    "versa": "hatch",
    "onix": "hatch",
    "prisma": "hatch",
    "ka": "hatch",
    "fiesta": "hatch",
    "focus": "hatch",
    "208": "hatch",
    "207": "hatch",
    "206": "hatch",
    "clio": "hatch",
    "sandero": "hatch",
    "logan": "hatch",
    
    # Sedans
    "jetta": "sedan",
    "passat": "sedan",
    "civic": "sedan",
    "accord": "sedan",
    "corolla": "sedan",
    "camry": "sedan",
    "elantra": "sedan",
    "azera": "sedan",
    "sentra": "sedan",
    "altima": "sedan",
    "cruze": "sedan",
    "cobalt": "sedan",
    "fusion": "sedan",
    "mondeo": "sedan",
    "408": "sedan",
    "508": "sedan",
    "fluence": "sedan",
    "megane": "sedan",
    
    # SUVs
    "tiguan": "suv",
    "amarok": "suv",
    "touareg": "suv",
    "crv": "suv",
    "hrv": "suv",
    "pilot": "suv",
    "rav4": "suv",
    "highlander": "suv",
    "tucson": "suv",
    "santa": "suv",
    "creta": "suv",
    "kicks": "suv",
    "frontier": "suv",
    "pathfinder": "suv",
    "equinox": "suv",
    "tahoe": "suv",
    "ecosport": "suv",
    "edge": "suv",
    "explorer": "suv",
    "2008": "suv",
    "3008": "suv",
    "5008": "suv",
    "captur": "suv",
    "duster": "suv",
    "oroch": "suv",
    
    # Pickups
    "saveiro": "pickup",
    "strada": "pickup",
    "toro": "pickup",
    "hilux": "pickup",
    "ranger": "pickup",
    "s10": "pickup",
    "montana": "pickup",
    
    # Motos
    "biz": "moto",
    "pop": "moto",
    "fan": "moto",
    "titan": "moto",
    "cb": "moto",
    "cbr": "moto",
    "hornet": "moto",
    "fazer": "moto",
    "factor": "moto",
    "crosser": "moto",
    "lander": "moto",
    "xt": "moto",
    "gs": "moto",
    "f": "moto",
    "ninja": "moto",
    "z": "moto",
    "er": "moto",
    "versys": "moto",
    "duke": "moto",
    "rc": "moto",
}

@dataclass
class SearchResult:
    """Resultado de uma busca com informações de fallback"""
    vehicles: List[Dict[str, Any]]
    total_found: int
    fallback_info: Dict[str, Any]
    removed_filters: List[str]
    debug_info: Dict[str, Any] = None

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
        debug_info = f"Comparando '{query_words}' com '{normalized_content}'"
        
        for word in query_words:
            normalized_word = self.normalize_text(word)
            if len(normalized_word) < 2:
                continue
                
            # Match exato
            if normalized_word in normalized_content:
                return True, f"exact_match: {normalized_word} in {normalized_content}"
                
            # Match fuzzy para palavras com 3+ caracteres
            if len(normalized_word) >= 3:
                partial_score = fuzz.partial_ratio(normalized_content, normalized_word)
                ratio_score = fuzz.ratio(normalized_content, normalized_word)
                max_score = max(partial_score, ratio_score)
                
                if max_score >= 80:  # Baixei de 85 para 80 para ser mais permissivo
                    return True, f"fuzzy_match: {normalized_word} vs {normalized_content} = {max_score}"
        
        return False, debug_info
    
    def apply_filters(self, vehicles: List[Dict], filters: Dict[str, str]) -> Tuple[List[Dict], Dict]:
        """Aplica filtros aos veículos com debug detalhado"""
        if not filters:
            return vehicles, {}
            
        debug_info = {
            "total_initial": len(vehicles),
            "filters_applied": {},
            "sample_vehicles": []
        }
        
        # Amostra dos primeiros 3 veículos para debug
        debug_info["sample_vehicles"] = [
            {
                "id": v.get("id"),
                "modelo": v.get("modelo"),
                "titulo": v.get("titulo"),
                "marca": v.get("marca"),
                "categoria": v.get("categoria")
            }
            for v in vehicles[:3]
        ]
        
        filtered_vehicles = list(vehicles)
        
        for filter_key, filter_value in filters.items():
            if not filter_value or not filtered_vehicles:
                continue
            
            initial_count = len(filtered_vehicles)
            
            if filter_key in self.fuzzy_fields:
                # Filtro fuzzy com debug
                multi_values = self.split_multi_value(filter_value)
                all_words = []
                for val in multi_values:
                    all_words.extend(val.split())
                
                matches = []
                no_matches = []
                
                for v in filtered_vehicles:
                    matched = False
                    match_info = []
                    
                    for field in self.fuzzy_fields:
                        field_value = str(v.get(field, ""))
                        if field_value:
                            is_match, debug_msg = self.fuzzy_match(all_words, field_value)
                            if is_match:
                                matched = True
                                match_info.append(f"{field}: {debug_msg}")
                    
                    if matched:
                        matches.append({
                            "id": v.get("id"),
                            "match_info": match_info
                        })
                    else:
                        no_matches.append({
                            "id": v.get("id"),
                            "modelo": v.get("modelo"),
                            "titulo": v.get("titulo")
                        })
                
                filtered_vehicles = [
                    v for v in filtered_vehicles
                    if any(
                        self.fuzzy_match(all_words, str(v.get(field, "")))[0]
                        for field in self.fuzzy_fields
                    )
                ]
                
                debug_info["filters_applied"][filter_key] = {
                    "type": "fuzzy",
                    "query_words": all_words,
                    "initial_count": initial_count,
                    "final_count": len(filtered_vehicles),
                    "matches": matches[:5],  # Primeiros 5 matches
                    "no_matches": no_matches[:5]  # Primeiros 5 sem match
                }
                
            elif filter_key in self.exact_fields:
                # Filtro exato com debug
                normalized_values = [
                    self.normalize_text(v) for v in self.split_multi_value(filter_value)
                ]
                
                matches = []
                no_matches = []
                
                for v in filtered_vehicles:
                    field_value = self.normalize_text(str(v.get(filter_key, "")))
                    if field_value in normalized_values:
                        matches.append({
                            "id": v.get("id"),
                            "field_value": field_value
                        })
                    else:
                        no_matches.append({
                            "id": v.get("id"),
                            "field_value": field_value
                        })
                
                filtered_vehicles = [
                    v for v in filtered_vehicles
                    if self.normalize_text(str(v.get(filter_key, ""))) in normalized_values
                ]
                
                debug_info["filters_applied"][filter_key] = {
                    "type": "exact",
                    "normalized_values": normalized_values,
                    "initial_count": initial_count,
                    "final_count": len(filtered_vehicles),
                    "matches": matches[:5],
                    "no_matches": no_matches[:5]
                }
        
        return filtered_vehicles, debug_info
    
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
                print(f"DEBUG: Filtro de ano - target: {target_year}, min_year: {min_year}")
                
                vehicles_before_filter = len(filtered_vehicles)
                filtered_vehicles = [
                    v for v in filtered_vehicles
                    if self.convert_year(v.get("ano")) is not None and
                    self.convert_year(v.get("ano")) >= min_year
                ]
                print(f"DEBUG: Filtro ano - antes: {vehicles_before_filter}, depois: {len(filtered_vehicles)}")
                
            except ValueError:
                print(f"DEBUG: Erro ao converter ano: {anomax}")
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
        
        print(f"DEBUG: Iniciando busca com filtros: {filters}")
        print(f"DEBUG: Range filters - ValorMax: {valormax}, AnoMax: {anomax}, KmMax: {kmmax}, CcMax: {ccmax}")
        print(f"DEBUG: Total de veículos disponíveis: {len(vehicles)}")
        
        # Primeira tentativa: busca normal com expansão automática
        filtered_vehicles, filter_debug = self.apply_filters(vehicles, filters)
        print(f"DEBUG: Após apply_filters: {len(filtered_vehicles)} veículos")
        
        # Print debug detalhado dos filtros
        for filter_name, filter_info in filter_debug.get("filters_applied", {}).items():
            print(f"DEBUG: Filtro '{filter_name}' ({filter_info['type']}): {filter_info['initial_count']} -> {filter_info['final_count']}")
            if filter_info.get("matches"):
                print(f"DEBUG: Matches: {filter_info['matches']}")
            if filter_info.get("no_matches"):
                print(f"DEBUG: No matches: {filter_info['no_matches']}")
        
        filtered_vehicles = self.apply_range_filters(filtered_vehicles, valormax, anomax, kmmax, ccmax)
        print(f"DEBUG: Após apply_range_filters: {len(filtered_vehicles)} veículos")
        
        if excluded_ids:
            filtered_vehicles = [
                v for v in filtered_vehicles
                if str(v.get("id")) not in excluded_ids
            ]
            print(f"DEBUG: Após exclusões: {len(filtered_vehicles)} veículos")
        
        if filtered_vehicles:
            sorted_vehicles = self.sort_vehicles(filtered_vehicles, valormax, anomax, kmmax, ccmax)
            print(f"DEBUG: Sucesso na primeira tentativa: {len(sorted_vehicles)} veículos")
            
            return SearchResult(
                vehicles=sorted_vehicles,
                total_found=len(sorted_vehicles),
                fallback_info={},
                removed_filters=[],
                debug_info=filter_debug
            )
        
        print("DEBUG: Iniciando fallback...")
        
        # Fallback: tentar removendo parâmetros progressivamente
        current_filters = dict(filters)
        current_valormax = valormax
        current_anomax = anomax
        current_kmmax = kmmax
        current_ccmax = ccmax
        removed_filters = []
        
        # Primeiro remove parâmetros de range
        for range_param in RANGE_FALLBACK:
            print(f"DEBUG: Tentando remover range param: {range_param}")
            
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
            
            print(f"DEBUG: Removido {range_param}, testando busca...")
            
            # Tenta busca sem este parâmetro de range
            filtered_vehicles, _ = self.apply_filters(vehicles, current_filters)
            filtered_vehicles = self.apply_range_filters(filtered_vehicles, current_valormax, current_anomax, current_kmmax, current_ccmax)
            
            if excluded_ids:
                filtered_vehicles = [
                    v for v in filtered_vehicles
                    if str(v.get("id")) not in excluded_ids
                ]
            
            print(f"DEBUG: Resultado após remover {range_param}: {len(filtered_vehicles)} veículos")
            
            if filtered_vehicles:
                sorted_vehicles = self.sort_vehicles(filtered_vehicles, current_valormax, current_anomax, current_kmmax, current_ccmax)
                fallback_info = {"fallback": {"removed_filters": removed_filters}}
                
                return SearchResult(
                    vehicles=sorted_vehicles,
                    total_found=len(sorted_vehicles),
                    fallback_info=fallback_info,
                    removed_filters=removed_filters,
                    debug_info=filter_debug
                )
        
        # Depois remove filtros normais
        for filter_to_remove in FALLBACK_PRIORITY:
            if filter_to_remove not in current_filters:
                continue
                
            print(f"DEBUG: Tentando remover filtro: {filter_to_remove}")
            
            # Nunca remove 'modelo' se for o único filtro restante
            remaining_filters = [k for k, v in current_filters.items() if v]
            if filter_to_remove == "modelo" and len(remaining_filters) == 1:
                print(f"DEBUG: Pulando remoção de 'modelo' pois é o único filtro restante")
                continue
            
            # SISTEMA ESPECIAL: Se está removendo 'modelo' e não há 'categoria', tenta mapear
            if filter_to_remove == "modelo" and "categoria" not in current_filters:
                model_value = current_filters.get("modelo")
                if model_value:
                    print(f"DEBUG: Tentando mapear modelo '{model_value}' para categoria")
                    # Busca categoria baseada no modelo
                    mapped_category = self.find_category_by_model(model_value)
                    if mapped_category:
                        print(f"DEBUG: Modelo mapeado para categoria: {mapped_category}")
                        # Remove modelo e adiciona categoria mapeada
                        new_filters = {k: v for k, v in current_filters.items() if k != "modelo"}
                        new_filters["categoria"] = mapped_category
                        removed_filters.append(f"modelo->categoria({mapped_category})")
                        
                        # Tenta busca com categoria mapeada
                        filtered_vehicles, _ = self.apply_filters(vehicles, new_filters)
                        filtered_vehicles = self.apply_range_filters(filtered_vehicles, current_valormax, current_anomax, current_kmmax, current_ccmax)
                        
                        if excluded_ids:
                            filtered_vehicles = [
                                v for v in filtered_vehicles
                                if str(v.get("id")) not in excluded_ids
                            ]
                        
                        print(f"DEBUG: Resultado com categoria mapeada: {len(filtered_vehicles)} veículos")
                        
                        if filtered_vehicles:
                            sorted_vehicles = self.sort_vehicles(filtered_vehicles, current_valormax, current_anomax, current_kmmax, current_ccmax)
                            fallback_info = {
                                "fallback": {
                                    "removed_filters": removed_filters,
                                    "model_to_category_mapping": {
                                        "original_model": model_value,
                                        "mapped_category": mapped_category
                                    }
                                }
                            }
                            
                            return SearchResult(
                                vehicles=sorted_vehicles,
                                total_found=len(sorted_vehicles),
                                fallback_info=fallback_info,
                                removed_filters=removed_filters,
                                debug_info=filter_debug
                            )
                        
                        # Se não encontrou com a categoria mapeada, continua o fallback normal
                        current_filters = new_filters
                        continue  # Pula para próximo filtro sem adicionar 'modelo' nos removed_filters
                    else:
                        print(f"DEBUG: Nenhuma categoria encontrada para o modelo '{model_value}'")
            
            # Remove o filtro atual (fallback normal)
            current_filters = {k: v for k, v in current_filters.items() if k != filter_to_remove}
            removed_filters.append(filter_to_remove)
            
            print(f"DEBUG: Removido filtro '{filter_to_remove}', filtros restantes: {current_filters}")
            
            # Tenta busca sem o filtro removido
            filtered_vehicles, _ = self.apply_filters(vehicles, current_filters)
            filtered_vehicles = self.apply_range_filters(filtered_vehicles, current_valormax, current_anomax, current_kmmax, current_ccmax)
            
            if excluded_ids:
                filtered_vehicles = [
                    v for v in filtered_vehicles
                    if str(v.get("id")) not in excluded_ids
                ]
            
            print(f"DEBUG: Resultado após remover '{filter_to_remove}': {len(filtered_vehicles)} veículos")
            
            if filtered_vehicles:
                sorted_vehicles = self.sort_vehicles(filtered_vehicles, current_valormax, current_anomax, current_kmmax, current_ccmax)
                fallback_info = {"fallback": {"removed_filters": removed_filters}}
                
                return SearchResult(
                    vehicles=sorted_vehicles,
                    total_found=len(sorted_vehicles),
                    fallback_info=fallback_info,
                    removed_filters=removed_filters,
                    debug_info=filter_debug
                )
        
        print("DEBUG: Nenhum resultado encontrado mesmo com fallback")
        
        # Nenhum resultado encontrado
        return SearchResult(
            vehicles=[],
            total_found=0,
            fallback_info={},
            removed_filters=removed_filters,
            debug_info=filter_debug
        )

# Instância global do motor de busca
search_engine = VehicleSearchEngine()

@app.on_event("startup")
def schedule_tasks():
    """Agenda tarefas de atualização de dados"""
    scheduler = BackgroundScheduler(timezone="America/Sao_Paulo")
    scheduler.add_job(fetch_and_convert_xml, "cron", hour="0,12")
    scheduler.start()
    fetch_and_convert_xml()

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
    debug_mode = query_params.pop("debug", None)  # Novo parâmetro para debug
    
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
    
    # Processa IDs a excluir
    excluded_ids = set()
    if excluir:
        excluded_ids = set(e.strip() for e in excluir.split(",") if e.strip())
    
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
    
    # Adiciona debug info se solicitado
    if debug_mode == "1" and result.debug_info:
        response_data["debug_info"] = result.debug_info
    
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

@app.get("/api/debug/models")
def debug_models():
    """Endpoint para debug - lista todos os modelos únicos no banco"""
    if not os.path.exists("data.json"):
        return JSONResponse(content={"error": "Nenhum dado disponível"}, status_code=404)
    
    try:
        with open("data.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        
        vehicles = data.get("veiculos", [])
        models = set()
        model_samples = {}
        
        for v in vehicles:
            modelo = v.get("modelo", "")
            titulo = v.get("titulo", "")
            if modelo:
                models.add(modelo)
                if modelo not in model_samples:
                    model_samples[modelo] = {
                        "titulo": titulo,
                        "marca": v.get("marca", ""),
                        "categoria": v.get("categoria", "")
                    }
        
        return JSONResponse(content={
            "total_models": len(models),
            "models": sorted(list(models)),
            "model_samples": model_samples
        })
        
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
