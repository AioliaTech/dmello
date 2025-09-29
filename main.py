from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from unidecode import unidecode
from rapidfuzz import fuzz
from apscheduler.schedulers.background import BackgroundScheduler
from json_fetcher import fetch_and_convert_json
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
    "observacao",     # Primeiro a ser removido
    "modelo",
    "categorias",
    "marca",          # Mais importante (nunca remove sozinho)
    "nome"            # Último recurso
]

@dataclass
class SearchResult:
    """Resultado de uma busca com informações de fallback"""
    products: List[Dict[str, Any]]
    total_found: int
    fallback_info: Dict[str, Any]
    removed_filters: List[str]

class ProductSearchEngine:
    """Engine de busca de produtos com sistema de fallback inteligente"""
    
    def __init__(self):
        self.exact_fields = ["marca", "modelo", "gtin", "codigo"]
        
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
            if isinstance(price_str, (int, float)):
                return float(price_str)
            
            cleaned = str(price_str).replace(",", ".").replace("R$", "").strip()
            return float(cleaned)
        except (ValueError, TypeError):
            return None
    
    def convert_stock(self, stock_str: Any) -> Optional[float]:
        """Converte string de estoque para float"""
        if not stock_str:
            return None
        try:
            if isinstance(stock_str, (int, float)):
                return float(stock_str)
            
            cleaned = str(stock_str).replace(",", ".").strip()
            return float(cleaned)
        except (ValueError, TypeError):
            return None
    
    def get_max_value_from_range_param(self, param_value: str) -> str:
        """Extrai o maior valor de parâmetros de range que podem ter múltiplos valores"""
        if not param_value:
            return param_value
        
        if ',' in param_value:
            try:
                values = [float(v.strip()) for v in param_value.split(',') if v.strip()]
                if values:
                    return str(max(values))
            except (ValueError, TypeError):
                pass
        
        return param_value
    
    def exact_match(self, query_words: List[str], field_content: str) -> Tuple[bool, str]:
        """Busca exata: todas as palavras devem estar presentes (substring)"""
        if not query_words or not field_content:
            return False, "empty_input"
            
        normalized_content = self.normalize_text(field_content)
        
        for word in query_words:
            normalized_word = self.normalize_text(word)
            if len(normalized_word) < 2:
                continue
                
            if normalized_word not in normalized_content:
                return False, f"exact_miss: '{normalized_word}' não encontrado"
        
        return True, f"exact_match: todas as palavras encontradas"
    
    def fuzzy_match(self, query_words: List[str], field_content: str) -> Tuple[bool, str]:
        """Verifica se há match fuzzy entre as palavras da query e o conteúdo do campo"""
        if not query_words or not field_content:
            return False, "empty_input"
        
        normalized_content = self.normalize_text(field_content)
        fuzzy_threshold = 85
        
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
    
    def field_match(self, query_words: List[str], field_content: str) -> Tuple[bool, str]:
        """Busca em três níveis: Exato → Fuzzy → Falha"""
        
        # NÍVEL 1: Busca exata
        exact_result, exact_reason = self.exact_match(query_words, field_content)
        if exact_result:
            return True, f"EXACT: {exact_reason}"
        
        # NÍVEL 2: Busca fuzzy
        fuzzy_result, fuzzy_reason = self.fuzzy_match(query_words, field_content)
        if fuzzy_result:
            return True, f"FUZZY: {fuzzy_reason}"
        
        # NÍVEL 3: Falha (vai para fallback)
        return False, f"NO_MATCH: exact({exact_reason}) + fuzzy({fuzzy_reason})"
    
    def split_multi_value(self, value: str) -> List[str]:
        """Divide valores múltiplos separados por vírgula"""
        if not value:
            return []
        return [v.strip() for v in str(value).split(',') if v.strip()]
    
    def apply_filters(self, products: List[Dict], filters: Dict[str, str]) -> List[Dict]:
        """Aplica filtros aos produtos"""
        if not filters:
            return products
            
        filtered_products = list(products)
        
        for filter_key, filter_value in filters.items():
            if not filter_value or not filtered_products:
                continue
            
            if filter_key == "nome":
                # Busca no nome do produto
                multi_values = self.split_multi_value(filter_value)
                all_words = []
                for val in multi_values:
                    all_words.extend(val.split())
                
                filtered_products = [
                    p for p in filtered_products
                    if self.field_match(all_words, str(p.get("nome", "")))[0]
                ]
                
            elif filter_key == "marca":
                multi_values = self.split_multi_value(filter_value)
                all_words = []
                for val in multi_values:
                    all_words.extend(val.split())
                
                filtered_products = [
                    p for p in filtered_products
                    if self.fuzzy_match(all_words, str(p.get("marca", "")))[0]
                ]
                
            elif filter_key == "modelo":
                multi_values = self.split_multi_value(filter_value)
                all_words = []
                for val in multi_values:
                    all_words.extend(val.split())
                
                filtered_products = [
                    p for p in filtered_products
                    if self.fuzzy_match(all_words, str(p.get("modelo", "")))[0]
                ]
                
            elif filter_key == "categorias":
                multi_values = self.split_multi_value(filter_value)
                all_words = []
                for val in multi_values:
                    all_words.extend(val.split())
                
                filtered_products = [
                    p for p in filtered_products
                    if self.fuzzy_match(all_words, str(p.get("categorias", "")))[0]
                ]
                
            elif filter_key == "observacao":
                multi_values = self.split_multi_value(filter_value)
                all_words = []
                for val in multi_values:
                    all_words.extend(val.split())
                
                filtered_products = [
                    p for p in filtered_products
                    if self.fuzzy_match(all_words, str(p.get("observacao", "")))[0]
                ]
                
            elif filter_key in self.exact_fields:
                normalized_values = [
                    self.normalize_text(v) for v in self.split_multi_value(filter_value)
                ]
                
                filtered_products = [
                    p for p in filtered_products
                    if self.normalize_text(str(p.get(filter_key, ""))) in normalized_values
                ]
        
        return filtered_products
    
    def apply_range_filters(self, products: List[Dict], precomax: Optional[str], 
                          estoquemin: Optional[str]) -> List[Dict]:
        """Aplica filtros de faixa"""
        filtered_products = list(products)
        
        # Filtro de preço máximo
        if precomax:
            try:
                max_price = float(precomax)
                filtered_products = [
                    p for p in filtered_products
                    if self.convert_price(p.get("preco")) is not None and
                    self.convert_price(p.get("preco")) <= max_price
                ]
            except ValueError:
                pass
        
        # Filtro de estoque mínimo
        if estoquemin:
            try:
                min_stock = float(estoquemin)
                filtered_products = [
                    p for p in filtered_products
                    if self.convert_stock(p.get("estoque")) is not None and
                    self.convert_stock(p.get("estoque")) >= min_stock
                ]
            except ValueError:
                pass
        
        return filtered_products
    
    def sort_products(self, products: List[Dict], precomax: Optional[str]) -> List[Dict]:
        """Ordena produtos baseado nos filtros aplicados"""
        if not products:
            return products
        
        # Se tem precomax, ordena por proximidade do valor
        if precomax:
            try:
                target_price = float(precomax)
                return sorted(products, key=lambda p: 
                    abs((self.convert_price(p.get("preco")) or 0) - target_price))
            except ValueError:
                pass
        
        # Ordenação padrão: por preço crescente
        return sorted(products, key=lambda p: self.convert_price(p.get("preco")) or 0)
    
    def search_with_fallback(self, products: List[Dict], filters: Dict[str, str],
                            precomax: Optional[str], estoquemin: Optional[str], 
                            excluded_ids: set) -> SearchResult:
        """Executa busca com fallback progressivo seguindo FALLBACK_PRIORITY"""
        
        # Primeira tentativa: busca normal
        filtered_products = self.apply_filters(products, filters)
        filtered_products = self.apply_range_filters(filtered_products, precomax, estoquemin)
        
        if excluded_ids:
            filtered_products = [
                p for p in filtered_products
                if str(p.get("id")) not in excluded_ids
            ]
        
        if filtered_products:
            sorted_products = self.sort_products(filtered_products, precomax)
            return SearchResult(
                products=sorted_products[:20],
                total_found=len(sorted_products),
                fallback_info={},
                removed_filters=[]
            )
        
        # Se não encontrou, inicia fallback
        current_filters = dict(filters)
        removed_filters = []
        current_precomax = precomax
        current_estoquemin = estoquemin
        
        for filter_to_remove in FALLBACK_PRIORITY:
            if filter_to_remove in current_filters:
                # Remove filtro
                current_filters = {k: v for k, v in current_filters.items() if k != filter_to_remove}
                removed_filters.append(filter_to_remove)
            else:
                continue
            
            # Testa busca após remoção
            filtered_products = self.apply_filters(products, current_filters)
            filtered_products = self.apply_range_filters(filtered_products, current_precomax, current_estoquemin)
            
            if excluded_ids:
                filtered_products = [
                    p for p in filtered_products
                    if str(p.get("id")) not in excluded_ids
                ]
            
            if filtered_products:
                sorted_products = self.sort_products(filtered_products, current_precomax)
                return SearchResult(
                    products=sorted_products[:20],
                    total_found=len(sorted_products),
                    fallback_info={"fallback": {"removed_filters": removed_filters}},
                    removed_filters=removed_filters
                )
        
        # Nenhum resultado
        return SearchResult(
            products=[],
            total_found=0,
            fallback_info={},
            removed_filters=removed_filters
        )

# Instância global do motor de busca
search_engine = ProductSearchEngine()

def save_update_status(success: bool, message: str = "", product_count: int = 0):
    """Salva o status da última atualização"""
    status = {
        "timestamp": datetime.now().isoformat(),
        "success": success,
        "message": message,
        "product_count": product_count
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
        "product_count": 0
    }

def wrapped_fetch_and_convert_json():
    """Wrapper para fetch_and_convert_json com logging de status"""
    try:
        print("Iniciando atualização dos dados...")
        fetch_and_convert_json()
        
        # Verifica quantos produtos foram carregados
        product_count = 0
        if os.path.exists("produtos.json"):
            try:
                with open("produtos.json", "r", encoding="utf-8") as f:
                    data = json.load(f)
                    product_count = len(data.get("produtos", []))
            except:
                pass
        
        save_update_status(True, "Dados atualizados com sucesso", product_count)
        print(f"Atualização concluída: {product_count} produtos carregados")
        
    except Exception as e:
        error_message = f"Erro na atualização: {str(e)}"
        save_update_status(False, error_message)
        print(error_message)

@app.on_event("startup")
def schedule_tasks():
    """Agenda tarefas de atualização de dados"""
    scheduler = BackgroundScheduler(timezone="America/Sao_Paulo")
    
    # Executa a cada 2 horas
    scheduler.add_job(wrapped_fetch_and_convert_json, "interval", hours=2)
    
    scheduler.start()
    wrapped_fetch_and_convert_json()  # Executa uma vez na inicialização

@app.get("/api/data")
def get_data(request: Request):
    """Endpoint principal para busca de produtos"""
    
    # Verifica se o arquivo de dados existe
    if not os.path.exists("produtos.json"):
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
        with open("produtos.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        
        products = data.get("produtos", [])
        if not isinstance(products, list):
            raise ValueError("Formato inválido: 'produtos' deve ser uma lista")
            
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
    precomax = search_engine.get_max_value_from_range_param(query_params.pop("PrecoMax", None))
    estoquemin = query_params.pop("EstoqueMin", None)
    simples = query_params.pop("simples", None)
    excluir = query_params.pop("excluir", None)
    
    # Parâmetro especial para busca por ID
    id_param = query_params.pop("id", None)
    
    # Filtros principais
    filters = {
        "nome": query_params.get("nome"),
        "marca": query_params.get("marca"),
        "modelo": query_params.get("modelo"),
        "categorias": query_params.get("categorias"),
        "observacao": query_params.get("observacao"),
        "gtin": query_params.get("gtin"),
        "codigo": query_params.get("codigo")
    }
    
    # Remove filtros vazios
    filters = {k: v for k, v in filters.items() if v}
    
    # BUSCA POR ID ESPECÍFICO
    if id_param:
        product_found = None
        for product in products:
            if str(product.get("id")) == str(id_param):
                product_found = product
                break
        
        if product_found:
            # Aplica modo simples se solicitado
            if simples == "1":
                imagens = product_found.get("imagens")
                if isinstance(imagens, list) and len(imagens) > 0:
                    product_found["imagens"] = [imagens[0]]
                else:
                    product_found["imagens"] = []
            
            return JSONResponse(content={
                "resultados": [product_found],
                "total_encontrado": 1,
                "info": f"Produto encontrado por ID: {id_param}"
            })
        else:
            return JSONResponse(content={
                "resultados": [],
                "total_encontrado": 0,
                "error": f"Produto com ID {id_param} não encontrado"
            })
    
    # Verifica se há filtros de busca reais
    has_search_filters = bool(filters) or precomax or estoquemin
    
    # Processa IDs a excluir
    excluded_ids = set()
    if excluir:
        excluded_ids = set(e.strip() for e in excluir.split(",") if e.strip())
    
    # Se não há filtros de busca, retorna todo o estoque
    if not has_search_filters:
        all_products = list(products)
        
        # Remove IDs excluídos se especificado
        if excluded_ids:
            all_products = [
                p for p in all_products
                if str(p.get("id")) not in excluded_ids
            ]
        
        # Ordena por preço crescente (padrão)
        sorted_products = sorted(all_products, key=lambda p: search_engine.convert_price(p.get("preco")) or 0)
        
        # Aplica modo simples se solicitado
        if simples == "1":
            for product in sorted_products:
                imagens = product.get("imagens")
                if isinstance(imagens, list) and len(imagens) > 0:
                    product["imagens"] = [imagens[0]]
                else:
                    product["imagens"] = []
        
        return JSONResponse(content={
            "resultados": sorted_products,
            "total_encontrado": len(sorted_products),
            "info": "Exibindo todo o estoque disponível"
        })
    
    # Executa a busca com fallback
    result = search_engine.search_with_fallback(
        products, filters, precomax, estoquemin, excluded_ids
    )
    
    # Aplica modo simples se solicitado
    if simples == "1" and result.products:
        for product in result.products:
            imagens = product.get("imagens")
            if isinstance(imagens, list) and len(imagens) > 0:
                product["imagens"] = [imagens[0]]
            else:
                product["imagens"] = []
    
    # Monta resposta
    response_data = {
        "resultados": result.products,
        "total_encontrado": result.total_found
    }
    
    # Adiciona informações de fallback apenas se houver filtros removidos
    if result.fallback_info:
        response_data.update(result.fallback_info)
    
    # Mensagem especial se não encontrou nada
    if result.total_found == 0:
        response_data["instrucao_ia"] = (
            "Não encontramos produtos com os parâmetros informados "
            "e também não encontramos opções próximas."
        )
    
    return JSONResponse(content=response_data)

@app.get("/api/health")
def health_check():
    """Endpoint de verificação de saúde"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/api/status")
def get_status():
    """Endpoint para verificar status da última atualização dos dados"""
    status = get_update_status()
    
    # Informações adicionais sobre os arquivos
    data_file_exists = os.path.exists("produtos.json")
    data_file_size = 0
    data_file_modified = None
    
    if data_file_exists:
        try:
            stat = os.stat("produtos.json")
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
