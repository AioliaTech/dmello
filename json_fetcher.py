import requests
import json
import os
import re
from datetime import datetime
from typing import Dict, List, Any, Optional
from abc import ABC, abstractmethod

# =================== CONFIGURAÇÕES GLOBAIS =======================

JSON_FILE = "produtos.json"

# =================== UTILS =======================

def converter_preco(valor: Any) -> float:
    if not valor: return 0.0
    try:
        if isinstance(valor, (int, float)): return float(valor)
        valor_str = str(valor)
        valor_str = valor_str.replace(',', '.')
        return float(valor_str) if valor_str else 0.0
    except (ValueError, TypeError): return 0.0

def safe_get(data: Dict, keys: List[str], default: Any = None) -> Any:
    for key in keys:
        if isinstance(data, dict) and key in data and data[key] is not None:
            return data[key]
    return default

def normalize_images(imagens_data: Any) -> List[str]:
    """
    Normaliza diferentes estruturas de imagens para uma lista simples de URLs.
    Remove parâmetros de query (?v...) das URLs.
    """
    if not imagens_data:
        return []
    
    result = []
    
    if isinstance(imagens_data, str):
        result.append(imagens_data.strip())
    elif isinstance(imagens_data, list):
        for item in imagens_data:
            if isinstance(item, str):
                result.append(item.strip())
            elif isinstance(item, dict):
                for key in ["url", "URL", "src", "path", "link", "href"]:
                    if key in item and item[key]:
                        result.append(str(item[key]).strip())
                        break
    
    # Remove duplicatas mantendo a ordem e limpa URLs
    seen = set()
    normalized = []
    for url in result:
        if url and url not in seen:
            # Limpa a URL: remove tudo depois da extensão do arquivo
            # Suporta: .png, .jpg, .jpeg, .gif, .webp, etc
            clean_url = re.sub(r'(\.(png|jpg|jpeg|gif|webp|bmp|svg))(\?.*)?$', r'\1', url, flags=re.IGNORECASE)
            
            seen.add(clean_url)
            normalized.append(clean_url)
    
    return normalized

# =================== PARSERS =======================

class BaseParser(ABC):
    @abstractmethod
    def can_parse(self, data: Any, url: str) -> bool: pass
    
    @abstractmethod
    def parse(self, data: Any, url: str) -> List[Dict]: pass
    
    def normalize_product(self, produto: Dict) -> Dict:
        # Aplica normalização nas imagens
        imagens = produto.get("imagens", [])
        produto["imagens"] = normalize_images(imagens)
        
        return {
            "codigo": produto.get("codigo"),
            "nome": produto.get("nome"),
            "complemento": produto.get("complemento"),
            "marca": produto.get("marca"),
            "modelo": produto.get("modelo"),
            "preco": produto.get("preco", 0.0),
            "peso": produto.get("peso", 0.0),
            "altura": produto.get("altura", 0.0),
            "largura": produto.get("largura", 0.0),
            "comprimento": produto.get("comprimento", 0.0),
            "categorias": produto.get("categorias"),
            "observacao": produto.get("observacao", ""),
            "imagens": produto.get("imagens", [])
        }

class ZettaBrasilParser(BaseParser):
    """
    Parser para o sistema Zetta Brasil - formato de embalagens e alimentos
    """
    def can_parse(self, data: Any, url: str) -> bool:
        # Verifica se é da Zetta Brasil pela URL ou estrutura dos dados
        if "zettabrasil.com.br" in url.lower():
            return True
        
        # Verifica estrutura típica do Zetta
        if isinstance(data, list) and len(data) > 0:
            primeiro_item = data[0]
            if isinstance(primeiro_item, dict):
                # Campos característicos do Zetta Brasil
                campos_zetta = ["pro_cod", "codigo_integracao", "gtin", "inativar_itens"]
                return any(campo in primeiro_item for campo in campos_zetta)
        
        return False
    
    def parse(self, data: Any, url: str) -> List[Dict]:
        # O JSON já vem como lista de produtos
        if not isinstance(data, list):
            print(f"[AVISO] Dados não estão em formato de lista")
            return []
        
        parsed_products = []
        
        for item in data:
            if not isinstance(item, dict):
                continue
            
            # Filtra produtos excluídos
            if item.get("excluido", False):
                continue
            
            parsed = self.normalize_product({
                "codigo": item.get("codigo"),
                "nome": item.get("nome"),
                "complemento": item.get("complemento"),
                "marca": item.get("marca"),
                "modelo": item.get("modelo"),
                "preco": converter_preco(item.get("preco")),
                "peso": float(item.get("peso", 0.0)),
                "altura": float(item.get("altura", 0.0)),
                "largura": float(item.get("largura", 0.0)),
                "comprimento": float(item.get("comprimento", 0.0)),
                "categorias": item.get("categorias"),
                "observacao": item.get("observacao", ""),
                "imagens": item.get("imagens", [])
            })
            
            parsed_products.append(parsed)
        
        return parsed_products

# =================== SISTEMA PRINCIPAL =======================

class UnifiedProductFetcher:
    def __init__(self):
        self.parsers = [
            ZettaBrasilParser()
        ]
        print("[INFO] Sistema de produtos iniciado")
    
    def get_urls(self) -> List[str]:
        """Busca URLs das variáveis de ambiente JSON_URL*"""
        return list({val for var, val in os.environ.items() if var.startswith("JSON_URL") and val})
    
    def select_parser(self, data: Any, url: str) -> Optional['BaseParser']:
        """Seleciona o parser apropriado baseado na URL e estrutura dos dados"""
        for parser in self.parsers:
            if parser.can_parse(data, url):
                print(f"[INFO] Parser selecionado: {parser.__class__.__name__}")
                return parser
        
        print(f"[ERRO] Nenhum parser encontrado para URL: {url}")
        return None
    
    def process_url(self, url: str) -> List[Dict]:
        print(f"[INFO] Processando URL: {url}")
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            print(f"[INFO] JSON carregado com sucesso")
            
            parser = self.select_parser(data, url)
            if parser:
                return parser.parse(data, url)
            else:
                print(f"[ERRO] Nenhum parser adequado encontrado para URL: {url}")
                return []
                
        except requests.RequestException as e:
            print(f"[ERRO] Erro de requisição para URL {url}: {e}")
            return []
        except json.JSONDecodeError as e:
            print(f"[ERRO] Erro ao decodificar JSON da URL {url}: {e}")
            return []
        except Exception as e:
            print(f"[ERRO] Erro crítico ao processar URL {url}: {e}")
            return []
    
    def fetch_all(self) -> Dict:
        urls = self.get_urls()
        if not urls:
            print("[AVISO] Nenhuma variável de ambiente 'JSON_URL' foi encontrada.")
            return {}
        
        print(f"[INFO] {len(urls)} URL(s) encontrada(s) para processar")
        all_products = [product for url in urls for product in self.process_url(url)]
        
        # Estatísticas
        stats = self._generate_stats(all_products)
        
        result = {
            "produtos": all_products,
            "_updated_at": datetime.now().isoformat(),
            "_total_count": len(all_products),
            "_sources_processed": len(urls),
            "_statistics": stats
        }
        
        try:
            with open(JSON_FILE, "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            print(f"\n[OK] Arquivo {JSON_FILE} salvo com sucesso!")
        except Exception as e:
            print(f"[ERRO] Erro ao salvar arquivo JSON: {e}")
        
        print(f"[OK] Total de produtos processados: {len(all_products)}")
        self._print_stats(stats)
        return result
    
    def _generate_stats(self, products: List[Dict]) -> Dict:
        """Gera estatísticas dos produtos processados"""
        stats = {
            "total_produtos": len(products),
            "com_imagem": sum(1 for p in products if p.get("imagens")),
            "sem_preco": sum(1 for p in products if p.get("preco", 0) <= 0),
            "top_marcas": {},
            "faixa_preco": {
                "ate_10": 0,
                "10_50": 0,
                "50_100": 0,
                "acima_100": 0
            }
        }
        
        for product in products:
            # Top marcas
            marca = product.get("marca", "Sem marca")
            if marca:  # Só conta se tiver marca
                stats["top_marcas"][marca] = stats["top_marcas"].get(marca, 0) + 1
            
            # Faixa de preço
            preco = product.get("preco", 0)
            if preco <= 10:
                stats["faixa_preco"]["ate_10"] += 1
            elif preco <= 50:
                stats["faixa_preco"]["10_50"] += 1
            elif preco <= 100:
                stats["faixa_preco"]["50_100"] += 1
            else:
                stats["faixa_preco"]["acima_100"] += 1
        
        return stats
    
    def _print_stats(self, stats: Dict):
        """Imprime estatísticas formatadas"""
        print(f"\n{'='*60}\nESTATÍSTICAS DO PROCESSAMENTO\n{'='*60}")
        
        print(f"\nResumo Geral:")
        print(f"  Total de produtos: {stats['total_produtos']}")
        print(f"  Produtos com imagem: {stats['com_imagem']}")
        print(f"  Produtos sem preço: {stats['sem_preco']}")
        
        if stats["top_marcas"]:
            print(f"\nTop 5 Marcas:")
            for marca, count in sorted(stats["top_marcas"].items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"  • {marca}: {count}")
        
        print(f"\nDistribuição por Faixa de Preço:")
        print(f"  • Até R$ 10: {stats['faixa_preco']['ate_10']}")
        print(f"  • R$ 10 - R$ 50: {stats['faixa_preco']['10_50']}")
        print(f"  • R$ 50 - R$ 100: {stats['faixa_preco']['50_100']}")
        print(f"  • Acima de R$ 100: {stats['faixa_preco']['acima_100']}")

# =================== FUNÇÃO PARA IMPORTAÇÃO =======================

def fetch_and_convert_json():
    """Função de alto nível para ser importada por outros módulos."""
    fetcher = UnifiedProductFetcher()
    return fetcher.fetch_all()

# =================== EXECUÇÃO PRINCIPAL =======================

if __name__ == "__main__":
    result = fetch_and_convert_json()
    
    if result and 'produtos' in result:
        total = result.get('_total_count', 0)
        print(f"\n{'='*50}\nRESUMO DO PROCESSAMENTO\n{'='*50}")
        print(f"Total de produtos: {total}")
        print(f"Atualizado em: {result.get('_updated_at', 'N/A')}")
        print(f"Fontes processadas: {result.get('_sources_processed', 0)}")
        
        if total > 0:
            print(f"\nExemplo dos primeiros 3 produtos:")
            for i, p in enumerate(result['produtos'][:3], 1):
                preco = p.get('preco', 0.0)
                print(f"{i}. {p.get('nome', 'N/A')} - {p.get('marca', 'N/A')} - R$ {preco:.2f}")
                if p.get('imagens'):
                    print(f"   Imagem: {p['imagens'][0]}")
