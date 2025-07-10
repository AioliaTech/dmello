from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from unidecode import unidecode
from rapidfuzz import fuzz
from apscheduler.schedulers.background import BackgroundScheduler
from xml_fetcher import fetch_and_convert_xml
import json, os

app = FastAPI()

MAPEAMENTO_CATEGORIAS = {
    # ... seu dicionário ...
}

FALLBACK_PRIORIDADE = [
    "modelo",
    "categoria",
    "ValorMax",
    "cambio",
    "AnoMax",
    "opcionais",
    "KmMax",
    "marca",
    "cor",
    "combustivel"
]

def inferir_categoria_por_modelo(modelo_buscado):
    modelo_norm = normalizar(modelo_buscado)
    return MAPEAMENTO_CATEGORIAS.get(modelo_norm)

def normalizar(texto: str) -> str:
    return unidecode(texto).lower().replace("-", "").replace(" ", "").strip()

def converter_preco(valor_str):
    try:
        return float(str(valor_str).replace(",", "").replace("R$", "").strip())
    except (ValueError, TypeError):
        return None

def converter_ano(valor_str):
    try:
        return int(str(valor_str).strip())
    except (ValueError, TypeError):
        return None

def converter_km(valor_str):
    try:
        return int(str(valor_str).replace(".", "").replace(",", "").strip())
    except (ValueError, TypeError):
        return None

def get_price_for_sort(price_val):
    converted = converter_preco(price_val)
    return converted if converted is not None else float('-inf')

def split_multi(valor):
    return [v.strip() for v in str(valor).split(',') if v.strip()]

def filtrar_veiculos(vehicles, filtros, valormax=None, anomax=None, kmmax=None):
    campos_fuzzy = ["modelo", "titulo", "cor", "opcionais"]
    vehicles_processados = list(vehicles)

    for chave_filtro, valor_filtro in filtros.items():
        if not valor_filtro:
            continue
        valores = split_multi(valor_filtro)
        veiculos_que_passaram_nesta_chave = set()
        if chave_filtro in campos_fuzzy:
            palavras_query_normalizadas = []
            for val in valores:
                palavras_query_normalizadas += [normalizar(p) for p in val.split() if p.strip()]
            palavras_query_normalizadas = [p for p in palavras_query_normalizadas if p]
            if not palavras_query_normalizadas:
                continue
            for v in vehicles_processados:
                for palavra_q_norm in palavras_query_normalizadas:
                    if not palavra_q_norm:
                        continue
                    for nome_campo_fuzzy_veiculo in campos_fuzzy:
                        conteudo_original_campo_veiculo = v.get(nome_campo_fuzzy_veiculo, "")
                        if not conteudo_original_campo_veiculo:
                            continue
                        texto_normalizado_campo_veiculo = normalizar(str(conteudo_original_campo_veiculo))
                        if not texto_normalizado_campo_veiculo:
                            continue
                        if palavra_q_norm in texto_normalizado_campo_veiculo:
                            veiculos_que_passaram_nesta_chave.add(id(v))
                            break
                        elif len(palavra_q_norm) >= 4:
                            score_partial = fuzz.partial_ratio(texto_normalizado_campo_veiculo, palavra_q_norm)
                            score_ratio = fuzz.ratio(texto_normalizado_campo_veiculo, palavra_q_norm)
                            achieved_score = max(score_partial, score_ratio)
                            if achieved_score >= 85:
                                veiculos_que_passaram_nesta_chave.add(id(v))
                                break
                    else:
                        continue
                    break
            vehicles_processados = [v for v in vehicles_processados if id(v) in veiculos_que_passaram_nesta_chave]
        else:
            valores_normalizados = [normalizar(v) for v in valores]
            vehicles_processados = [
                v for v in vehicles_processados
                if normalizar(str(v.get(chave_filtro, ""))) in valores_normalizados
            ]
        if not vehicles_processados:
            return []

    # Filtro de AnoMax
    if anomax:
        try:
            anomax_int = int(anomax)
            ano_min = anomax_int - 2
            ano_max = anomax_int + 2
            vehicles_processados = [v for v in vehicles_processados if v.get("ano") and ano_min <= converter_ano(v.get("ano")) <= ano_max]
        except Exception:
            vehicles_processados = []
    # Filtro de KmMax com margem de 15.000
    if kmmax:
        try:
            kmmax_int = int(kmmax)
            km_limite = kmmax_int + 15000
            vehicles_processados = [v for v in vehicles_processados if v.get("km") and converter_km(v.get("km")) <= km_limite]
        except Exception:
            vehicles_processados = []
    # Filtro de ValorMax
    if valormax:
        try:
            teto = float(valormax)
            max_price_limit = teto * 1.2
            vehicles_processados = [v for v in vehicles_processados if converter_preco(v.get("preco")) is not None and converter_preco(v.get("preco")) <= max_price_limit]
        except ValueError:
            vehicles_processados = []
    # Ordenação pós-filtro
    if kmmax:
        vehicles_processados.sort(key=lambda v: converter_km(v.get("km")) if v.get("km") else float('inf'))
    elif valormax and anomax:
        vehicles_processados.sort(
            key=lambda v: (
                abs(converter_preco(v.get("preco")) - float(valormax)) +
                abs(converter_ano(v.get("ano")) - int(anomax)) if v.get("preco") and v.get("ano") else float('inf')
            )
        )
    elif valormax:
        vehicles_processados.sort(
            key=lambda v: abs(converter_preco(v.get("preco")) - float(valormax)) if v.get("preco") else float('inf')
        )
    elif anomax:
        vehicles_processados.sort(
            key=lambda v: abs(converter_ano(v.get("ano")) - int(anomax)) if v.get("ano") else float('inf')
        )
    else:
        vehicles_processados.sort(
            key=lambda v: get_price_for_sort(v.get("preco")),
            reverse=True
        )
    return vehicles_processados

def fallback_progressivo(vehicles, filtros, valormax, anomax, kmmax, prioridade):
    filtros_base = dict(filtros)
    removidos = []
    while len(filtros_base) > 1:
        # Encontra o filtro ativo menos importante
        filtro_a_remover = None
        for chave in reversed(prioridade):
            if chave in filtros_base and filtros_base[chave]:
                filtro_a_remover = chave
                break
        if not filtro_a_remover:
            break  # Nenhum filtro removível encontrado

        # Antes de remover ValorMax, tenta as expansões
        if filtro_a_remover == "ValorMax" and valormax:
            filtros_base_temp = {k: v for k, v in filtros_base.items()}
            filtros_base_temp.pop("ValorMax")
            for i in range(1, 4):
                novo_valormax = float(valormax) + (12000 * i)
                resultado = filtrar_veiculos(
                    vehicles,
                    filtros_base_temp,
                    valormax=novo_valormax,
                    anomax=anomax,
                    kmmax=kmmax
                )
                if resultado:
                    removidos.append(f"{filtro_a_remover}_expandido_{novo_valormax}")
                    return resultado, removidos
            # Se não encontrou, aí sim remove ValorMax normalmente

        filtros_base_temp = {k: v for k, v in filtros_base.items()}
        filtros_base_temp.pop(filtro_a_remover)
        valormax_temp = valormax if filtro_a_remover != "ValorMax" else None
        anomax_temp = anomax if filtro_a_remover != "AnoMax" else None
        kmmax_temp = kmmax if filtro_a_remover != "KmMax" else None
        resultado = filtrar_veiculos(vehicles, filtros_base_temp, valormax_temp, anomax_temp, kmmax_temp)
        removidos.append(filtro_a_remover)
        if resultado:
