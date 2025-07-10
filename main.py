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
    for v in vehicles_processados:
        v['_relevance_score'] = 0.0
        v['_matched_word_count'] = 0
    active_fuzzy_filter_applied = False
    for chave_filtro, valor_filtro in filtros.items():
        if not valor_filtro:
            continue
        valores = split_multi(valor_filtro)
        veiculos_que_passaram_nesta_chave = []
        if chave_filtro in campos_fuzzy:
            active_fuzzy_filter_applied = True
            palavras_query_normalizadas = []
            for val in valores:
                palavras_query_normalizadas += [normalizar(p) for p in val.split() if p.strip()]
            palavras_query_normalizadas = [p for p in palavras_query_normalizadas if p]
            if not palavras_query_normalizadas:
                vehicles_processados = []
                break
            for v in vehicles_processados:
                vehicle_score_for_this_filter = 0.0
                vehicle_matched_words_for_this_filter = 0
                for palavra_q_norm in palavras_query_normalizadas:
                    if not palavra_q_norm:
                        continue
                    best_score_for_this_q_word_in_vehicle = 0.0
                    for nome_campo_fuzzy_veiculo in campos_fuzzy:
                        conteudo_original_campo_veiculo = v.get(nome_campo_fuzzy_veiculo, "")
                        if not conteudo_original_campo_veiculo:
                            continue
                        texto_normalizado_campo_veiculo = normalizar(str(conteudo_original_campo_veiculo))
                        if not texto_normalizado_campo_veiculo:
                            continue
                        current_field_match_score = 0.0
                        if palavra_q_norm in texto_normalizado_campo_veiculo:
                            current_field_match_score = 100.0
                        elif len(palavra_q_norm) >= 4:
                            score_partial = fuzz.partial_ratio(texto_normalizado_campo_veiculo, palavra_q_norm)
                            score_ratio = fuzz.ratio(texto_normalizado_campo_veiculo, palavra_q_norm)
                            achieved_score = max(score_partial, score_ratio)
                            if achieved_score >= 85:
                                current_field_match_score = achieved_score
                        if current_field_match_score > best_score_for_this_q_word_in_vehicle:
                            best_score_for_this_q_word_in_vehicle = current_field_match_score
                    if best_score_for_this_q_word_in_vehicle > 0:
                        vehicle_score_for_this_filter += best_score_for_this_q_word_in_vehicle
                        vehicle_matched_words_for_this_filter += 1
                if vehicle_matched_words_for_this_filter > 0:
                    v['_relevance_score'] += vehicle_score_for_this_filter
                    v['_matched_word_count'] += vehicle_matched_words_for_this_filter
                    veiculos_que_passaram_nesta_chave.append(v)
        else:
            valores_normalizados = [normalizar(v) for v in valores]
            for v in vehicles_processados:
                valor_campo_veiculo = normalizar(str(v.get(chave_filtro, "")))
                if valor_campo_veiculo in valores_normalizados:
                    veiculos_que_passaram_nesta_chave.append(v)
        vehicles_processados = veiculos_que_passaram_nesta_chave
        if not vehicles_processados:
            break
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
    if active_fuzzy_filter_applied:
        vehicles_processados = [v for v in vehicles_processados if v['_matched_word_count'] > 0]
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
    elif active_fuzzy_filter_applied:
        vehicles_processados.sort(
            key=lambda v: (
                v['_matched_word_count'],
                v['_relevance_score'],
                get_price_for_sort(v.get("preco"))
            ),
            reverse=True
        )
    else:
        vehicles_processados.sort(
            key=lambda v: get_price_for_sort(v.get("preco")),
            reverse=True
        )
    for v in vehicles_processados:
        v.pop('_relevance_score', None)
        v.pop('_matched_word_count', None)
    return vehicles_processados

def tentativas_valormax_incremental(vehicles, filtros, valormax, anomax, kmmax, tentativas=3, incremento=12000):
    """Tenta buscar aumentando o ValorMax até tentativas vezes."""
    resultados = []
    valormax_atual = float(valormax)
    for i in range(1, tentativas+1):
        novo_valormax = valormax_atual + (incremento * i)
        resultados = filtrar_veiculos(
            vehicles,
            filtros,
            valormax=novo_valormax,
            anomax=anomax,
            kmmax=kmmax
        )
        if resultados:
            return resultados, novo_valormax
    return [], None

def fallback_removendo_campos(vehicles, filtros, valormax, anomax, kmmax, prioridade):
    """Tenta buscar removendo um filtro por vez (do menos para o mais importante)."""
    filtros_base = dict(filtros)
    removidos = []
    chaves = [k for k in prioridade if k in filtros_base and filtros_base[k]]
    if len(chaves) < 2:
        return [], []
    for i in range(len(chaves)-1, -1, -1):
        filtro_a_remover = chaves[i]
        filtros_base_temp = {k: v for k, v in filtros_base.items()}
        filtros_base_temp.pop(filtro_a_remover)
        valormax_temp = valormax if filtro_a_remover != "ValorMax" else None
        anomax_temp = anomax if filtro_a_remover != "AnoMax" else None
        kmmax_temp = kmmax if filtro_a_remover != "KmMax" else None
        resultado = filtrar_veiculos(vehicles, filtros_base_temp, valormax_temp, anomax_temp, kmmax_temp)
        if resultado:
            removidos.append(filtro_a_remover)
            return resultado, removidos
        removidos.append(filtro_a_remover)
    return [], removidos

@app.on_event("startup")
def agendar_tarefas():
    scheduler = BackgroundScheduler(timezone="America/Sao_Paulo")
    scheduler.add_job(fetch_and_convert_xml, "cron", hour="0,12")
    scheduler.start()
    fetch_and_convert_xml()

@app.get("/api/data")
def get_data(request: Request):
    if not os.path.exists("data.json"):
        return JSONResponse(content={"error": "Nenhum dado disponível", "resultados": [], "total_encontrado": 0}, status_code=404)
    try:
        with open("data.json", "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError:
        return JSONResponse(content={"error": "Erro ao ler os dados (JSON inválido)", "resultados": [], "total_encontrado": 0}, status_code=500)
    try:
        vehicles = data["veiculos"]
        if not isinstance(vehicles, list):
            return JSONResponse(content={"error": "Formato de dados inválido (veiculos não é uma lista)", "resultados": [], "total_encontrado": 0}, status_code=500)
    except KeyError:
        return JSONResponse(content={"error": "Formato de dados inválido (chave 'veiculos' não encontrada)", "resultados": [], "total_encontrado": 0}, status_code=500)
    query_params = dict(request.query_params)
    valormax = query_params.pop("ValorMax", None)
    anomax = query_params.pop("AnoMax", None)
    kmmax = query_params.pop("KmMax", None)
    simples = query_params.pop("simples", None)
    excluir = query_params.pop("excluir", None)
    filtros_originais = {
        "id": query_params.get("id"),
        "tipo": query_params.get("tipo"),
        "modelo": query_params.get("modelo"),
        "categoria": query_params.get("categoria"),
        "ValorMax": valormax,
        "cambio": query_params.get("cambio"),
        "AnoMax": anomax,
        "opcionais": query_params.get("opcionais"),
        "KmMax": kmmax,
        "marca": query_params.get("marca"),
        "cor": query_params.get("cor"),
        "combustivel": query_params.get("combustivel")
    }
    filtros_ativos = {k: v for k, v in filtros_originais.items() if v}
    resultado = filtrar_veiculos(vehicles, filtros_ativos, valormax, anomax, kmmax)

    # EXCLUI IDs se solicitado
    ids_excluir = set()
    if excluir:
        ids_excluir = set(e.strip() for e in excluir.split(",") if e.strip())
    if ids_excluir:
        resultado = [v for v in resultado if str(v.get("id")) not in ids_excluir]

    tentativas_valormax_usadas = []
    fallback_info = {}

    if not resultado and valormax and len(filtros_ativos) > 1:
        # Tentativas de expandir o ValorMax em +12k até 3x
        for i in range(1, 4):
            novo_valormax = float(valormax) + (12000 * i)
            resultado_temp = filtrar_veiculos(vehicles, filtros_ativos, valormax=novo_valormax, anomax=anomax, kmmax=kmmax)
            if ids_excluir:
                resultado_temp = [v for v in resultado_temp if str(v.get("id")) not in ids_excluir]
            if resultado_temp:
                resultado = resultado_temp
                tentativas_valormax_usadas.append(novo_valormax)
                fallback_info = {
                    "tentativa_valormax": {
                        "valores_testados": [float(valormax) + 12000 * j for j in range(1, i+1)],
                        "valor_usado": novo_valormax
                    }
                }
                break

    # Fallback progressivo removendo filtros (caso ainda não tenha encontrado nada)
    if not resultado and len(filtros_ativos) > 1:
        resultado_fallback, filtros_removidos = fallback_removendo_campos(
            vehicles, filtros_ativos, valormax, anomax, kmmax, FALLBACK_PRIORIDADE
        )
        if ids_excluir:
            resultado_fallback = [v for v in resultado_fallback if str(v.get("id")) not in ids_excluir]
        if resultado_fallback:
            resultado = resultado_fallback
            fallback_info = {
                **fallback_info,
                "fallback": {"removidos": filtros_removidos}
            }

    # PROCESSA FOTOS SE SIMPLES=1
    if simples == "1":
        for v in resultado:
            fotos = v.get("fotos")
            if isinstance(fotos, list):
                v["fotos"] = fotos[:1] if fotos else []
            v.pop("opcionais", None)

    if resultado:
        resposta = {
            "resultados": resultado,
            "total_encontrado": len(resultado)
        }
        if fallback_info:
            resposta.update(fallback_info)
        return JSONResponse(content=resposta)

    return JSONResponse(content={
        "resultados": [],
        "total_encontrado": 0,
        "instrucao_ia": "Não encontramos veículos com os parâmetros informados e também não encontramos opções próximas."
    })
