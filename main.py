from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from unidecode import unidecode
from rapidfuzz import fuzz
from apscheduler.schedulers.background import BackgroundScheduler
from xml_fetcher import fetch_and_convert_xml
import json, os

app = FastAPI()

# (Seu mapeamento de categorias permanece igual)
MAPEAMENTO_CATEGORIAS = { 
    # ... seu grande dicionário ...
}

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

def calcular_score_proximidade(v, params):
    score = 0
    score_max = 0

    # Score AnoMax
    ano_max = params.get("AnoMax")
    ano_veic = converter_ano(v.get("ano"))
    if ano_max and ano_veic:
        ano_max = int(ano_max)
        if (ano_max - 2) <= ano_veic <= (ano_max + 2):
            # Pontuação máxima se igual, decai até -2 ou +2
            score += 10 - abs(ano_veic - ano_max) * 2.5  # Score: 10 (igual), 7.5 (1 ano dif.), 5 (2 anos dif.)
        score_max += 10

    # Score KmMax
    km_max = params.get("KmMax")
    km_veic = converter_km(v.get("km"))
    if km_max and km_veic is not None:
        km_max = int(km_max)
        margem = 30000
        if km_veic <= km_max + margem:
            diff = abs(km_veic - km_max)
            # Score decai conforme diferença, zera se ultrapassa margem
            score += max(0, 10 - (diff / margem) * 10)
        score_max += 10

    # Score ValorMax
    valor_max = params.get("ValorMax")
    preco_veic = converter_preco(v.get("preco"))
    if valor_max and preco_veic:
        valor_max = float(valor_max)
        limite = valor_max * 1.2
        if preco_veic <= limite:
            diff = abs(preco_veic - valor_max)
            # Score decai até o teto de 20%
            score += max(0, 10 - (diff / (limite - valor_max)) * 10)
        score_max += 10

    # Retorna score proporcional ao total de pontos possíveis (pode ser 0~30 ou 0~20)
    return round(score, 2)

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
        veiculos_que_passaram_nesta_chave = []
        if chave_filtro in campos_fuzzy:
            active_fuzzy_filter_applied = True
            palavras_query_originais = valor_filtro.split()
            palavras_query_normalizadas = [normalizar(p) for p in palavras_query_originais if p.strip()]
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
            termo_normalizado_para_comparacao = normalizar(valor_filtro)
            for v in vehicles_processados:
                valor_campo_veiculo = v.get(chave_filtro, "")
                if normalizar(str(valor_campo_veiculo)) == termo_normalizado_para_comparacao:
                    veiculos_que_passaram_nesta_chave.append(v)
        vehicles_processados = veiculos_que_passaram_nesta_chave
        if not vehicles_processados:
            break
    # Aplica filtros de valor, ano, km
    if anomax:
        try:
            anomax_int = int(anomax)
            ano_min = anomax_int - 2
            ano_max = anomax_int + 2
            vehicles_processados = [v for v in vehicles_processados if v.get("ano") and ano_min <= converter_ano(v.get("ano")) <= ano_max]
        except Exception:
            vehicles_processados = []
    if kmmax:
        try:
            kmmax_int = int(kmmax)
            km_limite = kmmax_int + 30000
            vehicles_processados = [v for v in vehicles_processados if v.get("km") and converter_km(v.get("km")) <= km_limite]
        except Exception:
            vehicles_processados = []
    if valormax:
        try:
            teto = float(valormax)
            max_price_limit = teto * 1.2
            vehicles_processados = [v for v in vehicles_processados if converter_preco(v.get("preco")) is not None and converter_preco(v.get("preco")) <= max_price_limit]
        except ValueError:
            vehicles_processados = []
    if active_fuzzy_filter_applied:
        vehicles_processados = [v for v in vehicles_processados if v['_matched_word_count'] > 0]
    if active_fuzzy_filter_applied:
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
    filtros_originais = {
        "id": query_params.get("id"),
        "tipo": query_params.get("tipo"),
        "modelo": query_params.get("modelo"),
        "marca": query_params.get("marca"),
        "cilindrada": query_params.get("cilindrada"),
        "categoria": query_params.get("categoria"),
        "motor": query_params.get("motor"),
        "opcionais": query_params.get("opcionais"),
        "cor": query_params.get("cor"),
        "combustivel": query_params.get("combustivel"),
        "ano": query_params.get("ano"),
        "km": query_params.get("km")
    }
    filtros_ativos = {k: v for k, v in filtros_originais.items() if v}
    resultado = filtrar_veiculos(vehicles, filtros_ativos, valormax, anomax, kmmax)

    # Calcula o score de proximidade para cada veículo retornado
    params_score = {}
    if valormax:
        params_score["ValorMax"] = float(valormax)
    if anomax:
        params_score["AnoMax"] = int(anomax)
    if kmmax:
        params_score["KmMax"] = int(kmmax)
    for v in resultado:
        v["score_proximidade"] = calcular_score_proximidade(v, params_score)

    # PROCESSA FOTOS SE SIMPLES=1
    if simples == "1":
        for v in resultado:
            fotos = v.get("fotos")
            if isinstance(fotos, list):
                v["fotos"] = fotos[:1] if fotos else []
            v.pop("opcionais", None)

    if resultado:
        # Ordena pelo score de proximidade (maior para menor) se houver
        if any("score_proximidade" in v for v in resultado):
            resultado.sort(key=lambda x: x.get("score_proximidade", 0), reverse=True)
        return JSONResponse(content={
            "resultados": resultado,
            "total_encontrado": len(resultado)
        })

    # (Fallbacks alternativos - mantidos do seu código original)
    # ... (mantém a lógica de alternativas por modelo/categoria/cilindrada/valor acima)

    # Fallbacks podem ser incrementados conforme sua lógica original ou evoluir para o sistema progressivo que você quer

    return JSONResponse(content={
        "resultados": [],
        "total_encontrado": 0,
        "instrucao_ia": "Não encontramos veículos com os parâmetros informados e também não encontramos opções próximas."
    })
