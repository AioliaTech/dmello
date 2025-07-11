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

def tentativas_progressivas(vehicles, filtros, valormax, anomax, kmmax, ids_excluir):
    # Tenta ValorMax progressivo primeiro, SE estiver presente
    if valormax is not None:
        for i in range(4):
            novo_valor = float(valormax) + 5000 * i
            resultado = filtrar_veiculos(vehicles, filtros, valormax=novo_valor, anomax=anomax, kmmax=kmmax)
            if ids_excluir:
                resultado = [v for v in resultado if str(v.get("id")) not in ids_excluir]
            if resultado:
                return resultado, {
                    "tentativa_valormax": {
                        "valores_testados": [float(valormax) + 5000 * j for j in range(i+1)],
                        "valor_usado": novo_valor
                    }
                }
    # Tenta KmMax progressivo, SE estiver presente
    if kmmax is not None:
        for i in range(4):
            novo_km = int(kmmax) + 2000 * i
            resultado = filtrar_veiculos(vehicles, filtros, valormax=valormax, anomax=anomax, kmmax=novo_km)
            if ids_excluir:
                resultado = [v for v in resultado if str(v.get("id")) not in ids_excluir]
            if resultado:
                return resultado, {
                    "tentativa_kmmax": {
                        "valores_testados": [int(kmmax) + 2000 * j for j in range(i+1)],
                        "valor_usado": novo_km
                    }
                }
    # Tenta AnoMax progressivo, SE estiver presente
    if anomax is not None:
        anos = [int(anomax)]
        for i in range(1, 4):
            anos.append(int(anomax) + i)
            anos.append(int(anomax) - i)
        for ano in anos:
            resultado = filtrar_veiculos(vehicles, filtros, valormax=valormax, anomax=ano, kmmax=kmmax)
            if ids_excluir:
                resultado = [v for v in resultado if str(v.get("id")) not in ids_excluir]
            if resultado:
                return resultado, {
                    "tentativa_anomax": {
                        "valores_testados": anos[:anos.index(ano)+1],
                        "valor_usado": ano
                    }
                }
    return None, {}

def fallback_progressivo(vehicles, filtros, valormax, anomax, kmmax, prioridade, ids_excluir):
    filtros_base = dict(filtros)
    removidos = []
    valormax_fallback = valormax
    anomax_fallback = anomax
    kmmax_fallback = kmmax
    while len(filtros_base) > 1:
        filtro_a_remover = None
        for chave in reversed(prioridade):
            if chave in filtros_base and filtros_base[chave]:
                filtro_a_remover = chave
                break
        if not filtro_a_remover:
            break

        filtros_base_temp = {k: v for k, v in filtros_base.items()}
        filtros_base_temp.pop(filtro_a_remover)

        # Progressiva ANTES de remover filtro especial
        if filtro_a_remover in ["ValorMax", "KmMax", "AnoMax"]:
            valor_original = None
            if filtro_a_remover == "ValorMax":
                valor_original = valormax_fallback
            elif filtro_a_remover == "KmMax":
                valor_original = kmmax_fallback
            elif filtro_a_remover == "AnoMax":
                valor_original = anomax_fallback

            resultado_temp, info = tentativas_progressivas(
                vehicles, filtros_base_temp,
                valormax_fallback if filtro_a_remover != "ValorMax" else valor_original,
                anomax_fallback if filtro_a_remover != "AnoMax" else valor_original,
                kmmax_fallback if filtro_a_remover != "KmMax" else valor_original,
                ids_excluir
            )
            if resultado_temp:
                removidos.append(f"{filtro_a_remover}_expandido")
                return resultado_temp, removidos

        # Agora remove filtro definitivamente
        if filtro_a_remover == "ValorMax":
            valormax_fallback = None
        if filtro_a_remover == "KmMax":
            kmmax_fallback = None
        if filtro_a_remover == "AnoMax":
            anomax_fallback = None

        resultado = filtrar_veiculos(
            vehicles,
            filtros_base_temp,
            valormax=valormax_fallback,
            anomax=anomax_fallback,
            kmmax=kmmax_fallback
        )
        if ids_excluir:
            resultado = [v for v in resultado if str(v.get("id")) not in ids_excluir]
        removidos.append(filtro_a_remover)
        if resultado:
            return resultado, removidos
        filtros_base = filtros_base_temp
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
    ids_excluir = set()
    if excluir:
        ids_excluir = set(e.strip() for e in excluir.split(",") if e.strip())

    # Busca inicial normal
    resultado = filtrar_veiculos(vehicles, filtros_ativos, valormax, anomax, kmmax)
    if ids_excluir:
        resultado = [v for v in resultado if str(v.get("id")) not in ids_excluir]

    fallback_info = {}

    # Tentativas progressivas ANTES do fallback
    if not resultado:
        resultado, info = tentativas_progressivas(
            vehicles, filtros_ativos, valormax, anomax, kmmax, ids_excluir
        )
        if resultado:
            fallback_info = info

    # Só entra no fallback se ainda não encontrou nada
    if not resultado and len(filtros_ativos) > 1:
        resultado_fallback, filtros_removidos = fallback_progressivo(
            vehicles, filtros_ativos, valormax, anomax, kmmax, FALLBACK_PRIORIDADE, ids_excluir
        )
        if resultado_fallback:
            resultado = resultado_fallback
            fallback_info = {
                **fallback_info,
                "fallback": {"removidos": filtros_removidos}
            }

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
