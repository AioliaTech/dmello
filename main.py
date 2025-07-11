from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI()

veiculos = [
    {
        "id": "6637025",
        "km": "77000",
        "titulo": "bmw m140i 2017"
    },
    {
        "id": "6414802",
        "km": "60000",
        "titulo": "troller t4 xlt 3.2 2017"
    },
    {
        "id": "6554965",
        "km": "156000",
        "titulo": "ford ranger ltdcd4a32c 2021"
    }
]

def converter_km(valor_str):
    try:
        return int(str(valor_str).replace(".", "").replace(",", "").strip())
    except (ValueError, TypeError):
        return None

def filtrar_por_kmmax(veiculos, kmmax):
    try:
        km_limite = int(kmmax) + 15000
        print(f"\nLimite de KM: {km_limite}")
        print("ANTES:", [(v.get("id"), v.get("km")) for v in veiculos])
        filtrados = [
            v for v in veiculos
            if converter_km(v.get("km")) is not None and converter_km(v.get("km")) <= km_limite
        ]
        print("DEPOIS:", [(v.get("id"), v.get("km")) for v in filtrados])
        return filtrados
    except Exception as e:
        print("Erro no filtro:", e)
        return []

@app.get("/api/data")
def get_data(request: Request):
    params = dict(request.query_params)
    kmmax = params.get("KmMax", None)

    if kmmax:
        resultado = filtrar_por_kmmax(veiculos, kmmax)
    else:
        resultado = veiculos

    return JSONResponse(content={
        "resultados": resultado,
        "total_encontrado": len(resultado)
    })
