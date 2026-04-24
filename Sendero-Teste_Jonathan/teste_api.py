import requests

processos = [
    "0016150-46.2000.8.26.0361",
    "0003641-16.1998.8.26.0309",
    "0000028-94.1994.8.26.0125",
    "0945469-75.1999.8.26.0100",
    "0834911-75.1995.8.26.0100",
]

BASE = "https://comunicaapi.pje.jus.br/api/v1/comunicacao"

for proc in processos:
    resp = requests.get(BASE, params={"numeroProcesso": proc}, timeout=15)
    print(f"{proc} → HTTP {resp.status_code} | Resposta: {resp.text[:200]}")