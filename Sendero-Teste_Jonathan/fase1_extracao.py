"""
FASE 1 — EXTRAÇÃO
Consulta a API pública do Comunica PJe para cada número de processo
e salva os dados brutos em JSON para uso nas fases seguintes.

Endpoint base: https://comunicaapi.pje.jus.br/api/v1
Documentação:  https://comunicaapi.pje.jus.br/swagger/index.html
"""

import requests
import json
import os
import time
import logging
import pathlib
from datetime import datetime, date

# ── Diretório base (pasta onde está este .py) ─────────────────────────────────
BASE_DIR = pathlib.Path(__file__).resolve().parent

# ── Constantes ───────────────────────────────────────────────────────────────
BASE_URL = "https://comunicaapi.pje.jus.br/api/v1"
ENDPOINT_COMUNICACOES = f"{BASE_URL}/comunicacao"

DATA_INICIO = "2000-01-01"
DATA_FIM    = "2026-04-24"

TAMANHO_PAGINA = 20
PAUSA_ENTRE_REQUESTS = 0.5   # segundos — evita sobrecarga na API
MAX_RETRIES = 3
TIMEOUT = 30

PASTA_SAIDA     = BASE_DIR / "dados_brutos"
ARQUIVO_RAW     = PASTA_SAIDA / "comunicacoes_raw.json"
ARQUIVO_ALERTAS = PASTA_SAIDA / "alertas_extracao.json"

# ── Configuração de log ──────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(BASE_DIR / "pipeline.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


# ── Funções auxiliares ────────────────────────────────────────────────────────

def ler_processos(caminho) -> list[str]:
    """Lê o arquivo de processos e retorna lista de CNJs."""
    with open(caminho, encoding="utf-8") as f:
        processos = [linha.strip() for linha in f if linha.strip()]
    logger.info(f"{len(processos)} processo(s) lido(s) de '{caminho}'.")
    return processos


def consultar_comunicacoes_processo(numero_processo: str) -> tuple[list[dict], list[dict]]:
    """
    Consulta todas as páginas de comunicações para um único processo.
    Retorna (lista_comunicacoes, lista_alertas).
    """
    comunicacoes = []
    alertas = []
    pagina = 1

    while True:
        params = {
            "numeroProcesso":             numero_processo,
            "dataDisponibilizacaoInicio": DATA_INICIO,
            "dataDisponibilizacaoFim":    DATA_FIM,
            "pagina":                     pagina,
            "tamanhoPagina":              TAMANHO_PAGINA,
        }

        for tentativa in range(1, MAX_RETRIES + 1):
            try:
                resp = requests.get(
                    ENDPOINT_COMUNICACOES,
                    params=params,
                    timeout=TIMEOUT,
                    headers={"Accept": "application/json"},
                )

                if resp.status_code == 200:
                    dados = resp.json()
                    break

                elif resp.status_code == 404:
                    logger.warning(f"[{numero_processo}] Processo não encontrado (404).")
                    alertas.append({
                        "processo":  numero_processo,
                        "tipo":      "processo_nao_encontrado",
                        "descricao": f"HTTP 404 na página {pagina}",
                    })
                    return comunicacoes, alertas

                elif resp.status_code == 429:
                    espera = 5 * tentativa
                    logger.warning(f"[{numero_processo}] Rate-limit (429). Aguardando {espera}s…")
                    time.sleep(espera)

                else:
                    logger.warning(
                        f"[{numero_processo}] HTTP {resp.status_code} na tentativa {tentativa}."
                    )
                    if tentativa == MAX_RETRIES:
                        alertas.append({
                            "processo":  numero_processo,
                            "tipo":      "erro_http",
                            "descricao": f"HTTP {resp.status_code} após {MAX_RETRIES} tentativas",
                        })
                        return comunicacoes, alertas
                    time.sleep(2 * tentativa)

            except requests.exceptions.Timeout:
                logger.warning(f"[{numero_processo}] Timeout na tentativa {tentativa}.")
                if tentativa == MAX_RETRIES:
                    alertas.append({
                        "processo":  numero_processo,
                        "tipo":      "timeout",
                        "descricao": f"Timeout após {MAX_RETRIES} tentativas",
                    })
                    return comunicacoes, alertas
                time.sleep(2 * tentativa)

            except requests.exceptions.RequestException as exc:
                logger.error(f"[{numero_processo}] Erro de rede: {exc}")
                alertas.append({
                    "processo":  numero_processo,
                    "tipo":      "erro_rede",
                    "descricao": str(exc),
                })
                return comunicacoes, alertas

        # ── Processa a resposta ──────────────────────────────────────────────
        # A API pode retornar a lista diretamente ou dentro de uma chave como
        # "items", "content", "comunicacoes" etc. Tentamos as mais comuns.
        if isinstance(dados, list):
            itens = dados
        elif isinstance(dados, dict):
            itens = (
                dados.get("items")
                or dados.get("content")
                or dados.get("comunicacoes")
                or dados.get("data")
                or dados.get("result")
                or []
            )
        else:
            itens = []

        if not itens:
            logger.info(f"[{numero_processo}] Página {pagina}: sem mais itens.")
            break

        # Marca o processo de origem em cada registro
        for item in itens:
            item["_numeroProcessoConsultado"] = numero_processo

        comunicacoes.extend(itens)
        logger.info(
            f"[{numero_processo}] Página {pagina}: {len(itens)} comunicação(ões) coletada(s)."
        )

        # Verifica se existe próxima página
        tem_proxima = False
        if isinstance(dados, dict):
            total_paginas = dados.get("totalPaginas") or dados.get("totalPages")
            if total_paginas and pagina < int(total_paginas):
                tem_proxima = True
            elif dados.get("hasNext") or dados.get("hasNextPage"):
                tem_proxima = True

        if not tem_proxima or len(itens) < TAMANHO_PAGINA:
            break

        pagina += 1
        time.sleep(PAUSA_ENTRE_REQUESTS)

    if not comunicacoes:
        alertas.append({
            "processo":  numero_processo,
            "tipo":      "sem_comunicacoes_no_periodo",
            "descricao": f"Nenhuma comunicação entre {DATA_INICIO} e {DATA_FIM}",
        })

    return comunicacoes, alertas


# ── Função principal ──────────────────────────────────────────────────────────

def executar(caminho_processos=None) -> dict:
    """
    Ponto de entrada da Fase 1.
    Retorna dicionário com 'comunicacoes' e 'alertas'.
    """
    if caminho_processos is None:
        caminho_processos = BASE_DIR / "processos.txt"

    os.makedirs(PASTA_SAIDA, exist_ok=True)

    processos = ler_processos(caminho_processos)
    todas_comunicacoes = []
    todos_alertas      = []

    for i, proc in enumerate(processos, 1):
        logger.info(f"[{i}/{len(processos)}] Consultando processo {proc}…")
        comuns, alertas = consultar_comunicacoes_processo(proc)
        todas_comunicacoes.extend(comuns)
        todos_alertas.extend(alertas)
        time.sleep(PAUSA_ENTRE_REQUESTS)

    logger.info(
        f"Extração concluída: {len(todas_comunicacoes)} comunicação(ões) | "
        f"{len(todos_alertas)} alerta(s)."
    )

    # Persiste os dados brutos
    with open(ARQUIVO_RAW, "w", encoding="utf-8") as f:
        json.dump(todas_comunicacoes, f, ensure_ascii=False, indent=2, default=str)

    with open(ARQUIVO_ALERTAS, "w", encoding="utf-8") as f:
        json.dump(todos_alertas, f, ensure_ascii=False, indent=2)

    logger.info(f"Dados brutos salvos em '{ARQUIVO_RAW}' e alertas em '{ARQUIVO_ALERTAS}'.")

    return {"comunicacoes": todas_comunicacoes, "alertas": todos_alertas}


if __name__ == "__main__":
    executar()
