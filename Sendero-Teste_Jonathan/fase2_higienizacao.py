"""
FASE 2 — HIGIENIZAÇÃO
Recebe os dados brutos (JSON) gerados pela Fase 1,
padroniza campos, limpa textos e entrega um DataFrame
pronto para classificação.
"""

import json
import re
import os
import logging
import pathlib
import unicodedata
from datetime import datetime
from html.parser import HTMLParser

import pandas as pd

logger = logging.getLogger(__name__)

# ── Diretório base (pasta onde está este .py) ─────────────────────────────────
BASE_DIR = pathlib.Path(__file__).resolve().parent

# ── Caminhos ──────────────────────────────────────────────────────────────────
PASTA_BRUTOS  = BASE_DIR / "dados_brutos"
ARQUIVO_RAW   = PASTA_BRUTOS / "comunicacoes_raw.json"
ARQUIVO_CLEAN = PASTA_BRUTOS / "comunicacoes_higienizadas.csv"


# ── Mapeamento de campos da API ───────────────────────────────────────────────
# A API do Comunica PJe pode retornar variações de nomes de campo.
# Este mapa normaliza tudo para nomes internos fixos.
MAPA_CAMPOS = {
    # número do processo
    "numeroProcesso":              "numero_processo",
    "numProcesso":                 "numero_processo",
    "processo":                    "numero_processo",
    "_numeroProcessoConsultado":   "numero_processo",   # injetado na Fase 1

    # data de disponibilização
    "dataDisponibilizacao":        "data_comunicacao",
    "dataPublicacao":              "data_comunicacao",
    "dataComunicacao":             "data_comunicacao",
    "dataEnvio":                   "data_comunicacao",

    # tipo
    "tipoComunicacao":             "tipo_comunicacao",
    "tipo":                        "tipo_comunicacao",
    "modalidade":                  "tipo_comunicacao",

    # texto / conteúdo
    "texto":                       "texto_original",
    "conteudo":                    "texto_original",
    "teor":                        "texto_original",
    "descricao":                   "texto_original",
    "mensagem":                    "texto_original",

    # destinatário
    "destinatario":                "destinatario",
    "nomeDestinatario":            "destinatario",

    # identificador único da comunicação
    "id":                          "id_comunicacao",
    "idComunicacao":               "id_comunicacao",
    "protocolo":                   "id_comunicacao",
}

CAMPOS_INTERNOS = [
    "id_comunicacao",
    "numero_processo",
    "data_comunicacao",
    "tipo_comunicacao",
    "texto_original",
    "destinatario",
]


# ── Limpeza de HTML ───────────────────────────────────────────────────────────
class _ExtractorTexto(HTMLParser):
    """Remove tags HTML e retorna apenas o texto."""
    def __init__(self):
        super().__init__()
        self._partes = []

    def handle_data(self, data):
        self._partes.append(data)

    def get_texto(self):
        return " ".join(self._partes)


def remover_html(texto: str) -> str:
    if not texto:
        return ""
    parser = _ExtractorTexto()
    try:
        parser.feed(texto)
        return parser.get_texto()
    except Exception:
        # Fallback: regex simples
        return re.sub(r"<[^>]+>", " ", texto)


# ── Funções de limpeza de texto ───────────────────────────────────────────────

def normalizar_texto(texto: str) -> str:
    """Pipeline completo de limpeza de texto."""
    if not texto or not isinstance(texto, str):
        return ""

    # 1. Remove HTML
    texto = remover_html(texto)

    # 2. Decodifica entidades HTML (ex: &nbsp; &amp;)
    texto = re.sub(r"&nbsp;",  " ",   texto)
    texto = re.sub(r"&amp;",   "&",   texto)
    texto = re.sub(r"&lt;",    "<",   texto)
    texto = re.sub(r"&gt;",    ">",   texto)
    texto = re.sub(r"&quot;",  '"',   texto)
    texto = re.sub(r"&#\d+;",  " ",   texto)

    # 3. Normaliza unicode (NFC) — preserva acentos
    texto = unicodedata.normalize("NFC", texto)

    # 4. Remove caracteres de controle (exceto \n e \t)
    texto = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", " ", texto)

    # 5. Colapsa espaços múltiplos e quebras de linha
    texto = re.sub(r"[ \t]+",   " ",    texto)
    texto = re.sub(r"\n{3,}",   "\n\n", texto)

    return texto.strip()


def normalizar_data(valor) -> str | None:
    """Tenta converter diversas representações de data para YYYY-MM-DD."""
    if not valor:
        return None
    if isinstance(valor, datetime):
        return valor.strftime("%Y-%m-%d")

    s = str(valor).strip()

    formatos = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y",
        "%d-%m-%Y",
    ]
    for fmt in formatos:
        try:
            return datetime.strptime(s[:len(fmt)], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    return None   # Data não reconhecida — será registrada como alerta


def normalizar_tipo(valor: str) -> str:
    """Padroniza o tipo da comunicação em letras minúsculas, sem acento."""
    if not valor or not isinstance(valor, str):
        return "desconhecido"
    v = unicodedata.normalize("NFD", valor.strip().lower())
    v = re.sub(r"[\u0300-\u036f]", "", v)   # remove diacríticos
    return v


def normalizar_numero_processo(valor: str) -> str:
    """Remove espaços extras do número CNJ."""
    if not valor or not isinstance(valor, str):
        return ""
    return valor.strip()


# ── Normalização de um registro bruto ────────────────────────────────────────

def normalizar_registro(raw: dict) -> dict:
    """
    Transforma um dicionário bruto num registro com campos internos
    padronizados. Campos não mapeados são preservados como metadados.
    """
    reg: dict = {campo: None for campo in CAMPOS_INTERNOS}

    for chave_raw, valor in raw.items():
        nome_interno = MAPA_CAMPOS.get(chave_raw)
        if nome_interno:
            # Aplica a transformação específica de cada campo
            if nome_interno == "numero_processo":
                if not reg["numero_processo"]:   # primeira ocorrência vence
                    reg["numero_processo"] = normalizar_numero_processo(str(valor))
            elif nome_interno == "data_comunicacao":
                if not reg["data_comunicacao"]:
                    reg["data_comunicacao"] = normalizar_data(valor)
            elif nome_interno == "tipo_comunicacao":
                if not reg["tipo_comunicacao"]:
                    reg["tipo_comunicacao"] = normalizar_tipo(str(valor))
            elif nome_interno == "texto_original":
                if not reg["texto_original"]:
                    reg["texto_original"] = normalizar_texto(str(valor))
            else:
                if reg[nome_interno] is None:
                    reg[nome_interno] = valor

    # Garante que o número de processo venha do campo injetado na Fase 1,
    # mesmo que a API não retorne "numeroProcesso" no corpo do item.
    if not reg["numero_processo"] and raw.get("_numeroProcessoConsultado"):
        reg["numero_processo"] = normalizar_numero_processo(
            raw["_numeroProcessoConsultado"]
        )

    return reg


# ── Função principal ──────────────────────────────────────────────────────────

def executar(dados_brutos: list[dict] | None = None) -> tuple[pd.DataFrame, list[dict]]:
    """
    Ponto de entrada da Fase 2.
    Aceita dados_brutos diretamente ou os lê de ARQUIVO_RAW.
    Retorna (DataFrame higienizado, lista de alertas de higienização).
    """

    # 1. Carrega dados brutos
    if dados_brutos is None:
        if not ARQUIVO_RAW.exists():
            raise FileNotFoundError(
                f"Arquivo '{ARQUIVO_RAW}' não encontrado. Execute a Fase 1 primeiro."
            )
        with open(ARQUIVO_RAW, encoding="utf-8") as f:
            dados_brutos = json.load(f)

    logger.info(f"Fase 2: {len(dados_brutos)} registros brutos recebidos.")

    alertas: list[dict] = []
    registros: list[dict] = []

    # 2. Normaliza cada registro
    for i, raw in enumerate(dados_brutos):
        reg = normalizar_registro(raw)
        registros.append(reg)

        # 3. Detecta inconsistências
        if not reg["numero_processo"]:
            alertas.append({
                "indice_raw": i,
                "tipo": "numero_processo_ausente",
                "descricao": "Campo numero_processo não encontrado no registro.",
            })

        if not reg["data_comunicacao"]:
            alertas.append({
                "indice_raw": i,
                "processo": reg["numero_processo"],
                "tipo": "data_invalida",
                "descricao": f"Não foi possível interpretar a data: {raw.get('dataDisponibilizacao') or raw.get('dataPublicacao')}",
            })

        if not reg["texto_original"]:
            alertas.append({
                "indice_raw": i,
                "processo": reg["numero_processo"],
                "tipo": "texto_ausente",
                "descricao": "Comunicação sem texto/conteúdo.",
            })

    # 4. Monta DataFrame
    df = pd.DataFrame(registros)

    # 5. Remove duplicatas exatas (mesmo id_comunicacao)
    total_antes = len(df)
    if "id_comunicacao" in df.columns and df["id_comunicacao"].notna().any():
        df = df.drop_duplicates(subset=["id_comunicacao"])
    else:
        # Fallback: duplicata por processo + data + primeiros 200 chars do texto
        df["_chave_dup"] = (
            df["numero_processo"].fillna("") + "|"
            + df["data_comunicacao"].fillna("") + "|"
            + df["texto_original"].fillna("").str[:200]
        )
        qtd_dup = total_antes - df["_chave_dup"].nunique()
        if qtd_dup > 0:
            alertas.append({
                "tipo": "duplicatas_removidas",
                "descricao": f"{qtd_dup} registro(s) duplicado(s) removido(s) (sem id único disponível).",
            })
        df = df.drop_duplicates(subset=["_chave_dup"]).drop(columns=["_chave_dup"])

    total_depois = len(df)
    if total_antes != total_depois:
        logger.info(f"Duplicatas removidas: {total_antes - total_depois} registro(s).")

    # 6. Renomeia a coluna de texto para o nome final usado nas outras fases
    df.rename(columns={"texto_original": "texto_higienizado"}, inplace=True)

    # 7. Salva CSV intermediário
    os.makedirs(PASTA_BRUTOS, exist_ok=True)
    df.to_csv(ARQUIVO_CLEAN, index=False, encoding="utf-8-sig")

    logger.info(
        f"Fase 2 concluída: {total_depois} registro(s) higienizado(s) | "
        f"{len(alertas)} alerta(s)."
    )

    return df, alertas


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df_limpo, alertas = executar()
    print(df_limpo.head())
