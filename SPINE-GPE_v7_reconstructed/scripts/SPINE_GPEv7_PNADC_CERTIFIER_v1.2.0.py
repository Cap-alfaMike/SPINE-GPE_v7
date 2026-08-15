#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SPINE-GPE v7 — PNADc Direct Platform Certification Engine v1.2.0
=================================================================

Certifica os microdados concentrados no 4º trimestre de 2022 e no 3º
trimestre de 2024 do módulo "Trabalho por meio de plataformas digitais".

O programa:
  * exige a liberação da Fase 0;
  * descobre microdados e layouts oficiais;
  * valida o casamento TXT fixed-width × layout;
  * lê somente as colunas necessárias em chunks;
  * preserva códigos brutos e proveniência;
  * constrói flags diretas a partir de SD14001/S140093, sem proxy;
  * reconstrói o desenho amostral (peso, estrato e UPA);
  * produz tabelas candidatas 2022, 2024 e pooled;
  * calcula estimativas design-based e diagnósticos de precisão;
  * consulta ou reutiliza snapshots SIDRA com contratos determinísticos por código;
  * oferece modo sidra-only, sem reler os TXT fixed-width;
  * bloqueia a certificação em caso de inconsistência crítica.

Uso Colab:
  python SPINE_GPEv7_PNADC_CERTIFIER_v1.2.0.py \
      --root /content/drive/MyDrive/aCidadeAlgoritmica/SPINE-GPEv7 \
      --mode sidra-only --sidra-source cache --strict

Atenção epistemológica:
  - S140093 mede uso de aplicativo de entrega; a classificação oficial exige SD14001=1 e S140093=1.
  - Ausência de S140093 nunca ativa proxy silenciosa.
  - 2022T4 e 2024T3 permanecem períodos distintos; pooling preserva período.
  - Nenhuma localização fina é inventada.
  - Estimativas locais pequenas recebem n, n efetivo, CV e status de precisão.
"""

from __future__ import annotations

import argparse
import csv
import dataclasses
import datetime as dt
import hashlib
import io
import json
import logging
import math
import os
import platform
import re
import shutil
import sys
import tempfile
import textwrap
import time
import unicodedata
import zipfile
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, MutableMapping, Sequence
from urllib.parse import urljoin, urlparse

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception as exc:  # pragma: no cover
    raise RuntimeError("pyarrow é obrigatório. Instale requirements do pacote.") from exc

try:
    from scipy.stats import norm, t as student_t
except Exception as exc:  # pragma: no cover
    raise RuntimeError("scipy é obrigatório. Instale requirements do pacote.") from exc


VERSION = "1.2.0"
SCHEMA_VERSION = "spine-gpe-v7-pnadc-certified-1.1.0"
VALIDATION_SCHEMA_VERSION = "spine-gpe-v7-pnadc-validation-1.2.0"
SCRIPT_NAME = "SPINE_GPEv7_PNADC_CERTIFIER_v1.2.0.py"
UTC = dt.timezone.utc

IBGE_PRODUCT_2022 = (
    "https://www.ibge.gov.br/estatisticas/sociais/trabalho/"
    "9171-pesquisa-nacional-por-amostra-de-domicilios-continua-mensal.html/"
    "17270-pnad-continua.html?edicao=37838"
)
IBGE_PRODUCT_2024 = (
    "https://www.ibge.gov.br/estatisticas/sociais/populacao/"
    "17270-pnad-continua.html?edicao=44741"
)
SIDRA_CLASSIC = "https://apisidra.ibge.gov.br/values"
SIDRA_META = "https://servicodados.ibge.gov.br/api/v3/agregados/{table}/metadados"

# Tabelas oficiais que, na data desta versão, sustentam os golden tests.
# O script também raspa as páginas do produto e registra alterações.
SIDRA_FALLBACK_TABLES: dict[str, int] = {
    "platform_any": 9432,
    "platform_type": 9441,
    "income": 9442,
    "hours": 9443,
    "informality": 9518,
    "social_security": 9642,
    "delivery_income_occupation_2024": 10257,
    "delivery_hours_occupation_2024": 10258,
}

# Contratos determinísticos derivados dos snapshots oficiais SIDRA.
# Nenhum seletor usa proximidade do valor observado; a seleção é feita
# exclusivamente por códigos oficiais de tabela, variável, unidade,
# classificação, território e ano.
SIDRA_CODE_CONTRACTS: dict[str, dict[str, Any]] = {
    "platform_any_total": {
        "table_key": "platform_any",
        "table": 9432,
        "selectors": {
            "Nível Territorial (Código)": "1",
            "Brasil (Código)": "1",
            "Variável (Código)": "12900",
            "Unidade de Medida (Código)": "1572",
            "Trabalho por meio de plataforma digital de serviço no trabalho principal (Código)": "59653",
        },
        "domain": "platform_any_direct",
        "estimand": "total",
        "multiplier": 1000.0,
        "tolerance": 0.015,
        "tolerance_kind": "relative",
        "severity": "critical",
    },
    "platform_any_percent": {
        "table_key": "platform_any",
        "table": 9432,
        "selectors": {
            "Nível Territorial (Código)": "1",
            "Brasil (Código)": "1",
            "Variável (Código)": "12902",
            "Unidade de Medida (Código)": "2",
            "Trabalho por meio de plataforma digital de serviço no trabalho principal (Código)": "59653",
        },
        "domain": "platform_any_direct",
        "estimand": "percent",
        "multiplier": 1.0,
        # SIDRA publica uma casa decimal; tolerância em ponto percentual.
        "tolerance": 0.06,
        "tolerance_kind": "absolute",
        "severity": "critical",
    },
    "platform_delivery_total": {
        "table_key": "platform_type",
        "table": 9441,
        "selectors": {
            "Nível Territorial (Código)": "1",
            "Brasil (Código)": "1",
            "Variável (Código)": "12904",
            "Unidade de Medida (Código)": "1572",
            "Tipo de plataforma de serviço utilizada no trabalho principal (Código)": "59658",
        },
        "domain": "platform_delivery_direct",
        "estimand": "total",
        "multiplier": 1000.0,
        "tolerance": 0.02,
        "tolerance_kind": "relative",
        "severity": "critical",
    },
    "platform_any_hours": {
        "table_key": "hours",
        "table": 9443,
        "selectors": {
            "Nível Territorial (Código)": "1",
            "Brasil (Código)": "1",
            "Variável (Código)": "12912",
            "Unidade de Medida (Código)": "1574",
            "Trabalho por meio de plataforma digital de serviço no trabalho principal (Código)": "59653",
        },
        "domain": "platform_any_direct",
        "estimand": "mean_weekly_hours",
        "multiplier": 1.0,
        # SIDRA publica uma casa decimal; tolerância em horas.
        "tolerance": 0.15,
        "tolerance_kind": "absolute",
        "severity": "critical",
    },
    "platform_any_income": {
        "table_key": "income",
        "table": 9442,
        "selectors": {
            "Nível Territorial (Código)": "1",
            "Brasil (Código)": "1",
            "Variável (Código)": "12910",
            "Unidade de Medida (Código)": "38",
            "Nível de instrução (Código)": "120704",
            "Trabalho por meio de plataforma digital de serviço no trabalho principal (Código)": "59653",
        },
        "domain": "platform_any_direct",
        "estimand": "mean_monthly_income",
        "multiplier": 1.0,
        "tolerance": 0.02,
        "tolerance_kind": "relative",
        # Rendimento real depende da harmonização monetária; falha é secundária
        # e conduz a CORE_CERTIFIED, não a falsa certificação integral.
        "severity": "high",
    },
    "platform_any_social_security_total": {
        "table_key": "social_security",
        "table": 9642,
        "selectors": {
            "Nível Territorial (Código)": "1",
            "Brasil (Código)": "1",
            "Variável (Código)": "12900",
            "Unidade de Medida (Código)": "1572",
            "Contribuição para instituto de previdência em qualquer trabalho (Código)": "99157",
            "Trabalho por meio de plataforma digital de serviço no trabalho principal (Código)": "59653",
        },
        "domain": "platform_any_social_security",
        "estimand": "total",
        "multiplier": 1000.0,
        "tolerance": 0.01,
        "tolerance_kind": "relative",
        "severity": "critical",
    },
}

# Variáveis centrais. Todas as variáveis S14*/SD14* existentes no layout
# também são preservadas para a construção posterior do ICA.
CORE_VARIABLE_CANDIDATES: tuple[str, ...] = (
    "Ano", "Trimestre", "UF", "Capital", "RM_RIDE", "UPA", "Estrato",
    "V1008", "V1014", "V1028", "V1030", "V1031", "V1032",
    "posest", "posest_sxi", "V2007", "V2009", "V2010", "VD3004",
    "VD4001", "VD4002", "VD4003", "VD4004A", "VD4005", "VD4008",
    "VD4009", "V4010", "V4012", "V40121", "V4013", "V4019", "V4020",
    "V4029", "V4029A", "V4029B", "V4039", "V4039C",
    "VD4012", "VD4016", "VD4017", "VD4018", "VD4019", "VD4020",
    "SD14001", "S140091", "S140092", "S140093", "S140094",
)

REQUIRED_DIRECT = {"SD14001", "S140093"}
REQUIRED_SURVEY_GROUPS: dict[str, tuple[str, ...]] = {
    "weight": ("V1028", "V1032", "V1031", "V1030"),
    "stratum": ("Estrato", "posest_sxi", "posest"),
    "psu": ("UPA",),
}

# Ocupações compatíveis com a função de entregador, conforme a Nota Técnica
# IBGE 04/2025: condutores de motocicletas, automóveis e caminhões; veículos
# acionados a pedal; tração animal; carregadores; mensageiros/entregadores.
# A lista é auditável e permanece separada da identificação direta por S140093.
OCCUPATION_COMPATIBLE = {"8321", "8322", "8332", "9331", "9332", "9333", "9621"}
DELIVERY_ACTIVITY_CODES = {"53002"}

OFFICIAL_BINARY_DOMAINS: dict[str, dict[str, str]] = {
    "SD14001": {"1": "Sim", "2": "Não"},
    "S140091": {"1": "Sim", "2": "Não"},
    "S140092": {"1": "Sim", "2": "Não"},
    "S140093": {"1": "Sim", "2": "Não"},
    "S140094": {"1": "Sim", "2": "Não"},
    "VD4012": {"1": "Contribuinte", "2": "Não contribuinte"},
}

# Atividades de comércio e alimentação explicitamente tratadas na Nota Técnica
# IBGE 04/2025 para excluir empregados que apenas usam app de entrega no trabalho.
DELIVERY_TRADE_FOOD_ACTIVITY_CODES = {
    "48020", "48030", "48041", "48042", "48050", "48060",
    "48071", "48072", "48073", "48074", "48075", "48076",
    "48077", "48078", "48079", "48080", "48090", "48100",
    "56011", "56012", "56020",
}

# Definição operacional oficial de informalidade no trabalho principal:
# empregados privados/domésticos sem carteira; empregadores e conta-própria
# sem CNPJ; trabalhadores familiares auxiliares. O universo do módulo já
# exclui empregados públicos e militares.
INFORMAL_EMPLOYEE_VD4009 = {"2", "4"}
FORMAL_EMPLOYEE_VD4009 = {"1", "3", "5", "7"}
BUSINESS_OWNER_VD4009 = {"8", "9"}
FAMILY_AUXILIARY_VD4009 = {"10"}

MISSING_TOKENS = {"", ".", "..", "...", "NA", "N/A", "NULL", "NAN"}


# -----------------------------------------------------------------------------
# Modelos de dados
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class LayoutField:
    variable: str
    start_1based: int
    end_1based: int
    width: int
    type: str = "string"
    decimals: int | None = None
    label: str | None = None
    source_path: str | None = None

    @property
    def colspec(self) -> tuple[int, int]:
        return self.start_1based - 1, self.end_1based


@dataclass
class LayoutCandidate:
    path: str
    parser: str
    fields: list[LayoutField]
    width: int
    has_s140093: bool
    year_score: dict[str, int] = field(default_factory=dict)
    sha256: str | None = None
    warnings: list[str] = field(default_factory=list)


@dataclass
class SourceSelection:
    year: int
    quarter: int
    txt_path: str
    txt_sha256: str
    record_width: int
    layout_path: str
    layout_sha256: str
    layout_width: int
    layout_parser: str
    source_kind: str
    direct_identifier: str = "SD14001=1 AND S140093=1"


@dataclass
class TestResult:
    test_id: str
    status: str
    severity: str
    year: int | None
    message: str
    observed: Any = None
    expected: Any = None
    tolerance: Any = None
    evidence: dict[str, Any] = field(default_factory=dict)


@dataclass
class SurveyEstimate:
    year: int
    quarter: int
    domain: str
    geography: str
    geography_code: str
    estimand: str
    estimate: float | None
    se: float | None
    ci_low: float | None
    ci_high: float | None
    cv_percent: float | None
    n_unweighted: int
    n_effective: float | None
    n_psu: int
    n_strata: int
    df_design: int
    precision_status: str
    n_universe: int = 0
    n_positive: int = 0
    n_outcome_valid: int = 0
    n_effective_domain: float | None = None
    notes: str = ""


# -----------------------------------------------------------------------------
# Utilidades
# -----------------------------------------------------------------------------

def utc_now() -> str:
    return dt.datetime.now(UTC).isoformat()


def run_id() -> str:
    return dt.datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def normalize_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", text).strip().casefold()


def normalize_code(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    if text.upper() in MISSING_TOKENS:
        return None
    # Remove .0 introduzido por planilhas sem apagar zeros à esquerda.
    if re.fullmatch(r"\d+\.0", text):
        text = text[:-2]
    return text


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace("\xa0", "")
    if text.upper() in MISSING_TOKENS or text in {"-", "X"}:
        return None
    # SIDRA em português pode usar ponto de milhar e vírgula decimal.
    if re.fullmatch(r"[-+]?\d{1,3}(?:\.\d{3})*,\d+", text):
        text = text.replace(".", "").replace(",", ".")
    elif "," in text and "." not in text:
        text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def sha256_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def atomic_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


def atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def json_load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def detect_encoding(path: Path) -> str:
    raw = path.read_bytes()[:100_000]
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin1"):
        try:
            raw.decode(enc)
            return enc
        except UnicodeDecodeError:
            continue
    return "latin1"


def record_width_sample(path: Path, sample_n: int = 5000) -> dict[str, Any]:
    lengths: list[int] = []
    total = 0
    with path.open("rb") as f:
        for raw in f:
            total += 1
            if len(lengths) < sample_n or total % 10000 == 0:
                lengths.append(len(raw.rstrip(b"\r\n")))
            if total >= max(sample_n, 100_000):
                break
    if not lengths:
        return {"min": None, "max": None, "mode": None, "sample": 0}
    mode = Counter(lengths).most_common(1)[0][0]
    return {
        "min": min(lengths), "max": max(lengths), "mode": mode,
        "sample": len(lengths), "distribution": dict(Counter(lengths).most_common(10)),
    }


def count_lines(path: Path, block_size: int = 16 * 1024 * 1024) -> int:
    count = 0
    with path.open("rb") as f:
        while True:
            block = f.read(block_size)
            if not block:
                break
            count += block.count(b"\n")
    return count


def is_colab() -> bool:
    return "google.colab" in sys.modules or Path("/content").exists()


def copy_to_cache(path: Path, cache_dir: Path, logger: logging.Logger) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    target = cache_dir / path.name
    marker = target.with_suffix(target.suffix + ".sha256")
    src_hash = sha256_file(path)
    if target.exists() and marker.exists() and marker.read_text().strip() == src_hash:
        logger.info("Cache local reutilizado: %s", target)
        return target
    logger.info("Copiando para cache local rápido: %s -> %s", path, target)
    shutil.copy2(path, target)
    marker.write_text(src_hash, encoding="utf-8")
    return target


def setup_logging(log_path: Path, verbose: bool) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("spine_pnadc_certifier")
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5, status=5,
        backoff_factor=1.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": f"SPINE-GPEv7-PNADC-Certifier/{VERSION} academic-research",
        "Accept": "application/json,text/html,application/xhtml+xml,*/*",
    })
    return session


# -----------------------------------------------------------------------------
# Descoberta da raiz e pré-condições
# -----------------------------------------------------------------------------

def autodetect_root(explicit: str | None) -> Path:
    if explicit:
        return Path(explicit).expanduser().resolve()
    candidates = [
        Path("/content/drive/MyDrive/aCidadeAlgoritmica/SPINE-GPEv7"),
        Path("D:/aCidadeAlgoritmica/SPINE-GPEv7"),
        Path.cwd(),
    ]
    for candidate in candidates:
        if (candidate / "00_admin").exists():
            return candidate.resolve()
    raise FileNotFoundError("Não foi possível detectar a raiz SPINE-GPEv7; use --root.")


def require_phase0_release(root: Path, tests: list[TestResult]) -> None:
    lock_path = root / "00_admin" / "PHASE0_LOCK.json"
    if not lock_path.exists():
        tests.append(TestResult(
            "phase0.lock.exists", "FAIL", "critical", None,
            "PHASE0_LOCK.json não encontrado.", evidence={"path": str(lock_path)}
        ))
        return
    data = json_load(lock_path)
    status = str(data.get("status", "")).upper()
    tests.append(TestResult(
        "phase0.lock.released", "PASS" if status == "RELEASED" else "FAIL",
        "critical", None,
        "Fase 0 liberada." if status == "RELEASED" else "Fase 0 não está RELEASED.",
        observed=status, expected="RELEASED", evidence={"path": str(lock_path)}
    ))


def make_tree(root: Path) -> dict[str, Path]:
    tree = {
        "admin": root / "00_admin",
        "registry": root / "00_admin" / "registry",
        "contracts": root / "00_admin" / "contracts",
        "logs": root / "00_admin" / "logs",
        "reports": root / "00_admin" / "reports",
        "raw_ibge": root / "01_raw" / "10_ibge",
        "interim": root / "02_interim" / "10_pnadc_certification",
        "processed": root / "03_processed" / "10_pnadc_certified",
        "outputs": root / "05_outputs" / "tables" / "pnadc",
        "reports_final": root / "06_reports" / "pnadc_certification",
    }
    for path in tree.values():
        path.mkdir(parents=True, exist_ok=True)
    return tree


# -----------------------------------------------------------------------------
# Layout: parsers e seleção
# -----------------------------------------------------------------------------

def parse_sas_layout(text: str, source: Path) -> list[LayoutField]:
    fields: list[LayoutField] = []
    # @1 Ano 4.  | @10 VAR $2. | @100 VAR 8.2
    rx = re.compile(
        r"@\s*(?P<start>\d+)\s+"
        r"(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s+"
        r"(?P<char>\$)?\s*(?P<width>\d+)"
        r"(?:\.(?P<decimals>\d+))?",
        re.I,
    )
    for line in text.splitlines():
        match = rx.search(line)
        if not match:
            continue
        start = int(match.group("start"))
        width = int(match.group("width"))
        decimals = int(match.group("decimals")) if match.group("decimals") else None
        fields.append(LayoutField(
            variable=match.group("name"), start_1based=start,
            end_1based=start + width - 1, width=width,
            type="string" if match.group("char") else "numeric",
            decimals=decimals, source_path=str(source),
        ))
    return deduplicate_layout(fields)


def parse_range_text_layout(text: str, source: Path) -> list[LayoutField]:
    fields: list[LayoutField] = []
    patterns = [
        re.compile(r"^\s*(?P<start>\d+)\s*[-:]\s*(?P<end>\d+)\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)", re.I),
        re.compile(r"^\s*(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s+(?P<start>\d+)\s*[-:]\s*(?P<end>\d+)", re.I),
    ]
    for line in text.splitlines():
        for rx in patterns:
            match = rx.search(line)
            if not match:
                continue
            start, end = int(match.group("start")), int(match.group("end"))
            if end < start:
                continue
            fields.append(LayoutField(
                variable=match.group("name"), start_1based=start, end_1based=end,
                width=end - start + 1, source_path=str(source),
            ))
            break
    return deduplicate_layout(fields)


def _header_score(value: str, concepts: Sequence[str]) -> int:
    normed = normalize_text(value)
    return max((100 if normed == c else 50 if c in normed else 0) for c in concepts)


def parse_excel_layout(path: Path) -> list[LayoutField]:
    try:
        book = pd.ExcelFile(path)
    except Exception:
        return []
    best: list[LayoutField] = []
    for sheet in book.sheet_names:
        try:
            raw = pd.read_excel(path, sheet_name=sheet, header=None, dtype=str, nrows=20000)
        except Exception:
            continue
        raw = raw.fillna("")
        # Detecta uma linha de cabeçalho entre as primeiras 80 linhas.
        for header_idx in range(min(80, len(raw))):
            header = [str(x) for x in raw.iloc[header_idx].tolist()]
            name_col = max(range(len(header)), key=lambda j: _header_score(header[j], (
                "variavel", "codigo da variavel", "nome da variavel", "variable", "codigo"
            ))) if header else 0
            start_col = max(range(len(header)), key=lambda j: _header_score(header[j], (
                "posicao inicial", "inicio", "posicao", "start", "posicao de inicio"
            ))) if header else 0
            width_col = max(range(len(header)), key=lambda j: _header_score(header[j], (
                "tamanho", "largura", "numero de caracteres", "width"
            ))) if header else 0
            end_col = max(range(len(header)), key=lambda j: _header_score(header[j], (
                "posicao final", "fim", "end"
            ))) if header else 0
            if _header_score(header[name_col], ("variavel", "codigo da variavel", "nome da variavel", "variable", "codigo")) == 0:
                continue
            if _header_score(header[start_col], ("posicao inicial", "inicio", "posicao", "start", "posicao de inicio")) == 0:
                continue
            has_width = _header_score(header[width_col], ("tamanho", "largura", "numero de caracteres", "width")) > 0
            has_end = _header_score(header[end_col], ("posicao final", "fim", "end")) > 0
            if not (has_width or has_end):
                continue
            rows: list[LayoutField] = []
            for _, row in raw.iloc[header_idx + 1:].iterrows():
                name = str(row.iloc[name_col]).strip()
                if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
                    continue
                start = safe_float(row.iloc[start_col])
                width = safe_float(row.iloc[width_col]) if has_width else None
                end = safe_float(row.iloc[end_col]) if has_end else None
                if start is None:
                    continue
                start_i = int(start)
                if width is not None and width > 0:
                    width_i = int(width)
                    end_i = start_i + width_i - 1
                elif end is not None and end >= start:
                    end_i = int(end)
                    width_i = end_i - start_i + 1
                else:
                    continue
                rows.append(LayoutField(
                    variable=name, start_1based=start_i, end_1based=end_i,
                    width=width_i, source_path=str(path),
                ))
            rows = deduplicate_layout(rows)
            if len(rows) > len(best):
                best = rows
            break
    return best


def deduplicate_layout(fields: Sequence[LayoutField]) -> list[LayoutField]:
    by_name: dict[str, LayoutField] = {}
    for item in fields:
        key = item.variable.upper()
        if key not in by_name:
            by_name[key] = dataclasses.replace(item, variable=item.variable)
        elif by_name[key].colspec != item.colspec:
            # Mantém a primeira; inconsistência será detectada no relatório.
            continue
    return sorted(by_name.values(), key=lambda x: (x.start_1based, x.variable))


def extract_archives_for_docs(root: Path, destination: Path, logger: logging.Logger) -> list[Path]:
    extracted: list[Path] = []
    destination.mkdir(parents=True, exist_ok=True)
    for archive in root.rglob("*.zip"):
        try:
            with zipfile.ZipFile(archive) as zf:
                members = [m for m in zf.namelist() if Path(m).suffix.lower() in {
                    ".sas", ".txt", ".xls", ".xlsx", ".pdf", ".csv"
                } and any(token in normalize_text(m) for token in ("input", "dicion", "layout", "document", "question"))]
                for member in members:
                    target = destination / archive.stem / Path(member).name
                    if not target.exists():
                        target.parent.mkdir(parents=True, exist_ok=True)
                        with zf.open(member) as src, target.open("wb") as dst:
                            shutil.copyfileobj(src, dst)
                    extracted.append(target)
        except zipfile.BadZipFile:
            logger.warning("ZIP inválido ignorado: %s", archive)
    return extracted


def discover_layout_candidates(search_roots: Sequence[Path], logger: logging.Logger) -> list[LayoutCandidate]:
    paths: set[Path] = set()
    patterns = ("*.sas", "*input*.txt", "*INPUT*.txt", "*layout*.txt", "*dicion*.xls*", "*Dicion*.xls*")
    for root in search_roots:
        if not root.exists():
            continue
        for pattern in patterns:
            paths.update(p for p in root.rglob(pattern) if p.is_file())
    candidates: list[LayoutCandidate] = []
    for path in sorted(paths):
        try:
            suffix = path.suffix.lower()
            if suffix in {".xls", ".xlsx"}:
                fields = parse_excel_layout(path)
                parser = "excel_heuristic"
            else:
                text = path.read_text(encoding=detect_encoding(path), errors="replace")
                fields = parse_sas_layout(text, path)
                parser = "sas_input"
                if not fields:
                    fields = parse_range_text_layout(text, path)
                    parser = "range_text"
            if len(fields) < 20:
                continue
            variables = {f.variable.upper() for f in fields}
            width = max(f.end_1based for f in fields)
            name = normalize_text(path.name)
            scores = {
                "2022": (30 if "2022" in name else 0) + (20 if any(x in name for x in ("trimestre4", "trimestre_4", "tri4", "t4")) else 0),
                "2024": (30 if "2024" in name else 0) + (20 if any(x in name for x in ("trimestre3", "trimestre_3", "tri3", "t3")) else 0),
            }
            candidate = LayoutCandidate(
                path=str(path), parser=parser, fields=fields, width=width,
                has_s140093="S140093" in variables, year_score=scores,
                sha256=sha256_file(path),
            )
            candidates.append(candidate)
        except Exception as exc:
            logger.warning("Falha ao interpretar layout %s: %s", path, exc)
    return candidates


def select_layout(candidates: Sequence[LayoutCandidate], year: int, record_width: int) -> LayoutCandidate:
    scored: list[tuple[int, LayoutCandidate]] = []
    for candidate in candidates:
        score = candidate.year_score.get(str(year), 0)
        if candidate.has_s140093:
            score += 100
        if candidate.width == record_width:
            score += 200
        else:
            score -= min(abs(candidate.width - record_width), 100)
        names = {f.variable.upper() for f in candidate.fields}
        score += 20 * len(REQUIRED_DIRECT & names)
        score += 10 if "V1028" in names else 0
        score += 10 if "UPA" in names else 0
        scored.append((score, candidate))
    if not scored:
        raise RuntimeError("Nenhum layout interpretável foi encontrado.")
    scored.sort(key=lambda pair: (pair[0], len(pair[1].fields)), reverse=True)
    best_score, best = scored[0]
    if not best.has_s140093 or best.width != record_width:
        summary = [{"score": s, "path": c.path, "width": c.width, "has_S140093": c.has_s140093} for s, c in scored[:10]]
        raise RuntimeError(
            f"Nenhum layout direto casou com a largura {record_width} para {year}. Candidatos: {summary}"
        )
    return best


# -----------------------------------------------------------------------------
# Dicionário de categorias
# -----------------------------------------------------------------------------

def looks_like_variable(text: str) -> bool:
    return bool(re.fullmatch(r"(?:S|SD|V|VD|C)[A-Z0-9_]{3,12}", text.strip(), re.I))


def extract_categories_from_excel(path: Path, target_vars: set[str]) -> dict[str, dict[str, str]]:
    result: dict[str, dict[str, str]] = defaultdict(dict)
    try:
        book = pd.ExcelFile(path)
    except Exception:
        return {}
    for sheet in book.sheet_names:
        try:
            df = pd.read_excel(path, sheet_name=sheet, header=None, dtype=str, nrows=30000).fillna("")
        except Exception:
            continue
        current: str | None = None
        for _, row in df.iterrows():
            cells = [str(v).strip() for v in row.tolist()]
            variable_hits = [c.upper() for c in cells if c.upper() in target_vars]
            if variable_hits:
                current = variable_hits[0]
            elif any(looks_like_variable(c) for c in cells):
                current = None
            if current is None:
                continue
            # Procura pares código-rótulo na mesma linha.
            for i, cell in enumerate(cells):
                code = normalize_code(cell)
                if code is None or not re.fullmatch(r"-?\d+(?:\.\d+)?", code):
                    continue
                labels = [x for x in cells[i + 1:] if x and normalize_code(x) != code]
                label = next((x for x in labels if not re.fullmatch(r"-?\d+(?:\.\d+)?", x)), None)
                if label and len(label) <= 500:
                    result[current].setdefault(code, label)
                    break
    return {k: v for k, v in result.items() if v}


def extract_categories_from_text(path: Path, target_vars: set[str]) -> dict[str, dict[str, str]]:
    result: dict[str, dict[str, str]] = defaultdict(dict)
    try:
        text = path.read_text(encoding=detect_encoding(path), errors="replace")
    except Exception:
        return {}
    lines = text.splitlines()
    current: str | None = None
    for line in lines:
        hits = re.findall(r"\b(?:S|SD|V|VD)[A-Z0-9_]{3,12}\b", line, re.I)
        hit = next((h.upper() for h in hits if h.upper() in target_vars), None)
        if hit:
            current = hit
            continue
        if current and hits and not hit:
            current = None
        if current:
            match = re.match(r"^\s*(\d+)\s*[-–—=:;\.]\s*(.{2,300})$", line)
            if match:
                result[current].setdefault(match.group(1), match.group(2).strip())
    return {k: v for k, v in result.items() if v}


def build_category_dictionary(paths: Sequence[Path], target_vars: set[str]) -> dict[str, dict[str, str]]:
    merged: dict[str, dict[str, str]] = defaultdict(dict)
    for path in paths:
        suffix = path.suffix.lower()
        if suffix in {".xls", ".xlsx"}:
            found = extract_categories_from_excel(path, target_vars)
        elif suffix in {".txt", ".sas", ".csv"}:
            found = extract_categories_from_text(path, target_vars)
        else:
            continue
        for variable, mapping in found.items():
            for code, label in mapping.items():
                merged[variable].setdefault(code, label)
    return dict(merged)


def infer_yes_code(variable: str, categories: Mapping[str, Mapping[str, str]], observed: set[str]) -> tuple[str | None, str]:
    variable = variable.upper()
    if variable in OFFICIAL_BINARY_DOMAINS:
        expected = set(OFFICIAL_BINARY_DOMAINS[variable])
        cleaned = {canonical for value in observed if (canonical := normalize_code(value)) is not None}
        if cleaned and not cleaned.issubset(expected):
            return None, "observed_domain_conflicts_official_contract"
        return "1", "official_contract"
    mapping = categories.get(variable, {})
    candidates = [
        code for code, label in mapping.items()
        if normalize_text(label).startswith("sim") or normalize_text(label) == "contribuinte"
    ]
    if len(candidates) == 1:
        return candidates[0], "dictionary_label"
    return None, "unresolved"


# -----------------------------------------------------------------------------
# Descoberta dos microdados
# -----------------------------------------------------------------------------

def find_txt_candidates(root: Path, year: int, quarter: int) -> list[tuple[int, Path, str]]:
    all_txt = [p for p in root.rglob("*.txt") if p.is_file()]
    scored: list[tuple[int, Path, str]] = []
    for path in all_txt:
        name = normalize_text(path.name)
        full = normalize_text(str(path))
        score = 0
        kind = "unknown"
        if str(year) in name or str(year) in full:
            score += 20
        if any(token in name for token in (f"trimestre{quarter}", f"trimestre_{quarter}", f"0{quarter}{year}", f"q{quarter}")):
            score += 30
        if "platform_direct_supplements" in full or "plataform" in full:
            score += 100
            kind = "official_concentrated_supplement"
        if "data_pnadc" in full:
            score += 15
            kind = "user_uploaded_quarterly"
        if path.stat().st_size > 100_000_000:
            score += 20
        if "document" in full or "input" in name or "dicion" in name:
            score -= 200
        if score > 0:
            scored.append((score, path, kind))
    scored.sort(key=lambda x: (x[0], x[1].stat().st_size), reverse=True)
    return scored


def choose_microdata(root: Path, year: int, quarter: int, layouts: Sequence[LayoutCandidate], logger: logging.Logger) -> tuple[Path, LayoutCandidate, dict[str, Any], str]:
    candidates = find_txt_candidates(root, year, quarter)
    evidence: list[dict[str, Any]] = []
    for score, path, kind in candidates:
        audit = record_width_sample(path)
        width = audit.get("mode")
        direct_layouts = [c for c in layouts if c.has_s140093 and c.width == width]
        evidence.append({"score": score, "path": str(path), "kind": kind, "width": width, "direct_layouts": len(direct_layouts)})
        if direct_layouts:
            layout = select_layout(direct_layouts, year, int(width))
            logger.info("Microdado direto %s selecionado: %s (largura=%s)", year, path, width)
            return path, layout, audit, kind
    raise RuntimeError(f"Não foi encontrado microdado direto compatível para {year}T{quarter}. Candidatos: {evidence[:20]}")


# -----------------------------------------------------------------------------
# Leitura fixed-width e certified tables
# -----------------------------------------------------------------------------

def choose_survey_variable(layout_columns: Sequence[str], group: str) -> str | None:
    actual = {str(name).upper(): str(name) for name in layout_columns}
    for candidate in REQUIRED_SURVEY_GROUPS[group]:
        if candidate.upper() in actual:
            return actual[candidate.upper()]
    return None


def selected_fields(layout: LayoutCandidate) -> list[LayoutField]:
    by_upper = {f.variable.upper(): f for f in layout.fields}
    wanted = {x.upper() for x in CORE_VARIABLE_CANDIDATES}
    wanted.update(name for name in by_upper if name.startswith("S14") or name.startswith("SD14"))
    selected = [by_upper[name] for name in wanted if name in by_upper]
    return sorted(selected, key=lambda x: x.start_1based)


def read_fwf_chunks(path: Path, fields: Sequence[LayoutField], chunk_rows: int) -> Iterator[pd.DataFrame]:
    colspecs = [field.colspec for field in fields]
    names = [field.variable for field in fields]
    yield from pd.read_fwf(
        path,
        colspecs=colspecs,
        names=names,
        dtype=str,
        chunksize=chunk_rows,
        encoding="latin1",
        keep_default_na=False,
        na_filter=False,
    )


def numeric_series(series: pd.Series) -> pd.Series:
    cleaned = series.astype("string").str.strip().replace(list(MISSING_TOKENS), pd.NA)
    return pd.to_numeric(cleaned.str.replace(",", ".", regex=False), errors="coerce")


def code_series(series: pd.Series) -> pd.Series:
    result = series.astype("string").str.strip()
    return result.mask(result.str.upper().isin(MISSING_TOKENS), pd.NA)


def canonical_integer_code(series: pd.Series) -> pd.Series:
    """Normaliza códigos inteiros preservando NA: '01' -> '1', '1.0' -> '1'."""
    raw = code_series(series)
    numeric = pd.to_numeric(raw, errors="coerce")
    out = numeric.round().astype("Int64").astype("string")
    return out.mask(raw.isna(), pd.NA)


def official_domain(variable: str, categories: Mapping[str, Mapping[str, str]]) -> dict[str, str]:
    variable = variable.upper()
    if variable in OFFICIAL_BINARY_DOMAINS:
        return dict(OFFICIAL_BINARY_DOMAINS[variable])
    return dict(categories.get(variable, {}))


def derive_informal_status(frame: pd.DataFrame) -> pd.Series:
    """Reconstrói a situação de informalidade conforme o conceito do IBGE.

    Retorna boolean nullable. Não classifica silenciosamente casos sem CNPJ
    informado para empregador/conta-própria.
    """
    result = pd.Series(pd.NA, index=frame.index, dtype="boolean")
    if "VD4009" not in frame:
        return result
    position = canonical_integer_code(frame["VD4009"])
    result.loc[position.isin(INFORMAL_EMPLOYEE_VD4009)] = True
    result.loc[position.isin(FORMAL_EMPLOYEE_VD4009)] = False
    result.loc[position.isin(FAMILY_AUXILIARY_VD4009)] = True
    owners = position.isin(BUSINESS_OWNER_VD4009)
    if "V4019" in frame:
        cnpj = canonical_integer_code(frame["V4019"])
        result.loc[owners & cnpj.eq("1")] = False
        result.loc[owners & cnpj.eq("2")] = True
    return result


def derive_social_security(frame: pd.DataFrame) -> pd.Series:
    """Contribuição previdenciária em qualquer trabalho via VD4012 (1/2)."""
    result = pd.Series(pd.NA, index=frame.index, dtype="boolean")
    if "VD4012" not in frame:
        return result
    code = canonical_integer_code(frame["VD4012"])
    result.loc[code.eq("1")] = True
    result.loc[code.eq("2")] = False
    return result


def stable_record_id(frame: pd.DataFrame, year: int, offset: int) -> pd.Series:
    key_candidates = [c for c in ("UPA", "V1008", "V1014", "V2003") if c in frame.columns]
    if key_candidates:
        keys = frame[key_candidates].astype("string").fillna("").agg("|".join, axis=1)
        keys = keys + f"|{year}|" + pd.Series(np.arange(offset, offset + len(frame)), index=frame.index).astype(str)
    else:
        keys = pd.Series([f"{year}|{i}" for i in range(offset, offset + len(frame))], index=frame.index)
    return keys.map(lambda x: hashlib.blake2b(x.encode("utf-8"), digest_size=12).hexdigest())


def write_parquet_stream(chunks: Iterator[pd.DataFrame], path: Path, schema_metadata: Mapping[str, str]) -> tuple[int, list[str]]:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    writer: pq.ParquetWriter | None = None
    total = 0
    columns: list[str] = []
    try:
        for frame in chunks:
            if frame.empty:
                continue
            table = pa.Table.from_pandas(frame, preserve_index=False)
            metadata = dict(table.schema.metadata or {})
            metadata.update({k.encode(): str(v).encode() for k, v in schema_metadata.items()})
            table = table.replace_schema_metadata(metadata)
            if writer is None:
                writer = pq.ParquetWriter(tmp, table.schema, compression="zstd", use_dictionary=True)
                columns = frame.columns.tolist()
            writer.write_table(table)
            total += len(frame)
    finally:
        if writer is not None:
            writer.close()
    if writer is None:
        raise RuntimeError("Nenhum registro foi escrito no Parquet.")
    tmp.replace(path)
    return total, columns


def certify_year(
    year: int,
    quarter: int,
    source_path: Path,
    layout: LayoutCandidate,
    categories: Mapping[str, Mapping[str, str]],
    output_path: Path,
    chunk_rows: int,
    cache_dir: Path | None,
    logger: logging.Logger,
    tests: list[TestResult],
) -> tuple[dict[str, Any], pd.DataFrame]:
    working_path = copy_to_cache(source_path, cache_dir, logger) if cache_dir else source_path
    fields = selected_fields(layout)
    names_upper = {f.variable.upper() for f in fields}
    missing_direct = sorted(REQUIRED_DIRECT - names_upper)
    tests.append(TestResult(
        "layout.direct_variables", "PASS" if not missing_direct else "FAIL", "critical", year,
        "Variáveis diretas presentes." if not missing_direct else "Variáveis diretas ausentes.",
        observed=sorted(REQUIRED_DIRECT & names_upper), expected=sorted(REQUIRED_DIRECT), evidence={"missing": missing_direct}
    ))
    survey_vars = {group: choose_survey_variable([f.variable for f in fields], group) for group in REQUIRED_SURVEY_GROUPS}
    for group, variable in survey_vars.items():
        tests.append(TestResult(
            f"survey.{group}.available", "PASS" if variable else "FAIL", "critical", year,
            f"Variável de {group} identificada: {variable}" if variable else f"Variável de {group} não identificada.",
            observed=variable, expected=list(REQUIRED_SURVEY_GROUPS[group])
        ))
    for variable, purpose in (
        ("VD4012", "contribuição previdenciária em qualquer trabalho"),
        ("VD4009", "posição/categoria no trabalho principal"),
        ("V4019", "registro CNPJ para empregador/conta-própria"),
        ("VD4019", "rendimento mensal habitual no trabalho principal"),
        ("V4039", "horas habituais no trabalho principal"),
    ):
        available = variable in names_upper
        tests.append(TestResult(
            f"{year}.semantic.{variable}.available",
            "PASS" if available else "FAIL",
            "critical", year,
            f"{variable} disponível para {purpose}." if available else f"{variable} ausente: não é possível certificar {purpose}.",
            observed=available, expected=True,
        ))
    # Primeiro passe curto para domínio observado e inferência do código Sim.
    observed_codes: dict[str, set[str]] = {"SD14001": set(), "S140093": set()}
    sample_reader = read_fwf_chunks(working_path, [f for f in fields if f.variable.upper() in observed_codes], min(chunk_rows, 50000))
    sampled = 0
    for frame in sample_reader:
        for variable in observed_codes:
            if variable in frame:
                observed_codes[variable].update(x for x in frame[variable].map(normalize_code).dropna().tolist())
        sampled += len(frame)
        if sampled >= 200000:
            break
    yes_codes: dict[str, str] = {}
    yes_sources: dict[str, str] = {}
    for variable in ("SD14001", "S140093"):
        code, source = infer_yes_code(variable, categories, observed_codes[variable])
        if code:
            yes_codes[variable] = code
        yes_sources[variable] = source
        tests.append(TestResult(
            f"domain.{variable}.yes_code", "PASS" if code and source in {"dictionary_label", "official_contract"} else "WARN" if code else "FAIL",
            "critical" if not code else "high", year,
            f"Código 'Sim' de {variable}: {code} ({source}).",
            observed=sorted(observed_codes[variable]), expected=official_domain(variable, categories),
        ))
    offset = 0
    diagnostics = Counter()
    summary_frames: list[pd.DataFrame] = []
    source_hash = sha256_file(source_path)
    layout_hash = layout.sha256 or ""

    def transformed_chunks() -> Iterator[pd.DataFrame]:
        nonlocal offset
        for raw in read_fwf_chunks(working_path, fields, chunk_rows):
            raw.columns = [str(c) for c in raw.columns]
            for column in raw.columns:
                raw[column] = code_series(raw[column])
            raw.insert(0, "record_id", stable_record_id(raw, year, offset))
            raw.insert(1, "source_year", year)
            raw.insert(2, "reference_quarter", quarter)
            raw.insert(3, "measurement_status", "observed_direct")
            raw.insert(4, "identification_method", "SD14001_and_S140093_direct")
            raw.insert(5, "source_file_sha256", source_hash)
            raw.insert(6, "layout_file_sha256", layout_hash)
            raw.insert(7, "schema_version", SCHEMA_VERSION)
            raw["eligible_platform_module"] = raw["SD14001"].notna() if "SD14001" in raw else False
            if yes_codes.get("SD14001"):
                raw["platform_any_direct"] = raw["SD14001"].eq(yes_codes["SD14001"])
            else:
                raw["platform_any_direct"] = pd.Series(pd.NA, index=raw.index, dtype="boolean")
            if yes_codes.get("S140093"):
                raw["delivery_app_use_raw"] = raw["S140093"].eq(yes_codes["S140093"])
            else:
                raw["delivery_app_use_raw"] = pd.Series(pd.NA, index=raw.index, dtype="boolean")

            # Identificação oficial: os totais por tipo partem de SD14001 e
            # filtram S140091–S140094. Mantemos NA fora do universo do módulo.
            raw["platform_delivery_direct"] = (
                raw["platform_any_direct"] & raw["delivery_app_use_raw"]
            ).astype("boolean")
            raw["delivery_app_use_nonplatform"] = (
                (~raw["platform_any_direct"]) & raw["delivery_app_use_raw"]
            ).astype("boolean")

            if "V4010" in raw:
                occupation = canonical_integer_code(raw["V4010"]).str.zfill(4)
                raw["delivery_occupation_compatible"] = occupation.isin(OCCUPATION_COMPATIBLE)
            else:
                raw["delivery_occupation_compatible"] = False
            if "V4013" in raw:
                activity = canonical_integer_code(raw["V4013"]).str.zfill(5)
                raw["delivery_activity_compatible"] = activity.isin(DELIVERY_ACTIVITY_CODES)
            else:
                activity = pd.Series(pd.NA, index=raw.index, dtype="string")
                raw["delivery_activity_compatible"] = False

            # Entregador plataformizado: plataforma de entrega + ocupação compatível.
            raw["platform_courier_direct"] = (
                raw["platform_delivery_direct"].fillna(False)
                & raw["delivery_occupation_compatible"].fillna(False)
            ).astype("boolean")

            # Diagnóstico da exclusão descrita na Nota Técnica 04/2025.
            if "VD4008" in raw:
                position = canonical_integer_code(raw["VD4008"])
            else:
                position = pd.Series(pd.NA, index=raw.index, dtype="string")
            family_help = (
                canonical_integer_code(raw["V40121"]).eq("1")
                if "V40121" in raw else pd.Series(False, index=raw.index)
            )
            raw["delivery_trade_food_employee_excluded"] = (
                raw["delivery_app_use_raw"].fillna(False)
                & activity.isin(DELIVERY_TRADE_FOOD_ACTIVITY_CODES)
                & ~position.isin({"4", "5"})
                & ~family_help.fillna(False)
            ).astype("boolean")

            # Variáveis canônicas, preservando as originais.
            if survey_vars["weight"]:
                raw["survey_weight"] = numeric_series(raw[survey_vars["weight"]])
            if survey_vars["stratum"]:
                raw["survey_stratum"] = raw[survey_vars["stratum"]].astype("string")
            if survey_vars["psu"]:
                raw["survey_psu"] = raw[survey_vars["psu"]].astype("string")
            if "V2009" in raw:
                raw["age_years"] = numeric_series(raw["V2009"])
            if "UF" in raw:
                raw["region_code"] = raw["UF"].astype("string").str.slice(0, 1).map({
                    "1": "N", "2": "NE", "3": "SE", "4": "S", "5": "CO"
                }).astype("string")

            # Previdência em qualquer trabalho: variável derivada oficial VD4012.
            raw["social_security_contributor"] = derive_social_security(raw)
            # Informalidade conforme carteira/CNPJ/posição no trabalho principal.
            raw["informal_status"] = derive_informal_status(raw)

            # Rendimento habitual mensal no trabalho principal. V4019 é CNPJ e
            # jamais pode ser usado como fallback de rendimento.
            if "VD4019" in raw:
                raw["monthly_income_usual"] = numeric_series(raw["VD4019"])
            else:
                raw["monthly_income_usual"] = pd.Series(np.nan, index=raw.index, dtype=float)
            if "V4039" in raw:
                raw["weekly_hours_usual"] = numeric_series(raw["V4039"])
            else:
                raw["weekly_hours_usual"] = pd.Series(np.nan, index=raw.index, dtype=float)
            raw["synthetic_location"] = False
            diagnostics["records"] += len(raw)
            diagnostics["eligible"] += int(raw["eligible_platform_module"].sum())
            diagnostics["platform_any"] += int(raw["platform_any_direct"].fillna(False).sum())
            diagnostics["delivery_app_use_raw"] += int(raw["delivery_app_use_raw"].fillna(False).sum())
            diagnostics["platform_delivery"] += int(raw["platform_delivery_direct"].fillna(False).sum())
            diagnostics["delivery_app_use_nonplatform"] += int(raw["delivery_app_use_nonplatform"].fillna(False).sum())
            diagnostics["platform_courier"] += int(raw["platform_courier_direct"].fillna(False).sum())
            # Amostra compacta para diagnósticos design-based sem carregar tudo na RAM.
            summary_cols = [c for c in (
                "survey_weight", "survey_stratum", "survey_psu", "UF", "Capital", "RM_RIDE",
                "eligible_platform_module", "platform_any_direct", "delivery_app_use_raw",
                "platform_delivery_direct", "delivery_app_use_nonplatform",
                "platform_courier_direct", "delivery_trade_food_employee_excluded",
                "monthly_income_usual", "weekly_hours_usual",
                "informal_status", "social_security_contributor", "region_code",
                "VD4008", "VD4009", "VD4012", "V4019", "V4013", "V4010",
                "V2007", "V2010", "VD3004"
            ) if c in raw]
            summary_frames.append(raw.loc[raw["eligible_platform_module"].fillna(False), summary_cols].copy())
            offset += len(raw)
            yield raw

    metadata = {
        "spine_schema_version": SCHEMA_VERSION,
        "year": str(year), "quarter": str(quarter),
        "direct_identifier": "SD14001=1 AND S140093=1",
        "source_sha256": sha256_file(source_path),
        "layout_sha256": layout.sha256 or "",
        "no_proxy_fill": "true",
        "synthetic_location": "false",
    }
    n_rows, output_columns = write_parquet_stream(transformed_chunks(), output_path, metadata)
    summary = pd.concat(summary_frames, ignore_index=True) if summary_frames else pd.DataFrame()
    manifest = {
        "year": year, "quarter": quarter, "created_at_utc": utc_now(),
        "source_path": str(source_path), "source_sha256": source_hash,
        "layout_path": layout.path, "layout_sha256": layout.sha256,
        "record_width": layout.width, "layout_parser": layout.parser,
        "rows": n_rows, "columns": output_columns,
        "survey_variables": survey_vars,
        "yes_codes": yes_codes, "yes_code_sources": yes_sources,
        "observed_domains": {k: sorted(v) for k, v in observed_codes.items()},
        "diagnostics": dict(diagnostics),
        "output_path": str(output_path), "output_sha256": sha256_file(output_path),
        "measurement_status": "observed_direct",
        "identification_method": "SD14001_and_S140093_direct",
        "no_proxy_fill": True, "synthetic_location": False,
    }
    return manifest, summary


# -----------------------------------------------------------------------------
# Desenho amostral e estimativas
# -----------------------------------------------------------------------------

def kish_effective_n(weights: pd.Series) -> float | None:
    w = pd.to_numeric(weights, errors="coerce").dropna()
    w = w[w > 0]
    if w.empty:
        return None
    denom = float(np.square(w).sum())
    return float(w.sum() ** 2 / denom) if denom > 0 else None


def _cluster_variance(linearized: pd.Series, strata: pd.Series, psu: pd.Series) -> tuple[float | None, int, int, int]:
    temp = pd.DataFrame({"z": linearized, "h": strata.astype("string"), "psu": psu.astype("string")}).dropna()
    if temp.empty:
        return None, 0, 0, 0
    cluster = temp.groupby(["h", "psu"], observed=True)["z"].sum().reset_index()
    variance = 0.0
    used_strata = 0
    singleton = 0
    for _, group in cluster.groupby("h", observed=True):
        m = len(group)
        if m < 2:
            singleton += 1
            continue
        used_strata += 1
        values = group["z"].to_numpy(dtype=float)
        variance += (m / (m - 1.0)) * float(np.square(values - values.mean()).sum())
    n_psu = int(cluster["psu"].nunique())
    n_strata = int(cluster["h"].nunique())
    return variance, n_psu, n_strata, singleton


def precision_status(cv: float | None, n_eff: float | None, n: int) -> str:
    if n < 30 or (n_eff is not None and n_eff < 30):
        return "insufficient"
    if cv is None:
        return "unknown"
    if cv <= 15:
        return "good"
    if cv <= 30:
        return "caution"
    return "unreliable"


def survey_total(frame: pd.DataFrame, y: pd.Series, year: int, quarter: int, domain: str, geography: str, geography_code: str) -> SurveyEstimate:
    valid = frame[["survey_weight", "survey_stratum", "survey_psu"]].copy()
    valid["y"] = pd.to_numeric(y.reindex(frame.index), errors="coerce")
    valid["w"] = pd.to_numeric(valid["survey_weight"], errors="coerce")
    valid = valid.dropna(subset=["y", "w", "survey_stratum", "survey_psu"])
    valid = valid[valid["w"] > 0]
    estimate = float((valid["w"] * valid["y"]).sum()) if not valid.empty else None
    linearized = valid["w"] * valid["y"]
    variance, n_psu, n_strata, singleton = _cluster_variance(
        linearized, valid["survey_stratum"], valid["survey_psu"]
    )
    se = math.sqrt(variance) if variance is not None and variance >= 0 else None
    df = max(n_psu - n_strata, 1)
    crit = float(student_t.ppf(0.975, df)) if df > 1 else 1.96
    ci_low = estimate - crit * se if estimate is not None and se is not None else None
    ci_high = estimate + crit * se if estimate is not None and se is not None else None
    cv = abs(se / estimate * 100) if estimate not in (None, 0) and se is not None else None
    positive = valid["y"].gt(0)
    domain_weights = valid.loc[positive, "w"] * valid.loc[positive, "y"].clip(lower=0)
    n_eff_domain = kish_effective_n(domain_weights)
    n_positive = int(positive.sum())
    n_universe = int(len(valid))
    return SurveyEstimate(
        year=year, quarter=quarter, domain=domain, geography=geography,
        geography_code=geography_code, estimand="total", estimate=estimate,
        se=se, ci_low=ci_low, ci_high=ci_high, cv_percent=cv,
        n_unweighted=n_positive, n_effective=n_eff_domain, n_psu=n_psu,
        n_strata=n_strata, df_design=df,
        precision_status=precision_status(cv, n_eff_domain, n_positive),
        n_universe=n_universe, n_positive=n_positive,
        n_outcome_valid=n_universe, n_effective_domain=n_eff_domain,
        notes=f"Taylor linearization por estrato/UPA; estratos singleton omitidos={singleton}; sem FPC."
    )


def survey_mean(
    frame: pd.DataFrame,
    y: pd.Series,
    year: int,
    quarter: int,
    domain: str,
    geography: str,
    geography_code: str,
    estimand: str = "mean",
    domain_indicator: pd.Series | None = None,
) -> SurveyEstimate:
    valid = frame[["survey_weight", "survey_stratum", "survey_psu"]].copy()
    valid["y"] = pd.to_numeric(y.reindex(frame.index), errors="coerce")
    valid["d"] = (
        1.0
        if domain_indicator is None
        else pd.to_numeric(domain_indicator.reindex(frame.index), errors="coerce").fillna(0.0)
    )
    valid["w"] = pd.to_numeric(valid["survey_weight"], errors="coerce")
    valid = valid.dropna(subset=["w", "survey_stratum", "survey_psu"])
    valid = valid[valid["w"] > 0]
    valid["domain_positive"] = valid["d"].gt(0)
    valid["dy_valid"] = valid["domain_positive"] & valid["y"].notna()

    denom = float((valid.loc[valid["dy_valid"], "w"] * valid.loc[valid["dy_valid"], "d"]).sum())
    estimate = (
        float(
            (
                valid.loc[valid["dy_valid"], "w"]
                * valid.loc[valid["dy_valid"], "d"]
                * valid.loc[valid["dy_valid"], "y"]
            ).sum()
            / denom
        )
        if denom > 0
        else None
    )
    if estimate is None:
        linearized = pd.Series(0.0, index=valid.index)
    else:
        y_centered = (valid["y"] - estimate).fillna(0.0)
        active = valid["dy_valid"].astype(float)
        linearized = valid["w"] * valid["d"] * active * y_centered / denom

    variance, n_psu, n_strata, singleton = _cluster_variance(
        linearized, valid["survey_stratum"], valid["survey_psu"]
    )
    se = math.sqrt(variance) if variance is not None and variance >= 0 else None
    df = max(n_psu - n_strata, 1)
    crit = float(student_t.ppf(0.975, df)) if df > 1 else 1.96
    ci_low = estimate - crit * se if estimate is not None and se is not None else None
    ci_high = estimate + crit * se if estimate is not None and se is not None else None
    cv = abs(se / estimate * 100) if estimate not in (None, 0) and se is not None else None

    domain_weights = valid.loc[valid["dy_valid"], "w"] * valid.loc[valid["dy_valid"], "d"]
    n_eff_domain = kish_effective_n(domain_weights)
    if estimand == "percent" and domain_indicator is None:
        # y está na escala 0–100; conta positivos do indicador, não todo o universo.
        n_positive = int((valid.loc[valid["dy_valid"], "y"] > 0).sum())
    else:
        n_positive = int(valid["domain_positive"].sum())
    n_outcome_valid = int(valid["dy_valid"].sum())

    return SurveyEstimate(
        year=year, quarter=quarter, domain=domain, geography=geography,
        geography_code=geography_code, estimand=estimand, estimate=estimate,
        se=se, ci_low=ci_low, ci_high=ci_high, cv_percent=cv,
        n_unweighted=n_outcome_valid, n_effective=n_eff_domain, n_psu=n_psu,
        n_strata=n_strata, df_design=df,
        precision_status=precision_status(cv, n_eff_domain, n_outcome_valid),
        n_universe=int(len(valid)), n_positive=n_positive,
        n_outcome_valid=n_outcome_valid, n_effective_domain=n_eff_domain,
        notes=f"Taylor linearization por estrato/UPA; estratos singleton omitidos={singleton}; sem FPC.",
    )


def domain_estimates(frame: pd.DataFrame, year: int, quarter: int) -> list[SurveyEstimate]:
    required = {"survey_weight", "survey_stratum", "survey_psu"}
    if not required.issubset(frame.columns):
        return []
    estimates: list[SurveyEstimate] = []
    universe = frame[frame.get("eligible_platform_module", False).fillna(False)].copy()
    if universe.empty:
        return estimates
    domains = {
        "eligible_platform_module": pd.Series(True, index=universe.index),
        "platform_any_direct": universe.get("platform_any_direct", pd.Series(False, index=universe.index)).fillna(False),
        "delivery_app_use_raw": universe.get("delivery_app_use_raw", pd.Series(False, index=universe.index)).fillna(False),
        "platform_delivery_direct": universe.get("platform_delivery_direct", pd.Series(False, index=universe.index)).fillna(False),
        "delivery_app_use_nonplatform": universe.get("delivery_app_use_nonplatform", pd.Series(False, index=universe.index)).fillna(False),
        "platform_courier_direct": universe.get("platform_courier_direct", pd.Series(False, index=universe.index)).fillna(False),
    }
    for name, indicator in domains.items():
        estimates.append(survey_total(universe, indicator.astype(float), year, quarter, name, "Brasil", "BR"))
        if name != "eligible_platform_module":
            estimates.append(survey_mean(universe, indicator.astype(float) * 100, year, quarter, name, "Brasil", "BR", estimand="percent"))
    platform_any_d = domains["platform_any_direct"].astype(float)
    platform_delivery_d = domains["platform_delivery_direct"].astype(float)
    if "weekly_hours_usual" in universe:
        estimates.append(survey_mean(universe, universe["weekly_hours_usual"], year, quarter, "platform_any_direct", "Brasil", "BR", "mean_weekly_hours", platform_any_d))
        estimates.append(survey_mean(universe, universe["weekly_hours_usual"], year, quarter, "platform_delivery_direct", "Brasil", "BR", "mean_weekly_hours", platform_delivery_d))
    if "monthly_income_usual" in universe:
        estimates.append(survey_mean(universe, universe["monthly_income_usual"], year, quarter, "platform_any_direct", "Brasil", "BR", "mean_monthly_income", platform_any_d))
        estimates.append(survey_mean(universe, universe["monthly_income_usual"], year, quarter, "platform_delivery_direct", "Brasil", "BR", "mean_monthly_income", platform_delivery_d))
    if "informal_status" in universe and universe["informal_status"].notna().any():
        informal_numeric = universe["informal_status"].astype("Float64")
        informal_platform = pd.Series(0.0, index=universe.index, dtype="Float64")
        informal_platform.loc[platform_any_d.gt(0)] = informal_numeric.loc[platform_any_d.gt(0)]
        estimates.append(survey_total(universe, informal_platform, year, quarter, "platform_any_informal", "Brasil", "BR"))
        estimates.append(survey_mean(universe, informal_numeric * 100, year, quarter, "platform_any_direct", "Brasil", "BR", "percent_informal", platform_any_d))
    if "social_security_contributor" in universe and universe["social_security_contributor"].notna().any():
        contributor_numeric = universe["social_security_contributor"].astype("Float64")
        contrib_platform = pd.Series(0.0, index=universe.index, dtype="Float64")
        contrib_platform.loc[platform_any_d.gt(0)] = contributor_numeric.loc[platform_any_d.gt(0)]
        estimates.append(survey_total(universe, contrib_platform, year, quarter, "platform_any_social_security", "Brasil", "BR"))
        estimates.append(survey_mean(universe, contributor_numeric * 100, year, quarter, "platform_any_direct", "Brasil", "BR", "percent_social_security", platform_any_d))
    # Diagnósticos territoriais model-based/design-based nos códigos publicamente disponíveis.
    for geo_col, geo_name in (("region_code", "Grande Região"), ("UF", "UF"), ("Capital", "Capital"), ("RM_RIDE", "RM_RIDE")):
        if geo_col not in universe:
            continue
        for geo_code, group in universe.groupby(geo_col, dropna=True):
            if len(group) < 5:
                continue
            indicator = group.get("platform_delivery_direct", pd.Series(False, index=group.index)).fillna(False)
            estimates.append(survey_total(group, indicator.astype(float), year, quarter, "platform_delivery_direct", geo_name, str(geo_code)))
    return estimates


def survey_design_summary(frame: pd.DataFrame, year: int, quarter: int, manifest: Mapping[str, Any]) -> dict[str, Any]:
    if frame.empty or not {"survey_weight", "survey_stratum", "survey_psu"}.issubset(frame.columns):
        return {"year": year, "quarter": quarter, "status": "BLOCKED", "reason": "design variables absent"}
    valid = frame.dropna(subset=["survey_weight", "survey_stratum", "survey_psu"]).copy()
    valid["survey_weight"] = pd.to_numeric(valid["survey_weight"], errors="coerce")
    valid = valid[valid["survey_weight"] > 0]
    psu_by_stratum = valid.groupby("survey_stratum", observed=True)["survey_psu"].nunique()
    return {
        "year": year, "quarter": quarter, "status": "READY",
        "variables": manifest.get("survey_variables"),
        "n_records": int(len(frame)), "n_design_valid": int(len(valid)),
        "weight_sum": float(valid["survey_weight"].sum()),
        "weight_min": float(valid["survey_weight"].min()) if not valid.empty else None,
        "weight_max": float(valid["survey_weight"].max()) if not valid.empty else None,
        "weight_cv_percent": float(valid["survey_weight"].std() / valid["survey_weight"].mean() * 100) if len(valid) > 1 else None,
        "kish_effective_n": kish_effective_n(valid["survey_weight"]),
        "n_strata": int(valid["survey_stratum"].nunique()),
        "n_psu": int(valid["survey_psu"].nunique()),
        "design_df": int(valid["survey_psu"].nunique() - valid["survey_stratum"].nunique()),
        "singleton_strata": int((psu_by_stratum < 2).sum()),
        "psu_per_stratum_min": int(psu_by_stratum.min()) if not psu_by_stratum.empty else None,
        "psu_per_stratum_median": float(psu_by_stratum.median()) if not psu_by_stratum.empty else None,
        "psu_per_stratum_max": int(psu_by_stratum.max()) if not psu_by_stratum.empty else None,
        "estimator_note": "Taylor linearization ultimate-cluster; sem FPC; estratos singleton sinalizados.",
    }


# -----------------------------------------------------------------------------
# SIDRA: descoberta e golden tests
# -----------------------------------------------------------------------------

def scrape_sidra_tables(session: requests.Session, url: str, logger: logging.Logger) -> dict[str, int]:
    mapping: dict[str, int] = {}
    try:
        response = session.get(url, timeout=60)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        for anchor in soup.find_all("a", href=True):
            href = anchor.get("href", "")
            match = re.search(r"sidra\.ibge\.gov\.br/tabela/(\d+)", href)
            if not match:
                continue
            label = normalize_text(anchor.get_text(" ", strip=True))
            mapping[label] = int(match.group(1))
    except Exception as exc:
        logger.warning("Não foi possível raspar tabelas SIDRA de %s: %s", url, exc)
    return mapping


def sidra_metadata(session: requests.Session, table: int) -> dict[str, Any]:
    response = session.get(SIDRA_META.format(table=table), timeout=60)
    response.raise_for_status()
    return response.json()


def extract_classification_ids(meta: Any) -> list[str]:
    ids: list[str] = []
    def walk(obj: Any) -> None:
        if isinstance(obj, dict):
            if "id" in obj and any(k in obj for k in ("categorias", "classificacoes", "nome")):
                name = normalize_text(obj.get("nome", ""))
                if "classific" in name or "categorias" in obj:
                    val = str(obj["id"])
                    if val.isdigit():
                        ids.append(val)
            for value in obj.values():
                walk(value)
        elif isinstance(obj, list):
            for value in obj:
                walk(value)
    walk(meta)
    # Metadados do agregado geralmente têm chave classificacoes.
    if isinstance(meta, dict):
        for item in meta.get("classificacoes", []) or []:
            val = str(item.get("id", ""))
            if val.isdigit():
                ids.append(val)
    return list(dict.fromkeys(ids))


def fetch_sidra_all(session: requests.Session, table: int, logger: logging.Logger) -> pd.DataFrame:
    meta = sidra_metadata(session, table)
    class_ids = extract_classification_ids(meta)
    path = f"{SIDRA_CLASSIC}/t/{table}/n1/all/v/all/p/all"
    for class_id in class_ids:
        path += f"/c{class_id}/all"
    response = session.get(path, timeout=120)
    if response.status_code >= 400 and class_ids:
        logger.warning("Consulta SIDRA com classificações falhou (%s); tentando consulta mínima.", response.status_code)
        response = session.get(f"{SIDRA_CLASSIC}/t/{table}/n1/all/v/all/p/all", timeout=120)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list) or not payload:
        return pd.DataFrame()
    header = payload[0]
    if isinstance(header, dict):
        columns = list(header.keys())
        labels = [str(header[k]) for k in columns]
        rows = [[item.get(k) for k in columns] for item in payload[1:]]
        df = pd.DataFrame(rows, columns=labels)
        # Preserva também os códigos técnicos para auditoria.
        df.attrs["technical_columns"] = columns
    else:
        df = pd.DataFrame(payload)
    df.attrs["table"] = table
    df.attrs["url"] = response.url
    df.attrs["metadata"] = meta
    return df


def sidra_row_text(row: pd.Series) -> str:
    return " | ".join(normalize_text(v) for v in row.tolist() if normalize_text(v))


def find_value_column(df: pd.DataFrame) -> str | None:
    for candidate in ("Valor", "V"):
        if candidate in df.columns:
            return candidate
    for col in df.columns:
        if normalize_text(col) == "valor":
            return col
    return None


SIDRA_TOKEN_ALIASES: dict[str, tuple[str, ...]] = {
    "trabalhou": ("trabalh",),
    "trabalharam": ("trabalh",),
    "plataforma": ("plataform",),
    "entrega": ("entrega",),
    "percentual": ("percent",),
    "hora": ("hora",),
    "reais": ("real",),
    "contribuicao": ("contribu",),
    "contribuiu": ("contribu",),
    "informalidade": ("informal",),
    "mil pessoas": ("mil pessoas", "1 000 pessoas"),
    "total": ("total",),
}

def _token_present(text: str, token: str) -> bool:
    alternatives = SIDRA_TOKEN_ALIASES.get(normalize_text(token), (normalize_text(token),))
    return any(alt in text for alt in alternatives)

def sidra_candidates(
    df: pd.DataFrame,
    year: int,
    includes: Sequence[str],
    excludes: Sequence[str] = (),
    exact_codes: Mapping[str, str] | None = None,
) -> pd.DataFrame:
    if df.empty:
        return df
    texts = df.apply(sidra_row_text, axis=1)
    mask = texts.map(lambda text: str(year) in text)
    for token in includes:
        mask &= texts.map(lambda text, tok=token: _token_present(text, tok))
    for token in excludes:
        mask &= ~texts.map(lambda text, tok=token: _token_present(text, tok))
    out = df.loc[mask].copy()
    if exact_codes:
        for column_fragment, expected in exact_codes.items():
            cols = [c for c in out.columns if normalize_text(column_fragment) in normalize_text(c) and "codigo" in normalize_text(c)]
            if len(cols) == 1:
                out = out[out[cols[0]].astype("string").str.strip().eq(str(expected))]
    return out


def official_value_from_candidates(candidates: pd.DataFrame) -> tuple[float | None, dict[str, Any]]:
    if candidates.empty:
        return None, {"reason": "no_match"}
    value_col = find_value_column(candidates)
    if not value_col:
        return None, {"reason": "value_column_missing", "columns": candidates.columns.tolist()}
    valid = candidates.copy()
    valid["__parsed_value"] = valid[value_col].map(safe_float)
    valid = valid.loc[valid["__parsed_value"].notna()].copy()
    if valid.empty:
        return None, {"reason": "no_numeric_value"}

    # APIs do SIDRA podem devolver duplicatas de rótulo/código. Se todas as
    # linhas restantes representam o mesmo valor, selecionamos de modo auditável.
    unique_values = sorted(valid["__parsed_value"].astype(float).unique().tolist())
    if len(unique_values) != 1:
        return None, {
            "reason": "ambiguous",
            "n_matches": len(valid),
            "unique_values": unique_values,
            "matches": valid.head(30).astype(str).to_dict(orient="records"),
        }
    value = float(unique_values[0])
    row = valid.iloc[0]
    row_text = sidra_row_text(row)
    multiplier = 1000.0 if "mil pessoas" in row_text or "1 000 pessoas" in row_text else 1.0
    return value * multiplier, {
        "row": row.drop(labels=["__parsed_value"]).astype(str).to_dict(),
        "multiplier": multiplier,
        "n_equivalent_matches": len(valid),
    }


def estimate_lookup(estimates: Sequence[SurveyEstimate], year: int, domain: str, estimand: str) -> SurveyEstimate | None:
    return next((x for x in estimates if x.year == year and x.domain == domain and x.estimand == estimand and x.geography == "Brasil"), None)


def _resolve_sidra_column(df: pd.DataFrame, requested: str) -> str | None:
    """Resolve coluna por rótulo normalizado, sem usar valores observados."""
    requested_norm = normalize_text(requested)
    exact = [col for col in df.columns if normalize_text(col) == requested_norm]
    if len(exact) == 1:
        return exact[0]
    # Defesa para pequenas mudanças de espaços/quebras no cabeçalho SIDRA.
    requested_tokens = [tok for tok in re.split(r"[^a-z0-9]+", requested_norm) if tok]
    candidates = []
    for col in df.columns:
        col_norm = normalize_text(col)
        if all(tok in col_norm for tok in requested_tokens):
            candidates.append(col)
    return candidates[0] if len(candidates) == 1 else None


def select_sidra_contract_value(
    df: pd.DataFrame,
    year: int,
    contract_name: str,
    contract: Mapping[str, Any],
) -> tuple[float | None, dict[str, Any]]:
    """Seleciona uma única linha SIDRA por contrato oficial de códigos."""
    if df.empty:
        return None, {"reason": "empty_table", "contract": contract_name}
    mask = pd.Series(True, index=df.index, dtype=bool)
    resolved_columns: dict[str, str] = {}
    selectors = dict(contract.get("selectors", {}))
    selectors["Ano (Código)"] = str(year)
    for requested_column, expected_code in selectors.items():
        column = _resolve_sidra_column(df, requested_column)
        if column is None:
            return None, {
                "reason": "selector_column_missing_or_ambiguous",
                "requested_column": requested_column,
                "columns": list(df.columns),
                "contract": contract_name,
            }
        resolved_columns[requested_column] = column
        normalized = df[column].map(normalize_code)
        mask &= normalized.eq(str(expected_code))

    candidates = df.loc[mask].copy()
    if candidates.empty:
        return None, {
            "reason": "no_match",
            "contract": contract_name,
            "selectors": selectors,
            "resolved_columns": resolved_columns,
        }
    value_col = find_value_column(candidates)
    if value_col is None:
        return None, {
            "reason": "value_column_missing",
            "contract": contract_name,
            "columns": list(candidates.columns),
        }
    candidates["__parsed_value"] = candidates[value_col].map(safe_float)
    valid = candidates.loc[candidates["__parsed_value"].notna()].copy()
    if valid.empty:
        return None, {
            "reason": "no_numeric_value",
            "contract": contract_name,
            "matches": candidates.astype(str).to_dict(orient="records"),
        }
    unique_values = sorted(valid["__parsed_value"].astype(float).unique().tolist())
    if len(unique_values) != 1:
        return None, {
            "reason": "ambiguous",
            "contract": contract_name,
            "n_matches": len(valid),
            "unique_values": unique_values,
            "matches": valid.head(30).astype(str).to_dict(orient="records"),
        }
    multiplier = float(contract.get("multiplier", 1.0))
    official = float(unique_values[0]) * multiplier
    row = valid.iloc[0].drop(labels=["__parsed_value"]).astype(str).to_dict()
    return official, {
        "contract": contract_name,
        "selectors": selectors,
        "resolved_columns": resolved_columns,
        "row": row,
        "multiplier": multiplier,
        "n_equivalent_matches": int(len(valid)),
    }


def compare_golden(
    test_id: str,
    year: int,
    observed: float | None,
    official: float | None,
    tolerance: float,
    evidence: dict[str, Any],
    *,
    tolerance_kind: str = "relative",
    severity: str = "critical",
) -> TestResult:
    if observed is None:
        return TestResult(
            test_id, "FAIL", severity, year,
            "Estimativa do microdado ausente.",
            observed=observed, expected=official, tolerance=tolerance,
            evidence=evidence,
        )
    if official is None:
        return TestResult(
            test_id, "BLOCKED", severity, year,
            "Valor oficial SIDRA não foi selecionado de forma inequívoca.",
            observed=observed, expected=official, tolerance=tolerance,
            evidence=evidence,
        )
    if tolerance_kind == "absolute":
        difference = abs(float(observed) - float(official))
        status = "PASS" if difference <= tolerance else "FAIL"
        message = f"Diferença absoluta microdado × SIDRA = {difference:.6g}."
        metric_evidence = {"absolute_difference": difference, "tolerance_kind": "absolute"}
    else:
        difference = abs(float(observed) - float(official)) / max(abs(float(official)), 1e-12)
        status = "PASS" if difference <= tolerance else "FAIL"
        message = f"Diferença relativa microdado × SIDRA = {difference:.4%}."
        metric_evidence = {"relative_difference": difference, "tolerance_kind": "relative"}
    return TestResult(
        test_id, status, severity, year, message,
        observed=observed, expected=official, tolerance=tolerance,
        evidence={**evidence, **metric_evidence},
    )


def resolve_sidra_tables(scraped: Mapping[str, Mapping[str, int]]) -> dict[str, int]:
    tables = dict(SIDRA_FALLBACK_TABLES)
    labels: dict[str, int] = {}
    for mapping in scraped.values():
        labels.update(mapping)
    rules = {
        "social_security": ("contribuicao para instituto de previdencia", "plataforma digital de servico"),
        "dependency": ("tipo de dependencia", "plataforma"),
        "journey_influence": ("tipo de influencia", "jornada", "plataforma"),
    }
    for key, terms in rules.items():
        matches = {table for label, table in labels.items() if all(term in label for term in terms)}
        if len(matches) == 1:
            tables[key] = next(iter(matches))
    return tables


def fetch_sidra_with_cache(
    session: requests.Session,
    table: int,
    key: str,
    output_dir: Path,
    logger: logging.Logger,
    *,
    prefer_cache: bool = False,
    live_only: bool = False,
    cache_only: bool = False,
) -> tuple[pd.DataFrame, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / f"sidra_{table}_{key}.csv"
    cache_candidates = sorted(
        output_dir.glob(f"sidra_{table}_*.csv"),
        key=lambda x: x.stat().st_mtime,
        reverse=True,
    )
    if (prefer_cache or cache_only) and cache_candidates and not live_only:
        logger.info("Usando snapshot SIDRA determinístico: %s", cache_candidates[0])
        return pd.read_csv(cache_candidates[0], dtype=str), "cache"
    if cache_only:
        raise RuntimeError(f"Snapshot SIDRA {table} ausente em {output_dir}")
    try:
        df = fetch_sidra_all(session, table, logger)
        df.to_csv(target, index=False, encoding="utf-8-sig")
        atomic_json(
            output_dir / f"sidra_{table}_{key}_metadata.json",
            df.attrs.get("metadata", {}),
        )
        return df, "live"
    except Exception as live_exc:
        if cache_candidates and not live_only:
            logger.warning("SIDRA %s indisponível; usando snapshot %s", table, cache_candidates[0])
            return pd.read_csv(cache_candidates[0], dtype=str), "cache"
        raise RuntimeError(
            f"SIDRA ao vivo falhou e não há snapshot local: {live_exc}"
        ) from live_exc


def run_sidra_golden_tests(
    session: requests.Session,
    estimates: Sequence[SurveyEstimate],
    output_dir: Path,
    logger: logging.Logger,
    skip: bool,
    scraped: Mapping[str, Mapping[str, int]] | None = None,
    *,
    prefer_cache: bool = False,
    live_only: bool = False,
    cache_only: bool = False,
) -> list[TestResult]:
    results: list[TestResult] = []
    if skip:
        return [TestResult(
            "sidra.validation", "SKIPPED", "critical", None,
            "Validação SIDRA desativada por argumento.",
        )]

    output_dir.mkdir(parents=True, exist_ok=True)
    tables = resolve_sidra_tables(scraped or {})
    # O contrato é persistido para auditoria e reprodução futura.
    atomic_json(
        output_dir / "sidra_code_contracts_v1.2.0.json",
        {
            "validation_schema_version": VALIDATION_SCHEMA_VERSION,
            "contracts": SIDRA_CODE_CONTRACTS,
            "created_at_utc": utc_now(),
        },
    )

    fetched: dict[str, tuple[pd.DataFrame, int, str]] = {}
    for contract_name, contract in SIDRA_CODE_CONTRACTS.items():
        key = str(contract["table_key"])
        table = int(tables.get(key, contract["table"]))
        if key not in fetched:
            try:
                df, source = fetch_sidra_with_cache(
                    session, table, key, output_dir, logger,
                    prefer_cache=prefer_cache, live_only=live_only,
                    cache_only=cache_only,
                )
                fetched[key] = (df, table, source)
                results.append(TestResult(
                    f"sidra.table_{table}.download",
                    "PASS" if source == "live" else "WARN",
                    "high", None,
                    f"Tabela SIDRA {table} materializada ({source}).",
                    evidence={
                        "rows": len(df),
                        "source": source,
                        "path": str(next(iter(sorted(output_dir.glob(f'sidra_{table}_*.csv'))), output_dir / f'sidra_{table}_{key}.csv')),
                    },
                ))
            except Exception as exc:
                severity = str(contract.get("severity", "critical"))
                results.append(TestResult(
                    f"sidra.table_{table}.download",
                    "BLOCKED", severity, None,
                    f"Falha ao obter tabela SIDRA {table} ({key}).",
                    evidence={"error": str(exc), "key": key},
                ))
                fetched[key] = (pd.DataFrame(), table, "missing")

        df, table, source = fetched[key]
        for year in (2022, 2024):
            official, evidence = select_sidra_contract_value(
                df, year, contract_name, contract
            )
            obs = estimate_lookup(
                estimates, year,
                str(contract["domain"]),
                str(contract["estimand"]),
            )
            results.append(compare_golden(
                f"golden.{year}.{contract_name}",
                year,
                obs.estimate if obs else None,
                official,
                float(contract["tolerance"]),
                {
                    "table": table,
                    "table_key": key,
                    "source": source,
                    **evidence,
                },
                tolerance_kind=str(contract.get("tolerance_kind", "relative")),
                severity=str(contract.get("severity", "critical")),
            ))

    # Informalidade permanece um outcome secundário até a tabela 9518 ser
    # materializada e seu contrato de códigos ser congelado.
    informality_table = int(tables.get("informality", 9518))
    try:
        df, source = fetch_sidra_with_cache(
            session, informality_table, "informality", output_dir, logger,
            prefer_cache=prefer_cache, live_only=live_only,
            cache_only=cache_only,
        )
        results.append(TestResult(
            f"sidra.table_{informality_table}.download",
            "PASS" if source == "live" else "WARN",
            "high", None,
            f"Tabela SIDRA {informality_table} materializada ({source}).",
            evidence={"rows": len(df), "source": source},
        ))
        for year in (2022, 2024):
            candidates = sidra_candidates(
                df, year,
                includes=("plataforma", "mil pessoas", "informal"),
                excludes=("nao trabalhou", "percentual"),
            )
            official, evidence = official_value_from_candidates(candidates)
            obs = estimate_lookup(estimates, year, "platform_any_informal", "total")
            results.append(compare_golden(
                f"golden.{year}.platform_any_informal_total",
                year,
                obs.estimate if obs else None,
                official,
                0.03,
                {"table": informality_table, "source": source, **evidence},
                tolerance_kind="relative",
                severity="high",
            ))
    except Exception as exc:
        results.append(TestResult(
            f"sidra.table_{informality_table}.download",
            "BLOCKED", "high", None,
            f"Falha ao obter tabela SIDRA {informality_table} (informality).",
            evidence={"error": str(exc), "key": "informality"},
        ))

    return results


# -----------------------------------------------------------------------------
# Pooling, contratos e relatórios
# -----------------------------------------------------------------------------

def build_pooled(paths: Sequence[Path], output: Path) -> dict[str, Any]:
    tables = [pq.read_table(path) for path in paths]
    all_names: list[str] = []
    seen: set[str] = set()
    for table in tables:
        for name in table.column_names:
            if name not in seen:
                seen.add(name)
                all_names.append(name)
    aligned: list[pa.Table] = []
    for table in tables:
        arrays = []
        for name in all_names:
            if name in table.column_names:
                arrays.append(table[name])
            else:
                arrays.append(pa.nulls(table.num_rows))
        aligned.append(pa.Table.from_arrays(arrays, names=all_names))
    unified = pa.concat_tables(aligned, promote_options="permissive")
    metadata = dict(unified.schema.metadata or {})
    metadata.update({
        b"spine_schema_version": SCHEMA_VERSION.encode(),
        b"pooling_rule": b"repeated_cross_sections; year and quarter preserved; not panel; not causal DiD",
        b"synthetic_location": b"false",
    })
    unified = unified.replace_schema_metadata(metadata)
    output.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(unified, output, compression="zstd", use_dictionary=True)
    common = set(tables[0].column_names)
    for table in tables[1:]:
        common &= set(table.column_names)
    return {
        "path": str(output), "sha256": sha256_file(output), "rows": unified.num_rows,
        "columns": unified.column_names, "common_columns": sorted(common),
        "pooling_rule": "repeated_cross_sections; 2022T4 and 2024T3 preserved; not a panel",
    }


def layout_json(candidate: LayoutCandidate, selection: SourceSelection) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "selection": asdict(selection),
        "layout": {
            "path": candidate.path, "parser": candidate.parser, "width": candidate.width,
            "sha256": candidate.sha256, "has_S140093": candidate.has_s140093,
            "n_fields": len(candidate.fields),
            "fields": [asdict(field) for field in candidate.fields],
            "warnings": candidate.warnings,
        },
    }


def certification_contract() -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "SPINE-GPE v7 PNADc certified table contract",
        "version": SCHEMA_VERSION,
        "required_columns": [
            "record_id", "source_year", "reference_quarter", "measurement_status",
            "identification_method", "SD14001", "S140093", "eligible_platform_module",
            "platform_any_direct", "delivery_app_use_raw", "platform_delivery_direct",
            "delivery_app_use_nonplatform", "survey_weight",
            "survey_stratum", "survey_psu", "synthetic_location",
        ],
        "semantic_rules": {
            "delivery_app_use_raw": "S140093 equals official Yes code; observed raw use of delivery app",
            "platform_delivery_direct": "SD14001=1 AND S140093=1; official platform-delivery classification",
            "delivery_app_use_nonplatform": "S140093=1 AND SD14001!=1; observed app use not classified as platform work",
            "platform_any_direct": "SD14001 equals official Yes code",
            "eligible_platform_module": "SD14001 is nonmissing; questionnaire-universe indicator",
            "synthetic_location": "always false in certified PNADc tables",
            "pooling": "repeated cross-sections with source_year/reference_quarter; not panel",
        },
        "forbidden": [
            "fill S140093 from occupation/activity proxy",
            "replace missing S140093 with zero outside questionnaire universe",
            "invent municipality/hexagon/person location",
            "drop nonmissing universe records solely because income/hours are missing",
            "interpret 2022T4–2024T3 difference as causal DiD",
        ],
    }


def summarize_tests(tests: Sequence[TestResult]) -> dict[str, Any]:
    counts = Counter(test.status for test in tests)
    blocking_statuses = {"FAIL", "BLOCKED", "SKIPPED"}
    critical_failures = [
        test for test in tests
        if test.severity == "critical" and test.status in blocking_statuses
    ]
    secondary_failures = [
        test for test in tests
        if test.severity != "critical" and test.status in {"FAIL", "BLOCKED"}
    ]
    warnings = [test for test in tests if test.status in {"WARN", "REVIEW"}]
    if critical_failures:
        status = "BLOCKED"
    elif secondary_failures:
        status = "CORE_CERTIFIED"
    else:
        status = "CERTIFIED"
    return {
        "counts": dict(counts),
        "critical_failures": [asdict(x) for x in critical_failures],
        "secondary_failures": [asdict(x) for x in secondary_failures],
        "warnings": [asdict(x) for x in warnings],
        "status": status,
    }


def render_report(
    rid: str,
    root: Path,
    selections: Mapping[int, SourceSelection],
    manifests: Mapping[int, Mapping[str, Any]],
    designs: Mapping[int, Mapping[str, Any]],
    estimates: Sequence[SurveyEstimate],
    tests: Sequence[TestResult],
    pooled: Mapping[str, Any] | None,
    execution_mode: str = "certify",
) -> str:
    summary = summarize_tests(tests)
    lines = [
        "# SPINE-GPE v7 — Relatório de Certificação PNADc Plataformas",
        "",
        f"- Run ID: `{rid}`",
        f"- Versão: `{VERSION}`",
        f"- Data schema: `{SCHEMA_VERSION}`",
        f"- Validation schema: `{VALIDATION_SCHEMA_VERSION}`",
        f"- Modo: `{execution_mode}`",
        f"- Status: **{summary['status']}**",
        f"- Raiz: `{root}`",
        f"- Gerado em UTC: `{utc_now()}`",
        "",
        "## Princípio de identificação",
        "",
        "`S140093` mede o uso observado de aplicativo de entrega. A classificação oficial de entrega plataformizada exige `SD14001=1` e `S140093=1`. Nenhuma proxy preenche essas variáveis.",
        "",
        "## Fontes e layouts selecionados",
        "",
        "| Ano | Trimestre | TXT | Largura | Layout | Parser | SHA-256 TXT |",
        "|---:|---:|---|---:|---|---|---|",
    ]
    for year, selection in selections.items():
        lines.append(
            f"| {year} | {selection.quarter} | `{selection.txt_path}` | {selection.record_width} | "
            f"`{selection.layout_path}` | {selection.layout_parser} | `{selection.txt_sha256[:16]}…` |"
        )
    lines.extend(["", "## Tabelas candidatas à certificação", "", "| Ano | Registros | Elegíveis módulo | Plataforma geral | Uso bruto app entrega | Entrega plataformizada | Entregadores compatíveis | Output |", "|---:|---:|---:|---:|---:|---:|---:|---|"])
    for year, manifest in manifests.items():
        d = manifest.get("diagnostics", {})
        lines.append(
            f"| {year} | {manifest.get('rows')} | {d.get('eligible')} | {d.get('platform_any')} | "
            f"{d.get('delivery_app_use_raw')} | {d.get('platform_delivery')} | {d.get('platform_courier')} | `{manifest.get('output_path')}` |"
        )
    if pooled:
        lines.extend(["", f"Pooled repeated cross-section: `{pooled.get('path')}` ({pooled.get('rows')} registros).", ""])
    lines.extend(["## Desenho amostral", ""])
    for year, design in designs.items():
        lines.append(f"### {year}")
        lines.append("")
        lines.append("```json")
        lines.append(json.dumps(design, ensure_ascii=False, indent=2))
        lines.append("```")
        lines.append("")
    lines.extend(["## Golden tests e gates", "", "| Status | Severidade | Ano | Teste | Mensagem |", "|---|---|---:|---|---|"])
    for test in tests:
        lines.append(f"| {test.status} | {test.severity} | {test.year or ''} | `{test.test_id}` | {test.message.replace('|', '/')} |")
    lines.extend(["", "## Estimativas design-based", "", "| Ano | Domínio | Geografia | Estimando | Estimativa | SE | CV% | n universo | n positivo | n outcome | n efetivo domínio | Precisão |", "|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|"])
    for est in estimates:
        lines.append(
            f"| {est.year} | {est.domain} | {est.geography}:{est.geography_code} | {est.estimand} | "
            f"{'' if est.estimate is None else f'{est.estimate:.4f}'} | {'' if est.se is None else f'{est.se:.4f}'} | "
            f"{'' if est.cv_percent is None else f'{est.cv_percent:.2f}'} | {est.n_universe} | "
            f"{est.n_positive} | {est.n_outcome_valid} | "
            f"{'' if est.n_effective_domain is None else f'{est.n_effective_domain:.1f}'} | {est.precision_status} |"
        )
    lines.extend([
        "", "## Limites congelados", "",
        "1. S140093 isolada mede uso bruto; entrega plataformizada oficial é SD14001=1 e S140093=1.",
        "2. 2022 refere-se ao 4º trimestre e 2024 ao 3º trimestre; a comparação pode conter sazonalidade.",
        "3. O pooled é uma repeated cross-section, não painel individual.",
        "4. A localização disponível na PNADc não é convertida em residência fina ou hexágono.",
        "5. Registros fora do universo do módulo não são recodificados como não plataformizados.",
        "6. Renda e horas faltantes não removem pessoas da tabela populacional certificada.",
        "7. Os Parquets são candidatos até PNADC_CERTIFICATION_LOCK.json registrar CERTIFIED.",
        "8. Testes SIDRA bloqueados ou ambíguos devem ser resolvidos antes da Fase 2.",
        "", "## Próximo gate", "",
        "Com as tabelas certificadas, construir o Índice de Controle Algorítmico e os comparadores internos, preservando desenho survey, universo e suporte comum.",
    ])
    return "\n".join(lines) + "\n"


# -----------------------------------------------------------------------------
# Modo SIDRA-only: certificação dos artefatos já processados
# -----------------------------------------------------------------------------

def _latest_file(paths: Iterable[Path]) -> Path | None:
    candidates = [path for path in paths if path.exists()]
    return max(candidates, key=lambda path: path.stat().st_mtime) if candidates else None


def _test_from_mapping(payload: Mapping[str, Any]) -> TestResult:
    fields = {field.name for field in dataclasses.fields(TestResult)}
    return TestResult(**{key: payload.get(key) for key in fields})


def _load_sidra_only_state(
    tree: Mapping[str, Path],
    logger: logging.Logger,
) -> tuple[
    dict[int, SourceSelection],
    dict[int, dict[str, Any]],
    dict[int, dict[str, Any]],
    dict[str, Any] | None,
    list[TestResult],
]:
    selections: dict[int, SourceSelection] = {}
    manifests: dict[int, dict[str, Any]] = {}
    designs: dict[int, dict[str, Any]] = {}
    for year in (2022, 2024):
        layout_path = tree["registry"] / f"pnadc_layout_{year}.json"
        if layout_path.exists():
            payload = json_load(layout_path)
            selection_payload = payload.get("selection", {})
            try:
                selections[year] = SourceSelection(**selection_payload)
            except TypeError:
                logger.warning("Seleção %s não pôde ser reconstruída de %s", year, layout_path)
        manifest_path = tree["registry"] / f"certified_pnadc_platform_{year}_manifest.json"
        if manifest_path.exists():
            manifests[year] = json_load(manifest_path)
        design_path = tree["registry"] / f"survey_design_{year}.json"
        if design_path.exists():
            designs[year] = json_load(design_path)

    pooled_path = tree["registry"] / "certified_pnadc_platform_pooled_manifest.json"
    pooled = json_load(pooled_path) if pooled_path.exists() else None

    previous_tests_path = _latest_file(
        tree["registry"].glob("pnadc_certification_tests_*.json")
    )
    base_tests: list[TestResult] = []
    if previous_tests_path is not None:
        for item in json_load(previous_tests_path):
            test_id = str(item.get("test_id", ""))
            # Golden tests/SIDRA são recalculados; controles SIDRA-only também.
            if test_id.startswith(("sidra.", "golden.", "sidra_only.")):
                continue
            if test_id == "phase0.lock.released":
                continue
            base_tests.append(_test_from_mapping(item))
        logger.info("Testes estruturais reutilizados de %s", previous_tests_path)
    return selections, manifests, designs, pooled, base_tests


def _recompute_sidra_only_estimates(
    tree: Mapping[str, Path],
    manifests: Mapping[int, Mapping[str, Any]],
    tests: list[TestResult],
    logger: logging.Logger,
) -> list[SurveyEstimate]:
    estimates: list[SurveyEstimate] = []
    columns_wanted = {
        "eligible_platform_module",
        "platform_any_direct",
        "delivery_app_use_raw",
        "platform_delivery_direct",
        "delivery_app_use_nonplatform",
        "platform_courier_direct",
        "survey_weight",
        "survey_stratum",
        "survey_psu",
        "weekly_hours_usual",
        "monthly_income_usual",
        "informal_status",
        "social_security_contributor",
    }
    for year, quarter in ((2022, 4), (2024, 3)):
        manifest = manifests.get(year, {})
        configured = manifest.get("output_path")
        parquet_path = Path(configured) if configured else (
            tree["processed"] / f"certified_pnadc_platform_{year}.parquet"
        )
        if not parquet_path.exists():
            tests.append(TestResult(
                f"sidra_only.{year}.parquet_available", "FAIL", "critical", year,
                "Parquet certificado candidato não encontrado.",
                evidence={"path": str(parquet_path)},
            ))
            continue
        expected_hash = str(manifest.get("output_sha256", ""))
        actual_hash = sha256_file(parquet_path)
        hash_ok = bool(expected_hash) and actual_hash == expected_hash
        tests.append(TestResult(
            f"sidra_only.{year}.parquet_integrity",
            "PASS" if hash_ok else "FAIL", "critical", year,
            "Hash do Parquet coincide com o manifest." if hash_ok else "Hash do Parquet diverge ou está ausente no manifest.",
            observed=actual_hash, expected=expected_hash or "manifest output_sha256",
            evidence={"path": str(parquet_path)},
        ))
        available = set(pq.ParquetFile(parquet_path).schema.names)
        selected = sorted(columns_wanted & available)
        required = {"eligible_platform_module", "platform_any_direct", "survey_weight", "survey_stratum", "survey_psu"}
        missing = sorted(required - set(selected))
        if missing:
            tests.append(TestResult(
                f"sidra_only.{year}.required_columns", "FAIL", "critical", year,
                "Colunas essenciais ausentes no Parquet.", observed=missing,
                expected=sorted(required), evidence={"available": sorted(available)},
            ))
            continue
        logger.info("Recalculando estimativas nacionais %s a partir de %s colunas", year, len(selected))
        frame = pd.read_parquet(parquet_path, columns=selected)
        year_estimates = domain_estimates(frame, year, quarter)
        estimates.extend(
            estimate for estimate in year_estimates
            if estimate.geography == "Brasil"
        )
        tests.append(TestResult(
            f"sidra_only.{year}.estimates_recomputed",
            "PASS" if year_estimates else "FAIL", "critical", year,
            "Estimativas nacionais recalculadas diretamente do Parquet candidato.",
            observed=len(year_estimates), expected=">0",
            evidence={"path": str(parquet_path), "columns": selected},
        ))
    return estimates


def run_sidra_only(
    args: argparse.Namespace,
    root: Path,
    tree: Mapping[str, Path],
    rid: str,
    logger: logging.Logger,
) -> int:
    if args.skip_sidra:
        raise ValueError("--mode sidra-only é incompatível com --skip-sidra.")
    tests: list[TestResult] = []
    require_phase0_release(root, tests)
    atomic_json(
        tree["contracts"] / "sidra_code_contracts_v1.2.0.json",
        {
            "validation_schema_version": VALIDATION_SCHEMA_VERSION,
            "contracts": SIDRA_CODE_CONTRACTS,
            "created_at_utc": utc_now(),
        },
    )
    selections, manifests, designs, pooled, base_tests = _load_sidra_only_state(tree, logger)
    tests.extend(base_tests)

    prerequisites_ok = all(year in manifests for year in (2022, 2024))
    tests.append(TestResult(
        "sidra_only.prerequisites",
        "PASS" if prerequisites_ok else "FAIL", "critical", None,
        "Manifests 2022/2024 disponíveis para validação sem microdados brutos."
        if prerequisites_ok else "Manifests 2022/2024 ausentes.",
        observed=sorted(manifests), expected=[2022, 2024],
    ))
    estimates = _recompute_sidra_only_estimates(tree, manifests, tests, logger)
    estimates_path = tree["outputs"] / f"pnadc_sidra_only_estimates_{rid}.csv"
    if estimates:
        pd.DataFrame([asdict(item) for item in estimates]).to_csv(
            estimates_path, index=False, encoding="utf-8-sig"
        )

    prefer_cache = args.sidra_source in {"auto", "cache"}
    live_only = args.sidra_source == "live"
    sidra_tests = run_sidra_golden_tests(
        http_session(), estimates,
        tree["interim"] / "sidra_snapshots",
        logger, False, {},
        prefer_cache=prefer_cache,
        live_only=live_only,
        cache_only=args.sidra_source == "cache",
    )
    tests.extend(sidra_tests)
    atomic_json(
        tree["registry"] / f"pnadc_certification_tests_{rid}.json",
        [asdict(test) for test in tests],
    )
    for year in (2022, 2024):
        atomic_json(
            tree["registry"] / f"golden_tests_{year}.json",
            [asdict(test) for test in tests if test.year in {None, year}],
        )

    report = render_report(
        rid, root, selections, manifests, designs, estimates, tests, pooled,
        execution_mode="sidra-only",
    )
    report_path = tree["reports_final"] / f"pnadc_certification_report_{rid}.md"
    atomic_text(report_path, report)
    atomic_text(tree["reports_final"] / "pnadc_certification_report_LATEST.md", report)
    summary = summarize_tests(tests)
    lock = {
        "run_id": rid,
        "script_version": VERSION,
        "data_schema_version": SCHEMA_VERSION,
        "validation_schema_version": VALIDATION_SCHEMA_VERSION,
        "mode": "sidra-only",
        "status": summary["status"],
        "critical_failures": summary["critical_failures"],
        "secondary_failures": summary["secondary_failures"],
        "warnings": summary["warnings"],
        "report": str(report_path),
        "estimates": str(estimates_path) if estimates else None,
        "outputs": {str(year): manifest.get("output_path") for year, manifest in manifests.items()},
        "pooled": pooled.get("path") if pooled else None,
        "created_at_utc": utc_now(),
    }
    atomic_json(tree["admin"] / "PNADC_CERTIFICATION_LOCK.json", lock)
    logger.info("SIDRA-only concluído | status=%s | relatório=%s", summary["status"], report_path)
    for failure in summary["critical_failures"]:
        logger.error("GATE CRÍTICO: %s — %s", failure["test_id"], failure["message"])
    return 0 if not args.strict or summary["status"] in {"CERTIFIED", "CORE_CERTIFIED"} else 2


# -----------------------------------------------------------------------------
# Pipeline principal
# -----------------------------------------------------------------------------

def run_pipeline(args: argparse.Namespace) -> int:
    root = autodetect_root(args.root)
    tree = make_tree(root)
    rid = run_id()
    logger = setup_logging(tree["logs"] / f"PNADC_CERTIFIER_{rid}.log", args.verbose)
    logger.info("SPINE-GPE PNADc Certifier v%s | root=%s | mode=%s", VERSION, root, args.mode)
    if args.mode == "sidra-only":
        return run_sidra_only(args, root, tree, rid, logger)
    tests: list[TestResult] = []
    require_phase0_release(root, tests)

    extracted_docs = extract_archives_for_docs(tree["raw_ibge"], tree["interim"] / "extracted_docs", logger)
    layout_roots = [tree["raw_ibge"], tree["interim"], root / "data_pnadc"]
    layouts = discover_layout_candidates(layout_roots, logger)
    atomic_json(tree["registry"] / f"pnadc_layout_candidates_{rid}.json", [
        {
            "path": c.path, "parser": c.parser, "width": c.width,
            "has_S140093": c.has_s140093, "n_fields": len(c.fields),
            "sha256": c.sha256, "year_score": c.year_score,
        } for c in layouts
    ])
    tests.append(TestResult(
        "layout.candidates.available", "PASS" if layouts else "FAIL", "critical", None,
        f"{len(layouts)} layouts interpretáveis encontrados.", observed=len(layouts), expected=">=1"
    ))

    selections: dict[int, SourceSelection] = {}
    chosen: dict[int, tuple[Path, LayoutCandidate]] = {}
    periods = {2022: 4, 2024: 3}
    for year, quarter in periods.items():
        try:
            txt, layout, audit, kind = choose_microdata(root, year, quarter, layouts, logger)
            selection = SourceSelection(
                year=year, quarter=quarter, txt_path=str(txt), txt_sha256=sha256_file(txt),
                record_width=int(audit["mode"]), layout_path=layout.path,
                layout_sha256=layout.sha256 or "", layout_width=layout.width,
                layout_parser=layout.parser, source_kind=kind,
            )
            selections[year] = selection
            chosen[year] = (txt, layout)
            tests.extend([
                TestResult(f"{year}.microdata.exists", "PASS", "critical", year, "Microdado direto encontrado.", evidence={"path": str(txt), "kind": kind}),
                TestResult(f"{year}.record_width.matches_layout", "PASS" if selection.record_width == selection.layout_width else "FAIL", "critical", year,
                           "Largura TXT coincide com layout." if selection.record_width == selection.layout_width else "Largura TXT diverge do layout.",
                           observed=selection.record_width, expected=selection.layout_width),
                TestResult(f"{year}.layout.S140093", "PASS" if layout.has_s140093 else "FAIL", "critical", year,
                           "S140093 existe no layout." if layout.has_s140093 else "S140093 ausente do layout."),
            ])
            atomic_json(tree["registry"] / f"pnadc_layout_{year}.json", layout_json(layout, selection))
        except Exception as exc:
            logger.exception("Falha na seleção %s", year)
            tests.append(TestResult(f"{year}.source_selection", "FAIL", "critical", year, str(exc)))

    if args.mode == "audit":
        summary = summarize_tests(tests)
        audit_status = "AUDIT_PASSED" if not summary["critical_failures"] else "AUDIT_BLOCKED"
        lock = {
            "run_id": rid,
            "script_version": VERSION,
            "data_schema_version": SCHEMA_VERSION,
            "validation_schema_version": VALIDATION_SCHEMA_VERSION,
            "status": audit_status,
            "mode": "audit",
            "tests": [asdict(t) for t in tests],
            "created_at_utc": utc_now(),
        }
        # Auditoria estrutural não promove nem sobrescreve o lock empírico.
        atomic_json(tree["admin"] / "PNADC_AUDIT_LOCK.json", lock)
        report = render_report(
            rid, root, selections, {}, {}, [], tests, None,
            execution_mode="audit",
        )
        report_path = tree["reports"] / f"PNADC_AUDIT_REPORT_{rid}.md"
        atomic_text(report_path, report)
        logger.info("Auditoria concluída: %s | status=%s", report_path, audit_status)
        return 0 if not args.strict or audit_status == "AUDIT_PASSED" else 2

    # Dicionário de categorias a partir de todos os documentos disponíveis.
    target_vars = {f.variable.upper() for _, layout in chosen.values() for f in layout.fields if f.variable.upper().startswith(("S14", "SD14")) or f.variable.upper() in {"VD4008", "VD4009", "VD4012", "V4019", "V2007", "V2010", "VD3004", "V4010", "V4012", "V40121", "V4013"}}
    doc_paths: list[Path] = []
    for base in layout_roots:
        if base.exists():
            doc_paths.extend(p for p in base.rglob("*") if p.is_file() and p.suffix.lower() in {".xls", ".xlsx", ".txt", ".sas", ".csv"})
    doc_paths.extend(extracted_docs)
    categories = build_category_dictionary(sorted(set(doc_paths)), target_vars)
    atomic_json(tree["registry"] / f"pnadc_category_dictionary_{rid}.json", categories)

    cache_dir = Path(args.cache_dir) if args.cache_dir else (Path("/content/spine_pnadc_cache") if is_colab() and args.local_cache else None)
    manifests: dict[int, dict[str, Any]] = {}
    summaries: dict[int, pd.DataFrame] = {}
    processed_paths: list[Path] = []
    for year, quarter in periods.items():
        if year not in chosen:
            continue
        txt, layout = chosen[year]
        output = tree["processed"] / f"certified_pnadc_platform_{year}.parquet"
        try:
            manifest, summary_frame = certify_year(
                year, quarter, txt, layout, categories, output,
                args.chunk_rows, cache_dir, logger, tests,
            )
            manifests[year] = manifest
            summaries[year] = summary_frame
            processed_paths.append(output)
            atomic_json(tree["registry"] / f"certified_pnadc_platform_{year}_manifest.json", manifest)
            tests.append(TestResult(f"{year}.certified_table.created", "PASS", "critical", year, "Certified table criada.", evidence={"path": str(output), "rows": manifest["rows"]}))
            # Domínio observado sem preenchimento de proxy.
            tests.append(TestResult(f"{year}.no_proxy_fill", "PASS" if manifest.get("no_proxy_fill") else "FAIL", "critical", year, "S140093 não foi preenchida por proxy."))
            tests.append(TestResult(f"{year}.no_synthetic_location", "PASS" if not manifest.get("synthetic_location") else "FAIL", "critical", year, "Nenhuma localização sintética adicionada."))
            diag = manifest.get("diagnostics", {})
            raw_delivery = int(diag.get("delivery_app_use_raw", 0))
            official_delivery = int(diag.get("platform_delivery", 0))
            nonplatform_delivery = int(diag.get("delivery_app_use_nonplatform", 0))
            tests.append(TestResult(
                f"{year}.delivery.official_subset_raw",
                "PASS" if 0 <= official_delivery <= raw_delivery else "FAIL",
                "critical", year,
                "Entrega plataformizada é subconjunto do uso bruto do aplicativo.",
                observed={"raw": raw_delivery, "official": official_delivery},
                expected="0 <= official <= raw",
            ))
            tests.append(TestResult(
                f"{year}.delivery.partition",
                "PASS" if official_delivery + nonplatform_delivery == raw_delivery else "FAIL",
                "critical", year,
                "Uso bruto de app de entrega foi particionado em plataformizado e não plataformizado.",
                observed={
                    "raw": raw_delivery,
                    "official": official_delivery,
                    "nonplatform": nonplatform_delivery,
                },
                expected="official + nonplatform == raw",
            ))
        except Exception as exc:
            logger.exception("Falha ao certificar %s", year)
            tests.append(TestResult(f"{year}.certified_table.created", "FAIL", "critical", year, f"Falha: {exc}"))

    designs: dict[int, dict[str, Any]] = {}
    estimates: list[SurveyEstimate] = []
    for year, frame in summaries.items():
        design = survey_design_summary(frame, year, periods[year], manifests[year])
        designs[year] = design
        atomic_json(tree["registry"] / f"survey_design_{year}.json", design)
        year_estimates = domain_estimates(frame, year, periods[year])
        estimates.extend(year_estimates)
        raw_est = estimate_lookup(year_estimates, year, "delivery_app_use_raw", "total")
        official_est = estimate_lookup(year_estimates, year, "platform_delivery_direct", "total")
        nonplatform_est = estimate_lookup(year_estimates, year, "delivery_app_use_nonplatform", "total")
        if raw_est and official_est and nonplatform_est and None not in {raw_est.estimate, official_est.estimate, nonplatform_est.estimate}:
            partition_error = float(raw_est.estimate - official_est.estimate - nonplatform_est.estimate)
            partition_scale = max(abs(float(raw_est.estimate)), 1.0)
            tests.append(TestResult(
                f"{year}.delivery.weighted_partition",
                "PASS" if abs(partition_error) / partition_scale < 1e-10 else "FAIL",
                "critical", year,
                "Totais ponderados preservam uso bruto = entrega plataformizada + uso não plataformizado.",
                observed={
                    "raw": raw_est.estimate,
                    "official": official_est.estimate,
                    "nonplatform": nonplatform_est.estimate,
                    "error": partition_error,
                },
                expected="erro relativo < 1e-10",
            ))
        tests.append(TestResult(
            f"{year}.survey.design_ready", "PASS" if design.get("status") == "READY" else "FAIL", "critical", year,
            "Desenho survey reconstruído." if design.get("status") == "READY" else "Desenho survey bloqueado.", evidence=design
        ))
        tests.append(TestResult(
            f"{year}.sample_size.plausible", "PASS" if manifests[year].get("rows", 0) > 100000 else "WARN", "high", year,
            "Número de registros plausível para a base concentrada.", observed=manifests[year].get("rows"), expected=">100000"
        ))

    pooled_manifest: dict[str, Any] | None = None
    if len(processed_paths) == 2:
        try:
            pooled_path = tree["processed"] / "certified_pnadc_platform_pooled.parquet"
            pooled_manifest = build_pooled(processed_paths, pooled_path)
            atomic_json(tree["registry"] / "certified_pnadc_platform_pooled_manifest.json", pooled_manifest)
            tests.append(TestResult("pooled.period_preserved", "PASS", "critical", None, "Pooled preserva source_year e reference_quarter.", evidence=pooled_manifest))
        except Exception as exc:
            logger.exception("Falha no pooling")
            tests.append(TestResult("pooled.created", "FAIL", "critical", None, f"Falha ao criar pooled: {exc}"))

    estimates_path = tree["outputs"] / "pnadc_certified_estimates.csv"
    if estimates:
        pd.DataFrame([asdict(x) for x in estimates]).to_csv(estimates_path, index=False, encoding="utf-8-sig")
    atomic_json(tree["contracts"] / "pnadc_certified_table_contract.json", certification_contract())
    atomic_json(
        tree["contracts"] / "sidra_code_contracts_v1.2.0.json",
        {
            "validation_schema_version": VALIDATION_SCHEMA_VERSION,
            "contracts": SIDRA_CODE_CONTRACTS,
            "created_at_utc": utc_now(),
        },
    )

    # Golden tests oficiais SIDRA.
    session = http_session()
    if args.skip_sidra:
        scraped = {}
    else:
        scraped = {
            "2022": scrape_sidra_tables(session, IBGE_PRODUCT_2022, logger),
            "2024": scrape_sidra_tables(session, IBGE_PRODUCT_2024, logger),
        }
    atomic_json(tree["registry"] / f"sidra_product_table_discovery_{rid}.json", scraped)
    sidra_tests = run_sidra_golden_tests(
        session, estimates, tree["interim"] / "sidra_snapshots",
        logger, args.skip_sidra, scraped,
        prefer_cache=args.sidra_source == "cache",
        live_only=args.sidra_source == "live",
        cache_only=args.sidra_source == "cache",
    )
    tests.extend(sidra_tests)

    # Domínios oficiais: códigos observados devem estar documentados quando o dicionário foi extraído.
    for year, manifest in manifests.items():
        for variable in ("SD14001", "S140093"):
            documented_map = official_domain(variable, categories)
            documented = set(documented_map)
            observed = set(manifest.get("observed_domains", {}).get(variable, []))
            unknown = sorted(observed - documented)
            tests.append(TestResult(
                f"{year}.domain.{variable}.documented", "PASS" if documented and not unknown else "FAIL", "critical", year,
                "Códigos observados pertencem ao domínio oficial." if documented and not unknown else "Há códigos fora do contrato oficial.",
                observed=sorted(observed), expected=documented_map, evidence={"unknown": unknown, "source": "official_contract" if variable in OFFICIAL_BINARY_DOMAINS else "dictionary"}
            ))

    # Salva golden tests por ano e agregado.
    for year in periods:
        atomic_json(tree["registry"] / f"golden_tests_{year}.json", [asdict(t) for t in tests if t.year in {None, year}])
    atomic_json(tree["registry"] / f"pnadc_certification_tests_{rid}.json", [asdict(t) for t in tests])

    report = render_report(
        rid, root, selections, manifests, designs, estimates, tests,
        pooled_manifest, execution_mode="certify",
    )
    report_path = tree["reports_final"] / f"pnadc_certification_report_{rid}.md"
    atomic_text(report_path, report)
    atomic_text(tree["reports_final"] / "pnadc_certification_report_LATEST.md", report)

    summary = summarize_tests(tests)
    lock = {
        "run_id": rid,
        "script_version": VERSION,
        "data_schema_version": SCHEMA_VERSION,
        "validation_schema_version": VALIDATION_SCHEMA_VERSION,
        "mode": "certify",
        "status": summary["status"], "critical_failures": summary["critical_failures"],
        "secondary_failures": summary["secondary_failures"],
        "warnings": summary["warnings"], "report": str(report_path),
        "outputs": {year: manifest.get("output_path") for year, manifest in manifests.items()},
        "pooled": pooled_manifest.get("path") if pooled_manifest else None,
        "created_at_utc": utc_now(),
    }
    atomic_json(tree["admin"] / "PNADC_CERTIFICATION_LOCK.json", lock)
    logger.info("Certificação concluída | status=%s | relatório=%s", summary["status"], report_path)
    if summary["critical_failures"]:
        for failure in summary["critical_failures"]:
            logger.error("GATE CRÍTICO: %s — %s", failure["test_id"], failure["message"])
    return 0 if not args.strict or summary["status"] in {"CERTIFIED", "CORE_CERTIFIED"} else 2


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def clean_jupyter_argv(argv: Sequence[str]) -> list[str]:
    cleaned: list[str] = []
    i = 0
    while i < len(argv):
        if argv[i] == "-f" and i + 1 < len(argv) and "kernel-" in argv[i + 1]:
            i += 2
            continue
        cleaned.append(argv[i])
        i += 1
    return cleaned


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Certificação PNADc direta 2022T4/2024T3 da SPINE-GPE v7.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--root", type=str, default=None, help="Raiz SPINE-GPEv7.")
    parser.add_argument("--mode", choices=("audit", "certify", "sidra-only"), default="certify")
    parser.add_argument("--chunk-rows", type=int, default=20000, help="Linhas por chunk fixed-width.")
    parser.add_argument("--local-cache", action=argparse.BooleanOptionalAction, default=True, help="Copia TXT do Drive para /content antes de processar.")
    parser.add_argument("--cache-dir", type=str, default=None, help="Diretório de cache alternativo.")
    parser.add_argument("--skip-sidra", action="store_true", help="Não consulta SIDRA; deixa golden tests oficiais como skipped.")
    parser.add_argument(
        "--sidra-source", choices=("auto", "cache", "live"), default="auto",
        help="Fonte SIDRA. Em sidra-only, auto prefere snapshots locais; live exige API.",
    )
    parser.add_argument("--strict", action="store_true", help="Retorna exit code 2 se houver gate crítico.")
    parser.add_argument("--verbose", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    raw = list(sys.argv[1:] if argv is None else argv)
    args = build_parser().parse_args(clean_jupyter_argv(raw))
    return run_pipeline(args)


if __name__ == "__main__":
    raise SystemExit(main())
