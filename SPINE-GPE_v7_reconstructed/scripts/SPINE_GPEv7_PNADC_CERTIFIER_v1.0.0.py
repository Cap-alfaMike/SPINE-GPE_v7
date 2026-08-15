#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SPINE-GPE v7 — PNADc Direct Platform Certification Engine v1.0.0
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
  * produz certified tables 2022, 2024 e pooled;
  * calcula estimativas design-based e diagnósticos de precisão;
  * consulta tabelas oficiais SIDRA para golden tests;
  * bloqueia a certificação em caso de inconsistência crítica.

Uso Colab:
  python SPINE_GPEv7_PNADC_CERTIFIER_v1.0.0.py \
      --root /content/drive/MyDrive/aCidadeAlgoritmica/SPINE-GPEv7 \
      --mode certify

Atenção epistemológica:
  - S140093 é o identificador direto de uso de plataforma de entrega.
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


VERSION = "1.0.0"
SCHEMA_VERSION = "spine-gpe-v7-pnadc-certified-1.0.0"
SCRIPT_NAME = "SPINE_GPEv7_PNADC_CERTIFIER_v1.0.0.py"
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
    "delivery_income_occupation_2024": 10257,
    "delivery_hours_occupation_2024": 10258,
}

# Variáveis centrais. Todas as variáveis S14*/SD14* existentes no layout
# também são preservadas para a construção posterior do ICA.
CORE_VARIABLE_CANDIDATES: tuple[str, ...] = (
    "Ano", "Trimestre", "UF", "Capital", "RM_RIDE", "UPA", "Estrato",
    "V1008", "V1014", "V1028", "V1030", "V1031", "V1032",
    "posest", "posest_sxi", "V2007", "V2009", "V2010", "VD3004",
    "VD4001", "VD4002", "VD4003", "VD4004A", "VD4005", "VD4008",
    "VD4009", "V4010", "V4012", "V4013", "V4019", "V4020",
    "V4029", "V4029A", "V4029B", "V4039", "V4039C",
    "VD4016", "VD4017", "VD4018", "VD4019", "VD4020",
    "SD14001", "S140091", "S140092", "S140093", "S140094",
)

REQUIRED_DIRECT = {"SD14001", "S140093"}
REQUIRED_SURVEY_GROUPS: dict[str, tuple[str, ...]] = {
    "weight": ("V1028", "V1032", "V1031", "V1030"),
    "stratum": ("Estrato", "posest_sxi", "posest"),
    "psu": ("UPA",),
}

OCCUPATION_COMPATIBLE = {"8321", "8322", "9331", "9621"}
DELIVERY_ACTIVITY_CODES = {"53002"}

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
    direct_identifier: str = "S140093"


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
    mapping = categories.get(variable.upper(), {})
    candidates = [code for code, label in mapping.items() if normalize_text(label) in {"sim", "sim, utilizou", "sim, trabalhou"} or normalize_text(label).startswith("sim")]
    if len(candidates) == 1:
        return candidates[0], "dictionary_label"
    # Convenção oficial recorrente; só é aceita se 1 estiver no domínio observado
    # e a cardinalidade for compatível com variável binária.
    cleaned = {x for x in observed if x is not None}
    if "1" in cleaned and cleaned.issubset({"1", "2", "9"}):
        return "1", "official_binary_convention_review_required"
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
            f"domain.{variable}.yes_code", "PASS" if code and source == "dictionary_label" else "WARN" if code else "FAIL",
            "critical" if not code else "high", year,
            f"Código 'Sim' de {variable}: {code} ({source}).",
            observed=sorted(observed_codes[variable]), expected=categories.get(variable, {}),
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
            raw.insert(4, "identification_method", "S140093_direct")
            raw.insert(5, "source_file_sha256", source_hash)
            raw.insert(6, "layout_file_sha256", layout_hash)
            raw.insert(7, "schema_version", SCHEMA_VERSION)
            raw["eligible_platform_module"] = raw["SD14001"].notna() if "SD14001" in raw else False
            if yes_codes.get("SD14001"):
                raw["platform_any_direct"] = raw["SD14001"].eq(yes_codes["SD14001"])
            else:
                raw["platform_any_direct"] = pd.Series(pd.NA, index=raw.index, dtype="boolean")
            if yes_codes.get("S140093"):
                raw["platform_delivery_direct"] = raw["S140093"].eq(yes_codes["S140093"])
            else:
                raw["platform_delivery_direct"] = pd.Series(pd.NA, index=raw.index, dtype="boolean")
            if "V4010" in raw:
                raw["delivery_occupation_compatible"] = raw["V4010"].isin(OCCUPATION_COMPATIBLE)
            else:
                raw["delivery_occupation_compatible"] = False
            if "V4013" in raw:
                raw["delivery_activity_compatible"] = raw["V4013"].isin(DELIVERY_ACTIVITY_CODES)
            else:
                raw["delivery_activity_compatible"] = False
            raw["platform_courier_direct"] = (
                raw["platform_delivery_direct"].fillna(False)
                & (raw["delivery_occupation_compatible"] | raw["delivery_activity_compatible"])
            )
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
            # Previdência: código 'Sim' inferido pelo dicionário quando disponível.
            if "V4029" in raw:
                v4029_yes, _ = infer_yes_code("V4029", categories, set(raw["V4029"].dropna().astype(str)))
                raw["social_security_contributor"] = raw["V4029"].eq(v4029_yes) if v4029_yes else pd.Series(pd.NA, index=raw.index, dtype="boolean")
            # Informalidade oficial aproximada pelas categorias rotuladas de VD4009.
            if "VD4009" in raw and categories.get("VD4009"):
                informal_codes = {
                    code for code, label in categories["VD4009"].items()
                    if any(token in normalize_text(label) for token in (
                        "sem carteira", "sem cnpj", "trabalhador familiar auxiliar"
                    ))
                }
                raw["informal_status"] = raw["VD4009"].isin(informal_codes)
            else:
                raw["informal_status"] = pd.Series(pd.NA, index=raw.index, dtype="boolean")
            if "VD4019" in raw:
                raw["monthly_income_usual"] = numeric_series(raw["VD4019"])
            elif "V4019" in raw:
                raw["monthly_income_usual"] = numeric_series(raw["V4019"])
            if "V4039" in raw:
                raw["weekly_hours_usual"] = numeric_series(raw["V4039"])
            elif "VD4039" in raw:
                raw["weekly_hours_usual"] = numeric_series(raw["VD4039"])
            raw["synthetic_location"] = False
            diagnostics["records"] += len(raw)
            diagnostics["eligible"] += int(raw["eligible_platform_module"].sum())
            diagnostics["platform_any"] += int(raw["platform_any_direct"].fillna(False).sum())
            diagnostics["platform_delivery"] += int(raw["platform_delivery_direct"].fillna(False).sum())
            # Amostra compacta para diagnósticos design-based sem carregar tudo na RAM.
            summary_cols = [c for c in (
                "survey_weight", "survey_stratum", "survey_psu", "UF", "Capital", "RM_RIDE",
                "eligible_platform_module", "platform_any_direct", "platform_delivery_direct",
                "platform_courier_direct", "monthly_income_usual", "weekly_hours_usual",
                "informal_status", "social_security_contributor", "region_code",
                "VD4009", "V4029", "V2007", "V2010", "VD3004"
            ) if c in raw]
            summary_frames.append(raw.loc[raw["eligible_platform_module"].fillna(False), summary_cols].copy())
            offset += len(raw)
            yield raw

    metadata = {
        "spine_schema_version": SCHEMA_VERSION,
        "year": str(year), "quarter": str(quarter),
        "direct_identifier": "S140093",
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
        "identification_method": "S140093_direct",
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
    valid["y"] = pd.to_numeric(y, errors="coerce")
    valid["w"] = pd.to_numeric(valid["survey_weight"], errors="coerce")
    valid = valid.dropna(subset=["y", "w", "survey_stratum", "survey_psu"])
    valid = valid[valid["w"] > 0]
    estimate = float((valid["w"] * valid["y"]).sum()) if not valid.empty else None
    linearized = valid["w"] * valid["y"]
    variance, n_psu, n_strata, singleton = _cluster_variance(linearized, valid["survey_stratum"], valid["survey_psu"])
    se = math.sqrt(variance) if variance is not None and variance >= 0 else None
    df = max(n_psu - n_strata, 1)
    crit = float(student_t.ppf(0.975, df)) if df > 1 else 1.96
    ci_low = estimate - crit * se if estimate is not None and se is not None else None
    ci_high = estimate + crit * se if estimate is not None and se is not None else None
    cv = abs(se / estimate * 100) if estimate not in (None, 0) and se is not None else None
    n_eff = kish_effective_n(valid["w"])
    return SurveyEstimate(
        year, quarter, domain, geography, geography_code, "total", estimate, se, ci_low, ci_high, cv,
        len(valid), n_eff, n_psu, n_strata, df, precision_status(cv, n_eff, len(valid)),
        notes=f"Taylor linearization por estrato/UPA; estratos singleton omitidos={singleton}; sem FPC."
    )


def survey_mean(frame: pd.DataFrame, y: pd.Series, year: int, quarter: int, domain: str, geography: str, geography_code: str, estimand: str = "mean", domain_indicator: pd.Series | None = None) -> SurveyEstimate:
    valid = frame[["survey_weight", "survey_stratum", "survey_psu"]].copy()
    valid["y"] = pd.to_numeric(y, errors="coerce")
    valid["d"] = 1.0 if domain_indicator is None else pd.to_numeric(domain_indicator.reindex(frame.index), errors="coerce").fillna(0.0)
    valid["w"] = pd.to_numeric(valid["survey_weight"], errors="coerce")
    # Mantém todas as UPAs do universo; y faltante só é relevante dentro do domínio.
    valid = valid.dropna(subset=["w", "survey_stratum", "survey_psu"])
    valid = valid[valid["w"] > 0]
    valid["dy_valid"] = valid["d"].gt(0) & valid["y"].notna()
    denom = float((valid.loc[valid["dy_valid"], "w"] * valid.loc[valid["dy_valid"], "d"]).sum())
    estimate = float((valid.loc[valid["dy_valid"], "w"] * valid.loc[valid["dy_valid"], "d"] * valid.loc[valid["dy_valid"], "y"]).sum() / denom) if denom > 0 else None
    if estimate is None:
        linearized = pd.Series(0.0, index=valid.index)
    else:
        y_centered = (valid["y"] - estimate).fillna(0.0)
        active = valid["dy_valid"].astype(float)
        linearized = valid["w"] * valid["d"] * active * y_centered / denom
    variance, n_psu, n_strata, singleton = _cluster_variance(linearized, valid["survey_stratum"], valid["survey_psu"])
    se = math.sqrt(variance) if variance is not None and variance >= 0 else None
    df = max(n_psu - n_strata, 1)
    crit = float(student_t.ppf(0.975, df)) if df > 1 else 1.96
    ci_low = estimate - crit * se if estimate is not None and se is not None else None
    ci_high = estimate + crit * se if estimate is not None and se is not None else None
    cv = abs(se / estimate * 100) if estimate not in (None, 0) and se is not None else None
    domain_weights = valid.loc[valid["dy_valid"], "w"] * valid.loc[valid["dy_valid"], "d"]
    n_eff = kish_effective_n(domain_weights)
    n_domain = int(valid["dy_valid"].sum())
    return SurveyEstimate(
        year, quarter, domain, geography, geography_code, estimand, estimate, se, ci_low, ci_high, cv,
        n_domain, n_eff, n_psu, n_strata, df, precision_status(cv, n_eff, n_domain),
        notes=f"Taylor linearization por estrato/UPA; estratos singleton omitidos={singleton}; sem FPC."
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
        "platform_delivery_direct": universe.get("platform_delivery_direct", pd.Series(False, index=universe.index)).fillna(False),
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
        informal_platform = platform_any_d * universe["informal_status"].fillna(False).astype(float)
        estimates.append(survey_total(universe, informal_platform, year, quarter, "platform_any_informal", "Brasil", "BR"))
        estimates.append(survey_mean(universe, universe["informal_status"].astype(float), year, quarter, "platform_any_direct", "Brasil", "BR", "percent_informal", platform_any_d))
    if "social_security_contributor" in universe and universe["social_security_contributor"].notna().any():
        contrib_platform = platform_any_d * universe["social_security_contributor"].fillna(False).astype(float)
        estimates.append(survey_total(universe, contrib_platform, year, quarter, "platform_any_social_security", "Brasil", "BR"))
        estimates.append(survey_mean(universe, universe["social_security_contributor"].astype(float), year, quarter, "platform_any_direct", "Brasil", "BR", "percent_social_security", platform_any_d))
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


def sidra_candidates(df: pd.DataFrame, year: int, includes: Sequence[str], excludes: Sequence[str] = ()) -> pd.DataFrame:
    if df.empty:
        return df
    mask = pd.Series(True, index=df.index)
    texts = df.apply(sidra_row_text, axis=1)
    mask &= texts.str.contains(str(year), regex=False)
    for token in includes:
        mask &= texts.str.contains(normalize_text(token), regex=False)
    for token in excludes:
        mask &= ~texts.str.contains(normalize_text(token), regex=False)
    return df.loc[mask].copy()


def official_value_from_candidates(candidates: pd.DataFrame) -> tuple[float | None, dict[str, Any]]:
    if candidates.empty:
        return None, {"reason": "no_match"}
    value_col = find_value_column(candidates)
    if not value_col:
        return None, {"reason": "value_column_missing", "columns": candidates.columns.tolist()}
    parsed = candidates[value_col].map(safe_float)
    valid = candidates.loc[parsed.notna()].copy()
    valid["__parsed_value"] = parsed.dropna().values
    if len(valid) != 1:
        return None, {
            "reason": "ambiguous", "n_matches": len(valid),
            "matches": valid.head(20).astype(str).to_dict(orient="records")
        }
    value = float(valid.iloc[0]["__parsed_value"])
    row_text = sidra_row_text(valid.iloc[0])
    multiplier = 1000.0 if "mil pessoas" in row_text or "1 000 pessoas" in row_text else 1.0
    return value * multiplier, {"row": valid.drop(columns="__parsed_value").iloc[0].astype(str).to_dict(), "multiplier": multiplier}


def estimate_lookup(estimates: Sequence[SurveyEstimate], year: int, domain: str, estimand: str) -> SurveyEstimate | None:
    return next((x for x in estimates if x.year == year and x.domain == domain and x.estimand == estimand and x.geography == "Brasil"), None)


def compare_golden(test_id: str, year: int, observed: float | None, official: float | None, rel_tol: float, evidence: dict[str, Any]) -> TestResult:
    if observed is None:
        return TestResult(test_id, "FAIL", "critical", year, "Estimativa do microdado ausente.", observed=observed, expected=official, tolerance=rel_tol, evidence=evidence)
    if official is None:
        return TestResult(test_id, "BLOCKED", "critical", year, "Valor oficial SIDRA não foi selecionado de forma inequívoca.", observed=observed, expected=official, tolerance=rel_tol, evidence=evidence)
    relative = abs(observed - official) / max(abs(official), 1e-12)
    status = "PASS" if relative <= rel_tol else "FAIL"
    return TestResult(
        test_id, status, "critical", year,
        f"Diferença relativa microdado × SIDRA = {relative:.4%}.",
        observed=observed, expected=official, tolerance=rel_tol,
        evidence={**evidence, "relative_difference": relative}
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


def run_sidra_golden_tests(session: requests.Session, estimates: Sequence[SurveyEstimate], output_dir: Path, logger: logging.Logger, skip: bool, scraped: Mapping[str, Mapping[str, int]] | None = None) -> list[TestResult]:
    results: list[TestResult] = []
    if skip:
        return [TestResult("sidra.validation", "SKIPPED", "critical", None, "Validação SIDRA desativada por argumento.")]
    output_dir.mkdir(parents=True, exist_ok=True)
    tables = resolve_sidra_tables(scraped or {})
    for key, table in tables.items():
        try:
            df = fetch_sidra_all(session, table, logger)
            df.to_csv(output_dir / f"sidra_{table}_{key}.csv", index=False, encoding="utf-8-sig")
            atomic_json(output_dir / f"sidra_{table}_{key}_metadata.json", df.attrs.get("metadata", {}))
        except Exception as exc:
            logger.error("Falha SIDRA tabela %s: %s", table, exc)
            results.append(TestResult(f"sidra.table_{table}.download", "BLOCKED", "critical", None, f"Falha ao baixar tabela SIDRA {table}.", evidence={"error": str(exc)}))
            continue
        results.append(TestResult(f"sidra.table_{table}.download", "PASS", "high", None, f"Tabela SIDRA {table} materializada.", evidence={"rows": len(df), "path": str(output_dir / f'sidra_{table}_{key}.csv')}))
        if key == "platform_any":
            for year in (2022, 2024):
                candidates = sidra_candidates(df, year, includes=("trabalhou", "plataforma", "mil pessoas"), excludes=("nao trabalhou", "não trabalhou", "percentual"))
                official, ev = official_value_from_candidates(candidates)
                obs = estimate_lookup(estimates, year, "platform_any_direct", "total")
                results.append(compare_golden(f"golden.{year}.platform_any_total", year, obs.estimate if obs else None, official, 0.015, {"table": table, **ev}))
                pct_candidates = sidra_candidates(df, year, includes=("trabalhou", "plataforma", "percentual"), excludes=("nao trabalhou", "não trabalhou", "mil pessoas"))
                official_pct, pct_ev = official_value_from_candidates(pct_candidates)
                obs_pct = estimate_lookup(estimates, year, "platform_any_direct", "percent")
                results.append(compare_golden(f"golden.{year}.platform_any_percent", year, obs_pct.estimate if obs_pct else None, official_pct, 0.02, {"table": table, **pct_ev}))
        elif key == "platform_type":
            for year in (2022, 2024):
                candidates = sidra_candidates(df, year, includes=("entrega", "mil pessoas"), excludes=("percentual",))
                official, ev = official_value_from_candidates(candidates)
                obs = estimate_lookup(estimates, year, "platform_delivery_direct", "total")
                results.append(compare_golden(f"golden.{year}.platform_delivery_total", year, obs.estimate if obs else None, official, 0.02, {"table": table, **ev}))
        elif key == "hours":
            for year in (2022, 2024):
                candidates = sidra_candidates(df, year, includes=("trabalhou", "plataforma", "hora"), excludes=("nao trabalhou", "não trabalhou", "percentual"))
                official, ev = official_value_from_candidates(candidates)
                obs = estimate_lookup(estimates, year, "platform_any_direct", "mean_weekly_hours")
                results.append(compare_golden(f"golden.{year}.platform_any_hours", year, obs.estimate if obs else None, official, 0.02, {"table": table, **ev}))
        elif key == "income":
            for year in (2022, 2024):
                candidates = sidra_candidates(df, year, includes=("trabalhou", "plataforma", "total", "reais"), excludes=("nao trabalhou", "não trabalhou", "percentual"))
                official, ev = official_value_from_candidates(candidates)
                obs = estimate_lookup(estimates, year, "platform_any_direct", "mean_monthly_income")
                results.append(compare_golden(f"golden.{year}.platform_any_income", year, obs.estimate if obs else None, official, 0.025, {"table": table, **ev}))
        elif key == "informality":
            for year in (2022, 2024):
                candidates = sidra_candidates(df, year, includes=("trabalhou", "plataforma", "mil pessoas"), excludes=("nao trabalhou", "não trabalhou", "percentual"))
                official, ev = official_value_from_candidates(candidates)
                obs = estimate_lookup(estimates, year, "platform_any_informal", "total")
                results.append(compare_golden(f"golden.{year}.platform_any_informal_total", year, obs.estimate if obs else None, official, 0.025, {"table": table, **ev}))
        elif key == "social_security":
            for year in (2022, 2024):
                candidates = sidra_candidates(df, year, includes=("plataforma", "mil pessoas"), excludes=("nao trabalhou", "não trabalhou", "nao contribuiu", "não contribuiu", "percentual"))
                # Exige marcador positivo de contribuição para evitar escolher o total.
                if not candidates.empty:
                    texts = candidates.apply(sidra_row_text, axis=1)
                    positive = texts.str.contains("contribuiu|contribuinte|sim", regex=True)
                    candidates = candidates.loc[positive]
                official, ev = official_value_from_candidates(candidates)
                obs = estimate_lookup(estimates, year, "platform_any_social_security", "total")
                results.append(compare_golden(f"golden.{year}.platform_any_social_security_total", year, obs.estimate if obs else None, official, 0.03, {"table": table, **ev}))
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
            "platform_any_direct", "platform_delivery_direct", "survey_weight",
            "survey_stratum", "survey_psu", "synthetic_location",
        ],
        "semantic_rules": {
            "platform_delivery_direct": "S140093 equals official Yes code; never proxy-filled",
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
    critical_failures = [test for test in tests if test.severity == "critical" and test.status in {"FAIL", "BLOCKED"}]
    warnings = [test for test in tests if test.status in {"WARN", "REVIEW"}]
    return {
        "counts": dict(counts),
        "critical_failures": [asdict(x) for x in critical_failures],
        "warnings": [asdict(x) for x in warnings],
        "status": "CERTIFIED" if not critical_failures else "BLOCKED",
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
) -> str:
    summary = summarize_tests(tests)
    lines = [
        "# SPINE-GPE v7 — Relatório de Certificação PNADc Plataformas",
        "",
        f"- Run ID: `{rid}`",
        f"- Versão: `{VERSION}`",
        f"- Schema: `{SCHEMA_VERSION}`",
        f"- Status: **{summary['status']}**",
        f"- Raiz: `{root}`",
        f"- Gerado em UTC: `{utc_now()}`",
        "",
        "## Princípio de identificação",
        "",
        "`S140093` é o único identificador direto primário de plataforma de entrega. Nenhuma proxy ocupacional ou territorial preenche essa variável.",
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
    lines.extend(["", "## Certified tables", "", "| Ano | Registros | Elegíveis módulo | Plataforma geral | Entrega | Output |", "|---:|---:|---:|---:|---:|---|"])
    for year, manifest in manifests.items():
        d = manifest.get("diagnostics", {})
        lines.append(
            f"| {year} | {manifest.get('rows')} | {d.get('eligible')} | {d.get('platform_any')} | "
            f"{d.get('platform_delivery')} | `{manifest.get('output_path')}` |"
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
    lines.extend(["", "## Estimativas design-based", "", "| Ano | Domínio | Geografia | Estimando | Estimativa | SE | CV% | n | n efetivo | Precisão |", "|---:|---|---|---|---:|---:|---:|---:|---:|---|"])
    for est in estimates:
        lines.append(
            f"| {est.year} | {est.domain} | {est.geography}:{est.geography_code} | {est.estimand} | "
            f"{'' if est.estimate is None else f'{est.estimate:.4f}'} | {'' if est.se is None else f'{est.se:.4f}'} | "
            f"{'' if est.cv_percent is None else f'{est.cv_percent:.2f}'} | {est.n_unweighted} | "
            f"{'' if est.n_effective is None else f'{est.n_effective:.1f}'} | {est.precision_status} |"
        )
    lines.extend([
        "", "## Limites congelados", "",
        "1. 2022 refere-se ao 4º trimestre e 2024 ao 3º trimestre; a comparação pode conter sazonalidade.",
        "2. O pooled é uma repeated cross-section, não painel individual.",
        "3. A localização disponível na PNADc não é convertida em residência fina ou hexágono.",
        "4. Registros fora do universo do módulo não são recodificados como não plataformizados.",
        "5. Renda e horas faltantes não removem pessoas da tabela populacional certificada.",
        "6. Testes SIDRA bloqueados ou ambíguos devem ser resolvidos antes da Fase 2.",
        "", "## Próximo gate", "",
        "Com as tabelas certificadas, construir o Índice de Controle Algorítmico e os comparadores internos, preservando desenho survey, universo e suporte comum.",
    ])
    return "\n".join(lines) + "\n"


# -----------------------------------------------------------------------------
# Pipeline principal
# -----------------------------------------------------------------------------

def run_pipeline(args: argparse.Namespace) -> int:
    root = autodetect_root(args.root)
    tree = make_tree(root)
    rid = run_id()
    logger = setup_logging(tree["logs"] / f"PNADC_CERTIFIER_{rid}.log", args.verbose)
    logger.info("SPINE-GPE PNADc Certifier v%s | root=%s | mode=%s", VERSION, root, args.mode)
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
        lock = {
            "run_id": rid, "version": VERSION, "schema_version": SCHEMA_VERSION,
            "status": summary["status"], "mode": "audit", "tests": [asdict(t) for t in tests],
            "created_at_utc": utc_now(),
        }
        atomic_json(tree["admin"] / "PNADC_CERTIFICATION_LOCK.json", lock)
        report = render_report(rid, root, selections, {}, {}, [], tests, None)
        report_path = tree["reports"] / f"PNADC_CERTIFICATION_REPORT_{rid}.md"
        atomic_text(report_path, report)
        logger.info("Auditoria concluída: %s | status=%s", report_path, summary["status"])
        return 0 if not args.strict or summary["status"] == "CERTIFIED" else 2

    # Dicionário de categorias a partir de todos os documentos disponíveis.
    target_vars = {f.variable.upper() for _, layout in chosen.values() for f in layout.fields if f.variable.upper().startswith(("S14", "SD14")) or f.variable.upper() in {"VD4009", "V4029", "V2007", "V2010", "VD3004", "V4010", "V4012", "V4013"}}
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
    sidra_tests = run_sidra_golden_tests(session, estimates, tree["interim"] / "sidra_snapshots", logger, args.skip_sidra, scraped)
    tests.extend(sidra_tests)

    # Domínios oficiais: códigos observados devem estar documentados quando o dicionário foi extraído.
    for year, manifest in manifests.items():
        for variable in ("SD14001", "S140093"):
            documented = set(categories.get(variable, {}).keys())
            observed = set(manifest.get("observed_domains", {}).get(variable, []))
            if documented:
                unknown = sorted(observed - documented)
                tests.append(TestResult(
                    f"{year}.domain.{variable}.documented", "PASS" if not unknown else "FAIL", "critical", year,
                    "Códigos observados pertencem ao domínio oficial." if not unknown else "Há códigos não documentados.",
                    observed=sorted(observed), expected=sorted(documented), evidence={"unknown": unknown}
                ))
            else:
                tests.append(TestResult(
                    f"{year}.domain.{variable}.documented", "BLOCKED", "critical", year,
                    "Categorias oficiais não foram extraídas do dicionário; revisar parser/documentação.", observed=sorted(observed)
                ))

    # Salva golden tests por ano e agregado.
    for year in periods:
        atomic_json(tree["registry"] / f"golden_tests_{year}.json", [asdict(t) for t in tests if t.year in {None, year}])
    atomic_json(tree["registry"] / f"pnadc_certification_tests_{rid}.json", [asdict(t) for t in tests])

    report = render_report(rid, root, selections, manifests, designs, estimates, tests, pooled_manifest)
    report_path = tree["reports_final"] / f"pnadc_certification_report_{rid}.md"
    atomic_text(report_path, report)
    atomic_text(tree["reports_final"] / "pnadc_certification_report_LATEST.md", report)

    summary = summarize_tests(tests)
    lock = {
        "run_id": rid, "script_version": VERSION, "schema_version": SCHEMA_VERSION,
        "status": summary["status"], "critical_failures": summary["critical_failures"],
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
    return 0 if not args.strict or summary["status"] == "CERTIFIED" else 2


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
    parser.add_argument("--mode", choices=("audit", "certify"), default="certify")
    parser.add_argument("--chunk-rows", type=int, default=20000, help="Linhas por chunk fixed-width.")
    parser.add_argument("--local-cache", action=argparse.BooleanOptionalAction, default=True, help="Copia TXT do Drive para /content antes de processar.")
    parser.add_argument("--cache-dir", type=str, default=None, help="Diretório de cache alternativo.")
    parser.add_argument("--skip-sidra", action="store_true", help="Não consulta SIDRA; deixa golden tests oficiais como skipped.")
    parser.add_argument("--strict", action="store_true", help="Retorna exit code 2 se houver gate crítico.")
    parser.add_argument("--verbose", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    raw = list(sys.argv[1:] if argv is None else argv)
    args = build_parser().parse_args(clean_jupyter_argv(raw))
    return run_pipeline(args)


if __name__ == "__main__":
    raise SystemExit(main())
