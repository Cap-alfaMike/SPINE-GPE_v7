#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SPINE-GPE v7 — PNAD COVID Certification Engine v1.0.0
=======================================================

Certifica a ponte pandêmica de trabalho de entrega na PNAD COVID-19 de 2020.

Estimando central
-----------------
`pandemic_delivery_observed` identifica pessoas ocupadas cuja ocupação declarada
em C007C é:
  * 16 — Motoboy;
  * 17 — Entregador de mercadorias, categoria cujo enunciado oficial inclui
    exemplos de aplicativos.

Limite epistemológico obrigatório
---------------------------------
A PNAD COVID-19 não pergunta diretamente se o trabalho foi intermediado por
plataforma. Por isso:
  * `pandemic_delivery_observed` é observado no questionário;
  * `platform_delivery_direct` permanece NA para todos os registros;
  * nenhuma proxy preenche uma variável direta de plataforma;
  * o artefato recebe evidence tier B, abaixo de S140093 da PNADc especial.

O engine:
  * exige PHASE0_LOCK=RELEASED;
  * exige o núcleo PNADc direto CORE_CERTIFIED ou CERTIFIED e o congela por hash;
  * descobre ou baixa os CSV/ZIP oficiais da PNAD COVID-19;
  * audita dicionários oficiais atualizados;
  * lê os CSV em chunks e grava Parquet com schema estável;
  * preserva mês, desenho survey e chave longitudinal anonimizada;
  * calcula estimativas mensais design-based por UPA/Estrato/V1032;
  * gera golden tests fail-closed;
  * impede pooling ingênuo dos erros-padrão, pois a amostra mensal é fixa/repetida.

Exemplo Colab — setembro de 2020:
  python SPINE_GPEv7_PNAD_COVID_CERTIFIER_v1.0.0.py \
    --root /content/drive/MyDrive/aCidadeAlgoritmica/SPINE-GPEv7 \
    --mode certify --months 9 --download --strict

Exemplo — série completa maio–novembro:
  python SPINE_GPEv7_PNAD_COVID_CERTIFIER_v1.0.0.py \
    --root /content/drive/MyDrive/aCidadeAlgoritmica/SPINE-GPEv7 \
    --mode certify --months all --download --strict
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import logging
import math
import os
import re
import shutil
import sys
import tempfile
import unicodedata
import zipfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception as exc:
    raise RuntimeError("pyarrow é obrigatório.") from exc

try:
    from scipy.stats import t as student_t
except Exception as exc:
    raise RuntimeError("scipy é obrigatório.") from exc

VERSION = "1.0.0"
SCHEMA_VERSION = "spine-gpe-v7-pnad-covid-delivery-1.0.0"
VALIDATION_SCHEMA_VERSION = "spine-gpe-v7-pnad-covid-validation-1.0.0"
UTC = dt.timezone.utc

DATA_BASE_URL = (
    "https://ftp.ibge.gov.br/Trabalho_e_Rendimento/"
    "Pesquisa_Nacional_por_Amostra_de_Domicilios_PNAD_COVID19/"
    "Microdados/Dados/"
)
DOC_BASE_URL = (
    "https://ftp.ibge.gov.br/Trabalho_e_Rendimento/"
    "Pesquisa_Nacional_por_Amostra_de_Domicilios_PNAD_COVID19/"
    "Microdados/Documentacao/"
)
DEFLATOR_URL = DOC_BASE_URL + "Deflatores.zip"
VALID_MONTHS = tuple(range(5, 12))
PRIMARY_MONTH = 9

# Contrato semântico oficial congelado para o estimando desta etapa.
SEMANTIC_CONTRACT: dict[str, Any] = {
    "source": "PNAD COVID-19 2020",
    "evidence_tier": "B",
    "estimand": "pandemic_delivery_observed",
    "direct_platform_identification": False,
    "occupation_variable": "C007C",
    "occupation_codes": {
        "16": "Motoboy",
        "17": (
            "Entregador de mercadorias (de restaurante, de farmácia, de loja, "
            "Uber Eats, IFood, Rappy etc.)"
        ),
    },
    "position_variable": "C007",
    "self_employed_code": "7",
    "social_security_variable": "C014",
    "weight": "V1032",
    "stratum": "Estrato",
    "psu": "UPA",
    "month": "V1013",
    "usual_hours": "C008",
    "actual_hours": "C009",
    "usual_cash_income": "C01012",
    "actual_cash_income": "C011A12",
    "epistemic_limit": (
        "A ocupação é observada, mas a intermediação por plataforma não é "
        "observada diretamente. Citações de aplicativos no rótulo de C007C=17 "
        "não transformam a categoria em identificação direta."
    ),
}

RAW_KEEP = [
    "Ano", "UF", "CAPITAL", "RM_RIDE", "UPA", "Estrato", "V1008",
    "V1012", "V1013", "V1016", "V1022", "V1023", "V1030", "V1031",
    "V1032", "posest", "A001", "A001A", "A001B1", "A001B2", "A001B3",
    "A002", "A003", "A004", "A005", "C001", "C002", "C006", "C007",
    "C007A", "C007B", "C007C", "C007D", "C008", "C009", "C010",
    "C0101", "C01012", "C0102", "C01022", "C011A", "C011A1",
    "C011A12", "C011A2", "C011A22", "C012", "C013", "C014",
]

OUTPUT_COLUMNS = [
    "source", "source_year", "reference_month", "reference_week",
    "record_index_source", "panel_person_key_hash", "panel_household_key_hash",
    "panel_key_quality", "UF", "CAPITAL", "RM_RIDE", "V1022", "V1023",
    "survey_weight", "survey_weight_pre_post", "survey_population_projection",
    "survey_stratum", "survey_psu", "survey_poststratum", "person_order",
    "age", "sex_code", "race_code", "education_code", "occupied_observed",
    "eligible_occupation_module", "C007", "C007B", "C007C", "C007D",
    "pandemic_motoboy_observed", "pandemic_goods_delivery_observed",
    "pandemic_delivery_observed", "pandemic_delivery_code",
    "pandemic_delivery_label", "pandemic_delivery_self_employed",
    "employee_formal_observed", "employee_without_card_observed",
    "social_security_contributor", "weekly_hours_usual", "weekly_hours_actual",
    "monthly_income_usual_cash_nominal", "monthly_income_usual_products_nominal",
    "monthly_income_usual_total_nominal", "monthly_income_actual_cash_nominal",
    "platform_delivery_direct", "platform_direct_available", "evidence_tier",
    "measurement_status", "identification_method", "no_proxy_fill",
    "synthetic_location", "source_file_sha256",
]


@dataclass
class TestResult:
    test_id: str
    status: str
    severity: str
    message: str
    month: int | None = None
    observed: Any = None
    expected: Any = None
    evidence: dict[str, Any] | None = None


@dataclass
class SurveyEstimate:
    year: int
    month: int
    domain: str
    geography: str
    geography_code: str
    estimand: str
    estimate: float | None
    se: float | None
    ci_low: float | None
    ci_high: float | None
    cv_percent: float | None
    n_universe: int
    n_positive: int
    n_outcome_valid: int
    n_effective_domain: float | None
    n_psu: int
    n_strata: int
    df_design: int
    precision_status: str
    notes: str


def utc_now() -> str:
    return dt.datetime.now(UTC).isoformat()


def run_id() -> str:
    return dt.datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def sha256_file(path: Path, chunk: int = 2**20) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            data = handle.read(chunk)
            if not data:
                break
            digest.update(data)
    return digest.hexdigest()


def json_dump(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def normalize_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", text.lower()).strip()


def nullable_bool(mask: pd.Series, eligible: pd.Series) -> pd.Series:
    out = pd.Series(pd.NA, index=mask.index, dtype="boolean")
    e = eligible.fillna(False).astype(bool)
    out.loc[e] = mask.loc[e].fillna(False).astype(bool)
    return out


def numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def clean_code(series: pd.Series) -> pd.Series:
    out = series.astype("string").str.strip()
    out = out.str.replace(r"\.0$", "", regex=True)
    out = out.mask(out.isin(["", "nan", "None", "<NA>"]))
    return out


def create_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=6, connect=6, read=6, status=6, backoff_factor=1.2,
        status_forcelist=(429, 500, 502, 503, 504), allowed_methods=("GET", "HEAD"),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({"User-Agent": "SPINE-GPE-PNAD-COVID-Certifier/1.0"})
    return session


def parse_months(value: str) -> list[int]:
    raw = value.strip().lower()
    if raw == "all":
        return list(VALID_MONTHS)
    months: list[int] = []
    for token in re.split(r"[,;\s]+", raw):
        if not token:
            continue
        month = int(token)
        if month not in VALID_MONTHS:
            raise argparse.ArgumentTypeError("Meses válidos: 5 a 11, ou 'all'.")
        months.append(month)
    if not months:
        raise argparse.ArgumentTypeError("Informe ao menos um mês.")
    return sorted(set(months))


def setup_logger(verbose: bool = False) -> logging.Logger:
    logger = logging.getLogger("pnad_covid_certifier")
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(handler)
    return logger


def read_lock(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def validate_upstream_locks(root: Path, tests: list[TestResult]) -> dict[str, Any]:
    phase0_path = root / "00_admin" / "PHASE0_LOCK.json"
    phase0 = read_lock(phase0_path)
    if phase0.get("status") == "RELEASED":
        tests.append(TestResult("phase0.lock.released", "PASS", "critical", "Fase 0 estrutural liberada."))
    else:
        tests.append(TestResult("phase0.lock.released", "FAIL", "critical", "PHASE0_LOCK não está RELEASED.", observed=phase0.get("status"), expected="RELEASED"))

    pnadc_path = root / "00_admin" / "PNADC_CERTIFICATION_LOCK.json"
    pnadc = read_lock(pnadc_path)
    allowed = {"CORE_CERTIFIED", "CERTIFIED"}
    if pnadc.get("status") in allowed:
        tests.append(TestResult("pnadc.direct.core_certified", "PASS", "critical", "Núcleo PNADc direto certificado e apto a congelamento.", observed=pnadc.get("status")))
    else:
        tests.append(TestResult("pnadc.direct.core_certified", "FAIL", "critical", "Núcleo PNADc direto ainda não certificado.", observed=pnadc.get("status"), expected=sorted(allowed)))
    return {"phase0_path": str(phase0_path), "phase0": phase0, "pnadc_path": str(pnadc_path), "pnadc": pnadc}


def freeze_pnadc_core(root: Path, upstream: Mapping[str, Any], current_run: str) -> dict[str, Any]:
    pnadc_lock_path = Path(str(upstream["pnadc_path"]))
    lock = upstream.get("pnadc", {})
    outputs = lock.get("outputs", {}) if isinstance(lock, dict) else {}
    candidates = [
        root / "03_processed" / "10_pnadc_certified" / "certified_pnadc_platform_2022.parquet",
        root / "03_processed" / "10_pnadc_certified" / "certified_pnadc_platform_2024.parquet",
        root / "03_processed" / "10_pnadc_certified" / "certified_pnadc_platform_pooled.parquet",
    ]
    for value in outputs.values() if isinstance(outputs, dict) else []:
        candidates.append(Path(value))
    frozen: dict[str, str] = {}
    for path in candidates:
        if path.exists() and str(path) not in frozen:
            frozen[str(path)] = sha256_file(path)
    payload = {
        "run_id": current_run,
        "created_at_utc": utc_now(),
        "upstream_lock": str(pnadc_lock_path),
        "upstream_lock_sha256": sha256_file(pnadc_lock_path) if pnadc_lock_path.exists() else None,
        "upstream_status": lock.get("status") if isinstance(lock, dict) else None,
        "frozen_artifacts": frozen,
        "rule": "read_only_upstream; nenhuma mutação dos artefatos PNADc nesta etapa",
    }
    path = root / "00_admin" / "PNADC_DIRECT_CORE_FREEZE.json"
    json_dump(path, payload)
    payload["path"] = str(path)
    return payload


def local_search_dirs(root: Path) -> list[Path]:
    return [
        root / "01_raw" / "10_ibge" / "pnad_covid_2020",
        root / "01_raw" / "10_ibge" / "pnad_covid",
        root / "data_pnad_covid",
        root / "uploads",
        root / "data",
        root,
    ]


def find_month_source(root: Path, month: int) -> Path | None:
    stem = f"PNAD_COVID_{month:02d}2020"
    patterns = [f"{stem}.csv", f"{stem}.CSV", f"{stem}.zip", f"{stem}_*.zip"]
    found: list[Path] = []
    for directory in local_search_dirs(root):
        if not directory.exists():
            continue
        for pattern in patterns:
            found.extend(directory.glob(pattern))
            found.extend(directory.glob(f"**/{pattern}"))
    found = [p for p in found if p.is_file() and "sample_data" not in p.parts]
    if not found:
        return None
    # CSV local tem prioridade; depois arquivo mais recente.
    found.sort(key=lambda p: (p.suffix.lower() != ".csv", -p.stat().st_mtime))
    return found[0]


def find_dictionary(root: Path, month: int) -> Path | None:
    token = f"Dicionario_PNAD_COVID_{month:02d}2020"
    found: list[Path] = []
    for directory in local_search_dirs(root):
        if not directory.exists():
            continue
        for suffix in ("*.xls", "*.xlsx"):
            found.extend(directory.glob(f"**/{token}{suffix}"))
            found.extend(directory.glob(f"**/{token}_*{suffix}"))
    found = [p for p in found if p.is_file()]
    return max(found, key=lambda p: p.stat().st_mtime) if found else None


def download_file(session: requests.Session, url: str, destination: Path, logger: logging.Logger) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp = destination.with_suffix(destination.suffix + ".part")
    logger.info("Baixando %s", url)
    with session.get(url, stream=True, timeout=180) as response:
        response.raise_for_status()
        with temp.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=2**20):
                if chunk:
                    handle.write(chunk)
    temp.replace(destination)
    return destination


def remote_dictionary_url(session: requests.Session, month: int) -> str | None:
    try:
        response = session.get(DOC_BASE_URL, timeout=60)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        prefix = normalize_text(f"Dicionario_PNAD_COVID_{month:02d}2020")
        candidates: list[str] = []
        for anchor in soup.find_all("a", href=True):
            href = anchor.get("href", "")
            if prefix in normalize_text(href) and href.lower().endswith((".xls", ".xlsx")):
                candidates.append(requests.compat.urljoin(DOC_BASE_URL, href))
        return sorted(candidates)[-1] if candidates else None
    except Exception:
        return None


def acquire_month(root: Path, month: int, download: bool, session: requests.Session, logger: logging.Logger) -> tuple[Path | None, Path | None]:
    raw_dir = root / "01_raw" / "10_ibge" / "pnad_covid_2020" / f"2020_{month:02d}"
    raw_dir.mkdir(parents=True, exist_ok=True)
    source = find_month_source(root, month)
    if source is None and download:
        source = download_file(session, DATA_BASE_URL + f"PNAD_COVID_{month:02d}2020.zip", raw_dir / f"PNAD_COVID_{month:02d}2020.zip", logger)
    dictionary = find_dictionary(root, month)
    if dictionary is None and download:
        url = remote_dictionary_url(session, month)
        if url:
            dictionary = download_file(session, url, raw_dir / Path(url).name, logger)
    return source, dictionary


def extract_csv(source: Path, cache_dir: Path, month: int, logger: logging.Logger) -> Path:
    if source.suffix.lower() == ".csv":
        return source
    cache_dir.mkdir(parents=True, exist_ok=True)
    target = cache_dir / f"PNAD_COVID_{month:02d}2020.csv"
    if target.exists() and target.stat().st_size > 0:
        return target
    if source.suffix.lower() != ".zip":
        raise ValueError(f"Formato não suportado: {source}")
    with zipfile.ZipFile(source) as archive:
        members = [m for m in archive.namelist() if m.lower().endswith(".csv")]
        if not members:
            raise RuntimeError(f"ZIP sem CSV: {source}")
        preferred = [m for m in members if f"_{month:02d}2020" in m]
        member = preferred[0] if preferred else members[0]
        logger.info("Extraindo %s -> %s", member, target)
        with archive.open(member) as src, target.open("wb") as dst:
            shutil.copyfileobj(src, dst)
    return target


def read_header(path: Path) -> list[str]:
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return list(pd.read_csv(path, nrows=0, encoding=encoding).columns)
        except UnicodeDecodeError:
            continue
    raise RuntimeError(f"Não foi possível ler cabeçalho de {path}")


def dictionary_evidence(path: Path | None) -> dict[str, Any]:
    evidence: dict[str, Any] = {"path": str(path) if path else None, "available": bool(path and path.exists())}
    if not path or not path.exists():
        return evidence
    evidence["sha256"] = sha256_file(path)
    cells: list[str] = []
    errors: list[str] = []
    try:
        book = pd.ExcelFile(path)
        evidence["sheets"] = book.sheet_names
        for sheet in book.sheet_names:
            try:
                frame = pd.read_excel(path, sheet_name=sheet, header=None, dtype="string")
                for value in frame.fillna("").astype(str).to_numpy().ravel().tolist():
                    if value.strip():
                        cells.append(value.strip())
            except Exception as exc:
                errors.append(f"{sheet}: {exc}")
    except Exception as exc:
        errors.append(str(exc))
    corpus = normalize_text(" | ".join(cells))
    evidence.update({
        "contains_C007C": "c007c" in corpus,
        "contains_code_16_motoboy": "motoboy" in corpus,
        "contains_code_17_delivery": "entregador de mercadorias" in corpus,
        "contains_app_examples": any(token in corpus for token in ("uber eats", "ifood", "rappy", "rappi")),
        "contains_C007_self_employed": "conta propria" in corpus,
        "contains_C014": "c014" in corpus,
        "errors": errors,
    })
    return evidence


def hash_key(parts: Sequence[pd.Series]) -> pd.Series:
    if not parts:
        return pd.Series(dtype="string")
    joined = parts[0].astype("string").fillna("<NA>")
    for part in parts[1:]:
        joined = joined + "|" + part.astype("string").fillna("<NA>")
    return joined.map(lambda x: hashlib.sha256(x.encode("utf-8")).hexdigest()[:32]).astype("string")


def transform_chunk(raw: pd.DataFrame, month: int, source_hash: str, offset: int) -> pd.DataFrame:
    frame = pd.DataFrame(index=raw.index)
    def col(name: str) -> pd.Series:
        return raw[name] if name in raw else pd.Series(pd.NA, index=raw.index, dtype="string")

    codes = {name: clean_code(col(name)) for name in ("C001", "C002", "C007", "C007B", "C007C", "C007D", "C014")}
    eligible = codes["C007C"].notna()
    occupied = codes["C001"].eq("1") | codes["C002"].eq("1") | eligible
    motoboy = codes["C007C"].eq("16")
    goods = codes["C007C"].eq("17")
    delivery = motoboy | goods

    frame["source"] = "PNAD_COVID19"
    frame["source_year"] = 2020
    frame["reference_month"] = month
    frame["reference_week"] = numeric(col("V1012")).astype("Int16")
    frame["record_index_source"] = np.arange(offset, offset + len(raw), dtype=np.int64)

    household_parts = [clean_code(col("UPA")), clean_code(col("V1008"))]
    person_parts = household_parts + [clean_code(col("A001")), clean_code(col("A001A")), clean_code(col("A003")), clean_code(col("A001B1")), clean_code(col("A001B2")), clean_code(col("A001B3"))]
    frame["panel_household_key_hash"] = hash_key(household_parts)
    frame["panel_person_key_hash"] = hash_key(person_parts)
    complete = pd.concat(person_parts, axis=1).notna().all(axis=1)
    frame["panel_key_quality"] = np.where(complete, "full_demographic_linkage_key", "partial_linkage_key")

    frame["UF"] = clean_code(col("UF"))
    frame["CAPITAL"] = clean_code(col("CAPITAL"))
    frame["RM_RIDE"] = clean_code(col("RM_RIDE"))
    frame["V1022"] = clean_code(col("V1022"))
    frame["V1023"] = clean_code(col("V1023"))
    frame["survey_weight"] = numeric(col("V1032")).astype("Float64")
    frame["survey_weight_pre_post"] = numeric(col("V1031")).astype("Float64")
    frame["survey_population_projection"] = numeric(col("V1030")).astype("Float64")
    frame["survey_stratum"] = clean_code(col("Estrato"))
    frame["survey_psu"] = clean_code(col("UPA"))
    frame["survey_poststratum"] = clean_code(col("posest"))
    frame["person_order"] = numeric(col("A001")).astype("Int16")
    frame["age"] = numeric(col("A002")).astype("Float64")
    frame["sex_code"] = clean_code(col("A003"))
    frame["race_code"] = clean_code(col("A004"))
    frame["education_code"] = clean_code(col("A005"))
    frame["occupied_observed"] = nullable_bool(occupied, pd.Series(True, index=raw.index))
    frame["eligible_occupation_module"] = eligible.astype("boolean")
    for name in ("C007", "C007B", "C007C", "C007D"):
        frame[name] = codes[name]

    frame["pandemic_motoboy_observed"] = nullable_bool(motoboy, eligible)
    frame["pandemic_goods_delivery_observed"] = nullable_bool(goods, eligible)
    frame["pandemic_delivery_observed"] = nullable_bool(delivery, eligible)
    frame["pandemic_delivery_code"] = codes["C007C"].where(delivery)
    labels = pd.Series(pd.NA, index=raw.index, dtype="string")
    labels.loc[motoboy] = SEMANTIC_CONTRACT["occupation_codes"]["16"]
    labels.loc[goods] = SEMANTIC_CONTRACT["occupation_codes"]["17"]
    frame["pandemic_delivery_label"] = labels
    frame["pandemic_delivery_self_employed"] = nullable_bool(codes["C007"].eq("7") & delivery, delivery)

    employee_relevant = codes["C007"].isin(["1", "4", "5"])
    frame["employee_formal_observed"] = nullable_bool(codes["C007B"].isin(["1", "2"]), employee_relevant)
    frame["employee_without_card_observed"] = nullable_bool(codes["C007B"].eq("3"), employee_relevant)
    frame["social_security_contributor"] = nullable_bool(codes["C014"].eq("1"), codes["C014"].notna())
    frame["weekly_hours_usual"] = numeric(col("C008")).where(lambda s: s.between(0, 120)).astype("Float64")
    frame["weekly_hours_actual"] = numeric(col("C009")).where(lambda s: s.between(0, 120)).astype("Float64")
    cash = numeric(col("C01012")).where(lambda s: s.ge(0))
    products = numeric(col("C01022")).where(lambda s: s.ge(0))
    frame["monthly_income_usual_cash_nominal"] = cash.astype("Float64")
    frame["monthly_income_usual_products_nominal"] = products.astype("Float64")
    total_income = cash.fillna(0) + products.fillna(0)
    total_income = total_income.mask(cash.isna() & products.isna())
    frame["monthly_income_usual_total_nominal"] = total_income.astype("Float64")
    frame["monthly_income_actual_cash_nominal"] = numeric(col("C011A12")).where(lambda s: s.ge(0)).astype("Float64")

    # Variável deliberadamente ausente: não há pergunta direta de plataforma.
    frame["platform_delivery_direct"] = pd.Series(pd.NA, index=raw.index, dtype="boolean")
    frame["platform_direct_available"] = False
    frame["evidence_tier"] = "B"
    frame["measurement_status"] = "observed_pandemic_delivery_occupation"
    frame["identification_method"] = "C007C_in_16_17_no_direct_platform_question"
    frame["no_proxy_fill"] = True
    frame["synthetic_location"] = False
    frame["source_file_sha256"] = source_hash
    return frame[OUTPUT_COLUMNS]


def write_month_to_writer(csv_path: Path, month: int, source_hash: str, writer: pq.ParquetWriter | None, output_path: Path, chunk_rows: int, logger: logging.Logger) -> tuple[pq.ParquetWriter, dict[str, Any]]:
    header = read_header(csv_path)
    usecols = [c for c in RAW_KEEP if c in header]
    required = {"Ano", "UF", "UPA", "Estrato", "V1032", "C007", "C007C"}
    missing = sorted(required - set(header))
    if missing:
        raise RuntimeError(f"CSV {month:02d}/2020 sem variáveis críticas: {missing}")
    rows = 0
    n_eligible = n_delivery = n_motoboy = n_goods = 0
    observed_c007c: set[str] = set()
    observed_month_codes: set[str] = set()
    observed_year_codes: set[str] = set()
    for raw in pd.read_csv(csv_path, dtype="string", usecols=usecols, chunksize=chunk_rows, low_memory=False):
        transformed = transform_chunk(raw, month, source_hash, rows)
        observed_c007c.update(transformed["C007C"].dropna().astype(str).unique().tolist())
        if "V1013" in raw:
            observed_month_codes.update(clean_code(raw["V1013"]).dropna().astype(str).unique().tolist())
        if "Ano" in raw:
            observed_year_codes.update(clean_code(raw["Ano"]).dropna().astype(str).unique().tolist())
        n_eligible += int(transformed["eligible_occupation_module"].fillna(False).sum())
        n_delivery += int(transformed["pandemic_delivery_observed"].fillna(False).sum())
        n_motoboy += int(transformed["pandemic_motoboy_observed"].fillna(False).sum())
        n_goods += int(transformed["pandemic_goods_delivery_observed"].fillna(False).sum())
        table = pa.Table.from_pandas(transformed, preserve_index=False)
        if writer is None:
            metadata = dict(table.schema.metadata or {})
            metadata.update({
                b"spine_schema_version": SCHEMA_VERSION.encode(),
                b"measurement": b"pandemic_delivery_observed",
                b"platform_direct_available": b"false",
                b"evidence_tier": b"B",
                b"no_proxy_fill": b"true",
            })
            schema = table.schema.with_metadata(metadata)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            writer = pq.ParquetWriter(output_path, schema=schema, compression="zstd")
            table = table.cast(schema)
        writer.write_table(table)
        rows += len(transformed)
        logger.info("Mês %02d/2020: %d registros processados", month, rows)
    if writer is None:
        raise RuntimeError(f"CSV vazio: {csv_path}")
    return writer, {
        "month": month, "rows": rows, "eligible_occupation_module": n_eligible,
        "pandemic_delivery_observed": n_delivery, "motoboy": n_motoboy,
        "goods_delivery": n_goods, "observed_C007C_codes": sorted(observed_c007c),
        "observed_month_codes": sorted(observed_month_codes),
        "observed_year_codes": sorted(observed_year_codes),
    }


def kish_effective_n(weights: pd.Series) -> float | None:
    w = numeric(weights).dropna()
    w = w[w > 0]
    if w.empty:
        return None
    denom = float(np.square(w).sum())
    return float(w.sum() ** 2 / denom) if denom > 0 else None


def cluster_variance(linearized: pd.Series, strata: pd.Series, psu: pd.Series) -> tuple[float | None, int, int, int]:
    temp = pd.DataFrame({"z": linearized, "h": strata.astype("string"), "psu": psu.astype("string")}).dropna()
    if temp.empty:
        return None, 0, 0, 0
    cluster = temp.groupby(["h", "psu"], observed=True)["z"].sum().reset_index()
    variance = 0.0
    singleton = 0
    for _, group in cluster.groupby("h", observed=True):
        m = len(group)
        if m < 2:
            singleton += 1
            continue
        values = group["z"].to_numpy(dtype=float)
        variance += (m / (m - 1.0)) * float(np.square(values - values.mean()).sum())
    return variance, int(cluster["psu"].nunique()), int(cluster["h"].nunique()), singleton


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


def survey_total(frame: pd.DataFrame, indicator: pd.Series, month: int, domain: str, geography: str = "Brasil", geography_code: str = "BR") -> SurveyEstimate:
    valid = frame[["survey_weight", "survey_stratum", "survey_psu"]].copy()
    valid["y"] = numeric(indicator.reindex(frame.index))
    valid["w"] = numeric(valid["survey_weight"])
    valid = valid.dropna(subset=["y", "w", "survey_stratum", "survey_psu"])
    valid = valid[valid["w"] > 0]
    estimate = float((valid["w"] * valid["y"]).sum()) if not valid.empty else None
    variance, n_psu, n_strata, singleton = cluster_variance(valid["w"] * valid["y"], valid["survey_stratum"], valid["survey_psu"])
    se = math.sqrt(variance) if variance is not None and variance >= 0 else None
    df = max(n_psu - n_strata, 1)
    crit = float(student_t.ppf(0.975, df)) if df > 1 else 1.96
    low = estimate - crit * se if estimate is not None and se is not None else None
    high = estimate + crit * se if estimate is not None and se is not None else None
    cv = abs(se / estimate * 100) if estimate not in (None, 0) and se is not None else None
    positive = valid["y"].gt(0)
    eff = kish_effective_n(valid.loc[positive, "w"] * valid.loc[positive, "y"].clip(lower=0))
    n_positive = int(positive.sum())
    return SurveyEstimate(2020, month, domain, geography, geography_code, "total", estimate, se, low, high, cv, len(valid), n_positive, len(valid), eff, n_psu, n_strata, df, precision_status(cv, eff, n_positive), f"Taylor linearization UPA/Estrato; singleton omitidos={singleton}; sem FPC.")


def survey_mean(frame: pd.DataFrame, outcome: pd.Series, domain_indicator: pd.Series, month: int, domain: str, estimand: str) -> SurveyEstimate:
    valid = frame[["survey_weight", "survey_stratum", "survey_psu"]].copy()
    valid["y"] = numeric(outcome.reindex(frame.index))
    valid["d"] = numeric(domain_indicator.reindex(frame.index)).fillna(0)
    valid["w"] = numeric(valid["survey_weight"])
    valid = valid.dropna(subset=["w", "survey_stratum", "survey_psu"])
    valid = valid[valid["w"] > 0]
    active = valid["d"].gt(0) & valid["y"].notna()
    denom = float((valid.loc[active, "w"] * valid.loc[active, "d"]).sum())
    estimate = float((valid.loc[active, "w"] * valid.loc[active, "d"] * valid.loc[active, "y"]).sum() / denom) if denom > 0 else None
    linearized = pd.Series(0.0, index=valid.index)
    if estimate is not None:
        linearized.loc[active] = valid.loc[active, "w"] * valid.loc[active, "d"] * (valid.loc[active, "y"] - estimate) / denom
    variance, n_psu, n_strata, singleton = cluster_variance(linearized, valid["survey_stratum"], valid["survey_psu"])
    se = math.sqrt(variance) if variance is not None and variance >= 0 else None
    df = max(n_psu - n_strata, 1)
    crit = float(student_t.ppf(0.975, df)) if df > 1 else 1.96
    low = estimate - crit * se if estimate is not None and se is not None else None
    high = estimate + crit * se if estimate is not None and se is not None else None
    cv = abs(se / estimate * 100) if estimate not in (None, 0) and se is not None else None
    eff = kish_effective_n(valid.loc[active, "w"] * valid.loc[active, "d"])
    n_positive = int(valid["d"].gt(0).sum())
    n_valid = int(active.sum())
    return SurveyEstimate(2020, month, domain, "Brasil", "BR", estimand, estimate, se, low, high, cv, len(valid), n_positive, n_valid, eff, n_psu, n_strata, df, precision_status(cv, eff, n_valid), f"Taylor linearization UPA/Estrato; singleton omitidos={singleton}; sem FPC.")


def monthly_estimates(frame: pd.DataFrame, month: int) -> list[SurveyEstimate]:
    eligible = frame[frame["eligible_occupation_module"].fillna(False)].copy()
    if eligible.empty:
        return []
    estimates: list[SurveyEstimate] = []
    domains = {
        "pandemic_delivery_observed": eligible["pandemic_delivery_observed"].fillna(False).astype(float),
        "pandemic_motoboy_observed": eligible["pandemic_motoboy_observed"].fillna(False).astype(float),
        "pandemic_goods_delivery_observed": eligible["pandemic_goods_delivery_observed"].fillna(False).astype(float),
    }
    population_frame = frame.dropna(subset=["survey_weight", "survey_stratum", "survey_psu"]).copy()
    estimates.append(survey_total(population_frame, pd.Series(1.0, index=population_frame.index), month, "weighted_population"))
    for name, indicator in domains.items():
        estimates.append(survey_total(eligible, indicator, month, name))
    delivery = domains["pandemic_delivery_observed"]
    for col, estimand in (
        ("pandemic_delivery_self_employed", "percent_self_employed"),
        ("social_security_contributor", "percent_social_security"),
        ("weekly_hours_usual", "mean_weekly_hours_usual"),
        ("weekly_hours_actual", "mean_weekly_hours_actual"),
        ("monthly_income_usual_cash_nominal", "mean_monthly_income_usual_cash_nominal"),
        ("monthly_income_usual_total_nominal", "mean_monthly_income_usual_total_nominal"),
    ):
        if col not in eligible:
            continue
        outcome = eligible[col].astype("Float64")
        if estimand.startswith("percent"):
            outcome = outcome * 100
        estimates.append(survey_mean(eligible, outcome, delivery, month, "pandemic_delivery_observed", estimand))
    return estimates


def design_summary(frame: pd.DataFrame, month: int) -> dict[str, Any]:
    valid = frame.dropna(subset=["survey_weight", "survey_stratum", "survey_psu"]).copy()
    valid["survey_weight"] = numeric(valid["survey_weight"])
    valid = valid[valid["survey_weight"] > 0]
    psu_by_stratum = valid.groupby("survey_stratum", observed=True)["survey_psu"].nunique()
    return {
        "year": 2020, "month": month, "status": "READY" if not valid.empty else "BLOCKED",
        "variables": {"weight": "V1032", "pre_post_weight": "V1031", "stratum": "Estrato", "psu": "UPA", "poststratum": "posest"},
        "n_records": int(len(frame)), "n_design_valid": int(len(valid)),
        "weight_sum": float(valid["survey_weight"].sum()) if not valid.empty else None,
        "weight_min": float(valid["survey_weight"].min()) if not valid.empty else None,
        "weight_max": float(valid["survey_weight"].max()) if not valid.empty else None,
        "weight_cv_percent": float(valid["survey_weight"].std() / valid["survey_weight"].mean() * 100) if len(valid) > 1 else None,
        "kish_effective_n": kish_effective_n(valid["survey_weight"]),
        "n_strata": int(valid["survey_stratum"].nunique()), "n_psu": int(valid["survey_psu"].nunique()),
        "design_df": int(valid["survey_psu"].nunique() - valid["survey_stratum"].nunique()),
        "singleton_strata": int((psu_by_stratum < 2).sum()) if not psu_by_stratum.empty else None,
        "psu_per_stratum_min": int(psu_by_stratum.min()) if not psu_by_stratum.empty else None,
        "psu_per_stratum_median": float(psu_by_stratum.median()) if not psu_by_stratum.empty else None,
        "psu_per_stratum_max": int(psu_by_stratum.max()) if not psu_by_stratum.empty else None,
        "estimator_note": "V1032 pós-estratificado; Taylor linearization ultimate-cluster por Estrato/UPA; sem pooling ingênuo entre meses.",
    }


def load_month_frame(parquet_path: Path, month: int) -> pd.DataFrame:
    columns = [
        "reference_month", "eligible_occupation_module", "pandemic_delivery_observed",
        "pandemic_motoboy_observed", "pandemic_goods_delivery_observed",
        "pandemic_delivery_self_employed", "social_security_contributor",
        "weekly_hours_usual", "weekly_hours_actual", "monthly_income_usual_cash_nominal",
        "monthly_income_usual_total_nominal", "survey_weight", "survey_stratum", "survey_psu",
        "platform_delivery_direct", "platform_direct_available", "C007C",
    ]
    return pd.read_parquet(parquet_path, columns=columns, filters=[("reference_month", "=", month)])


def build_tests(month: int, frame: pd.DataFrame, source_manifest: Mapping[str, Any], dictionary: Mapping[str, Any], design: Mapping[str, Any]) -> list[TestResult]:
    tests: list[TestResult] = []
    n = len(frame)
    observed_months = {str(x).zfill(2) for x in source_manifest.get("observed_month_codes", [])}
    expected_month = f"{month:02d}"
    month_ok = observed_months == {expected_month}
    tests.append(TestResult(f"2020m{month:02d}.period.month_matches_source", "PASS" if month_ok else "FAIL", "critical", "V1013 coincide com o mês solicitado." if month_ok else "V1013 diverge do mês solicitado.", month, sorted(observed_months), [expected_month]))
    observed_years = set(source_manifest.get("observed_year_codes", []))
    year_ok = observed_years == {"2020"}
    tests.append(TestResult(f"2020m{month:02d}.period.year_2020", "PASS" if year_ok else "FAIL", "critical", "Ano da fonte é 2020." if year_ok else "Ano da fonte diverge de 2020.", month, sorted(observed_years), ["2020"]))
    tests.append(TestResult(f"2020m{month:02d}.records.plausible", "PASS" if 250000 <= n <= 500000 else "FAIL", "critical", "Número de registros dentro da faixa plausível mensal." if 250000 <= n <= 500000 else "Número de registros fora da faixa plausível mensal.", month, n, "250000..500000"))
    tests.append(TestResult(f"2020m{month:02d}.survey.design_ready", "PASS" if design.get("status") == "READY" else "FAIL", "critical", "Desenho survey reconstruído com V1032/Estrato/UPA.", month, design.get("status"), "READY"))
    weight_sum = design.get("weight_sum")
    population_ok = weight_sum is not None and 190_000_000 <= weight_sum <= 230_000_000
    tests.append(TestResult(f"2020m{month:02d}.weighted_population.plausible", "PASS" if population_ok else "FAIL", "critical", "Soma dos pesos pós-estratificados é plausível para a população nacional de 2020.", month, weight_sum, "190M..230M"))

    observed = set(frame["C007C"].dropna().astype(str).unique())
    invalid = sorted(code for code in observed if code.isdigit() and not (1 <= int(code) <= 36))
    tests.append(TestResult(f"2020m{month:02d}.domain.C007C", "PASS" if not invalid else "FAIL", "critical", "Códigos observados de C007C pertencem ao domínio oficial 01–36." if not invalid else "C007C contém códigos fora do domínio.", month, sorted(observed), "01..36", {"invalid": invalid}))

    delivery = frame["pandemic_delivery_observed"].fillna(False)
    partition = frame["pandemic_motoboy_observed"].fillna(False).astype(int) + frame["pandemic_goods_delivery_observed"].fillna(False).astype(int)
    partition_ok = bool((partition.eq(delivery.astype(int))).all())
    tests.append(TestResult(f"2020m{month:02d}.delivery.partition", "PASS" if partition_ok else "FAIL", "critical", "Entrega observada particionada exatamente entre C007C=16 e C007C=17.", month))
    occupied_delivery_ok = bool(frame.loc[delivery, "eligible_occupation_module"].fillna(False).all())
    tests.append(TestResult(f"2020m{month:02d}.delivery.within_occupation_universe", "PASS" if occupied_delivery_ok else "FAIL", "critical", "Todos os casos de entrega pertencem ao universo observado de C007C.", month))

    platform_na = frame["platform_delivery_direct"].isna().all()
    tests.append(TestResult(f"2020m{month:02d}.platform_direct.absent", "PASS" if platform_na else "FAIL", "critical", "platform_delivery_direct permanece NA; nenhuma proxy foi promovida a identificação direta.", month, bool(platform_na), True))
    unavailable = (~frame["platform_direct_available"].fillna(True).astype(bool)).all()
    tests.append(TestResult(f"2020m{month:02d}.platform_direct.flag", "PASS" if unavailable else "FAIL", "critical", "Flag registra indisponibilidade de identificação direta de plataforma.", month))

    dict_core = bool(dictionary.get("contains_C007C") and dictionary.get("contains_code_16_motoboy") and dictionary.get("contains_code_17_delivery"))
    status = "PASS" if dict_core else "WARN"
    tests.append(TestResult(f"2020m{month:02d}.dictionary.delivery_labels", status, "high", "Dicionário oficial confirma C007C, Motoboy e Entregador de mercadorias." if dict_core else "Dicionário ausente ou leitura não confirmou todos os rótulos; contrato oficial congelado foi usado.", month, dictionary, {"C007C": {"16": "Motoboy", "17": "Entregador de mercadorias"}}))

    positives = int(delivery.sum())
    tests.append(TestResult(f"2020m{month:02d}.delivery.sample_positive", "PASS" if positives >= 30 else "FAIL", "critical", "Amostra positiva nacional suficiente para estimativa descritiva." if positives >= 30 else "Menos de 30 casos positivos nacionais.", month, positives, ">=30"))
    return tests


def render_report(path: Path, current_run: str, status: str, months: list[int], source_manifests: Mapping[int, Any], freeze: Mapping[str, Any], designs: Mapping[int, Any], estimates: list[SurveyEstimate], tests: list[TestResult], output_path: Path) -> None:
    lines = [
        "# SPINE-GPE v7 — Relatório de Certificação PNAD COVID 2020",
        "",
        f"- Run ID: `{current_run}`",
        f"- Versão: `{VERSION}`",
        f"- Schema: `{SCHEMA_VERSION}`",
        f"- Validation schema: `{VALIDATION_SCHEMA_VERSION}`",
        f"- Status: **{status}**",
        f"- Meses: `{','.join(f'{m:02d}' for m in months)}`",
        f"- Output: `{output_path}`",
        "",
        "## Limite epistemológico congelado",
        "",
        "`pandemic_delivery_observed` é ocupação observada em C007C=16/17. `platform_delivery_direct` permanece NA. O enunciado de C007C=17 menciona exemplos de apps, mas a pesquisa não pergunta diretamente pela intermediação por plataforma.",
        "",
        "## Upstream PNADc congelado",
        "",
        f"- Status upstream: `{freeze.get('upstream_status')}`",
        f"- Freeze: `{freeze.get('path')}`",
        f"- Artefatos congelados: `{len(freeze.get('frozen_artifacts', {}))}`",
        "",
        "## Fontes",
        "",
        "| Mês | Registros | Entrega | Motoboy | Entregador mercadorias | CSV | Dicionário |",
        "|---:|---:|---:|---:|---:|---|---|",
    ]
    for month in months:
        manifest = source_manifests[month]
        lines.append(f"| {month:02d} | {manifest.get('rows')} | {manifest.get('pandemic_delivery_observed')} | {manifest.get('motoboy')} | {manifest.get('goods_delivery')} | `{manifest.get('csv_path')}` | `{manifest.get('dictionary_path')}` |")
    lines += ["", "## Desenho amostral", ""]
    for month in months:
        lines += [f"### 2020-{month:02d}", "", "```json", json.dumps(designs[month], ensure_ascii=False, indent=2), "```", ""]
    lines += ["## Golden tests", "", "| Status | Severidade | Mês | Teste | Mensagem |", "|---|---|---:|---|---|"]
    for test in tests:
        lines.append(f"| {test.status} | {test.severity} | {test.month or ''} | `{test.test_id}` | {test.message} |")
    lines += ["", "## Estimativas mensais design-based", "", "| Mês | Domínio | Estimando | Estimativa | SE | CV% | n positivo | n outcome | n efetivo | Precisão |", "|---:|---|---|---:|---:|---:|---:|---:|---:|---|"]
    for e in estimates:
        lines.append(f"| {e.month:02d} | {e.domain} | {e.estimand} | {'' if e.estimate is None else f'{e.estimate:.4f}'} | {'' if e.se is None else f'{e.se:.4f}'} | {'' if e.cv_percent is None else f'{e.cv_percent:.2f}'} | {e.n_positive} | {e.n_outcome_valid} | {'' if e.n_effective_domain is None else f'{e.n_effective_domain:.1f}'} | {e.precision_status} |")
    lines += [
        "", "## Regras congeladas", "",
        "1. C007C=16/17 mede ocupação de entrega no choque pandêmico, não plataforma direta.",
        "2. C007=7 significa conta própria; não é, isoladamente, identificação de plataforma.",
        "3. Registros fora do universo de C007C permanecem NA, não são recodificados como não entregadores.",
        "4. O Parquet com vários meses é pessoa-mês em amostra fixa/repetida; não é uma repeated cross-section independente.",
        "5. Erros-padrão são calculados por mês; pooling longitudinal exige método próprio na fase analítica.",
        "6. Renda é nominal nesta versão; harmonização monetária é trilha separada da Fase 0C.",
        "7. Nenhuma localização fina ou plataforma foi imputada.",
        "", "## Próximo gate", "",
        "Após este lock, certificar a PNADc histórica e construir a calibração da proxy ocupação × atividade contra a PNADc direta 2022/2024.",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def audit_mode(root: Path, months: list[int], download: bool, strict: bool, logger: logging.Logger) -> int:
    current_run = run_id()
    tests: list[TestResult] = []
    upstream = validate_upstream_locks(root, tests)
    session = create_session()
    sources: dict[int, Any] = {}
    for month in months:
        source, dictionary = acquire_month(root, month, download, session, logger)
        if source is None:
            tests.append(TestResult(f"2020m{month:02d}.source.exists", "FAIL", "critical", "Arquivo PNAD COVID não encontrado.", month))
            continue
        tests.append(TestResult(f"2020m{month:02d}.source.exists", "PASS", "critical", "Arquivo PNAD COVID encontrado.", month, str(source)))
        sources[month] = {"source": str(source), "dictionary": str(dictionary) if dictionary else None, "source_sha256": sha256_file(source)}
    critical = [asdict(t) for t in tests if t.severity == "critical" and t.status in {"FAIL", "BLOCKED"}]
    status = "AUDIT_BLOCKED" if critical else "AUDIT_PASSED"
    payload = {"run_id": current_run, "version": VERSION, "status": status, "months": months, "sources": sources, "tests": [asdict(t) for t in tests], "created_at_utc": utc_now()}
    json_dump(root / "00_admin" / "PNAD_COVID_AUDIT_LOCK.json", payload)
    logger.info("Auditoria PNAD COVID concluída | status=%s", status)
    return 2 if strict and critical else 0


def certify_mode(root: Path, months: list[int], download: bool, strict: bool, chunk_rows: int, cache_dir: Path, logger: logging.Logger) -> int:
    current_run = run_id()
    tests: list[TestResult] = []
    upstream = validate_upstream_locks(root, tests)
    if any(t.status == "FAIL" and t.severity == "critical" for t in tests):
        status = "BLOCKED"
        lock = {"run_id": current_run, "script_version": VERSION, "status": status, "critical_failures": [asdict(t) for t in tests if t.status == "FAIL"], "created_at_utc": utc_now()}
        json_dump(root / "00_admin" / "PNAD_COVID_CERTIFICATION_LOCK.json", lock)
        return 2
    freeze = freeze_pnadc_core(root, upstream, current_run)
    session = create_session()
    output_dir = root / "03_processed" / "20_pnad_covid_certified"
    output_path = output_dir / "certified_pnad_covid_delivery_2020.parquet"
    if output_path.exists():
        backup = output_path.with_suffix(f".{current_run}.bak.parquet")
        output_path.replace(backup)
        logger.info("Output anterior preservado em %s", backup)

    source_manifests: dict[int, Any] = {}
    layout_months: dict[str, Any] = {}
    writer: pq.ParquetWriter | None = None
    try:
        for month in months:
            source, dictionary = acquire_month(root, month, download, session, logger)
            if source is None:
                tests.append(TestResult(f"2020m{month:02d}.source.exists", "FAIL", "critical", "Arquivo PNAD COVID não encontrado.", month))
                continue
            csv_path = extract_csv(source, cache_dir, month, logger)
            source_hash = sha256_file(source)
            csv_hash = sha256_file(csv_path)
            header = read_header(csv_path)
            dict_ev = dictionary_evidence(dictionary)
            writer, summary = write_month_to_writer(csv_path, month, csv_hash, writer, output_path, chunk_rows, logger)
            summary.update({
                "source_path": str(source), "source_sha256": source_hash,
                "csv_path": str(csv_path), "csv_sha256": csv_hash,
                "dictionary_path": str(dictionary) if dictionary else None,
                "dictionary_evidence": dict_ev,
            })
            source_manifests[month] = summary
            layout_months[str(month)] = {
                "month": month, "source": str(source), "csv": str(csv_path),
                "header_columns": header, "n_columns": len(header),
                "required_columns": RAW_KEEP, "required_present": sorted(set(RAW_KEEP) & set(header)),
                "required_missing": sorted(set(RAW_KEEP) - set(header)),
                "dictionary": dict_ev,
            }
    finally:
        if writer is not None:
            writer.close()

    if writer is None or not output_path.exists():
        tests.append(TestResult("certified_table.created", "FAIL", "critical", "Parquet certificado não foi criado."))
    else:
        tests.append(TestResult("certified_table.created", "PASS", "critical", "Parquet candidato criado."))

    designs: dict[int, Any] = {}
    estimates: list[SurveyEstimate] = []
    if output_path.exists():
        for month in months:
            if month not in source_manifests:
                continue
            frame = load_month_frame(output_path, month)
            design = design_summary(frame, month)
            designs[month] = design
            estimates.extend(monthly_estimates(frame, month))
            tests.extend(build_tests(month, frame, source_manifests[month], source_manifests[month]["dictionary_evidence"], design))

        months_in_output = sorted(pd.read_parquet(output_path, columns=["reference_month"])["reference_month"].dropna().astype(int).unique().tolist())
        tests.append(TestResult("period.months_preserved", "PASS" if months_in_output == months else "FAIL", "critical", "Meses solicitados preservados no Parquet." if months_in_output == months else "Meses do output divergem dos solicitados.", observed=months_in_output, expected=months))
        direct_values = pd.read_parquet(output_path, columns=["platform_delivery_direct"])["platform_delivery_direct"]
        tests.append(TestResult("global.no_proxy_platform_fill", "PASS" if direct_values.isna().all() else "FAIL", "critical", "Nenhum registro recebeu identificação direta de plataforma por proxy."))

    critical_failures = [asdict(t) for t in tests if t.severity == "critical" and t.status in {"FAIL", "BLOCKED"}]
    secondary_failures = [asdict(t) for t in tests if t.severity != "critical" and t.status in {"FAIL", "BLOCKED"}]
    warnings = [asdict(t) for t in tests if t.status == "WARN"]
    dictionary_all = all(source_manifests[m]["dictionary_evidence"].get("contains_code_16_motoboy") and source_manifests[m]["dictionary_evidence"].get("contains_code_17_delivery") for m in source_manifests)
    status = "BLOCKED" if critical_failures else ("CERTIFIED" if dictionary_all else "CORE_CERTIFIED")

    registry = root / "00_admin" / "registry"
    layout_path = registry / "pnad_covid_layout_2020.json"
    design_path = registry / "survey_design_pnad_covid_2020.json"
    golden_path = registry / "golden_tests_pnad_covid_2020.json"
    manifest_path = registry / "certified_pnad_covid_delivery_2020_manifest.json"
    json_dump(layout_path, {"schema_version": SCHEMA_VERSION, "created_at_utc": utc_now(), "semantic_contract": SEMANTIC_CONTRACT, "months": layout_months})
    json_dump(design_path, {"schema_version": VALIDATION_SCHEMA_VERSION, "created_at_utc": utc_now(), "designs": designs, "panel_note": "Amostra fixa/repetida; estimativas e variâncias certificadas por mês."})
    json_dump(golden_path, {"schema_version": VALIDATION_SCHEMA_VERSION, "status": status, "tests": [asdict(t) for t in tests]})
    manifest = {
        "run_id": current_run, "script_version": VERSION, "schema_version": SCHEMA_VERSION,
        "status": status, "months": months, "primary_month": PRIMARY_MONTH,
        "output": str(output_path), "output_sha256": sha256_file(output_path) if output_path.exists() else None,
        "rows": int(pq.ParquetFile(output_path).metadata.num_rows) if output_path.exists() else None,
        "sources": source_manifests, "semantic_contract": SEMANTIC_CONTRACT,
        "upstream_freeze": freeze, "no_proxy_fill": True, "synthetic_location": False,
        "platform_direct_available": False, "created_at_utc": utc_now(),
    }
    json_dump(manifest_path, manifest)

    estimates_path = root / "05_outputs" / "tables" / "pnad_covid" / "pnad_covid_delivery_estimates_2020.csv"
    estimates_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([asdict(e) for e in estimates]).to_csv(estimates_path, index=False, encoding="utf-8-sig")

    report_path = root / "06_reports" / "pnad_covid_certification" / "pnad_covid_certification_report.md"
    render_report(report_path, current_run, status, months, source_manifests, freeze, designs, estimates, tests, output_path)
    stamped_report = report_path.with_name(f"pnad_covid_certification_report_{current_run}.md")
    shutil.copy2(report_path, stamped_report)

    lock = {
        "run_id": current_run, "script_version": VERSION, "schema_version": SCHEMA_VERSION,
        "validation_schema_version": VALIDATION_SCHEMA_VERSION, "mode": "certify", "status": status,
        "critical_failures": critical_failures, "secondary_failures": secondary_failures,
        "warnings": warnings, "report": str(report_path), "output": str(output_path),
        "manifest": str(manifest_path), "layout": str(layout_path), "survey_design": str(design_path),
        "golden_tests": str(golden_path), "estimates": str(estimates_path),
        "pnadc_core_freeze": freeze.get("path"), "created_at_utc": utc_now(),
    }
    lock_path = root / "00_admin" / "PNAD_COVID_CERTIFICATION_LOCK.json"
    json_dump(lock_path, lock)
    logger.info("Certificação PNAD COVID concluída | status=%s | relatório=%s", status, report_path)
    for failure in critical_failures:
        logger.error("GATE CRÍTICO: %s — %s", failure["test_id"], failure["message"])
    return 2 if strict and critical_failures else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SPINE-GPE v7 — PNAD COVID Certification Engine")
    parser.add_argument("--root", type=Path, required=True, help="Raiz SPINE-GPEv7")
    parser.add_argument("--mode", choices=("audit", "certify"), default="audit")
    parser.add_argument("--months", type=parse_months, default=[PRIMARY_MONTH], help="9, 5,6,7... ou all")
    parser.add_argument("--download", action="store_true", help="Baixar fontes oficiais ausentes")
    parser.add_argument("--strict", action="store_true", help="Exit code 2 em gate crítico")
    parser.add_argument("--chunk-rows", type=int, default=50000)
    parser.add_argument("--cache-dir", type=Path, default=Path("/content/spine_pnad_covid_cache"))
    parser.add_argument("--verbose", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logger = setup_logger(args.verbose)
    root = args.root.expanduser().resolve()
    logger.info("SPINE-GPE PNAD COVID Certifier v%s | root=%s | mode=%s | months=%s", VERSION, root, args.mode, args.months)
    if args.mode == "audit":
        return audit_mode(root, args.months, args.download, args.strict, logger)
    return certify_mode(root, args.months, args.download, args.strict, args.chunk_rows, args.cache_dir, logger)


if __name__ == "__main__":
    raise SystemExit(main())
