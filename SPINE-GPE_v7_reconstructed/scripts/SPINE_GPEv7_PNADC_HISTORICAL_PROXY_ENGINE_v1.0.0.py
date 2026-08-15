#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SPINE-GPE v7 — PNADc Historical Certification & Proxy Calibration Engine v1.0.0
================================================================================

Certifica a camada histórica da PNAD Contínua trimestral e calibra, contra os
módulos diretos de 2022T4 e 2024T3, uma probabilidade de entrega por plataforma
baseada apenas em variáveis observáveis também disponíveis na série regular.

Princípio epistemológico
------------------------
* `platform_delivery_direct` existe somente nos módulos especiais certificados,
  definidos por SD14001=1 e S140093=1.
* Na PNADc regular histórica, plataforma direta permanece NA.
* A saída histórica principal é
  `platform_delivery_probability_calibrated`, evidence tier C.
* A classificação primária usa exclusivamente ocupação × atividade × posição.
  Renda, horas, sexo, raça e educação não entram no modelo primário, evitando
  vazamento mecânico para análises distributivas posteriores.
* Probabilidade modelada não é observação individual nem prova de plataforma.

Dependência upstream
--------------------
O engine reutiliza o parser fixed-width já certificado do
`SPINE_GPEv7_PNADC_CERTIFIER_v1.2.0.py`. O arquivo deve estar em `root/scripts`
ou ao lado deste script. Sua versão e seu SHA-256 são registrados no lock.

Uso Colab
---------
Auditoria:
  python SPINE_GPEv7_PNADC_HISTORICAL_PROXY_ENGINE_v1.0.0.py \
    --root /content/drive/MyDrive/aCidadeAlgoritmica/SPINE-GPEv7 \
    --mode audit --periods auto --strict

Calibração + certificação das fontes descobertas:
  python SPINE_GPEv7_PNADC_HISTORICAL_PROXY_ENGINE_v1.0.0.py \
    --root /content/drive/MyDrive/aCidadeAlgoritmica/SPINE-GPEv7 \
    --mode full --periods auto --chunk-rows 50000 --strict

Períodos explícitos:
  --periods 2019q1:2021q4
  --periods 2020q1,2020q2,2022q4,2024q3
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import importlib.util
import json
import logging
import math
import os
import re
import shutil
import sys
import tempfile
import textwrap
import zipfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence
from urllib.parse import urljoin

import joblib
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
    raise RuntimeError("pyarrow é obrigatório.") from exc

try:
    from scipy.stats import t as student_t
except Exception as exc:  # pragma: no cover
    raise RuntimeError("scipy é obrigatório.") from exc

try:
    from sklearn.compose import ColumnTransformer
    from sklearn.isotonic import IsotonicRegression
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import (
        average_precision_score,
        brier_score_loss,
        log_loss,
        precision_recall_curve,
        roc_auc_score,
    )
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder
except Exception as exc:  # pragma: no cover
    raise RuntimeError("scikit-learn é obrigatório.") from exc

VERSION = "1.0.0"
SCHEMA_VERSION = "spine-gpe-v7-pnadc-historical-proxy-1.0.0"
VALIDATION_SCHEMA_VERSION = "spine-gpe-v7-pnadc-historical-validation-1.0.0"
MODEL_SCHEMA_VERSION = "spine-gpe-v7-pnadc-proxy-model-1.0.0"
SCRIPT_NAME = "SPINE_GPEv7_PNADC_HISTORICAL_PROXY_ENGINE_v1.0.0.py"
UTC = dt.timezone.utc

DIRECT_CERTIFIER_NAME = "SPINE_GPEv7_PNADC_CERTIFIER_v1.2.0.py"
DIRECT_CERTIFIER_VERSION = "1.2.0"
FTP_BASE = (
    "https://ftp.ibge.gov.br/Trabalho_e_Rendimento/"
    "Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/"
    "Trimestral/Microdados/"
)

PRIMARY_FEATURES = ["occupation_code", "activity_code", "position_code"]
SENSITIVITY_FEATURES = [
    "occupation_code", "activity_code", "position_code", "region_code",
    "sex_code", "age_band", "education_code", "race_code",
]
FORBIDDEN_PRIMARY_FEATURES = {
    "monthly_income_usual", "weekly_hours_usual", "weekly_hours_actual",
    "sex_code", "race_code", "education_code", "age_years", "age_band",
    "social_security_contributor", "informal_status",
}
THRESHOLD_GRID = np.round(np.arange(0.05, 0.951, 0.01), 2)
SENSITIVITY_THRESHOLDS = (0.25, 0.50, 0.75)

# Regras candidatas transparentes. A lista oficial de ocupações é importada do
# certifier direto v1.2.0, evitando duas fontes de verdade.
RULE_NAMES = (
    "occupation_compatible",
    "delivery_activity",
    "occupation_or_activity",
    "occupation_and_activity",
)

HISTORICAL_RAW_KEEP = (
    "Ano", "Trimestre", "UF", "Capital", "CAPITAL", "RM_RIDE", "UPA",
    "Estrato", "V1008", "V1014", "V1028", "V1030", "V1031", "V1032",
    "posest", "posest_sxi", "V2007", "V2009", "V2010", "VD3004",
    "VD4001", "VD4002", "VD4003", "VD4004A", "VD4005", "VD4008",
    "VD4009", "V4010", "V4012", "V40121", "V4013", "V4019", "V4020",
    "V4029", "V4029A", "V4029B", "V4039", "V4039C", "VD4012",
    "VD4016", "VD4017", "VD4018", "VD4019", "VD4020",
)

DIRECT_READ_COLUMNS = (
    "source_year", "reference_quarter", "survey_weight", "survey_stratum",
    "survey_psu", "eligible_platform_module", "platform_delivery_direct",
    "delivery_occupation_compatible", "delivery_activity_compatible",
    "V4010", "V4013", "VD4009", "VD4008", "V4012", "V2007", "V2009",
    "V2010", "VD3004", "UF", "region_code",
)


@dataclass
class TestResult:
    test_id: str
    status: str
    severity: str
    message: str
    period: str | None = None
    observed: Any = None
    expected: Any = None
    evidence: dict[str, Any] | None = None


@dataclass(frozen=True, order=True)
class Period:
    year: int
    quarter: int

    @property
    def key(self) -> str:
        return f"{self.year}q{self.quarter}"


@dataclass
class HistoricalSource:
    period: str
    path: str
    suffix: str
    sha256: str
    record_width: int | None
    layout_path: str | None
    layout_sha256: str | None
    layout_width: int | None
    source_kind: str


@dataclass
class FoldMetric:
    model: str
    train_year: int
    test_year: int
    n_test: int
    n_positive: int
    weighted_prevalence: float
    roc_auc: float | None
    average_precision: float | None
    brier: float | None
    null_brier: float | None
    brier_skill: float | None
    log_loss: float | None
    ece_10: float | None


def utc_now() -> str:
    return dt.datetime.now(UTC).isoformat()


def make_run_id() -> str:
    return dt.datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def sha256_file(path: Path, chunk_size: int = 2**20) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def atomic_write_json(path: Path, payload: Any) -> None:
    atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n")


def copy_atomic(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp = destination.with_suffix(destination.suffix + ".tmp")
    shutil.copy2(source, tmp)
    os.replace(tmp, destination)


def setup_logger(verbose: bool = False) -> logging.Logger:
    logger = logging.getLogger("spine_pnadc_historical")
    logger.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    return logger


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def find_existing(paths: Iterable[Path]) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def canonical_code(series: pd.Series) -> pd.Series:
    values = series.astype("string").str.strip()
    values = values.replace({"": pd.NA, ".": pd.NA, "..": pd.NA, "...": pd.NA, "nan": pd.NA, "None": pd.NA})
    numeric_like = values.str.fullmatch(r"[+-]?\d+(?:\.0+)?", na=False)
    if numeric_like.any():
        numbers = pd.to_numeric(values.loc[numeric_like], errors="coerce")
        values.loc[numeric_like] = numbers.round().astype("Int64").astype("string")
    return values


def numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def bool_series(series: pd.Series) -> pd.Series:
    if str(series.dtype) == "boolean":
        return series
    if pd.api.types.is_bool_dtype(series):
        return series.astype("boolean")
    code = canonical_code(series)
    return code.map({"1": True, "0": False, "2": False, "True": True, "False": False}).astype("boolean")


def age_band(age: pd.Series) -> pd.Series:
    x = numeric(age)
    return pd.cut(
        x,
        bins=[13, 17, 24, 34, 44, 54, 64, np.inf],
        labels=["14-17", "18-24", "25-34", "35-44", "45-54", "55-64", "65+"],
    ).astype("string")


def region_from_uf(uf: pd.Series) -> pd.Series:
    first = canonical_code(uf).str.zfill(2).str.slice(0, 1)
    return first.map({"1": "N", "2": "NE", "3": "SE", "4": "S", "5": "CO"}).astype("string")


def safe_one_hot_encoder() -> OneHotEncoder:
    try:
        return OneHotEncoder(handle_unknown="ignore", min_frequency=5, sparse_output=True)
    except TypeError:  # sklearn < 1.2
        return OneHotEncoder(handle_unknown="ignore", min_frequency=5, sparse=True)


def requests_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=4, connect=4, read=4, backoff_factor=1.0, status_forcelist=(429, 500, 502, 503, 504))
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": "SPINE-GPEv7-PNADc-Historical/1.0"})
    return session


def parse_period_token(token: str) -> Period:
    match = re.fullmatch(r"\s*(20\d{2})\s*[qQtT]?\s*([1-4])\s*", token)
    if not match:
        raise ValueError(f"Período inválido: {token!r}. Use 2022q4.")
    return Period(int(match.group(1)), int(match.group(2)))


def period_sequence(start: Period, end: Period) -> list[Period]:
    if start > end:
        raise ValueError("Intervalo de períodos invertido.")
    out: list[Period] = []
    y, q = start.year, start.quarter
    while (y, q) <= (end.year, end.quarter):
        out.append(Period(y, q))
        q += 1
        if q == 5:
            q = 1
            y += 1
    return out


def parse_periods(spec: str, discovered: Sequence[Period] | None = None) -> list[Period]:
    spec = spec.strip()
    if spec.lower() == "auto":
        return sorted(set(discovered or []))
    periods: list[Period] = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" in part:
            left, right = part.split(":", 1)
            periods.extend(period_sequence(parse_period_token(left), parse_period_token(right)))
        else:
            periods.append(parse_period_token(part))
    return sorted(set(periods))


def period_from_filename(path: Path) -> Period | None:
    name = path.name
    patterns = (
        r"PNADC[_-]?0?([1-4])([12]\d{3})",
        r"PNADC[_-]?([12]\d{3})[_-]?(?:TRIMESTRE)?0?([1-4])",
        r"([12]\d{3})[qQtT]([1-4])",
    )
    for i, pattern in enumerate(patterns):
        match = re.search(pattern, name, flags=re.IGNORECASE)
        if not match:
            continue
        if i == 0:
            quarter, year = int(match.group(1)), int(match.group(2))
        else:
            year, quarter = int(match.group(1)), int(match.group(2))
        if 2012 <= year <= 2035 and 1 <= quarter <= 4:
            return Period(year, quarter)
    return None


def read_record_width(path: Path) -> int:
    with path.open("rb") as handle:
        line = handle.readline().rstrip(b"\r\n")
    return len(line)


def import_direct_certifier(root: Path) -> tuple[Any, Path]:
    candidates = [
        root / "scripts" / DIRECT_CERTIFIER_NAME,
        Path(__file__).resolve().parent / DIRECT_CERTIFIER_NAME,
        Path("/mnt/data") / DIRECT_CERTIFIER_NAME,
    ]
    path = find_existing(candidates)
    if path is None:
        raise FileNotFoundError(
            f"Dependência {DIRECT_CERTIFIER_NAME} não encontrada em root/scripts nem ao lado do engine."
        )
    spec = importlib.util.spec_from_file_location("spine_pnadc_direct_upstream", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Não foi possível carregar {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    if getattr(module, "VERSION", None) != DIRECT_CERTIFIER_VERSION:
        raise RuntimeError(
            f"Versão upstream incompatível: {getattr(module, 'VERSION', None)}; esperada {DIRECT_CERTIFIER_VERSION}."
        )
    return module, path


def validate_upstream(root: Path, tests: list[TestResult]) -> dict[str, Any]:
    phase_lock_path = find_existing([
        root / "00_admin" / "PHASE0_LOCK.json",
        root / "00_admin" / "phase0_lock.json",
    ])
    if phase_lock_path is None:
        tests.append(TestResult("phase0.lock", "FAIL", "critical", "PHASE0_LOCK.json ausente."))
        phase_lock = {}
    else:
        phase_lock = load_json(phase_lock_path)
        ok = phase_lock.get("status") == "RELEASED"
        tests.append(TestResult(
            "phase0.lock", "PASS" if ok else "FAIL", "critical",
            "Fase 0 estrutural liberada." if ok else "Fase 0 não está RELEASED.",
            observed=phase_lock.get("status"), expected="RELEASED",
        ))

    direct_lock_path = root / "00_admin" / "PNADC_CERTIFICATION_LOCK.json"
    if not direct_lock_path.exists():
        tests.append(TestResult("pnadc.direct.lock", "FAIL", "critical", "PNADC_CERTIFICATION_LOCK.json ausente."))
        direct_lock = {}
    else:
        direct_lock = load_json(direct_lock_path)
        ok = direct_lock.get("status") in {"CORE_CERTIFIED", "CERTIFIED"}
        tests.append(TestResult(
            "pnadc.direct.lock", "PASS" if ok else "FAIL", "critical",
            "Núcleo PNADc direto disponível para calibração." if ok else "Núcleo PNADc direto não certificado.",
            observed=direct_lock.get("status"), expected="CORE_CERTIFIED|CERTIFIED",
        ))

    covid_freeze_path = root / "00_admin" / "PNAD_COVID_CORE_FREEZE.json"
    if not covid_freeze_path.exists():
        tests.append(TestResult(
            "pnad_covid.freeze", "WARN", "high",
            "PNAD_COVID_CORE_FREEZE.json ausente; não bloqueia a calibração PNADc, mas quebra a sequência planejada.",
        ))
        covid_freeze = {}
    else:
        covid_freeze = load_json(covid_freeze_path)
        ok = covid_freeze.get("status") == "FROZEN"
        tests.append(TestResult(
            "pnad_covid.freeze", "PASS" if ok else "WARN", "high",
            "PNAD COVID core congelada." if ok else "PNAD COVID freeze encontrado, mas não está FROZEN.",
            observed=covid_freeze.get("status"), expected="FROZEN",
        ))
    return {
        "phase_lock_path": str(phase_lock_path) if phase_lock_path else None,
        "phase_lock": phase_lock,
        "direct_lock_path": str(direct_lock_path),
        "direct_lock": direct_lock,
        "covid_freeze_path": str(covid_freeze_path),
        "covid_freeze": covid_freeze,
    }


def resolve_direct_parquets(root: Path, direct_lock: Mapping[str, Any], tests: list[TestResult]) -> dict[int, Path]:
    outputs = direct_lock.get("outputs") or {}
    candidates: dict[int, list[Path]] = {
        2022: [
            Path(outputs.get("2022", "")) if outputs.get("2022") else Path("/__missing__"),
            root / "03_processed/10_pnadc_certified/certified_pnadc_platform_2022.parquet",
        ],
        2024: [
            Path(outputs.get("2024", "")) if outputs.get("2024") else Path("/__missing__"),
            root / "03_processed/10_pnadc_certified/certified_pnadc_platform_2024.parquet",
        ],
    }
    resolved: dict[int, Path] = {}
    for year, paths in candidates.items():
        path = find_existing(paths)
        ok = path is not None
        tests.append(TestResult(
            f"direct.{year}.parquet", "PASS" if ok else "FAIL", "critical",
            f"Parquet direto {year} localizado." if ok else f"Parquet direto {year} ausente.",
            observed=str(path) if path else None,
        ))
        if path:
            resolved[year] = path
    return resolved


def discover_historical_files(root: Path) -> dict[Period, list[Path]]:
    roots = [
        root / "01_raw/10_ibge/pnadc_historical",
        root / "data_pnadc",
        root / "01_raw/10_ibge",
    ]
    found: dict[Period, list[Path]] = {}
    excluded_tokens = {
        "platform_direct_supplements", "pnadc_platform_2022q4_documentation",
        "pnadc_platform_2024q3_documentation", "pnadc_certification",
        "10_pnadc_certified", "20_pnad_covid_certified",
    }
    for directory in roots:
        if not directory.exists():
            continue
        for suffix in ("*.txt", "*.csv", "*.parquet"):
            for path in directory.rglob(suffix):
                lowered = str(path).lower()
                if any(token in lowered for token in excluded_tokens):
                    continue
                period = period_from_filename(path)
                if period is None:
                    continue
                found.setdefault(period, []).append(path)
    return found


def source_score(path: Path) -> tuple[int, int, float]:
    text = str(path).lower()
    preferred = 0
    if "pnadc_historical" in text:
        preferred += 30
    if "data_pnadc" in text:
        preferred += 20
    if path.suffix.lower() == ".parquet":
        preferred += 10
    elif path.suffix.lower() == ".csv":
        preferred += 5
    exact = 1 if re.search(r"pnadc_0?[1-4]20\d{2}(?:_|\.)", path.name.lower()) else 0
    return preferred, exact, path.stat().st_mtime


def select_source(paths: Sequence[Path]) -> Path:
    return sorted(paths, key=source_score, reverse=True)[0]


def list_remote_year_files(session: requests.Session, year: int) -> list[str]:
    url = urljoin(FTP_BASE, f"{year}/")
    response = session.get(url, timeout=90)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")
    names: list[str] = []
    for link in soup.find_all("a", href=True):
        href = str(link.get("href"))
        name = href.rsplit("/", 1)[-1]
        if name:
            names.append(name)
    # fallback para listagem textual
    if not names:
        names.extend(re.findall(r"PNADC_[^\s\"'<>]+\.zip", response.text, flags=re.IGNORECASE))
    return sorted(set(names))


def download_period(root: Path, period: Period, logger: logging.Logger) -> Path:
    session = requests_session()
    names = list_remote_year_files(session, period.year)
    pattern = re.compile(rf"^PNADC_0?{period.quarter}{period.year}.*\.zip$", re.IGNORECASE)
    matches = sorted([name for name in names if pattern.match(name)])
    if not matches:
        raise FileNotFoundError(f"Arquivo oficial não localizado para {period.key} em {FTP_BASE}{period.year}/")
    name = matches[-1]
    url = urljoin(FTP_BASE, f"{period.year}/{name}")
    destination_dir = root / "01_raw/10_ibge/pnadc_historical" / period.key
    destination_dir.mkdir(parents=True, exist_ok=True)
    zip_path = destination_dir / name
    if not zip_path.exists():
        logger.info("Baixando %s", url)
        with session.get(url, stream=True, timeout=180) as response:
            response.raise_for_status()
            tmp = zip_path.with_suffix(".zip.part")
            with tmp.open("wb") as handle:
                for chunk in response.iter_content(2**20):
                    if chunk:
                        handle.write(chunk)
            os.replace(tmp, zip_path)
    with zipfile.ZipFile(zip_path) as archive:
        txt_members = [m for m in archive.namelist() if m.lower().endswith(".txt") and "pnadc" in Path(m).name.lower()]
        if not txt_members:
            raise RuntimeError(f"ZIP sem TXT PNADC: {zip_path}")
        member = sorted(txt_members)[0]
        target = destination_dir / Path(member).name
        if not target.exists():
            logger.info("Extraindo %s", member)
            with archive.open(member) as src, target.open("wb") as dst:
                shutil.copyfileobj(src, dst)
    return target


def discover_layouts(upstream: Any, root: Path, logger: logging.Logger) -> list[Any]:
    roots = [
        root / "01_raw/10_ibge",
        root / "data_pnadc",
        root / "scripts",
        root,
    ]
    return upstream.discover_layout_candidates(roots, logger)


def choose_layout_for_source(upstream: Any, source: Path, period: Period, layouts: Sequence[Any]) -> tuple[Any | None, int | None]:
    if source.suffix.lower() != ".txt":
        return None, None
    width = read_record_width(source)
    layout = upstream.select_layout(layouts, period.year, width)
    variables = {field.variable.upper() for field in layout.fields}
    if "S140093" in variables or "SD14001" in variables:
        raise RuntimeError(
            f"Fonte histórica {source} foi pareada com layout suplementar direto ({layout.path}); execução bloqueada."
        )
    return layout, width


def read_columns_available(path: Path) -> list[str]:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pq.ParquetFile(path).schema_arrow.names
    if suffix == ".csv":
        return list(pd.read_csv(path, nrows=0).columns)
    return []


def choose_survey_vars(columns: Sequence[str], upstream: Any) -> dict[str, str | None]:
    return {
        "weight": upstream.choose_survey_variable(columns, "weight"),
        "stratum": upstream.choose_survey_variable(columns, "stratum"),
        "psu": upstream.choose_survey_variable(columns, "psu"),
    }


def direct_feature_frame(frame: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=frame.index)
    out["occupation_code"] = canonical_code(frame.get("V4010", pd.Series(pd.NA, index=frame.index))).str.zfill(4)
    out["activity_code"] = canonical_code(frame.get("V4013", pd.Series(pd.NA, index=frame.index))).str.zfill(5)
    position_source = "VD4009" if "VD4009" in frame.columns else None
    out["position_code"] = canonical_code(frame[position_source]) if position_source else pd.Series(pd.NA, index=frame.index, dtype="string")
    out["region_code"] = (
        frame["region_code"].astype("string") if "region_code" in frame.columns else region_from_uf(frame.get("UF", pd.Series(pd.NA, index=frame.index)))
    )
    out["sex_code"] = canonical_code(frame.get("V2007", pd.Series(pd.NA, index=frame.index)))
    out["race_code"] = canonical_code(frame.get("V2010", pd.Series(pd.NA, index=frame.index)))
    out["education_code"] = canonical_code(frame.get("VD3004", pd.Series(pd.NA, index=frame.index)))
    out["age_band"] = age_band(frame.get("V2009", pd.Series(np.nan, index=frame.index)))
    for column in out.columns:
        out[column] = out[column].astype("string").fillna("__MISSING__")
    return out


def available_parquet_columns(path: Path, desired: Sequence[str]) -> list[str]:
    names = set(pq.ParquetFile(path).schema_arrow.names)
    return [column for column in desired if column in names]


def load_direct_calibration_data(paths: Mapping[int, Path], upstream: Any, tests: list[TestResult]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for year, path in sorted(paths.items()):
        columns = available_parquet_columns(path, DIRECT_READ_COLUMNS)
        frame = pd.read_parquet(path, columns=columns)
        required = {"survey_weight", "eligible_platform_module", "platform_delivery_direct", "V4010", "V4013", "VD4009"}
        missing = sorted(required - set(frame.columns))
        if missing:
            tests.append(TestResult(
                f"direct.{year}.schema", "FAIL", "critical",
                f"Parquet direto {year} sem colunas essenciais: {missing}",
            ))
            continue
        eligible = bool_series(frame["eligible_platform_module"]).fillna(False)
        target = bool_series(frame["platform_delivery_direct"])
        keep = eligible & target.notna() & numeric(frame["survey_weight"]).gt(0)
        frame = frame.loc[keep].copy()
        frame["target"] = target.loc[keep].astype(int)
        frame["survey_weight"] = numeric(frame["survey_weight"])
        frame["calibration_year"] = year
        features = direct_feature_frame(frame)
        for column in features:
            frame[column] = features[column]
        frames.append(frame)
        n_positive = int(frame["target"].sum())
        tests.append(TestResult(
            f"direct.{year}.calibration_sample", "PASS" if n_positive >= 100 else "FAIL", "critical",
            "Amostra direta positiva suficiente para calibração temporal." if n_positive >= 100 else "Poucos positivos diretos para calibração.",
            observed={"n": len(frame), "n_positive": n_positive}, expected="n_positive>=100",
        ))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def fit_pipeline(features: Sequence[str]) -> Pipeline:
    transformer = ColumnTransformer(
        [("categorical", safe_one_hot_encoder(), list(features))],
        remainder="drop",
    )
    classifier = LogisticRegression(
        C=1.0, solver="liblinear", max_iter=3000,
        random_state=20260721,
    )
    return Pipeline([("preprocess", transformer), ("classifier", classifier)])


def normalize_weights(weights: pd.Series) -> np.ndarray:
    w = numeric(weights).to_numpy(dtype=float)
    positive = np.isfinite(w) & (w > 0)
    if not positive.any():
        raise ValueError("Pesos inválidos.")
    mean = float(np.mean(w[positive]))
    w[~positive] = 0.0
    return w / mean


def expected_calibration_error(y: np.ndarray, p: np.ndarray, w: np.ndarray, bins: int = 10) -> float:
    edges = np.linspace(0, 1, bins + 1)
    total = float(w.sum())
    if total <= 0:
        return float("nan")
    ece = 0.0
    for left, right in zip(edges[:-1], edges[1:]):
        mask = (p >= left) & (p < right if right < 1 else p <= right)
        if not mask.any():
            continue
        weight = float(w[mask].sum())
        observed = float(np.average(y[mask], weights=w[mask]))
        predicted = float(np.average(p[mask], weights=w[mask]))
        ece += (weight / total) * abs(observed - predicted)
    return float(ece)


def safe_metric(function: Any, *args: Any, **kwargs: Any) -> float | None:
    try:
        value = float(function(*args, **kwargs))
        return value if math.isfinite(value) else None
    except Exception:
        return None


def fold_metrics(name: str, train_year: int, test_year: int, y: np.ndarray, p: np.ndarray, w: np.ndarray) -> FoldMetric:
    prevalence = float(np.average(y, weights=w))
    null = np.full_like(p, prevalence, dtype=float)
    brier = safe_metric(brier_score_loss, y, p, sample_weight=w)
    null_brier = safe_metric(brier_score_loss, y, null, sample_weight=w)
    skill = None if brier is None or null_brier in (None, 0) else 1.0 - brier / null_brier
    return FoldMetric(
        model=name,
        train_year=train_year,
        test_year=test_year,
        n_test=len(y),
        n_positive=int(y.sum()),
        weighted_prevalence=prevalence,
        roc_auc=safe_metric(roc_auc_score, y, p, sample_weight=w),
        average_precision=safe_metric(average_precision_score, y, p, sample_weight=w),
        brier=brier,
        null_brier=null_brier,
        brier_skill=skill,
        log_loss=safe_metric(log_loss, y, np.column_stack([1 - p, p]), sample_weight=w, labels=[0, 1]),
        ece_10=safe_metric(expected_calibration_error, y, p, w),
    )


def temporal_predictions(data: pd.DataFrame, features: Sequence[str], name: str) -> tuple[pd.Series, list[FoldMetric]]:
    predictions = pd.Series(np.nan, index=data.index, dtype=float)
    metrics: list[FoldMetric] = []
    years = sorted(data["calibration_year"].dropna().astype(int).unique())
    if years != [2022, 2024]:
        raise RuntimeError(f"Calibração temporal requer 2022 e 2024; encontrados {years}.")
    for test_year in years:
        train_year = years[1] if test_year == years[0] else years[0]
        train = data["calibration_year"].eq(train_year)
        test = data["calibration_year"].eq(test_year)
        model = fit_pipeline(features)
        model.fit(
            data.loc[train, list(features)],
            data.loc[train, "target"].astype(int),
            classifier__sample_weight=normalize_weights(data.loc[train, "survey_weight"]),
        )
        p = model.predict_proba(data.loc[test, list(features)])[:, 1]
        predictions.loc[test] = p
        metrics.append(fold_metrics(
            name, train_year, test_year,
            data.loc[test, "target"].to_numpy(dtype=int),
            p,
            normalize_weights(data.loc[test, "survey_weight"]),
        ))
    return predictions, metrics


def fit_calibrated_model(data: pd.DataFrame, features: Sequence[str], name: str) -> dict[str, Any]:
    oof, metrics = temporal_predictions(data, features, name)
    valid = oof.notna()
    y = data.loc[valid, "target"].to_numpy(dtype=int)
    raw_p = oof.loc[valid].to_numpy(dtype=float)
    w = normalize_weights(data.loc[valid, "survey_weight"])
    calibrator = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
    calibrator.fit(raw_p, y, sample_weight=w)
    calibrated_oof = np.asarray(calibrator.transform(raw_p), dtype=float)
    final_model = fit_pipeline(features)
    final_model.fit(
        data[list(features)], data["target"].astype(int),
        classifier__sample_weight=normalize_weights(data["survey_weight"]),
    )
    threshold_rows: list[dict[str, Any]] = []
    best_threshold = 0.5
    best_f1 = -1.0
    for threshold in THRESHOLD_GRID:
        pred = calibrated_oof >= threshold
        tp = float(w[(pred) & (y == 1)].sum())
        fp = float(w[(pred) & (y == 0)].sum())
        fn = float(w[(~pred) & (y == 1)].sum())
        precision = tp / (tp + fp) if tp + fp > 0 else 0.0
        recall = tp / (tp + fn) if tp + fn > 0 else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall > 0 else 0.0
        threshold_rows.append({
            "model": name, "threshold": float(threshold), "weighted_precision": precision,
            "weighted_recall": recall, "weighted_f1": f1,
        })
        if f1 > best_f1:
            best_f1 = f1
            best_threshold = float(threshold)
    oof_metric = fold_metrics(name + "_calibrated_oof", 0, 0, y, calibrated_oof, w)
    return {
        "name": name,
        "features": list(features),
        "pipeline": final_model,
        "calibrator": calibrator,
        "temporal_metrics": metrics,
        "oof_metric": oof_metric,
        "threshold": best_threshold,
        "threshold_table": pd.DataFrame(threshold_rows),
        "oof_raw": raw_p,
        "oof_calibrated": calibrated_oof,
        "oof_y": y,
        "oof_w": w,
    }


def calibrated_predict(bundle: Mapping[str, Any], features: pd.DataFrame) -> np.ndarray:
    raw = bundle["pipeline"].predict_proba(features[list(bundle["features"])])[:, 1]
    return np.asarray(bundle["calibrator"].transform(raw), dtype=float)


def binary_rule_metrics(y: np.ndarray, pred: np.ndarray, w: np.ndarray) -> dict[str, float]:
    pred = pred.astype(bool)
    yb = y.astype(bool)
    tp = float(w[pred & yb].sum())
    fp = float(w[pred & ~yb].sum())
    fn = float(w[~pred & yb].sum())
    tn = float(w[~pred & ~yb].sum())
    return {
        "weighted_sensitivity": tp / (tp + fn) if tp + fn else float("nan"),
        "weighted_specificity": tn / (tn + fp) if tn + fp else float("nan"),
        "weighted_ppv": tp / (tp + fp) if tp + fp else float("nan"),
        "weighted_npv": tn / (tn + fn) if tn + fn else float("nan"),
        "weighted_accuracy": (tp + tn) / (tp + fp + fn + tn) if tp + fp + fn + tn else float("nan"),
    }


def rule_benchmark(data: pd.DataFrame, upstream: Any) -> pd.DataFrame:
    occupation = data["occupation_code"].isin(set(upstream.OCCUPATION_COMPATIBLE))
    activity = data["activity_code"].isin(set(upstream.DELIVERY_ACTIVITY_CODES))
    rules = {
        "occupation_compatible": occupation,
        "delivery_activity": activity,
        "occupation_or_activity": occupation | activity,
        "occupation_and_activity": occupation & activity,
    }
    rows: list[dict[str, Any]] = []
    for year in (2022, 2024):
        mask = data["calibration_year"].eq(year)
        y = data.loc[mask, "target"].to_numpy(dtype=int)
        w = normalize_weights(data.loc[mask, "survey_weight"])
        for name, values in rules.items():
            row = {"year": year, "rule": name, "n": int(mask.sum()), "n_positive": int(y.sum())}
            row.update(binary_rule_metrics(y, values.loc[mask].to_numpy(dtype=bool), w))
            rows.append(row)
    return pd.DataFrame(rows)


def calibration_curve_table(bundle: Mapping[str, Any], bins: int = 10) -> pd.DataFrame:
    p = np.asarray(bundle["oof_calibrated"], dtype=float)
    y = np.asarray(bundle["oof_y"], dtype=int)
    w = np.asarray(bundle["oof_w"], dtype=float)
    edges = np.linspace(0, 1, bins + 1)
    rows: list[dict[str, Any]] = []
    for idx, (left, right) in enumerate(zip(edges[:-1], edges[1:]), start=1):
        mask = (p >= left) & (p < right if right < 1 else p <= right)
        if not mask.any():
            continue
        rows.append({
            "bin": idx, "left": left, "right": right, "n": int(mask.sum()),
            "weighted_n": float(w[mask].sum()),
            "mean_predicted": float(np.average(p[mask], weights=w[mask])),
            "observed_rate": float(np.average(y[mask], weights=w[mask])),
        })
    return pd.DataFrame(rows)


def derive_allowed_position_codes(data: pd.DataFrame) -> list[str]:
    values = canonical_code(data["position_code"]).dropna().unique().tolist()
    return sorted(str(v) for v in values)


def historical_transform(
    raw: pd.DataFrame,
    period: Period,
    source_sha: str,
    layout_sha: str | None,
    survey_vars: Mapping[str, str | None],
    upstream: Any,
    allowed_positions: set[str],
    model_bundle: Mapping[str, Any] | None,
    offset: int,
) -> pd.DataFrame:
    frame = raw.copy()
    frame.columns = [str(c) for c in frame.columns]
    for column in frame.columns:
        if not pd.api.types.is_numeric_dtype(frame[column]):
            frame[column] = canonical_code(frame[column])

    result = pd.DataFrame(index=frame.index)
    result["record_id"] = [
        hashlib.sha256(f"PNADC-HIST|{period.key}|{offset + i}".encode()).hexdigest()[:24]
        for i in range(len(frame))
    ]
    result["source"] = "PNADc regular trimestral"
    result["source_year"] = period.year
    result["reference_quarter"] = period.quarter
    result["source_period"] = period.key
    result["source_file_sha256"] = source_sha
    result["layout_file_sha256"] = layout_sha or ""
    result["schema_version"] = SCHEMA_VERSION

    result["survey_weight"] = numeric(frame[survey_vars["weight"]]) if survey_vars.get("weight") else np.nan
    result["survey_stratum"] = frame[survey_vars["stratum"]].astype("string") if survey_vars.get("stratum") else pd.NA
    result["survey_psu"] = frame[survey_vars["psu"]].astype("string") if survey_vars.get("psu") else pd.NA
    result["UF"] = canonical_code(frame.get("UF", pd.Series(pd.NA, index=frame.index))).str.zfill(2)
    capital_col = "Capital" if "Capital" in frame else "CAPITAL" if "CAPITAL" in frame else None
    result["Capital"] = canonical_code(frame[capital_col]) if capital_col else pd.NA
    result["RM_RIDE"] = canonical_code(frame.get("RM_RIDE", pd.Series(pd.NA, index=frame.index)))
    result["region_code"] = region_from_uf(result["UF"])
    result["age_years"] = numeric(frame.get("V2009", pd.Series(np.nan, index=frame.index)))
    result["age_band"] = age_band(result["age_years"])
    result["sex_code"] = canonical_code(frame.get("V2007", pd.Series(pd.NA, index=frame.index)))
    result["race_code"] = canonical_code(frame.get("V2010", pd.Series(pd.NA, index=frame.index)))
    result["education_code"] = canonical_code(frame.get("VD3004", pd.Series(pd.NA, index=frame.index)))
    result["occupation_code"] = canonical_code(frame.get("V4010", pd.Series(pd.NA, index=frame.index))).str.zfill(4)
    result["activity_code"] = canonical_code(frame.get("V4013", pd.Series(pd.NA, index=frame.index))).str.zfill(5)
    position_source = "VD4009" if "VD4009" in frame else None
    result["position_code"] = canonical_code(frame[position_source]) if position_source else pd.NA
    result["position_source_variable"] = position_source or ""

    occupied = canonical_code(frame.get("VD4002", pd.Series(pd.NA, index=frame.index))).eq("1")
    if "VD4002" not in frame:
        occupied = result["occupation_code"].notna()
    age_ok = result["age_years"].ge(14) | result["age_years"].isna()
    position_ok = result["position_code"].isin(allowed_positions) if allowed_positions else result["position_code"].notna()
    covariates_ok = result[["occupation_code", "activity_code", "position_code"]].notna().all(axis=1)
    result["occupied_observed"] = occupied.astype("boolean")
    result["eligible_historical_proxy_universe"] = (occupied & age_ok & position_ok & covariates_ok).astype("boolean")

    result["delivery_occupation_compatible"] = result["occupation_code"].isin(set(upstream.OCCUPATION_COMPATIBLE)).astype("boolean")
    result["delivery_activity_compatible"] = result["activity_code"].isin(set(upstream.DELIVERY_ACTIVITY_CODES)).astype("boolean")
    result["historical_rule_occupation"] = result["delivery_occupation_compatible"]
    result["historical_rule_activity"] = result["delivery_activity_compatible"]
    result["historical_rule_occ_or_activity"] = (
        result["delivery_occupation_compatible"].fillna(False) | result["delivery_activity_compatible"].fillna(False)
    ).astype("boolean")
    result["historical_rule_occ_and_activity"] = (
        result["delivery_occupation_compatible"].fillna(False) & result["delivery_activity_compatible"].fillna(False)
    ).astype("boolean")

    # Outcomes são preservados, mas nunca entram no modelo primário.
    result["monthly_income_usual"] = numeric(frame.get("VD4019", pd.Series(np.nan, index=frame.index)))
    result["weekly_hours_usual"] = numeric(frame.get("V4039", pd.Series(np.nan, index=frame.index)))
    try:
        result["informal_status"] = upstream.derive_informal_status(frame)
    except Exception:
        result["informal_status"] = pd.Series(pd.NA, index=frame.index, dtype="boolean")
    try:
        result["social_security_contributor"] = upstream.derive_social_security(frame)
    except Exception:
        result["social_security_contributor"] = pd.Series(pd.NA, index=frame.index, dtype="boolean")

    result["platform_delivery_direct"] = pd.Series(pd.NA, index=frame.index, dtype="boolean")
    result["platform_direct_available"] = False
    result["evidence_tier"] = "C"
    result["measurement_status"] = "model_imputed_probability" if model_bundle else "historical_proxy_observed_covariates"
    result["identification_method"] = "calibrated_occupation_activity_position" if model_bundle else "occupation_activity_position_contract"
    result["no_proxy_fill_direct"] = True
    result["synthetic_location"] = False

    if model_bundle:
        feature_frame = result.copy()
        for column in model_bundle["features"]:
            feature_frame[column] = feature_frame[column].astype("string").fillna("__MISSING__")
        probability = calibrated_predict(model_bundle, feature_frame)
        eligible_mask = result["eligible_historical_proxy_universe"].fillna(False).to_numpy(dtype=bool)
        probability = np.where(eligible_mask, probability, np.nan)
        result["platform_delivery_probability_calibrated"] = probability
        result["platform_delivery_proxy_class"] = pd.array(
            np.where(np.isnan(probability), pd.NA, probability >= float(model_bundle["threshold"])),
            dtype="boolean",
        )
        result["proxy_class_threshold"] = float(model_bundle["threshold"])
        result["proxy_model_name"] = str(model_bundle["name"])
    else:
        result["platform_delivery_probability_calibrated"] = np.nan
        result["platform_delivery_proxy_class"] = pd.Series(pd.NA, index=frame.index, dtype="boolean")
        result["proxy_class_threshold"] = np.nan
        result["proxy_model_name"] = ""

    # O artefato histórico contém somente o universo comparável ao módulo direto.
    return result.loc[result["eligible_historical_proxy_universe"].fillna(False)].reset_index(drop=True)


def raw_chunks(
    source: Path,
    layout: Any | None,
    upstream: Any,
    chunk_rows: int,
) -> Iterator[pd.DataFrame]:
    suffix = source.suffix.lower()
    if suffix == ".txt":
        if layout is None:
            raise RuntimeError("Layout obrigatório para TXT fixed-width.")
        wanted = set(HISTORICAL_RAW_KEEP)
        fields = [field for field in layout.fields if field.variable in wanted]
        if not fields:
            raise RuntimeError(f"Layout {layout.path} não contém variáveis históricas essenciais.")
        yield from upstream.read_fwf_chunks(source, fields, chunk_rows)
    elif suffix == ".csv":
        available = read_columns_available(source)
        usecols = [c for c in HISTORICAL_RAW_KEEP if c in available]
        for chunk in pd.read_csv(source, dtype="string", usecols=usecols, chunksize=chunk_rows, low_memory=False):
            yield chunk
    elif suffix == ".parquet":
        available = read_columns_available(source)
        columns = [c for c in HISTORICAL_RAW_KEEP if c in available]
        parquet = pq.ParquetFile(source)
        for batch in parquet.iter_batches(batch_size=chunk_rows, columns=columns):
            yield batch.to_pandas()
    else:
        raise ValueError(f"Formato histórico não suportado: {source}")


def write_historical_period(
    source: Path,
    period: Period,
    layout: Any | None,
    upstream: Any,
    allowed_positions: set[str],
    model_bundle: Mapping[str, Any] | None,
    output: Path,
    chunk_rows: int,
    logger: logging.Logger,
) -> tuple[dict[str, Any], pd.DataFrame]:
    source_sha = sha256_file(source)
    layout_sha = getattr(layout, "sha256", None) if layout else None
    if source.suffix.lower() == ".txt":
        columns = [field.variable for field in layout.fields]
    else:
        columns = read_columns_available(source)
    survey_vars = choose_survey_vars(columns, upstream)
    if not all(survey_vars.values()):
        raise RuntimeError(f"Desenho survey incompleto em {period.key}: {survey_vars}")
    required = {"V4010", "V4013", "VD4009"}
    missing = required - set(columns)
    if missing:
        raise RuntimeError(f"Fonte {period.key} sem variáveis essenciais: {sorted(missing)}")

    output.parent.mkdir(parents=True, exist_ok=True)
    tmp = output.with_suffix(".parquet.tmp")
    writer: pq.ParquetWriter | None = None
    offset = 0
    rows = 0
    summaries: list[pd.DataFrame] = []
    output_columns: list[str] = []
    try:
        for raw in raw_chunks(source, layout, upstream, chunk_rows):
            transformed = historical_transform(
                raw, period, source_sha, layout_sha, survey_vars, upstream,
                allowed_positions, model_bundle, offset,
            )
            offset += len(raw)
            if transformed.empty:
                continue
            table = pa.Table.from_pandas(transformed, preserve_index=False)
            if writer is None:
                metadata = dict(table.schema.metadata or {})
                metadata.update({
                    b"spine_schema_version": SCHEMA_VERSION.encode(),
                    b"evidence_tier": b"C",
                    b"platform_direct_available": b"false",
                    b"source_period": period.key.encode(),
                })
                table = table.replace_schema_metadata(metadata)
                writer = pq.ParquetWriter(tmp, table.schema, compression="zstd")
                output_columns = table.schema.names
            writer.write_table(table)
            rows += len(transformed)
            summary_cols = [
                "survey_weight", "survey_stratum", "survey_psu", "source_period",
                "platform_delivery_probability_calibrated", "platform_delivery_proxy_class",
                "historical_rule_occupation", "historical_rule_activity",
                "historical_rule_occ_or_activity", "historical_rule_occ_and_activity",
            ]
            summaries.append(transformed[summary_cols].copy())
            logger.info("%s: %d registros brutos lidos; %d no universo histórico", period.key, offset, rows)
        if writer is None:
            raise RuntimeError(f"Nenhum registro elegível produzido para {period.key}")
        writer.close()
        writer = None
        os.replace(tmp, output)
    finally:
        if writer is not None:
            writer.close()
        if tmp.exists():
            tmp.unlink()
    summary = pd.concat(summaries, ignore_index=True) if summaries else pd.DataFrame()
    manifest = {
        "period": period.key,
        "source_path": str(source),
        "source_sha256": source_sha,
        "layout_path": getattr(layout, "path", None) if layout else None,
        "layout_sha256": layout_sha,
        "survey_variables": survey_vars,
        "rows_eligible": rows,
        "columns": output_columns,
        "output_path": str(output),
        "output_sha256": sha256_file(output),
        "schema_version": SCHEMA_VERSION,
        "evidence_tier": "C",
        "platform_direct_available": False,
        "no_proxy_fill_direct": True,
        "synthetic_location": False,
    }
    return manifest, summary


def kish_effective_n(weights: pd.Series) -> float | None:
    w = numeric(weights).dropna()
    w = w[w > 0]
    if w.empty:
        return None
    denominator = float(np.square(w).sum())
    return float(w.sum() ** 2 / denominator) if denominator > 0 else None


def survey_total_and_ratio(summary: pd.DataFrame, values: pd.Series) -> dict[str, float | int | None]:
    frame = summary[["survey_weight", "survey_stratum", "survey_psu"]].copy()
    frame["value"] = numeric(values)
    frame["weight"] = numeric(frame["survey_weight"])
    frame = frame.dropna(subset=["weight", "survey_stratum", "survey_psu", "value"])
    frame = frame[frame["weight"] > 0]
    if frame.empty:
        return {"total": None, "total_se": None, "share": None, "share_se": None, "n": 0, "n_eff": None}
    frame["wy"] = frame["weight"] * frame["value"]
    total = float(frame["wy"].sum())
    weight_total = float(frame["weight"].sum())
    share = total / weight_total if weight_total > 0 else float("nan")

    clusters = frame.groupby(["survey_stratum", "survey_psu"], observed=True).agg(
        cluster_wy=("wy", "sum"), cluster_w=("weight", "sum")
    ).reset_index()
    total_var = 0.0
    ratio_var_num = 0.0
    for _, stratum in clusters.groupby("survey_stratum", observed=True):
        m = len(stratum)
        if m <= 1:
            continue
        total_deviation = stratum["cluster_wy"] - stratum["cluster_wy"].mean()
        ratio_cluster = stratum["cluster_wy"] - share * stratum["cluster_w"]
        ratio_deviation = ratio_cluster - ratio_cluster.mean()
        total_var += m / (m - 1) * float(np.square(total_deviation).sum())
        ratio_var_num += m / (m - 1) * float(np.square(ratio_deviation).sum())
    return {
        "total": total,
        "total_se": math.sqrt(max(total_var, 0.0)),
        "share": share,
        "share_se": math.sqrt(max(ratio_var_num, 0.0)) / weight_total if weight_total > 0 else None,
        "n": len(frame),
        "n_eff": kish_effective_n(frame["weight"]),
        "n_strata": int(frame["survey_stratum"].nunique()),
        "n_psu": int(frame["survey_psu"].nunique()),
    }


def historical_estimates(period: Period, summary: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    value_specs: list[tuple[str, pd.Series]] = []
    if "platform_delivery_probability_calibrated" in summary:
        value_specs.append(("model_expected_probability", summary["platform_delivery_probability_calibrated"]))
    for column, label in (
        ("historical_rule_occupation", "rule_occupation"),
        ("historical_rule_activity", "rule_activity"),
        ("historical_rule_occ_or_activity", "rule_occ_or_activity"),
        ("historical_rule_occ_and_activity", "rule_occ_and_activity"),
    ):
        if column in summary:
            value_specs.append((label, summary[column].astype(float)))
    if "platform_delivery_probability_calibrated" in summary:
        p = numeric(summary["platform_delivery_probability_calibrated"])
        for threshold in SENSITIVITY_THRESHOLDS:
            value_specs.append((f"model_class_threshold_{threshold:.2f}", p.ge(threshold).astype(float)))

    for estimand, values in value_specs:
        stats = survey_total_and_ratio(summary, values)
        rows.append({"period": period.key, "estimand": estimand, **stats})
    return rows


def tests_have_critical_failures(tests: Sequence[TestResult]) -> bool:
    return any(test.severity == "critical" and test.status in {"FAIL", "BLOCKED"} for test in tests)


def metric_gate(metrics: Sequence[FoldMetric], tests: list[TestResult]) -> None:
    for metric in metrics:
        if metric.train_year == 0:
            continue
        auc_ok = metric.roc_auc is not None and metric.roc_auc >= 0.65
        tests.append(TestResult(
            f"calibration.temporal.{metric.train_year}_to_{metric.test_year}.auc",
            "PASS" if auc_ok else "FAIL", "critical",
            "Discriminação temporal mínima atingida." if auc_ok else "AUC temporal abaixo do gate 0,65.",
            observed=metric.roc_auc, expected=">=0.65",
        ))
        skill_ok = metric.brier_skill is not None and metric.brier_skill > 0
        tests.append(TestResult(
            f"calibration.temporal.{metric.train_year}_to_{metric.test_year}.brier_skill",
            "PASS" if skill_ok else "FAIL", "critical",
            "Modelo supera preditor nulo de prevalência." if skill_ok else "Modelo não supera o Brier nulo.",
            observed=metric.brier_skill, expected=">0",
        ))
        ece_ok = metric.ece_10 is not None and metric.ece_10 <= 0.15
        tests.append(TestResult(
            f"calibration.temporal.{metric.train_year}_to_{metric.test_year}.ece",
            "PASS" if ece_ok else "WARN", "high",
            "Erro de calibração temporal aceitável." if ece_ok else "ECE temporal acima de 0,15; usar probabilidades com cautela.",
            observed=metric.ece_10, expected="<=0.15",
        ))


def markdown_tests(tests: Sequence[TestResult]) -> str:
    lines = ["| Status | Severidade | Período | Teste | Mensagem |", "|---|---|---|---|---|"]
    for test in tests:
        lines.append(
            f"| {test.status} | {test.severity} | {test.period or ''} | `{test.test_id}` | {test.message} |"
        )
    return "\n".join(lines)


def write_model_card(
    path: Path,
    bundle: Mapping[str, Any],
    metrics_df: pd.DataFrame,
    rule_df: pd.DataFrame,
    direct_hashes: Mapping[int, str],
) -> None:
    metric_table = metrics_df.to_markdown(index=False) if not metrics_df.empty else "Sem métricas."
    rule_table = rule_df.to_markdown(index=False) if not rule_df.empty else "Sem benchmark."
    text = f"""# SPINE-GPE v7 — Model Card da proxy histórica PNADc

- Modelo: `{bundle['name']}`
- Schema: `{MODEL_SCHEMA_VERSION}`
- Features primárias: `{', '.join(bundle['features'])}`
- Threshold diagnóstico: `{bundle['threshold']:.2f}`
- Target direto: `SD14001=1 AND S140093=1`
- Aplicação histórica: probabilidade modelada, evidence tier C
- Hash direto 2022: `{direct_hashes.get(2022, '')}`
- Hash direto 2024: `{direct_hashes.get(2024, '')}`

## Uso autorizado

A probabilidade pode ser agregada com pesos amostrais para produzir uma estimativa
model-based de exposição histórica compatível com o padrão observado em 2022/2024.
Ela não identifica individualmente trabalhadores de plataforma e não substitui
`platform_delivery_direct`.

## Uso proibido

- chamar a proxy de observação direta;
- preencher `S140093` ou `SD14001`;
- usar a classe binária como verdade sem sensibilidade de threshold;
- interpretar diferenças 2020→2022→2024 como causalidade;
- usar o modelo primário para provar discriminação ou penalidade salarial.

## Proteção contra leakage

Renda, jornada, sexo, raça, idade, educação, previdência e informalidade não entram
no modelo primário. O estimando é calibrado somente com ocupação, atividade e posição.

## Validação temporal

{metric_table}

## Benchmark de regras transparentes

{rule_table}
"""
    atomic_write_text(path, text)


def build_report(
    run: str,
    status: str,
    sources: Sequence[HistoricalSource],
    manifests: Sequence[Mapping[str, Any]],
    tests: Sequence[TestResult],
    metrics_df: pd.DataFrame,
    rule_df: pd.DataFrame,
    estimates_df: pd.DataFrame,
    model_path: Path | None,
) -> str:
    source_rows = [
        f"| {source.period} | `{source.path}` | {source.record_width or ''} | `{source.layout_path or ''}` | `{source.sha256[:16]}…` |"
        for source in sources
    ]
    source_table = "\n".join([
        "| Período | Fonte | Largura | Layout | SHA-256 |",
        "|---|---|---:|---|---|",
        *source_rows,
    ]) if source_rows else "Nenhuma fonte histórica selecionada."
    metrics_table = metrics_df.to_markdown(index=False) if not metrics_df.empty else "Não executado."
    rule_table = rule_df.to_markdown(index=False) if not rule_df.empty else "Não executado."
    estimates_table = estimates_df.to_markdown(index=False) if not estimates_df.empty else "Não executado."
    return f"""# SPINE-GPE v7 — PNADc Historical Certification & Proxy Calibration

- Run ID: `{run}`
- Versão: `{VERSION}`
- Data schema: `{SCHEMA_VERSION}`
- Validation schema: `{VALIDATION_SCHEMA_VERSION}`
- Model schema: `{MODEL_SCHEMA_VERSION}`
- Status: **{status}**
- Modelo: `{str(model_path) if model_path else 'não gerado'}`

## Limite epistemológico congelado

`platform_delivery_direct` permanece NA na série histórica. A variável principal é
`platform_delivery_probability_calibrated`, estimada a partir de ocupação × atividade × posição,
com target direto observado em 2022T4 e 2024T3. O artefato histórico recebe evidence tier C.

## Fontes históricas

{source_table}

## Validação temporal do modelo primário

{metrics_table}

## Benchmark das regras observáveis

{rule_table}

## Estimativas históricas nacionais

{estimates_table}

## Golden tests e gates

{markdown_tests(tests)}

## Regras congeladas

1. Probabilidade histórica não é identificação direta individual.
2. Nenhuma proxy preenche SD14001 ou S140093.
3. Renda, jornada e atributos demográficos não entram no modelo primário.
4. A classe binária é apenas diagnóstico de threshold; o estimando principal é a soma ponderada das probabilidades.
5. Comparações intertemporais são descritivas/model-based, não um DiD causal.
6. Estimativas locais exigem gates próprios de n, n efetivo, CV e estabilidade; este engine publica apenas resultados nacionais.
7. Outputs são imutáveis por Run ID e acompanhados por hashes.
"""


def audit_mode(root: Path, periods_spec: str, download_missing: bool, strict: bool, logger: logging.Logger) -> int:
    run = make_run_id()
    tests: list[TestResult] = []
    upstream_info = validate_upstream(root, tests)
    upstream, upstream_path = import_direct_certifier(root)
    tests.append(TestResult(
        "upstream.parser.version", "PASS", "critical",
        f"Parser upstream {DIRECT_CERTIFIER_VERSION} carregado.",
        observed={"path": str(upstream_path), "sha256": sha256_file(upstream_path)},
    ))
    direct_paths = resolve_direct_parquets(root, upstream_info["direct_lock"], tests)
    discovered_map = discover_historical_files(root)
    requested = parse_periods(periods_spec, list(discovered_map))
    if download_missing and periods_spec.lower() != "auto":
        for period in requested:
            if period not in discovered_map:
                try:
                    path = download_period(root, period, logger)
                    discovered_map.setdefault(period, []).append(path)
                except Exception as exc:
                    tests.append(TestResult(
                        f"historical.{period.key}.download", "FAIL", "critical",
                        f"Falha ao baixar período: {exc}", period=period.key,
                    ))
    if not requested:
        tests.append(TestResult(
            "historical.sources", "FAIL", "critical",
            "Nenhuma fonte histórica descoberta/solicitada.",
        ))
    layouts = discover_layouts(upstream, root, logger)
    tests.append(TestResult(
        "historical.layouts.available", "PASS" if layouts else "FAIL", "critical",
        f"{len(layouts)} layouts interpretáveis encontrados." if layouts else "Nenhum layout interpretável encontrado.",
    ))
    sources: list[HistoricalSource] = []
    for period in requested:
        paths = discovered_map.get(period, [])
        if not paths:
            tests.append(TestResult(
                f"historical.{period.key}.source", "FAIL", "critical",
                "Fonte histórica solicitada ausente.", period=period.key,
            ))
            continue
        source = select_source(paths)
        try:
            layout, width = choose_layout_for_source(upstream, source, period, layouts)
            columns = {f.variable for f in layout.fields} if layout else set(read_columns_available(source))
            required = {"V4010", "V4013", "VD4009"}
            missing = sorted(required - columns)
            ok = not missing
            tests.append(TestResult(
                f"historical.{period.key}.schema", "PASS" if ok else "FAIL", "critical",
                "Variáveis ocupação/atividade disponíveis." if ok else f"Variáveis ausentes: {missing}",
                period=period.key,
            ))
            survey_vars = choose_survey_vars(list(columns), upstream)
            survey_ok = all(survey_vars.values())
            tests.append(TestResult(
                f"historical.{period.key}.survey", "PASS" if survey_ok else "FAIL", "critical",
                "Peso, estrato e UPA identificados." if survey_ok else f"Desenho survey incompleto: {survey_vars}",
                period=period.key,
            ))
            sources.append(HistoricalSource(
                period=period.key, path=str(source), suffix=source.suffix.lower(),
                sha256=sha256_file(source), record_width=width,
                layout_path=getattr(layout, "path", None) if layout else None,
                layout_sha256=getattr(layout, "sha256", None) if layout else None,
                layout_width=getattr(layout, "width", None) if layout else None,
                source_kind="regular_quarterly",
            ))
        except Exception as exc:
            tests.append(TestResult(
                f"historical.{period.key}.layout", "FAIL", "critical",
                f"Falha de layout/source kind: {exc}", period=period.key,
            ))
    status = "AUDIT_BLOCKED" if tests_have_critical_failures(tests) else "AUDIT_PASSED"
    lock = {
        "run_id": run, "script_version": VERSION,
        "validation_schema_version": VALIDATION_SCHEMA_VERSION,
        "mode": "audit", "status": status,
        "critical_failures": [asdict(t) for t in tests if t.severity == "critical" and t.status in {"FAIL", "BLOCKED"}],
        "warnings": [asdict(t) for t in tests if t.status == "WARN"],
        "requested_periods": [p.key for p in requested],
        "sources": [asdict(s) for s in sources],
        "direct_parquets": {str(k): str(v) for k, v in direct_paths.items()},
        "upstream_parser": {"path": str(upstream_path), "sha256": sha256_file(upstream_path)},
        "created_at_utc": utc_now(),
    }
    lock_path = root / "00_admin" / "PNADC_HISTORICAL_PROXY_AUDIT_LOCK.json"
    atomic_write_json(lock_path, lock)
    logger.info("Auditoria concluída | status=%s | períodos=%s", status, [p.key for p in requested])
    print(json.dumps(lock, ensure_ascii=False, indent=2))
    return 2 if strict and status != "AUDIT_PASSED" else 0


def full_mode(
    root: Path,
    periods_spec: str,
    download_missing: bool,
    chunk_rows: int,
    strict: bool,
    logger: logging.Logger,
) -> int:
    run = make_run_id()
    tests: list[TestResult] = []
    upstream_info = validate_upstream(root, tests)
    upstream, upstream_path = import_direct_certifier(root)
    upstream_hash = sha256_file(upstream_path)
    tests.append(TestResult(
        "upstream.parser.version", "PASS", "critical",
        f"Parser upstream {DIRECT_CERTIFIER_VERSION} carregado e congelado por hash.",
        observed={"path": str(upstream_path), "sha256": upstream_hash},
    ))
    direct_paths = resolve_direct_parquets(root, upstream_info["direct_lock"], tests)
    direct_data = load_direct_calibration_data(direct_paths, upstream, tests)
    if direct_data.empty:
        tests.append(TestResult("calibration.data", "FAIL", "critical", "Dados diretos de calibração indisponíveis."))

    leakage = sorted(set(PRIMARY_FEATURES) & FORBIDDEN_PRIMARY_FEATURES)
    tests.append(TestResult(
        "calibration.primary.no_outcome_demographic_leakage",
        "PASS" if not leakage else "FAIL", "critical",
        "Modelo primário usa somente ocupação × atividade × posição." if not leakage else f"Features proibidas: {leakage}",
        observed=PRIMARY_FEATURES,
    ))

    output_base = root / "05_outputs/models/pnadc_historical_proxy"
    table_base = root / "05_outputs/tables/pnadc_historical_proxy"
    report_base = root / "06_reports/pnadc_historical_proxy"
    processed_base = root / "03_processed/30_pnadc_historical_proxy"
    registry_base = root / "00_admin/registry"
    for directory in (output_base, table_base, report_base, processed_base, registry_base):
        directory.mkdir(parents=True, exist_ok=True)

    primary_bundle: dict[str, Any] | None = None
    metrics_df = pd.DataFrame()
    rule_df = pd.DataFrame()
    curve_df = pd.DataFrame()
    threshold_df = pd.DataFrame()
    model_path: Path | None = None
    model_card_path: Path | None = None
    if not direct_data.empty and not tests_have_critical_failures(tests):
        primary_bundle = fit_calibrated_model(direct_data, PRIMARY_FEATURES, "weighted_logit_occ_activity_position")
        sensitivity_bundle = fit_calibrated_model(direct_data, SENSITIVITY_FEATURES, "weighted_logit_extended_sensitivity")
        primary_metrics = [asdict(m) for m in primary_bundle["temporal_metrics"]] + [asdict(primary_bundle["oof_metric"])]
        sensitivity_metrics = [asdict(m) for m in sensitivity_bundle["temporal_metrics"]] + [asdict(sensitivity_bundle["oof_metric"])]
        metrics_df = pd.DataFrame(primary_metrics + sensitivity_metrics)
        metric_gate(primary_bundle["temporal_metrics"], tests)
        rule_df = rule_benchmark(direct_data, upstream)
        curve_df = calibration_curve_table(primary_bundle)
        threshold_df = primary_bundle["threshold_table"]
        allowed_positions = derive_allowed_position_codes(direct_data)
        model_payload = {
            "model_schema_version": MODEL_SCHEMA_VERSION,
            "created_at_utc": utc_now(),
            "name": primary_bundle["name"],
            "features": primary_bundle["features"],
            "pipeline": primary_bundle["pipeline"],
            "calibrator": primary_bundle["calibrator"],
            "threshold": primary_bundle["threshold"],
            "allowed_position_codes": allowed_positions,
            "occupation_compatible_codes": sorted(upstream.OCCUPATION_COMPATIBLE),
            "delivery_activity_codes": sorted(upstream.DELIVERY_ACTIVITY_CODES),
            "target": "platform_delivery_direct = SD14001=1 AND S140093=1",
            "evidence_tier_application": "C",
            "forbidden_claims": [
                "individual direct identification", "causal platform effect", "fill SD14001/S140093",
            ],
            "direct_input_hashes": {year: sha256_file(path) for year, path in direct_paths.items()},
            "upstream_parser_sha256": upstream_hash,
        }
        model_path = output_base / f"pnadc_historical_proxy_model_{run}.joblib"
        joblib.dump(model_payload, model_path, compress=3)
        model_latest = output_base / "pnadc_historical_proxy_model.joblib"
        copy_atomic(model_path, model_latest)
        feature_contract = {
            "schema_version": MODEL_SCHEMA_VERSION,
            "primary_features": PRIMARY_FEATURES,
            "sensitivity_features": SENSITIVITY_FEATURES,
            "forbidden_primary_features": sorted(FORBIDDEN_PRIMARY_FEATURES),
            "allowed_position_codes": allowed_positions,
            "target": "SD14001=1 AND S140093=1",
            "application_measurement_status": "model_imputed_probability",
            "evidence_tier": "C",
            "direct_variables_remain_na_historical": ["SD14001", "S140093", "platform_delivery_direct"],
        }
        atomic_write_json(registry_base / f"pnadc_historical_proxy_feature_contract_{run}.json", feature_contract)
        metrics_df.to_csv(table_base / f"pnadc_proxy_temporal_metrics_{run}.csv", index=False)
        rule_df.to_csv(table_base / f"pnadc_proxy_rule_benchmark_{run}.csv", index=False)
        curve_df.to_csv(table_base / f"pnadc_proxy_calibration_curve_{run}.csv", index=False)
        threshold_df.to_csv(table_base / f"pnadc_proxy_threshold_sensitivity_{run}.csv", index=False)
        model_card_path = report_base / f"pnadc_historical_proxy_model_card_{run}.md"
        write_model_card(
            model_card_path, primary_bundle, metrics_df[metrics_df["model"].str.contains("occ_activity_position", na=False)],
            rule_df, {year: sha256_file(path) for year, path in direct_paths.items()},
        )
        tests.append(TestResult(
            "calibration.model.saved", "PASS", "critical",
            "Modelo primário e calibrador isotônico salvos em artefato imutável.",
            observed={"path": str(model_path), "sha256": sha256_file(model_path), "threshold": primary_bundle["threshold"]},
        ))
    else:
        allowed_positions = set()

    discovered_map = discover_historical_files(root)
    requested = parse_periods(periods_spec, list(discovered_map))
    if download_missing and periods_spec.lower() != "auto":
        for period in requested:
            if period not in discovered_map:
                try:
                    path = download_period(root, period, logger)
                    discovered_map.setdefault(period, []).append(path)
                except Exception as exc:
                    tests.append(TestResult(
                        f"historical.{period.key}.download", "FAIL", "critical",
                        f"Falha ao baixar período: {exc}", period=period.key,
                    ))
    if not requested:
        tests.append(TestResult("historical.sources", "FAIL", "critical", "Nenhuma fonte histórica selecionada."))

    layouts = discover_layouts(upstream, root, logger)
    sources: list[HistoricalSource] = []
    manifests: list[dict[str, Any]] = []
    estimate_rows: list[dict[str, Any]] = []
    period_outputs: dict[str, str] = {}
    model_bundle_for_apply = None
    if model_path and model_path.exists():
        loaded = joblib.load(model_path)
        model_bundle_for_apply = {
            "name": loaded["name"], "features": loaded["features"],
            "pipeline": loaded["pipeline"], "calibrator": loaded["calibrator"],
            "threshold": loaded["threshold"],
        }
        allowed_positions = set(loaded["allowed_position_codes"])

    for period in requested:
        paths = discovered_map.get(period, [])
        if not paths:
            tests.append(TestResult(
                f"historical.{period.key}.source", "FAIL", "critical",
                "Fonte histórica solicitada ausente.", period=period.key,
            ))
            continue
        source = select_source(paths)
        try:
            layout, width = choose_layout_for_source(upstream, source, period, layouts)
            source_record = HistoricalSource(
                period=period.key, path=str(source), suffix=source.suffix.lower(), sha256=sha256_file(source),
                record_width=width, layout_path=getattr(layout, "path", None) if layout else None,
                layout_sha256=getattr(layout, "sha256", None) if layout else None,
                layout_width=getattr(layout, "width", None) if layout else None,
                source_kind="regular_quarterly",
            )
            sources.append(source_record)
            output = processed_base / f"certified_pnadc_historical_proxy_{period.key}_{run}.parquet"
            manifest, summary = write_historical_period(
                source, period, layout, upstream, set(allowed_positions), model_bundle_for_apply,
                output, chunk_rows, logger,
            )
            manifests.append(manifest)
            period_outputs[period.key] = str(output)
            estimate_rows.extend(historical_estimates(period, summary))
            direct_na_ok = True
            platform_direct_columns = available_parquet_columns(output, ["platform_delivery_direct", "platform_direct_available", "no_proxy_fill_direct"])
            check = pd.read_parquet(output, columns=platform_direct_columns)
            if "platform_delivery_direct" in check:
                direct_na_ok = check["platform_delivery_direct"].isna().all()
            flags_ok = (
                (not check.get("platform_direct_available", pd.Series(False)).astype(bool).any())
                and check.get("no_proxy_fill_direct", pd.Series(True)).astype(bool).all()
            )
            tests.append(TestResult(
                f"historical.{period.key}.direct_absent", "PASS" if direct_na_ok and flags_ok else "FAIL", "critical",
                "Plataforma direta permanece ausente; proxy não preenche variáveis diretas." if direct_na_ok and flags_ok else "Violação da separação direto/proxy.",
                period=period.key,
            ))
            tests.append(TestResult(
                f"historical.{period.key}.output", "PASS", "critical",
                "Parquet histórico imutável criado.", period=period.key,
                observed={"rows": manifest["rows_eligible"], "sha256": manifest["output_sha256"]},
            ))
        except Exception as exc:
            tests.append(TestResult(
                f"historical.{period.key}.processing", "FAIL", "critical",
                f"Falha no processamento histórico: {exc}", period=period.key,
            ))

    estimates_df = pd.DataFrame(estimate_rows)
    if not estimates_df.empty:
        estimates_path = table_base / f"pnadc_historical_proxy_estimates_{run}.csv"
        estimates_df.to_csv(estimates_path, index=False)
    else:
        estimates_path = table_base / f"pnadc_historical_proxy_estimates_{run}.csv"
        pd.DataFrame().to_csv(estimates_path, index=False)

    manifests_path = registry_base / f"pnadc_historical_proxy_manifests_{run}.json"
    atomic_write_json(manifests_path, manifests)

    status = "BLOCKED" if tests_have_critical_failures(tests) else "CERTIFIED"
    report_path = report_base / f"pnadc_historical_proxy_report_{run}.md"
    report_text = build_report(
        run, status, sources, manifests, tests, metrics_df, rule_df, estimates_df,
        model_path,
    )
    atomic_write_text(report_path, report_text)

    artifact_hashes: dict[str, str] = {
        "report": sha256_file(report_path),
        "manifests": sha256_file(manifests_path),
        "estimates": sha256_file(estimates_path),
        "upstream_parser": upstream_hash,
    }
    if model_path:
        artifact_hashes["model"] = sha256_file(model_path)
    if model_card_path:
        artifact_hashes["model_card"] = sha256_file(model_card_path)

    lock = {
        "run_id": run,
        "script_version": VERSION,
        "schema_version": SCHEMA_VERSION,
        "validation_schema_version": VALIDATION_SCHEMA_VERSION,
        "model_schema_version": MODEL_SCHEMA_VERSION,
        "mode": "full",
        "status": status,
        "critical_failures": [asdict(t) for t in tests if t.severity == "critical" and t.status in {"FAIL", "BLOCKED"}],
        "warnings": [asdict(t) for t in tests if t.status == "WARN"],
        "requested_periods": [p.key for p in requested],
        "certified_periods": sorted(period_outputs),
        "direct_inputs": {str(year): {"path": str(path), "sha256": sha256_file(path)} for year, path in direct_paths.items()},
        "upstream_parser": {"path": str(upstream_path), "version": DIRECT_CERTIFIER_VERSION, "sha256": upstream_hash},
        "model": str(model_path) if model_path else None,
        "model_card": str(model_card_path) if model_card_path else None,
        "model_threshold_diagnostic": primary_bundle["threshold"] if primary_bundle else None,
        "primary_features": PRIMARY_FEATURES,
        "historical_outputs": period_outputs,
        "manifests": str(manifests_path),
        "estimates": str(estimates_path),
        "report": str(report_path),
        "artifact_hashes": artifact_hashes,
        "epistemic_limit": "Probabilidade histórica modelada; plataforma direta permanece não observada fora dos módulos especiais.",
        "claim_ceiling": "Estimativa model-based agregada de compatibilidade histórica com entrega por plataforma, calibrada em 2022T4/2024T3.",
        "created_at_utc": utc_now(),
    }
    lock_path = root / "00_admin" / "PNADC_HISTORICAL_PROXY_CERTIFICATION_LOCK.json"
    atomic_write_json(lock_path, lock)

    if status == "CERTIFIED":
        latest_report = report_base / "pnadc_historical_proxy_report.md"
        copy_atomic(report_path, latest_report)
        freeze = {
            "freeze_id": run,
            "status": "FROZEN",
            "component": "PNADC_HISTORICAL_PROXY_CORE",
            "certification_lock": str(lock_path),
            "model": str(model_path),
            "model_sha256": sha256_file(model_path) if model_path else None,
            "certified_periods": sorted(period_outputs),
            "historical_outputs": period_outputs,
            "read_only": True,
            "evidence_tier": "C",
            "claim_ceiling": lock["claim_ceiling"],
            "created_at_utc": utc_now(),
        }
        atomic_write_json(root / "00_admin" / "PNADC_HISTORICAL_PROXY_CORE_FREEZE.json", freeze)

    logger.info("PNADc histórica concluída | status=%s | períodos=%s", status, sorted(period_outputs))
    print(json.dumps(lock, ensure_ascii=False, indent=2))
    return 2 if strict and status != "CERTIFIED" else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Certifica PNADc histórica e calibra proxy probabilística contra 2022T4/2024T3."
    )
    parser.add_argument("--root", type=Path, required=True, help="Raiz do SPINE-GPEv7.")
    parser.add_argument("--mode", choices=("audit", "full"), default="audit")
    parser.add_argument("--periods", default="auto", help="auto, 2019q1:2021q4 ou lista separada por vírgulas.")
    parser.add_argument("--download-missing", action="store_true", help="Baixa períodos explícitos ausentes do diretório oficial IBGE.")
    parser.add_argument("--chunk-rows", type=int, default=50000)
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = args.root.expanduser().resolve()
    logger = setup_logger(args.verbose)
    logger.info(
        "SPINE-GPE PNADc Historical Proxy Engine v%s | root=%s | mode=%s | periods=%s",
        VERSION, root, args.mode, args.periods,
    )
    if args.mode == "audit":
        return audit_mode(root, args.periods, args.download_missing, args.strict, logger)
    return full_mode(root, args.periods, args.download_missing, args.chunk_rows, args.strict, logger)


if __name__ == "__main__":
    raise SystemExit(main())
