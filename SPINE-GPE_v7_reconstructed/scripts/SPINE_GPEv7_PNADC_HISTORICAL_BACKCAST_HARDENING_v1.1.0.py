#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SPINE-GPE v7 — PNADc Historical Backcast Hardening & Uncertainty Engine v1.1.0
================================================================================

Endurece, sem reler os TXT brutos, o backcast probabilístico de compatibilidade
histórica com o trabalho de entrega por plataforma produzido pelo engine
SPINE_GPEv7_PNADC_HISTORICAL_PROXY_ENGINE_v1.0.1.

O engine consome exclusivamente:
  * Parquets diretos certificados de 2022T4 e 2024T3;
  * Parquets históricos certificados pelo engine v1.0.1;
  * lock, manifests e layouts já registrados;
  * parser certificado SPINE_GPEv7_PNADC_CERTIFIER_v1.2.0.

Camadas implementadas
---------------------
1. Integridade upstream e equivalência de layouts essenciais.
2. Três mappings logísticos survey-weighted, L2: 2022, 2024 e pooled.
3. Calibração isotônica interna por folds de UPA e avaliação held-out temporal.
4. Golden totals contra a identificação direta em 2022/2024.
5. MCA ponderada das células ocupação × atividade × posição e suporte histórico.
6. Clustering nas coordenadas MCA para tipologias, nunca como rótulo de plataforma.
7. KNN ponderado como benchmark não paramétrico, nunca como estimando oficial.
8. Incerteza amostral, bootstrap de modelo por UPA e sensibilidade de transporte.
9. Lock final com teto de afirmação e separação entre observado e modelado.

Estimando principal
-------------------
  N_hat_t = sum_i w_it * p_hat_it^(pooled)

A saída não identifica indivíduos nem demonstra causalidade. Fora dos módulos
especiais, plataforma direta permanece não observada.

Uso
---
Auditoria leve:
  python SPINE_GPEv7_PNADC_HISTORICAL_BACKCAST_HARDENING_v1.1.0.py \
    --root /content/drive/MyDrive/aCidadeAlgoritmica/SPINE-GPEv7 \
    --mode audit --strict

Hardening completo:
  python SPINE_GPEv7_PNADC_HISTORICAL_BACKCAST_HARDENING_v1.1.0.py \
    --root /content/drive/MyDrive/aCidadeAlgoritmica/SPINE-GPEv7 \
    --mode full --bootstrap-reps 30 --strict
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
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import joblib
import numpy as np
import pandas as pd
try:
    import pyarrow.parquet as pq
except Exception:  # pragma: no cover
    pq = None
from scipy.optimize import minimize
from scipy.special import expit, logit
from scipy.stats import spearmanr
from sklearn.cluster import KMeans
from sklearn.compose import ColumnTransformer
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    adjusted_rand_score,
    average_precision_score,
    brier_score_loss,
    log_loss,
    roc_auc_score,
    silhouette_score,
)
from sklearn.neighbors import NearestNeighbors
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.utils.extmath import randomized_svd

VERSION = "1.1.0"
SCRIPT_NAME = "SPINE_GPEv7_PNADC_HISTORICAL_BACKCAST_HARDENING_v1.1.0.py"
SCHEMA_VERSION = "spine-gpe-v7-pnadc-historical-backcast-hardening-1.1.0"
MODEL_SCHEMA_VERSION = "spine-gpe-v7-pnadc-backcast-model-1.1.0"
VALIDATION_SCHEMA_VERSION = "spine-gpe-v7-pnadc-backcast-validation-1.1.0"
UPSTREAM_HISTORICAL_VERSION = "1.0.1"
UPSTREAM_HISTORICAL_SCRIPT = "SPINE_GPEv7_PNADC_HISTORICAL_PROXY_ENGINE_v1.0.1.py"
UPSTREAM_DIRECT_VERSION = "1.2.0"
UPSTREAM_DIRECT_SCRIPT = "SPINE_GPEv7_PNADC_CERTIFIER_v1.2.0.py"
UTC = dt.timezone.utc

FEATURES = ["occupation_code", "activity_code", "position_code"]
DIRECT_COLUMNS = [
    "survey_weight", "survey_stratum", "survey_psu",
    "eligible_platform_module", "platform_delivery_direct",
    "V4010", "V4013", "VD4009",
]
HISTORICAL_COLUMNS = [
    "source_period", "survey_weight", "survey_stratum", "survey_psu",
    "occupation_code", "activity_code", "position_code",
    "platform_delivery_probability_calibrated",
    "platform_delivery_direct", "platform_direct_available",
]
ESSENTIAL_LAYOUT_VARIABLES = [
    "V4010", "V4013", "VD4009", "V1028", "ESTRATO", "UPA",
    "UF", "CAPITAL", "RM_RIDE",
]
SUPPORT_ORDER = ["STRONG", "MODERATE", "WEAK", "OUT_OF_SUPPORT"]


@dataclass
class TestResult:
    test_id: str
    status: str
    severity: str
    message: str
    observed: Any = None
    expected: Any = None
    evidence: dict[str, Any] | None = None


@dataclass
class MetricRow:
    model: str
    train_year: int
    test_year: int
    n: int
    n_positive_unweighted: int
    weighted_prevalence: float
    roc_auc: float | None
    average_precision: float | None
    brier: float | None
    null_brier: float | None
    brier_skill: float | None
    log_loss: float | None
    ece_10: float | None
    ece_relative_prevalence: float | None
    calibration_intercept: float | None
    calibration_slope: float | None


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
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def atomic_write_json(path: Path, payload: Any) -> None:
    atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n")


def copy_atomic(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    shutil.copy2(source, temporary)
    os.replace(temporary, destination)


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def setup_logger(verbose: bool = False) -> logging.Logger:
    logger = logging.getLogger("spine_pnadc_backcast_hardening")
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(handler)
    return logger


def canonical_code(series: pd.Series) -> pd.Series:
    values = series.astype("string").str.strip()
    values = values.replace({
        "": pd.NA, ".": pd.NA, "..": pd.NA, "...": pd.NA,
        "nan": pd.NA, "None": pd.NA, "<NA>": pd.NA,
    })
    numeric_like = values.str.fullmatch(r"[+-]?\d+(?:\.0+)?", na=False)
    if numeric_like.any():
        numbers = pd.to_numeric(values.loc[numeric_like], errors="coerce")
        values.loc[numeric_like] = numbers.round().astype("Int64").astype("string")
    return values


def bool_series(series: pd.Series) -> pd.Series:
    if str(series.dtype) == "boolean":
        return series
    if pd.api.types.is_bool_dtype(series):
        return series.astype("boolean")
    values = canonical_code(series)
    return values.map({
        "1": True, "0": False, "2": False,
        "True": True, "False": False, "true": True, "false": False,
    }).astype("boolean")


def numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def safe_one_hot_encoder() -> OneHotEncoder:
    try:
        return OneHotEncoder(handle_unknown="ignore", sparse_output=True, dtype=np.float64)
    except TypeError:  # sklearn < 1.2
        return OneHotEncoder(handle_unknown="ignore", sparse=True, dtype=np.float64)


def fit_pipeline() -> Pipeline:
    transformer = ColumnTransformer(
        [("categorical", safe_one_hot_encoder(), FEATURES)],
        remainder="drop",
    )
    classifier = LogisticRegression(
        C=1.0, solver="liblinear", max_iter=4000,
        random_state=20260723,
    )
    return Pipeline([("preprocess", transformer), ("classifier", classifier)])


def normalize_sample_weights(weights: np.ndarray) -> np.ndarray:
    values = np.asarray(weights, dtype=float)
    good = np.isfinite(values) & (values > 0)
    if not good.any():
        raise ValueError("Nenhum peso positivo para ajuste.")
    mean = float(values[good].mean())
    output = np.zeros_like(values, dtype=float)
    output[good] = values[good] / mean
    return output


def available_columns(path: Path, desired: Sequence[str]) -> list[str]:
    if pq is None:
        raise RuntimeError("pyarrow é obrigatório para ler os Parquets certificados.")
    names = set(pq.ParquetFile(path).schema_arrow.names)
    return [column for column in desired if column in names]


def import_module(path: Path, name: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Não foi possível importar {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def resolve_script(root: Path, filename: str, local_fallback: Path | None = None) -> Path:
    candidates = [root / "scripts" / filename]
    if local_fallback is not None:
        candidates.append(local_fallback)
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError(f"Script upstream ausente: {filename}")


def tests_have_critical_failures(tests: Sequence[TestResult]) -> bool:
    return any(t.severity == "critical" and t.status in {"FAIL", "BLOCKED"} for t in tests)


def validate_path_hash(path: Path, expected: str | None, test_id: str, tests: list[TestResult]) -> bool:
    if not path.exists():
        tests.append(TestResult(test_id, "FAIL", "critical", f"Arquivo ausente: {path}"))
        return False
    observed = sha256_file(path)
    ok = not expected or observed == expected
    tests.append(TestResult(
        test_id, "PASS" if ok else "FAIL", "critical",
        "Hash imutável confirmado." if ok else "Hash diverge do lock upstream.",
        observed={"path": str(path), "sha256": observed}, expected=expected,
    ))
    return ok


def validate_upstream(root: Path, tests: list[TestResult]) -> dict[str, Any]:
    lock_path = root / "00_admin" / "PNADC_HISTORICAL_PROXY_CERTIFICATION_LOCK.json"
    if not lock_path.exists():
        tests.append(TestResult("upstream.historical.lock", "FAIL", "critical", f"Lock ausente: {lock_path}"))
        return {}
    lock = load_json(lock_path)
    status_ok = lock.get("status") == "CERTIFIED"
    version_ok = str(lock.get("script_version")) == UPSTREAM_HISTORICAL_VERSION
    tests.append(TestResult(
        "upstream.historical.status", "PASS" if status_ok else "FAIL", "critical",
        "Core histórico upstream certificado." if status_ok else "Core histórico upstream não certificado.",
        observed=lock.get("status"), expected="CERTIFIED",
    ))
    tests.append(TestResult(
        "upstream.historical.version", "PASS" if version_ok else "FAIL", "critical",
        "Versão upstream esperada confirmada." if version_ok else "Versão upstream divergente.",
        observed=lock.get("script_version"), expected=UPSTREAM_HISTORICAL_VERSION,
    ))
    for year, info in sorted((lock.get("direct_inputs") or {}).items()):
        validate_path_hash(Path(info["path"]), info.get("sha256"), f"upstream.direct.{year}.hash", tests)
    manifest_path = Path(lock.get("manifests", ""))
    if manifest_path.exists():
        expected = (lock.get("artifact_hashes") or {}).get("manifests")
        validate_path_hash(manifest_path, expected, "upstream.manifests.hash", tests)
    else:
        tests.append(TestResult("upstream.manifests", "FAIL", "critical", "Manifest upstream ausente."))
    return {"lock_path": str(lock_path), "lock": lock}


def direct_feature_frame(frame: pd.DataFrame) -> pd.DataFrame:
    result = pd.DataFrame(index=frame.index)
    result["occupation_code"] = canonical_code(frame["V4010"]).str.zfill(4)
    result["activity_code"] = canonical_code(frame["V4013"]).str.zfill(5)
    result["position_code"] = canonical_code(frame["VD4009"])
    for column in FEATURES:
        result[column] = result[column].fillna("__MISSING__").astype("string")
    return result


def load_direct_psu_cells(path: Path, year: int, tests: list[TestResult]) -> pd.DataFrame:
    columns = available_columns(path, DIRECT_COLUMNS)
    required = set(DIRECT_COLUMNS)
    missing = sorted(required - set(columns))
    if missing:
        tests.append(TestResult(
            f"direct.{year}.schema", "FAIL", "critical",
            f"Parquet direto sem colunas essenciais: {missing}",
        ))
        return pd.DataFrame()
    frame = pd.read_parquet(path, columns=columns)
    eligible = bool_series(frame["eligible_platform_module"]).fillna(False)
    target = bool_series(frame["platform_delivery_direct"])
    weight = numeric(frame["survey_weight"])
    keep = eligible & target.notna() & weight.gt(0)
    frame = frame.loc[keep].copy()
    frame["target"] = target.loc[keep].astype(int)
    frame["survey_weight"] = weight.loc[keep]
    features = direct_feature_frame(frame)
    for column in FEATURES:
        frame[column] = features[column]
    frame["survey_stratum"] = frame["survey_stratum"].astype("string").fillna("__MISSING_STRATUM__")
    frame["survey_psu"] = frame["survey_psu"].astype("string").fillna("__MISSING_PSU__")
    frame["year"] = int(year)
    frame["weighted_positive"] = frame["survey_weight"] * frame["target"]
    group_cols = ["year", "survey_stratum", "survey_psu", *FEATURES]
    cells = frame.groupby(group_cols, observed=True, dropna=False).agg(
        weight_total=("survey_weight", "sum"),
        weight_positive=("weighted_positive", "sum"),
        n=("target", "size"),
        n_positive=("target", "sum"),
    ).reset_index()
    positive_count = int(frame["target"].sum())
    tests.append(TestResult(
        f"direct.{year}.sample", "PASS" if positive_count >= 100 else "FAIL", "critical",
        "Amostra direta suficiente para hardening." if positive_count >= 100 else "Poucos positivos diretos.",
        observed={"n": int(len(frame)), "n_positive": positive_count, "psu_cells": int(len(cells))},
        expected="n_positive>=100",
    ))
    return cells


def expand_binomial_cells(cells: pd.DataFrame) -> tuple[pd.DataFrame, np.ndarray, np.ndarray]:
    positive = cells.loc[cells["weight_positive"] > 0, FEATURES].copy()
    y_positive = np.ones(len(positive), dtype=int)
    w_positive = cells.loc[cells["weight_positive"] > 0, "weight_positive"].to_numpy(dtype=float)

    negative_weight = cells["weight_total"] - cells["weight_positive"]
    negative_mask = negative_weight > 0
    negative = cells.loc[negative_mask, FEATURES].copy()
    y_negative = np.zeros(len(negative), dtype=int)
    w_negative = negative_weight.loc[negative_mask].to_numpy(dtype=float)

    features = pd.concat([positive, negative], ignore_index=True)
    y = np.concatenate([y_positive, y_negative])
    weights = np.concatenate([w_positive, w_negative])
    return features, y, weights


def fit_logistic_from_cells(cells: pd.DataFrame) -> Pipeline:
    features, y, weights = expand_binomial_cells(cells)
    if len(np.unique(y)) < 2:
        raise RuntimeError("Ajuste logístico requer as duas classes.")
    model = fit_pipeline()
    model.fit(features, y, classifier__sample_weight=normalize_sample_weights(weights))
    return model


def hash_fold(year: Any, stratum: Any, psu: Any, n_splits: int, seed: int) -> int:
    token = f"{year}|{stratum}|{psu}|{seed}".encode("utf-8")
    return int(hashlib.sha256(token).hexdigest()[:12], 16) % n_splits


def internal_oof_predictions(cells: pd.DataFrame, n_splits: int, seed: int) -> np.ndarray:
    folds = np.array([
        hash_fold(row.year, row.survey_stratum, row.survey_psu, n_splits, seed)
        for row in cells[["year", "survey_stratum", "survey_psu"]].itertuples(index=False)
    ])
    predictions = np.full(len(cells), np.nan, dtype=float)
    for fold in range(n_splits):
        train = folds != fold
        test = folds == fold
        if not test.any():
            continue
        train_cells = cells.loc[train]
        if train_cells["weight_positive"].sum() <= 0 or (train_cells["weight_total"] - train_cells["weight_positive"]).sum() <= 0:
            raise RuntimeError(f"Fold interno {fold} sem as duas classes.")
        model = fit_logistic_from_cells(train_cells)
        predictions[test] = model.predict_proba(cells.loc[test, FEATURES])[:, 1]
    if np.isnan(predictions).any():
        raise RuntimeError("Previsões OOF internas incompletas.")
    return predictions


def fit_isotonic_from_cell_predictions(cells: pd.DataFrame, raw_predictions: np.ndarray) -> IsotonicRegression:
    p = np.asarray(raw_predictions, dtype=float)
    positive = cells["weight_positive"].to_numpy(dtype=float)
    negative = (cells["weight_total"] - cells["weight_positive"]).to_numpy(dtype=float)
    raw = np.concatenate([p[positive > 0], p[negative > 0]])
    y = np.concatenate([np.ones((positive > 0).sum()), np.zeros((negative > 0).sum())])
    weights = np.concatenate([positive[positive > 0], negative[negative > 0]])
    calibrator = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
    calibrator.fit(raw, y, sample_weight=normalize_sample_weights(weights))
    return calibrator


def fit_mapping_bundle(cells: pd.DataFrame, name: str, n_splits: int = 5, seed: int = 20260723) -> dict[str, Any]:
    oof_raw = internal_oof_predictions(cells, n_splits=n_splits, seed=seed)
    calibrator = fit_isotonic_from_cell_predictions(cells, oof_raw)
    final_model = fit_logistic_from_cells(cells)
    return {
        "name": name,
        "features": FEATURES,
        "pipeline": final_model,
        "calibrator": calibrator,
        "internal_oof_raw": oof_raw,
        "n_splits": n_splits,
        "seed": seed,
        "train_years": sorted(cells["year"].unique().astype(int).tolist()),
    }


def bundle_predict(bundle: Mapping[str, Any], frame: pd.DataFrame) -> np.ndarray:
    raw = bundle["pipeline"].predict_proba(frame[FEATURES])[:, 1]
    return np.asarray(bundle["calibrator"].transform(raw), dtype=float)


def cell_metric_arrays(cells: pd.DataFrame, predictions: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    p = np.asarray(predictions, dtype=float)
    positive = cells["weight_positive"].to_numpy(dtype=float)
    negative = (cells["weight_total"] - cells["weight_positive"]).to_numpy(dtype=float)
    y = np.concatenate([np.ones((positive > 0).sum()), np.zeros((negative > 0).sum())]).astype(int)
    pred = np.concatenate([p[positive > 0], p[negative > 0]])
    weights = np.concatenate([positive[positive > 0], negative[negative > 0]])
    return y, pred, normalize_sample_weights(weights)


def expected_calibration_error(y: np.ndarray, p: np.ndarray, w: np.ndarray, bins: int = 10) -> float:
    order = np.argsort(p)
    y, p, w = y[order], p[order], w[order]
    cumulative = np.cumsum(w) / w.sum()
    labels = np.minimum((cumulative * bins).astype(int), bins - 1)
    error = 0.0
    for label in range(bins):
        mask = labels == label
        if not mask.any():
            continue
        share = w[mask].sum() / w.sum()
        error += share * abs(np.average(y[mask], weights=w[mask]) - np.average(p[mask], weights=w[mask]))
    return float(error)


def calibration_intercept_slope(y: np.ndarray, p: np.ndarray, w: np.ndarray) -> tuple[float | None, float | None]:
    clipped = np.clip(p, 1e-8, 1 - 1e-8)
    x = logit(clipped)
    w = normalize_sample_weights(w)

    def objective(params: np.ndarray) -> float:
        eta = params[0] + params[1] * x
        probability = expit(eta)
        probability = np.clip(probability, 1e-12, 1 - 1e-12)
        return float(-np.sum(w * (y * np.log(probability) + (1 - y) * np.log(1 - probability))))

    result = minimize(objective, np.array([0.0, 1.0]), method="BFGS")
    if not result.success or not np.all(np.isfinite(result.x)):
        return None, None
    return float(result.x[0]), float(result.x[1])


def safe_metric(function: Any, *args: Any, **kwargs: Any) -> float | None:
    try:
        value = float(function(*args, **kwargs))
        return value if math.isfinite(value) else None
    except Exception:
        return None


def evaluate_predictions(model_name: str, train_year: int, test_year: int, cells: pd.DataFrame, predictions: np.ndarray) -> MetricRow:
    y, p, w = cell_metric_arrays(cells, predictions)
    prevalence = float(np.average(y, weights=w))
    null = np.full_like(p, prevalence, dtype=float)
    brier = safe_metric(brier_score_loss, y, p, sample_weight=w)
    null_brier = safe_metric(brier_score_loss, y, null, sample_weight=w)
    skill = None if brier is None or null_brier in (None, 0) else 1.0 - brier / null_brier
    ece = safe_metric(expected_calibration_error, y, p, w)
    intercept, slope = calibration_intercept_slope(y, p, w)
    return MetricRow(
        model=model_name,
        train_year=train_year,
        test_year=test_year,
        n=int(cells["n"].sum()),
        n_positive_unweighted=int(cells["n_positive"].sum()),
        weighted_prevalence=prevalence,
        roc_auc=safe_metric(roc_auc_score, y, p, sample_weight=w),
        average_precision=safe_metric(average_precision_score, y, p, sample_weight=w),
        brier=brier,
        null_brier=null_brier,
        brier_skill=skill,
        log_loss=safe_metric(log_loss, y, np.column_stack([1 - p, p]), sample_weight=w, labels=[0, 1]),
        ece_10=ece,
        ece_relative_prevalence=None if ece is None or prevalence <= 0 else ece / prevalence,
        calibration_intercept=intercept,
        calibration_slope=slope,
    )


def reliability_table(model: str, train_year: int, test_year: int, cells: pd.DataFrame, predictions: np.ndarray, bins: int = 10) -> pd.DataFrame:
    frame = cells[["weight_total", "weight_positive"]].copy()
    frame["prediction"] = predictions
    frame = frame.sort_values("prediction").reset_index(drop=True)
    cumulative = frame["weight_total"].cumsum() / frame["weight_total"].sum()
    frame["bin"] = np.minimum((cumulative * bins).astype(int) + 1, bins)
    rows: list[dict[str, Any]] = []
    for label, group in frame.groupby("bin", observed=True):
        total = float(group["weight_total"].sum())
        rows.append({
            "model": model,
            "train_year": train_year,
            "test_year": test_year,
            "bin": int(label),
            "n_cells": int(len(group)),
            "weighted_population": total,
            "mean_predicted": float(np.average(group["prediction"], weights=group["weight_total"])),
            "observed_rate": float(group["weight_positive"].sum() / total) if total > 0 else np.nan,
            "min_predicted": float(group["prediction"].min()),
            "max_predicted": float(group["prediction"].max()),
        })
    return pd.DataFrame(rows)


def top_risk_table(model: str, train_year: int, test_year: int, cells: pd.DataFrame, predictions: np.ndarray) -> pd.DataFrame:
    frame = cells[["weight_total", "weight_positive"]].copy()
    frame["prediction"] = predictions
    frame = frame.sort_values("prediction", ascending=False).reset_index(drop=True)
    frame["cum_weight"] = frame["weight_total"].cumsum()
    total_weight = float(frame["weight_total"].sum())
    rows: list[dict[str, Any]] = []
    for share in (0.01, 0.05, 0.10):
        selected = frame[frame["cum_weight"] <= total_weight * share]
        if selected.empty:
            selected = frame.iloc[:1]
        population = float(selected["weight_total"].sum())
        rows.append({
            "model": model,
            "train_year": train_year,
            "test_year": test_year,
            "top_share": share,
            "weighted_population": population,
            "mean_predicted": float(np.average(selected["prediction"], weights=selected["weight_total"])),
            "observed_rate": float(selected["weight_positive"].sum() / population) if population > 0 else np.nan,
            "capture_share_of_positives": float(selected["weight_positive"].sum() / frame["weight_positive"].sum()) if frame["weight_positive"].sum() > 0 else np.nan,
        })
    return pd.DataFrame(rows)


def survey_total_from_psu_cells(cells: pd.DataFrame, contribution: np.ndarray, denominator: np.ndarray | None = None) -> dict[str, Any]:
    frame = cells[["survey_stratum", "survey_psu"]].copy()
    frame["contribution"] = np.asarray(contribution, dtype=float)
    if denominator is None:
        frame["denominator"] = cells["weight_total"].to_numpy(dtype=float)
    else:
        frame["denominator"] = np.asarray(denominator, dtype=float)
    psu = frame.groupby(["survey_stratum", "survey_psu"], observed=True).agg(
        numerator=("contribution", "sum"), denominator=("denominator", "sum")
    ).reset_index()
    total = float(psu["numerator"].sum())
    denominator_total = float(psu["denominator"].sum())
    share = total / denominator_total if denominator_total > 0 else np.nan
    total_var = 0.0
    ratio_var_num = 0.0
    for _, stratum in psu.groupby("survey_stratum", observed=True):
        m = len(stratum)
        if m <= 1:
            continue
        dev_total = stratum["numerator"] - stratum["numerator"].mean()
        ratio_cluster = stratum["numerator"] - share * stratum["denominator"]
        dev_ratio = ratio_cluster - ratio_cluster.mean()
        total_var += m / (m - 1) * float(np.square(dev_total).sum())
        ratio_var_num += m / (m - 1) * float(np.square(dev_ratio).sum())
    total_se = math.sqrt(max(total_var, 0.0))
    share_se = math.sqrt(max(ratio_var_num, 0.0)) / denominator_total if denominator_total > 0 else np.nan
    return {
        "total": total,
        "total_se": total_se,
        "share": share,
        "share_se": share_se,
        "n_strata": int(psu["survey_stratum"].nunique()),
        "n_psu": int(len(psu)),
    }


def golden_totals(year: int, cells: pd.DataFrame, model_predictions: Mapping[str, np.ndarray]) -> pd.DataFrame:
    direct = survey_total_from_psu_cells(cells, cells["weight_positive"].to_numpy(dtype=float))
    rows: list[dict[str, Any]] = []
    for name, predictions in model_predictions.items():
        model_stats = survey_total_from_psu_cells(cells, cells["weight_total"].to_numpy(dtype=float) * predictions)
        difference = model_stats["total"] - direct["total"]
        relative = difference / direct["total"] if direct["total"] else np.nan
        rows.append({
            "year": year,
            "model": name,
            "direct_total": direct["total"],
            "direct_total_se": direct["total_se"],
            "direct_share": direct["share"],
            "direct_share_se": direct["share_se"],
            "model_total": model_stats["total"],
            "model_total_se_conditional": model_stats["total_se"],
            "model_share": model_stats["share"],
            "model_share_se_conditional": model_stats["share_se"],
            "difference_absolute": difference,
            "difference_relative": relative,
            "predicted_to_observed_ratio": model_stats["total"] / direct["total"] if direct["total"] else np.nan,
        })
    return pd.DataFrame(rows)


def parse_layout_candidate(upstream: Any, path: Path) -> Any | None:
    candidates = upstream.discover_layout_candidates([path.parent], logging.getLogger("layout_discovery"))
    for candidate in candidates:
        if Path(candidate.path).resolve() == path.resolve():
            return candidate
    return None


def layout_equivalence(root: Path, upstream: Any, historical_lock: Mapping[str, Any], tests: list[TestResult]) -> pd.DataFrame:
    manifests_path = Path(historical_lock["manifests"])
    manifests = load_json(manifests_path)
    selected_paths = sorted({Path(item["layout_path"]) for item in manifests if item.get("layout_path")})
    candidates: dict[str, Any] = {}
    for path in selected_paths:
        candidate = parse_layout_candidate(upstream, path)
        if candidate is not None:
            candidates[str(path)] = candidate

    search_roots = [
        root / "01_raw" / "10_ibge",
        root / "02_interim" / "10_pnadc_certification" / "extracted_docs",
        root / "02_interim" / "pnadc_documentation_extracted",
    ]
    try:
        for candidate in upstream.discover_layout_candidates(search_roots, logging.getLogger("layout_discovery")):
            names = {f.variable.upper() for f in candidate.fields}
            if not candidate.has_s140093 and set(["V4010", "V4013", "VD4009"]).issubset(names):
                candidates.setdefault(candidate.path, candidate)
    except Exception as exc:
        tests.append(TestResult("layout.discovery", "WARN", "high", f"Falha parcial na descoberta de layouts: {exc}"))

    rows: list[dict[str, Any]] = []
    for path_str, candidate in sorted(candidates.items()):
        path = Path(path_str)
        year_tokens = re.findall(r"20\d{2}", str(path))
        inferred_year = year_tokens[-1] if year_tokens else "unspecified"
        by_name = {f.variable.upper(): f for f in candidate.fields}
        for variable in ESSENTIAL_LAYOUT_VARIABLES:
            field = by_name.get(variable.upper())
            rows.append({
                "layout_path": str(path),
                "layout_sha256": candidate.sha256 or sha256_file(path),
                "layout_width": int(candidate.width),
                "inferred_year": inferred_year,
                "variable": variable,
                "present": field is not None,
                "start_1based": getattr(field, "start_1based", np.nan),
                "end_1based": getattr(field, "end_1based", np.nan),
                "width": getattr(field, "width", np.nan),
                "type": getattr(field, "type", None),
                "label": getattr(field, "label", None),
                "selected_upstream": str(path) in {str(p) for p in selected_paths},
            })
    matrix = pd.DataFrame(rows)
    if matrix.empty:
        tests.append(TestResult("layout.equivalence", "FAIL", "critical", "Nenhum layout regular interpretável."))
        return matrix

    selected = matrix[matrix["selected_upstream"]]
    required_present = selected.groupby("layout_path")["present"].all().all() if not selected.empty else False
    tests.append(TestResult(
        "layout.selected.essential_variables", "PASS" if required_present else "FAIL", "critical",
        "Variáveis essenciais presentes nos layouts selecionados." if required_present else "Variáveis essenciais ausentes em layout selecionado.",
    ))

    reference = selected.iloc[0:0]
    if not selected.empty:
        first_path = selected["layout_path"].iloc[0]
        reference = selected[selected["layout_path"] == first_path].set_index("variable")
    exact = True
    for path, group in selected.groupby("layout_path", observed=True):
        current = group.set_index("variable")
        for variable in ESSENTIAL_LAYOUT_VARIABLES:
            if variable not in reference.index or variable not in current.index:
                exact = False
                continue
            cols = ["present", "start_1based", "end_1based", "width", "type"]
            if any(str(reference.loc[variable, col]) != str(current.loc[variable, col]) for col in cols):
                exact = False
    tests.append(TestResult(
        "layout.selected.signature_equivalence", "PASS" if exact else "FAIL", "critical",
        "Assinaturas essenciais equivalentes entre layouts selecionados." if exact else "Assinaturas essenciais divergentes.",
    ))

    independent_years = sorted(set(matrix.loc[matrix["inferred_year"] != "unspecified", "inferred_year"]))
    if len(independent_years) < 2:
        tests.append(TestResult(
            "layout.independent_year_versions", "WARN", "high",
            "Somente uma ou nenhuma versão anual independente foi localizada; equivalência semântica anual permanece documentada como limitação.",
            observed=independent_years, expected=">=2 annual versions",
        ))
    else:
        tests.append(TestResult(
            "layout.independent_year_versions", "PASS", "secondary",
            "Múltiplas versões anuais de layout foram localizadas para comparação.", observed=independent_years,
        ))
    return matrix


def load_historical_psu_cells(lock: Mapping[str, Any], tests: list[TestResult], logger: logging.Logger) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    manifests = {item["period"]: item for item in load_json(Path(lock["manifests"]))}
    for period, path_str in sorted((lock.get("historical_outputs") or {}).items()):
        path = Path(path_str)
        manifest = manifests.get(period, {})
        if not validate_path_hash(path, manifest.get("output_sha256"), f"historical.{period}.hash", tests):
            continue
        columns = available_columns(path, HISTORICAL_COLUMNS)
        required = {"source_period", "survey_weight", "survey_stratum", "survey_psu", *FEATURES}
        missing = sorted(required - set(columns))
        if missing:
            tests.append(TestResult(f"historical.{period}.schema", "FAIL", "critical", f"Colunas ausentes: {missing}"))
            continue
        frame = pd.read_parquet(path, columns=columns)
        if "platform_delivery_direct" in frame and not frame["platform_delivery_direct"].isna().all():
            tests.append(TestResult(f"historical.{period}.direct_absent", "FAIL", "critical", "Proxy preencheu identificação direta."))
            continue
        if "platform_direct_available" in frame and frame["platform_direct_available"].astype(bool).any():
            tests.append(TestResult(f"historical.{period}.direct_flag", "FAIL", "critical", "Flag direto indevidamente verdadeiro."))
            continue
        for column in FEATURES:
            frame[column] = canonical_code(frame[column]).fillna("__MISSING__")
            if column == "occupation_code":
                frame[column] = frame[column].str.zfill(4)
            if column == "activity_code":
                frame[column] = frame[column].str.zfill(5)
        frame["survey_weight"] = numeric(frame["survey_weight"])
        frame = frame[frame["survey_weight"].gt(0)].copy()
        frame["survey_stratum"] = frame["survey_stratum"].astype("string").fillna("__MISSING_STRATUM__")
        frame["survey_psu"] = frame["survey_psu"].astype("string").fillna("__MISSING_PSU__")
        if "platform_delivery_probability_calibrated" in frame:
            frame["old_probability_weighted"] = frame["survey_weight"] * numeric(frame["platform_delivery_probability_calibrated"]).fillna(0)
        else:
            frame["old_probability_weighted"] = 0.0
        group_cols = ["source_period", "survey_stratum", "survey_psu", *FEATURES]
        cells = frame.groupby(group_cols, observed=True, dropna=False).agg(
            weight_total=("survey_weight", "sum"),
            old_probability_weighted=("old_probability_weighted", "sum"),
            n=("survey_weight", "size"),
        ).reset_index()
        frames.append(cells)
        tests.append(TestResult(
            f"historical.{period}.loaded", "PASS", "critical",
            "Parquet histórico imutável agregado por UPA e célula.",
            observed={"rows": int(len(frame)), "psu_cells": int(len(cells))},
        ))
        logger.info("Histórico carregado | %s | registros=%d | células-PSU=%d", period, len(frame), len(cells))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def feature_cells_from_psu(cells: pd.DataFrame, direct: bool = False) -> pd.DataFrame:
    group_cols = FEATURES
    aggregations: dict[str, tuple[str, str]] = {
        "weight_total": ("weight_total", "sum"),
        "n": ("n", "sum"),
    }
    if direct:
        aggregations["weight_positive"] = ("weight_positive", "sum")
        aggregations["n_positive"] = ("n_positive", "sum")
    return cells.groupby(group_cols, observed=True, dropna=False).agg(**aggregations).reset_index()


def fit_weighted_mca(calibration_feature_cells: pd.DataFrame, n_components: int = 8) -> dict[str, Any]:
    encoder = safe_one_hot_encoder()
    z_sparse = encoder.fit_transform(calibration_feature_cells[FEATURES])
    z = z_sparse.toarray().astype(np.float32, copy=False)
    q = float(len(FEATURES))
    weights = calibration_feature_cells["weight_total"].to_numpy(dtype=float)
    total_weight = float(weights.sum())
    column_mass = (weights[:, None] * z).sum(axis=0) / (total_weight * q)
    column_mass = np.clip(column_mass, 1e-12, None).astype(np.float32)
    x = ((z / q - column_mass[None, :]) / np.sqrt(column_mass[None, :])).astype(np.float32, copy=False)
    row_mass = weights / total_weight
    a = np.sqrt(row_mass[:, None]) * x
    max_components = max(1, min(n_components, a.shape[0] - 1, a.shape[1] - 1))
    _, singular_values, vt = randomized_svd(a, n_components=max_components, random_state=20260723)
    coordinates = x @ vt.T
    total_inertia = float(np.square(a).sum())
    explained = np.square(singular_values) / total_inertia if total_inertia > 0 else np.zeros_like(singular_values)
    feature_names = encoder.get_feature_names_out(FEATURES).tolist()
    return {
        "encoder": encoder,
        "column_mass": column_mass,
        "components": vt,
        "singular_values": singular_values,
        "explained_inertia_ratio": explained,
        "coordinates": coordinates,
        "feature_names": feature_names,
        "q": q,
    }


def project_mca(mca: Mapping[str, Any], feature_frame: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    encoder: OneHotEncoder = mca["encoder"]
    z = encoder.transform(feature_frame[FEATURES]).toarray().astype(np.float32, copy=False)
    q = float(mca["q"])
    c = np.asarray(mca["column_mass"], dtype=np.float32)
    x = ((z / q - c[None, :]) / np.sqrt(c[None, :])).astype(np.float32, copy=False)
    coordinates = x @ np.asarray(mca["components"], dtype=float).T
    unseen_count = np.zeros(len(feature_frame), dtype=int)
    categories = encoder.categories_
    for idx, column in enumerate(FEATURES):
        known = set(str(value) for value in categories[idx])
        unseen_count += (~feature_frame[column].astype(str).isin(known)).to_numpy(dtype=int)
    return coordinates, unseen_count


def mca_category_coordinates(mca: Mapping[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    components = np.asarray(mca["components"], dtype=float)
    for index, name in enumerate(mca["feature_names"]):
        feature, category = name.split("_", 1)
        row = {
            "encoded_category": name,
            "feature": feature,
            "category": category,
            "column_mass": float(mca["column_mass"][index]),
        }
        for axis in range(components.shape[0]):
            row[f"axis_{axis + 1}_loading"] = float(components[axis, index])
        rows.append(row)
    return pd.DataFrame(rows)


def support_mapping(
    direct_2022_features: pd.DataFrame,
    direct_2024_features: pd.DataFrame,
    pooled_features: pd.DataFrame,
    historical_features: pd.DataFrame,
    mca: Mapping[str, Any],
) -> tuple[pd.DataFrame, dict[str, float]]:
    calibration_coords = np.asarray(mca["coordinates"], dtype=float)
    nearest = NearestNeighbors(n_neighbors=min(2, len(calibration_coords)), metric="euclidean")
    nearest.fit(calibration_coords)
    distances_cal, _ = nearest.kneighbors(calibration_coords)
    loo = distances_cal[:, 1] if distances_cal.shape[1] > 1 else distances_cal[:, 0]
    thresholds = {
        "q75": float(np.quantile(loo, 0.75)),
        "q95": float(np.quantile(loo, 0.95)),
        "q99": float(np.quantile(loo, 0.99)),
    }
    hist_coords, unseen = project_mca(mca, historical_features)
    hist_distance, hist_neighbor = nearest.kneighbors(hist_coords, n_neighbors=1)
    hist_distance = hist_distance[:, 0]
    hist_neighbor = hist_neighbor[:, 0]

    keys_2022 = set(map(tuple, direct_2022_features[FEATURES].astype(str).to_numpy()))
    keys_2024 = set(map(tuple, direct_2024_features[FEATURES].astype(str).to_numpy()))
    historical_keys = list(map(tuple, historical_features[FEATURES].astype(str).to_numpy()))

    classes: list[str] = []
    exact_2022: list[bool] = []
    exact_2024: list[bool] = []
    for key, distance, n_unseen in zip(historical_keys, hist_distance, unseen):
        in_2022 = key in keys_2022
        in_2024 = key in keys_2024
        exact_2022.append(in_2022)
        exact_2024.append(in_2024)
        if n_unseen > 0:
            support = "OUT_OF_SUPPORT"
        elif in_2022 and in_2024:
            support = "STRONG"
        elif in_2022 or in_2024 or distance <= thresholds["q75"]:
            support = "MODERATE"
        elif distance <= thresholds["q95"]:
            support = "WEAK"
        else:
            support = "OUT_OF_SUPPORT"
        classes.append(support)

    output = historical_features[FEATURES].copy()
    for axis in range(hist_coords.shape[1]):
        output[f"mca_axis_{axis + 1}"] = hist_coords[:, axis]
    output["nearest_calibration_distance"] = hist_distance
    output["nearest_calibration_cell_index"] = hist_neighbor
    output["unseen_category_count"] = unseen
    output["exact_cell_2022"] = exact_2022
    output["exact_cell_2024"] = exact_2024
    output["support_class"] = pd.Categorical(classes, categories=SUPPORT_ORDER, ordered=True)
    return output, thresholds


def choose_and_fit_clusters(calibration_cells: pd.DataFrame, coordinates: np.ndarray, k_min: int = 4, k_max: int = 8) -> tuple[KMeans, pd.DataFrame, pd.DataFrame]:
    n = len(calibration_cells)
    if n < k_min:
        raise RuntimeError("Poucas células para clustering.")
    rng = np.random.default_rng(20260723)
    sample_index = np.arange(n) if n <= 5000 else rng.choice(n, size=5000, replace=False)
    evaluation_rows: list[dict[str, Any]] = []
    best_model: KMeans | None = None
    best_score = -np.inf
    for k in range(k_min, min(k_max, n - 1) + 1):
        model = KMeans(n_clusters=k, n_init=20, random_state=20260723)
        labels = model.fit_predict(coordinates, sample_weight=calibration_cells["weight_total"].to_numpy(dtype=float))
        score = safe_metric(silhouette_score, coordinates[sample_index], labels[sample_index])
        evaluation_rows.append({"k": k, "silhouette": score})
        if score is not None and score > best_score:
            best_score = score
            best_model = model
    if best_model is None:
        best_model = KMeans(n_clusters=k_min, n_init=20, random_state=20260723).fit(
            coordinates, sample_weight=calibration_cells["weight_total"].to_numpy(dtype=float)
        )

    reference = best_model.labels_
    stability_rows: list[dict[str, Any]] = []
    for seed in (20260724, 20260725, 20260726, 20260727, 20260728):
        alternative = KMeans(n_clusters=best_model.n_clusters, n_init=10, random_state=seed).fit(
            coordinates, sample_weight=calibration_cells["weight_total"].to_numpy(dtype=float)
        )
        stability_rows.append({
            "reference_seed": 20260723,
            "alternative_seed": seed,
            "k": best_model.n_clusters,
            "adjusted_rand_index": float(adjusted_rand_score(reference, alternative.labels_)),
        })
    return best_model, pd.DataFrame(evaluation_rows), pd.DataFrame(stability_rows)


def cluster_profiles(calibration_cells: pd.DataFrame, labels: np.ndarray) -> pd.DataFrame:
    frame = calibration_cells.copy()
    frame["cluster"] = labels
    rows: list[dict[str, Any]] = []
    for cluster, group in frame.groupby("cluster", observed=True):
        total = float(group["weight_total"].sum())
        positive = float(group.get("weight_positive", pd.Series(0, index=group.index)).sum())
        row: dict[str, Any] = {
            "cluster": int(cluster),
            "weighted_population": total,
            "direct_weighted_prevalence": positive / total if total > 0 else np.nan,
            "n_feature_cells": int(len(group)),
        }
        for feature in FEATURES:
            top = group.groupby(feature, observed=True)["weight_total"].sum().sort_values(ascending=False).head(5)
            row[f"top_{feature}"] = "; ".join(f"{index}:{value / total:.4f}" for index, value in top.items())
        rows.append(row)
    return pd.DataFrame(rows)


def temporal_knn_benchmark(
    cells_by_year: Mapping[int, pd.DataFrame],
    mca: Mapping[str, Any],
    k_grid: Sequence[int] = (5, 10, 20, 30, 50),
) -> tuple[int, pd.DataFrame]:
    rows: list[dict[str, Any]] = []
    for k in k_grid:
        for train_year, test_year in ((2022, 2024), (2024, 2022)):
            train = feature_cells_from_psu(cells_by_year[train_year], direct=True)
            test = feature_cells_from_psu(cells_by_year[test_year], direct=True)
            train_coords, _ = project_mca(mca, train)
            test_coords, _ = project_mca(mca, test)
            n_neighbors = min(k, len(train))
            nn = NearestNeighbors(n_neighbors=n_neighbors).fit(train_coords)
            distances, indices = nn.kneighbors(test_coords)
            train_rate = np.divide(
                train["weight_positive"].to_numpy(dtype=float),
                train["weight_total"].to_numpy(dtype=float),
                out=np.zeros(len(train), dtype=float),
                where=train["weight_total"].to_numpy(dtype=float) > 0,
            )
            train_weight = train["weight_total"].to_numpy(dtype=float)
            prior = float(train["weight_positive"].sum() / train["weight_total"].sum())
            neighbor_weight = np.sqrt(train_weight[indices]) / np.maximum(distances, 1e-6)
            numerator = (neighbor_weight * train_rate[indices]).sum(axis=1)
            denominator = neighbor_weight.sum(axis=1)
            smoothing = np.median(denominator[denominator > 0]) * 0.05 if np.any(denominator > 0) else 1.0
            prediction = (numerator + smoothing * prior) / (denominator + smoothing)
            metric = evaluate_predictions(f"weighted_knn_k{k}", train_year, test_year, test, prediction)
            rows.append(asdict(metric))
    metrics = pd.DataFrame(rows)
    grouped = metrics.groupby("model", observed=True)["brier_skill"].mean().sort_values(ascending=False)
    if grouped.empty:
        return int(k_grid[0]), metrics
    best_name = str(grouped.index[0])
    best_k = int(re.search(r"k(\d+)$", best_name).group(1))
    return best_k, metrics


def fit_knn_pooled(calibration_cells: pd.DataFrame, mca: Mapping[str, Any], k: int) -> dict[str, Any]:
    feature_cells = feature_cells_from_psu(calibration_cells, direct=True)
    coords, _ = project_mca(mca, feature_cells)
    nn = NearestNeighbors(n_neighbors=min(k, len(feature_cells))).fit(coords)
    rates = np.divide(
        feature_cells["weight_positive"].to_numpy(dtype=float),
        feature_cells["weight_total"].to_numpy(dtype=float),
        out=np.zeros(len(feature_cells), dtype=float),
        where=feature_cells["weight_total"].to_numpy(dtype=float) > 0,
    )
    prior = float(feature_cells["weight_positive"].sum() / feature_cells["weight_total"].sum())
    return {
        "k": k,
        "neighbors": nn,
        "coordinates": coords,
        "rates": rates,
        "weights": feature_cells["weight_total"].to_numpy(dtype=float),
        "prior": prior,
    }


def knn_predict(bundle: Mapping[str, Any], coordinates: np.ndarray) -> np.ndarray:
    distances, indices = bundle["neighbors"].kneighbors(coordinates)
    weights = np.sqrt(bundle["weights"][indices]) / np.maximum(distances, 1e-6)
    numerator = (weights * bundle["rates"][indices]).sum(axis=1)
    denominator = weights.sum(axis=1)
    smoothing = np.median(denominator[denominator > 0]) * 0.05 if np.any(denominator > 0) else 1.0
    return (numerator + smoothing * bundle["prior"]) / (denominator + smoothing)


def bootstrap_psu_multipliers(cells: pd.DataFrame, rng: np.random.Generator) -> pd.Series:
    group_frame = cells[["year", "survey_stratum", "survey_psu"]].drop_duplicates()
    counts: dict[tuple[Any, Any, Any], int] = {}
    for (year, stratum), group in group_frame.groupby(["year", "survey_stratum"], observed=True):
        psus = group["survey_psu"].astype(str).to_numpy()
        sampled = rng.choice(psus, size=len(psus), replace=True)
        unique, frequency = np.unique(sampled, return_counts=True)
        for psu, count in zip(unique, frequency):
            counts[(year, stratum, psu)] = int(count)
    keys = list(zip(cells["year"], cells["survey_stratum"], cells["survey_psu"].astype(str)))
    return pd.Series([counts.get(key, 0) for key in keys], index=cells.index, dtype=float)


def bootstrap_model_uncertainty(
    direct_cells: pd.DataFrame,
    historical_feature_cells: pd.DataFrame,
    historical_period_feature_weights: pd.DataFrame,
    reps: int,
    n_splits: int,
    logger: logging.Logger,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if reps <= 0:
        return pd.DataFrame(), pd.DataFrame()
    rng = np.random.default_rng(20260723)
    predictions: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for replicate in range(1, reps + 1):
        try:
            multiplier = bootstrap_psu_multipliers(direct_cells, rng)
            boot = direct_cells.copy()
            boot["weight_total"] *= multiplier
            boot["weight_positive"] *= multiplier
            boot = boot[boot["weight_total"] > 0].copy()
            bundle = fit_mapping_bundle(boot, f"bootstrap_{replicate}", n_splits=n_splits, seed=20260723 + replicate)
            p = bundle_predict(bundle, historical_feature_cells)
            mapping = historical_feature_cells[FEATURES].copy()
            mapping["probability"] = p
            merged = historical_period_feature_weights.merge(mapping, on=FEATURES, how="left", validate="many_to_one")
            for period, group in merged.groupby("source_period", observed=True):
                predictions.append({
                    "replicate": replicate,
                    "source_period": period,
                    "total": float((group["weight_total"] * group["probability"]).sum()),
                    "share": float((group["weight_total"] * group["probability"]).sum() / group["weight_total"].sum()),
                })
            logger.info("Bootstrap de modelo %d/%d concluído", replicate, reps)
        except Exception as exc:
            failures.append({"replicate": replicate, "error": str(exc)})
            logger.warning("Bootstrap %d falhou: %s", replicate, exc)
    raw = pd.DataFrame(predictions)
    if raw.empty:
        return raw, pd.DataFrame(failures)
    summary = raw.groupby("source_period", observed=True).agg(
        model_bootstrap_reps=("replicate", "nunique"),
        model_total_mean=("total", "mean"),
        model_total_sd=("total", "std"),
        model_total_p025=("total", lambda x: np.quantile(x, 0.025)),
        model_total_median=("total", "median"),
        model_total_p975=("total", lambda x: np.quantile(x, 0.975)),
        model_share_mean=("share", "mean"),
        model_share_sd=("share", "std"),
        model_share_p025=("share", lambda x: np.quantile(x, 0.025)),
        model_share_p975=("share", lambda x: np.quantile(x, 0.975)),
    ).reset_index()
    summary.attrs["failures"] = failures
    return raw, summary


def historical_predictions_and_estimates(
    historical_psu_cells: pd.DataFrame,
    bundles: Mapping[str, Mapping[str, Any]],
    support_table: pd.DataFrame,
    cluster_model: KMeans,
    mca: Mapping[str, Any],
    knn_bundle: Mapping[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    unique_features = historical_psu_cells[FEATURES].drop_duplicates().reset_index(drop=True)
    feature_map = unique_features.copy()
    for name, bundle in bundles.items():
        feature_map[f"probability_{name}"] = bundle_predict(bundle, unique_features)
    coords, _ = project_mca(mca, unique_features)
    feature_map["probability_knn"] = knn_predict(knn_bundle, coords)
    feature_map["cluster"] = cluster_model.predict(coords)
    feature_map = feature_map.merge(support_table, on=FEATURES, how="left", validate="one_to_one")

    psu = historical_psu_cells.merge(feature_map, on=FEATURES, how="left", validate="many_to_one")
    estimate_rows: list[dict[str, Any]] = []
    support_rows: list[dict[str, Any]] = []
    divergence_rows: list[dict[str, Any]] = []
    for period, group in psu.groupby("source_period", observed=True):
        period_result: dict[str, Any] = {"source_period": period}
        for mapping_name in ("mapping_2022", "mapping_2024", "mapping_pooled", "knn"):
            column = f"probability_{mapping_name}"
            contribution = group["weight_total"].to_numpy(dtype=float) * group[column].to_numpy(dtype=float)
            stats = survey_total_from_psu_cells(group, contribution)
            prefix = mapping_name.replace("mapping_", "")
            period_result[f"total_{prefix}"] = stats["total"]
            period_result[f"total_se_survey_{prefix}"] = stats["total_se"]
            period_result[f"share_{prefix}"] = stats["share"]
            period_result[f"share_se_survey_{prefix}"] = stats["share_se"]
        period_result["transport_total_min"] = min(period_result["total_2022"], period_result["total_2024"], period_result["total_pooled"])
        period_result["transport_total_max"] = max(period_result["total_2022"], period_result["total_2024"], period_result["total_pooled"])
        period_result["transport_range_width"] = period_result["transport_total_max"] - period_result["transport_total_min"]
        period_result["knn_relative_difference_from_pooled"] = (
            period_result["total_knn"] - period_result["total_pooled"]
        ) / period_result["total_pooled"] if period_result["total_pooled"] else np.nan
        estimate_rows.append(period_result)

        total_expected = float((group["weight_total"] * group["probability_mapping_pooled"]).sum())
        for support_class in SUPPORT_ORDER:
            selected = group[group["support_class"].astype(str) == support_class]
            support_rows.append({
                "source_period": period,
                "support_class": support_class,
                "weighted_population": float(selected["weight_total"].sum()),
                "population_share": float(selected["weight_total"].sum() / group["weight_total"].sum()) if group["weight_total"].sum() else np.nan,
                "expected_total_pooled": float((selected["weight_total"] * selected["probability_mapping_pooled"]).sum()),
                "expected_total_share": float((selected["weight_total"] * selected["probability_mapping_pooled"]).sum() / total_expected) if total_expected else np.nan,
                "n_psu_cells": int(len(selected)),
            })

        feature_period = group.groupby(FEATURES, observed=True).agg(
            weight_total=("weight_total", "sum"),
            probability_mapping_pooled=("probability_mapping_pooled", "first"),
            probability_knn=("probability_knn", "first"),
        ).reset_index()
        correlation = safe_metric(spearmanr, feature_period["probability_mapping_pooled"], feature_period["probability_knn"])
        if isinstance(correlation, tuple):
            correlation = correlation[0]
        # scipy retorna SignificanceResult, tratado diretamente abaixo.
        try:
            correlation_value = float(spearmanr(
                feature_period["probability_mapping_pooled"], feature_period["probability_knn"]
            ).statistic)
        except Exception:
            correlation_value = np.nan
        divergence_rows.append({
            "source_period": period,
            "spearman_rank_correlation": correlation_value,
            "mean_absolute_probability_difference": float(np.average(
                np.abs(feature_period["probability_mapping_pooled"] - feature_period["probability_knn"]),
                weights=feature_period["weight_total"],
            )),
            "pooled_total": period_result["total_pooled"],
            "knn_total": period_result["total_knn"],
            "relative_total_difference": period_result["knn_relative_difference_from_pooled"],
        })

    return feature_map, pd.DataFrame(estimate_rows), pd.DataFrame(support_rows), pd.DataFrame(divergence_rows)


def combine_uncertainty(estimates: pd.DataFrame, model_summary: pd.DataFrame) -> pd.DataFrame:
    output = estimates.copy()
    if not model_summary.empty:
        output = output.merge(model_summary, on="source_period", how="left")
    else:
        for column in ["model_total_sd", "model_total_p025", "model_total_p975", "model_share_sd"]:
            output[column] = np.nan
    survey_se = output["total_se_survey_pooled"].fillna(0).to_numpy(dtype=float)
    model_sd = output.get("model_total_sd", pd.Series(0, index=output.index)).fillna(0).to_numpy(dtype=float)
    combined = np.sqrt(np.square(survey_se) + np.square(model_sd))
    output["total_se_survey_plus_model"] = combined
    share_survey_se = output["share_se_survey_pooled"].fillna(0).to_numpy(dtype=float)
    share_model_sd = output.get("model_share_sd", pd.Series(0, index=output.index)).fillna(0).to_numpy(dtype=float)
    share_combined = np.sqrt(np.square(share_survey_se) + np.square(share_model_sd))
    output["share_se_survey_plus_model"] = share_combined
    output["share_ci95_lower_survey_plus_model"] = np.maximum(0, output["share_pooled"] - 1.96 * share_combined)
    output["share_ci95_upper_survey_plus_model"] = np.minimum(1, output["share_pooled"] + 1.96 * share_combined)
    output["total_ci95_lower_survey_plus_model"] = np.maximum(0, output["total_pooled"] - 1.96 * combined)
    output["total_ci95_upper_survey_plus_model"] = output["total_pooled"] + 1.96 * combined
    output["claim_status"] = "MODEL_BASED_HISTORICAL_COMPATIBILITY"
    output["direct_platform_observed"] = False
    output["evidence_tier"] = "C"
    return output


def test_metric_gates(metrics: pd.DataFrame, goldens: pd.DataFrame, tests: list[TestResult], min_auc: float, golden_max_relative_error: float) -> None:
    temporal = metrics[(metrics["train_year"].isin([2022, 2024])) & (metrics["test_year"].isin([2022, 2024]))]
    for row in temporal.itertuples(index=False):
        auc_ok = pd.notna(row.roc_auc) and row.roc_auc >= min_auc
        tests.append(TestResult(
            f"temporal.{row.train_year}_to_{row.test_year}.auc", "PASS" if auc_ok else "FAIL", "critical",
            "Discriminação temporal atingiu o gate." if auc_ok else "AUC temporal abaixo do gate.",
            observed=row.roc_auc, expected=f">={min_auc}",
        ))
        skill_ok = pd.notna(row.brier_skill) and row.brier_skill > 0
        tests.append(TestResult(
            f"temporal.{row.train_year}_to_{row.test_year}.brier_skill", "PASS" if skill_ok else "FAIL", "critical",
            "Probabilidade supera o preditor nulo." if skill_ok else "Brier skill não positivo.",
            observed=row.brier_skill, expected=">0",
        ))
        slope_ok = pd.notna(row.calibration_slope) and 0.75 <= row.calibration_slope <= 1.25
        tests.append(TestResult(
            f"temporal.{row.train_year}_to_{row.test_year}.calibration_slope", "PASS" if slope_ok else "WARN", "high",
            "Slope de calibração em faixa pré-especificada." if slope_ok else "Slope fora de 0,75–1,25; interpretar probabilidades com cautela.",
            observed=row.calibration_slope, expected="0.75<=slope<=1.25",
        ))
    pooled = goldens[goldens["model"] == "mapping_pooled"]
    for row in pooled.itertuples(index=False):
        ok = pd.notna(row.difference_relative) and abs(row.difference_relative) <= golden_max_relative_error
        tests.append(TestResult(
            f"golden.{row.year}.pooled_total", "PASS" if ok else "FAIL", "critical",
            "Total pooled reproduz o direto dentro do gate." if ok else "Total pooled fora do erro relativo permitido.",
            observed=row.difference_relative, expected=f"abs(error)<={golden_max_relative_error}",
        ))


def markdown_tests(tests: Sequence[TestResult]) -> str:
    lines = ["| Status | Severidade | Teste | Mensagem |", "|---|---|---|---|"]
    for test in tests:
        lines.append(f"| {test.status} | {test.severity} | `{test.test_id}` | {test.message} |")
    return "\n".join(lines)


def build_model_card(
    run_id: str,
    metrics: pd.DataFrame,
    goldens: pd.DataFrame,
    support_summary: pd.DataFrame,
    divergence: pd.DataFrame,
    mca: Mapping[str, Any],
    cluster_profiles_df: pd.DataFrame,
    bootstrap_reps: int,
) -> str:
    return f"""# PNADc Historical Backcast — Model Card v{VERSION}

## Identificação

- Run ID: `{run_id}`
- Schema: `{MODEL_SCHEMA_VERSION}`
- Target direto: `platform_delivery_direct = SD14001=1 AND S140093=1`
- Features substantivas: `{', '.join(FEATURES)}`
- Estimando oficial: soma ponderada das probabilidades do mapping pooled.
- Threshold binário: diagnóstico apenas.

## Modelos centrais

1. `mapping_2022`: regressão logística L2 survey-weighted treinada em 2022, calibrada internamente por UPA.
2. `mapping_2024`: regressão logística L2 survey-weighted treinada em 2024, calibrada internamente por UPA.
3. `mapping_pooled`: regressão logística L2 survey-weighted treinada em 2022+2024, calibrada por OOF de UPA.

## Validação temporal calibrada

{metrics.to_markdown(index=False) if not metrics.empty else 'Sem métricas.'}

## Golden totals

{goldens.to_markdown(index=False) if not goldens.empty else 'Sem goldens.'}

## MCA e suporte

- Componentes retidos: {len(mca.get('singular_values', []))}
- Inércia explicada acumulada: {float(np.sum(mca.get('explained_inertia_ratio', []))):.6f}
- Suporte é um diagnóstico de interpolação/extrapolação; não é rótulo de plataforma.

{support_summary.head(40).to_markdown(index=False) if not support_summary.empty else 'Sem suporte.'}

## Tipologias

Clusters são tipologias ocupacionais no espaço MCA. Nenhum cluster é chamado de “plataforma”.

{cluster_profiles_df.to_markdown(index=False) if not cluster_profiles_df.empty else 'Sem clusters.'}

## Benchmark KNN

KNN ponderado é sensibilidade não paramétrica e não substitui o estimando pooled.

{divergence.to_markdown(index=False) if not divergence.empty else 'Sem divergência.'}

## Incerteza

- Survey: linearização por estrato e UPA sobre contribuições previstas.
- Modelo: {bootstrap_reps} réplicas solicitadas de bootstrap de UPA, com refit da logística e do calibrador.
- Transporte: faixa entre mappings 2022, 2024 e pooled.
- Suporte: reportado separadamente, não somado mecanicamente à variância.

## Teto de afirmação

“Reconstrução retrospectiva probabilística da compatibilidade histórica com o trabalho de entrega por plataforma, calibrada nos módulos diretos de 2022T4 e 2024T3, acompanhada de incerteza amostral, incerteza de modelo, sensibilidade de transporte e diagnóstico de suporte.”

## Afirmações proibidas

- identificação individual direta em 2019–2021;
- contagem observada de plataforma fora dos módulos especiais;
- efeito causal da plataforma;
- cluster interpretado como plataforma;
- KNN tratado como série oficial;
- faixa de transporte chamada de intervalo de confiança.
"""


def build_report(
    run_id: str,
    status: str,
    tests: Sequence[TestResult],
    layout_matrix: pd.DataFrame,
    metrics: pd.DataFrame,
    goldens: pd.DataFrame,
    estimates: pd.DataFrame,
    support_summary: pd.DataFrame,
    divergence: pd.DataFrame,
    bootstrap_summary: pd.DataFrame,
) -> str:
    return f"""# SPINE-GPE v7 — PNADc Historical Backcast Hardening Report

## Resultado

- Run ID: `{run_id}`
- Versão: `{VERSION}`
- Status: **{status}**
- Schema: `{SCHEMA_VERSION}`

## Interpretação

O hardening não transforma o backcast em observação direta. O produto final permanece uma estimativa model-based de compatibilidade histórica, evidence tier C.

## Gates

{markdown_tests(tests)}

## Equivalência de layouts

{layout_matrix.head(80).to_markdown(index=False) if not layout_matrix.empty else 'Sem matriz.'}

## Métricas temporais held-out

{metrics.to_markdown(index=False) if not metrics.empty else 'Sem métricas.'}

## Golden totals

{goldens.to_markdown(index=False) if not goldens.empty else 'Sem goldens.'}

## Estimativas finais

{estimates.to_markdown(index=False) if not estimates.empty else 'Sem estimativas.'}

## Suporte histórico

{support_summary.to_markdown(index=False) if not support_summary.empty else 'Sem suporte.'}

## Divergência logística × KNN

{divergence.to_markdown(index=False) if not divergence.empty else 'Sem benchmark.'}

## Bootstrap de modelo

{bootstrap_summary.to_markdown(index=False) if not bootstrap_summary.empty else 'Bootstrap não executado ou sem réplicas válidas.'}

## Claim ceiling

> Estimativa agregada model-based da compatibilidade histórica com entrega por plataforma, calibrada em 2022T4/2024T3. Plataforma direta permanece não observada fora dos módulos especiais.
"""


def output_paths(root: Path, run_id: str) -> dict[str, Path]:
    table = root / "05_outputs" / "tables" / "pnadc_historical_backcast_hardening"
    model = root / "05_outputs" / "models" / "pnadc_historical_backcast_hardening"
    report = root / "06_reports" / "pnadc_historical_backcast_hardening"
    registry = root / "00_admin" / "registry"
    for directory in (table, model, report, registry):
        directory.mkdir(parents=True, exist_ok=True)
    return {
        "table": table,
        "model": model,
        "report": report,
        "registry": registry,
        "layout": table / f"pnadc_layout_equivalence_matrix_{run_id}.csv",
        "metrics": table / f"pnadc_proxy_temporal_calibrated_metrics_{run_id}.csv",
        "reliability": table / f"pnadc_proxy_reliability_deciles_{run_id}.csv",
        "toprisk": table / f"pnadc_proxy_top_risk_{run_id}.csv",
        "goldens": table / f"pnadc_proxy_direct_aggregate_goldens_{run_id}.csv",
        "mca_categories": table / f"pnadc_mca_category_coordinates_{run_id}.csv",
        "mca_cells": table / f"pnadc_mca_historical_cell_coordinates_{run_id}.csv",
        "support": table / f"pnadc_historical_transport_support_{run_id}.csv",
        "support_summary": table / f"pnadc_historical_backcast_support_summary_{run_id}.csv",
        "cluster_cells": table / f"pnadc_historical_cell_typologies_{run_id}.csv",
        "cluster_eval": table / f"pnadc_cluster_selection_{run_id}.csv",
        "cluster_stability": table / f"pnadc_cluster_stability_{run_id}.csv",
        "cluster_profiles": table / f"pnadc_cluster_profiles_{run_id}.csv",
        "knn_metrics": table / f"pnadc_weighted_knn_benchmark_{run_id}.csv",
        "divergence": table / f"pnadc_logit_knn_divergence_{run_id}.csv",
        "mapping_2022": table / f"pnadc_backcast_mapping_2022_{run_id}.csv",
        "mapping_2024": table / f"pnadc_backcast_mapping_2024_{run_id}.csv",
        "mapping_pooled": table / f"pnadc_backcast_mapping_pooled_{run_id}.csv",
        "survey_uncertainty": table / f"pnadc_proxy_survey_uncertainty_{run_id}.csv",
        "bootstrap_raw": table / f"pnadc_proxy_model_bootstrap_replicates_{run_id}.csv",
        "bootstrap_summary": table / f"pnadc_proxy_model_uncertainty_{run_id}.csv",
        "transport": table / f"pnadc_proxy_transport_sensitivity_{run_id}.csv",
        "final_estimates": table / f"pnadc_historical_backcast_final_estimates_{run_id}.csv",
        "bundle": model / f"pnadc_historical_backcast_hardening_bundle_{run_id}.joblib",
        "model_card": report / f"pnadc_historical_backcast_model_card_{run_id}.md",
        "report_file": report / f"pnadc_historical_backcast_hardening_report_{run_id}.md",
        "registry_file": registry / f"pnadc_historical_backcast_hardening_registry_{run_id}.json",
    }


def audit_mode(root: Path, strict: bool, logger: logging.Logger) -> int:
    run_id = make_run_id()
    tests: list[TestResult] = []
    upstream_info = validate_upstream(root, tests)
    lock = upstream_info.get("lock", {})
    direct_script = resolve_script(root, UPSTREAM_DIRECT_SCRIPT, Path(__file__).with_name(UPSTREAM_DIRECT_SCRIPT))
    upstream = import_module(direct_script, "spine_direct_certifier_hardening")
    tests.append(TestResult(
        "upstream.direct.parser", "PASS" if getattr(upstream, "VERSION", None) == UPSTREAM_DIRECT_VERSION else "FAIL", "critical",
        "Parser direto certificado carregado.", observed={"version": getattr(upstream, "VERSION", None), "sha256": sha256_file(direct_script)},
        expected=UPSTREAM_DIRECT_VERSION,
    ))
    layout_matrix = layout_equivalence(root, upstream, lock, tests) if lock else pd.DataFrame()
    status = "AUDIT_BLOCKED" if tests_have_critical_failures(tests) else "AUDIT_PASSED"
    paths = output_paths(root, run_id)
    layout_matrix.to_csv(paths["layout"], index=False)
    payload = {
        "run_id": run_id,
        "script_version": VERSION,
        "schema_version": SCHEMA_VERSION,
        "mode": "audit",
        "status": status,
        "critical_failures": [asdict(t) for t in tests if t.severity == "critical" and t.status in {"FAIL", "BLOCKED"}],
        "warnings": [asdict(t) for t in tests if t.status == "WARN"],
        "layout_matrix": str(paths["layout"]),
        "upstream_lock": upstream_info.get("lock_path"),
        "created_at_utc": utc_now(),
    }
    lock_path = root / "00_admin" / "PNADC_HISTORICAL_BACKCAST_HARDENING_AUDIT_LOCK.json"
    atomic_write_json(lock_path, payload)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    logger.info("Auditoria do hardening concluída | status=%s", status)
    return 0 if status == "AUDIT_PASSED" else 2


def full_mode(args: argparse.Namespace, logger: logging.Logger) -> int:
    root = Path(args.root).expanduser().resolve()
    run_id = make_run_id()
    tests: list[TestResult] = []
    paths = output_paths(root, run_id)

    upstream_info = validate_upstream(root, tests)
    lock = upstream_info.get("lock", {})
    if not lock:
        status = "HARDENING_BLOCKED"
        payload = {"run_id": run_id, "status": status, "critical_failures": [asdict(t) for t in tests]}
        atomic_write_json(root / "00_admin" / "PNADC_HISTORICAL_BACKCAST_HARDENING_LOCK.json", payload)
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 2

    direct_script = resolve_script(root, UPSTREAM_DIRECT_SCRIPT, Path(__file__).with_name(UPSTREAM_DIRECT_SCRIPT))
    upstream = import_module(direct_script, "spine_direct_certifier_hardening")
    parser_ok = getattr(upstream, "VERSION", None) == UPSTREAM_DIRECT_VERSION
    tests.append(TestResult(
        "upstream.direct.parser", "PASS" if parser_ok else "FAIL", "critical",
        "Parser direto certificado carregado." if parser_ok else "Versão do parser direto divergente.",
        observed={"version": getattr(upstream, "VERSION", None), "sha256": sha256_file(direct_script)}, expected=UPSTREAM_DIRECT_VERSION,
    ))

    layout_matrix = layout_equivalence(root, upstream, lock, tests)
    layout_matrix.to_csv(paths["layout"], index=False)

    direct_cells_by_year: dict[int, pd.DataFrame] = {}
    for year in (2022, 2024):
        info = (lock.get("direct_inputs") or {}).get(str(year), {})
        if not info:
            tests.append(TestResult(f"direct.{year}.input", "FAIL", "critical", "Input direto ausente no lock."))
            continue
        direct_cells_by_year[year] = load_direct_psu_cells(Path(info["path"]), year, tests)
    if len(direct_cells_by_year) != 2 or any(frame.empty for frame in direct_cells_by_year.values()):
        tests.append(TestResult("direct.complete", "FAIL", "critical", "Dados diretos 2022/2024 incompletos."))

    historical_psu = load_historical_psu_cells(lock, tests, logger)
    if historical_psu.empty:
        tests.append(TestResult("historical.complete", "FAIL", "critical", "Nenhum histórico certificado carregado."))

    if tests_have_critical_failures(tests):
        status = "HARDENING_BLOCKED"
        report_text = build_report(run_id, status, tests, layout_matrix, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
        atomic_write_text(paths["report_file"], report_text)
        payload = {
            "run_id": run_id, "script_version": VERSION, "schema_version": SCHEMA_VERSION,
            "status": status,
            "critical_failures": [asdict(t) for t in tests if t.severity == "critical" and t.status in {"FAIL", "BLOCKED"}],
            "warnings": [asdict(t) for t in tests if t.status == "WARN"],
            "report": str(paths["report_file"]), "created_at_utc": utc_now(),
        }
        atomic_write_json(root / "00_admin" / "PNADC_HISTORICAL_BACKCAST_HARDENING_LOCK.json", payload)
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 2

    cells_2022 = direct_cells_by_year[2022]
    cells_2024 = direct_cells_by_year[2024]
    direct_pooled = pd.concat([cells_2022, cells_2024], ignore_index=True)

    logger.info("Ajustando mapping 2022")
    bundle_2022 = fit_mapping_bundle(cells_2022, "mapping_2022", n_splits=args.calibration_folds)
    logger.info("Ajustando mapping 2024")
    bundle_2024 = fit_mapping_bundle(cells_2024, "mapping_2024", n_splits=args.calibration_folds)
    logger.info("Ajustando mapping pooled")
    bundle_pooled = fit_mapping_bundle(direct_pooled, "mapping_pooled", n_splits=args.calibration_folds)
    bundles = {
        "mapping_2022": bundle_2022,
        "mapping_2024": bundle_2024,
        "mapping_pooled": bundle_pooled,
    }

    metric_rows: list[dict[str, Any]] = []
    reliability_frames: list[pd.DataFrame] = []
    top_frames: list[pd.DataFrame] = []
    temporal_specs = [
        (bundle_2022, 2022, 2024, cells_2024),
        (bundle_2024, 2024, 2022, cells_2022),
    ]
    for bundle, train_year, test_year, test_cells in temporal_specs:
        prediction = bundle_predict(bundle, test_cells)
        metric = evaluate_predictions(bundle["name"], train_year, test_year, test_cells, prediction)
        metric_rows.append(asdict(metric))
        reliability_frames.append(reliability_table(bundle["name"], train_year, test_year, test_cells, prediction))
        top_frames.append(top_risk_table(bundle["name"], train_year, test_year, test_cells, prediction))
    metrics_df = pd.DataFrame(metric_rows)
    reliability_df = pd.concat(reliability_frames, ignore_index=True)
    top_df = pd.concat(top_frames, ignore_index=True)
    metrics_df.to_csv(paths["metrics"], index=False)
    reliability_df.to_csv(paths["reliability"], index=False)
    top_df.to_csv(paths["toprisk"], index=False)

    golden_frames: list[pd.DataFrame] = []
    for year, cells in ((2022, cells_2022), (2024, cells_2024)):
        predictions = {name: bundle_predict(bundle, cells) for name, bundle in bundles.items()}
        golden_frames.append(golden_totals(year, cells, predictions))
    goldens_df = pd.concat(golden_frames, ignore_index=True)
    goldens_df.to_csv(paths["goldens"], index=False)
    test_metric_gates(metrics_df, goldens_df, tests, args.min_auc, args.golden_max_relative_error)

    direct_features_2022 = feature_cells_from_psu(cells_2022, direct=True)
    direct_features_2024 = feature_cells_from_psu(cells_2024, direct=True)
    direct_features_pooled = feature_cells_from_psu(direct_pooled, direct=True)
    historical_features = feature_cells_from_psu(historical_psu, direct=False)

    logger.info("Ajustando MCA ponderada em %d células diretas", len(direct_features_pooled))
    mca = fit_weighted_mca(direct_features_pooled, n_components=args.mca_components)
    category_coords = mca_category_coordinates(mca)
    category_coords.to_csv(paths["mca_categories"], index=False)

    support_table, support_thresholds = support_mapping(
        direct_features_2022, direct_features_2024, direct_features_pooled,
        historical_features, mca,
    )
    support_table.to_csv(paths["support"], index=False)
    support_table.to_csv(paths["mca_cells"], index=False)

    logger.info("Selecionando tipologias no espaço MCA")
    cluster_model, cluster_eval, cluster_stability = choose_and_fit_clusters(
        direct_features_pooled, np.asarray(mca["coordinates"]), args.cluster_k_min, args.cluster_k_max,
    )
    cluster_eval.to_csv(paths["cluster_eval"], index=False)
    cluster_stability.to_csv(paths["cluster_stability"], index=False)
    cluster_profiles_df = cluster_profiles(direct_features_pooled, cluster_model.labels_)
    cluster_profiles_df.to_csv(paths["cluster_profiles"], index=False)

    logger.info("Selecionando KNN ponderado temporal")
    best_k, knn_metrics = temporal_knn_benchmark(direct_cells_by_year, mca)
    knn_metrics.to_csv(paths["knn_metrics"], index=False)
    knn_bundle = fit_knn_pooled(direct_pooled, mca, best_k)

    feature_map, estimates, support_summary, divergence = historical_predictions_and_estimates(
        historical_psu, bundles, support_table, cluster_model, mca, knn_bundle,
    )
    support_summary.to_csv(paths["support_summary"], index=False)
    divergence.to_csv(paths["divergence"], index=False)
    feature_map.to_csv(paths["cluster_cells"], index=False)

    for mapping in ("2022", "2024", "pooled"):
        columns = FEATURES + [f"probability_mapping_{mapping}", "support_class", "cluster"]
        feature_map[columns].to_csv(paths[f"mapping_{mapping}"], index=False)

    estimates[[
        "source_period", "total_2022", "total_2024", "total_pooled",
        "transport_total_min", "transport_total_max", "transport_range_width",
    ]].to_csv(paths["transport"], index=False)

    # Diagnósticos KNN e suporte.
    for row in divergence.itertuples(index=False):
        corr_ok = pd.notna(row.spearman_rank_correlation) and row.spearman_rank_correlation >= args.knn_min_rank_correlation
        diff_ok = pd.notna(row.relative_total_difference) and abs(row.relative_total_difference) <= args.knn_max_relative_difference
        tests.append(TestResult(
            f"knn.{row.source_period}.rank", "PASS" if corr_ok else "WARN", "high",
            "Ranking logit/KNN compatível." if corr_ok else "Baixa correlação de ranking entre logística e KNN.",
            observed=row.spearman_rank_correlation, expected=f">={args.knn_min_rank_correlation}",
        ))
        tests.append(TestResult(
            f"knn.{row.source_period}.total", "PASS" if diff_ok else "WARN", "high",
            "Total KNN dentro da sensibilidade prevista." if diff_ok else "Divergência agregada elevada entre logística e KNN.",
            observed=row.relative_total_difference, expected=f"abs(diff)<={args.knn_max_relative_difference}",
        ))

    out_support = support_summary[support_summary["support_class"] == "OUT_OF_SUPPORT"]
    for row in out_support.itertuples(index=False):
        ok = pd.notna(row.expected_total_share) and row.expected_total_share <= args.max_out_of_support_expected_share
        tests.append(TestResult(
            f"support.{row.source_period}.out_expected_share", "PASS" if ok else "WARN", "high",
            "Dependência de extrapolação dentro do limite." if ok else "Parcela relevante da estimativa depende de perfis fora do suporte.",
            observed=row.expected_total_share, expected=f"<={args.max_out_of_support_expected_share}",
        ))

    stability_mean = float(cluster_stability["adjusted_rand_index"].mean()) if not cluster_stability.empty else np.nan
    tests.append(TestResult(
        "cluster.stability", "PASS" if pd.notna(stability_mean) and stability_mean >= 0.70 else "WARN", "secondary",
        "Tipologias estáveis entre sementes." if pd.notna(stability_mean) and stability_mean >= 0.70 else "Tipologias sensíveis à inicialização.",
        observed=stability_mean, expected=">=0.70",
    ))

    historical_period_feature_weights = historical_psu.groupby(["source_period", *FEATURES], observed=True).agg(
        weight_total=("weight_total", "sum")
    ).reset_index()
    bootstrap_raw, bootstrap_summary = bootstrap_model_uncertainty(
        direct_pooled, historical_features, historical_period_feature_weights,
        reps=args.bootstrap_reps, n_splits=args.bootstrap_calibration_folds, logger=logger,
    )
    bootstrap_raw.to_csv(paths["bootstrap_raw"], index=False)
    bootstrap_summary.to_csv(paths["bootstrap_summary"], index=False)
    if args.bootstrap_reps > 0:
        successful = int(bootstrap_raw["replicate"].nunique()) if not bootstrap_raw.empty else 0
        minimum = max(1, math.ceil(args.bootstrap_reps * 0.80))
        tests.append(TestResult(
            "bootstrap.model.successful_reps", "PASS" if successful >= minimum else "WARN", "high",
            "Réplicas de bootstrap suficientes." if successful >= minimum else "Muitas réplicas de bootstrap falharam.",
            observed=successful, expected=f">={minimum}",
        ))

    final_estimates = combine_uncertainty(estimates, bootstrap_summary)
    survey_columns = [column for column in final_estimates.columns if column == "source_period" or "survey" in column or column in {"total_pooled", "share_pooled"}]
    final_estimates[survey_columns].to_csv(paths["survey_uncertainty"], index=False)
    final_estimates.to_csv(paths["final_estimates"], index=False)

    # Bundle executável e auditável.
    hardening_bundle = {
        "model_schema_version": MODEL_SCHEMA_VERSION,
        "created_at_utc": utc_now(),
        "features": FEATURES,
        "bundles": bundles,
        "mca": {
            "encoder": mca["encoder"],
            "column_mass": mca["column_mass"],
            "components": mca["components"],
            "singular_values": mca["singular_values"],
            "explained_inertia_ratio": mca["explained_inertia_ratio"],
            "feature_names": mca["feature_names"],
            "q": mca["q"],
        },
        "support_thresholds": support_thresholds,
        "cluster_model": cluster_model,
        "knn": {
            "k": knn_bundle["k"],
            "coordinates": knn_bundle["coordinates"],
            "rates": knn_bundle["rates"],
            "weights": knn_bundle["weights"],
            "prior": knn_bundle["prior"],
        },
        "claim_ceiling": "Estimativa model-based agregada de compatibilidade histórica com entrega por plataforma, não identificação direta.",
    }
    joblib.dump(hardening_bundle, paths["bundle"], compress=3)

    status = "HARDENING_BLOCKED" if tests_have_critical_failures(tests) else "FINAL_CERTIFIED"
    model_card = build_model_card(
        run_id, metrics_df, goldens_df, support_summary, divergence, mca,
        cluster_profiles_df, args.bootstrap_reps,
    )
    atomic_write_text(paths["model_card"], model_card)
    report = build_report(
        run_id, status, tests, layout_matrix, metrics_df, goldens_df,
        final_estimates, support_summary, divergence, bootstrap_summary,
    )
    atomic_write_text(paths["report_file"], report)

    artifacts = {name: str(path) for name, path in paths.items() if path.exists()}
    hashes = {name: sha256_file(path) for name, path in paths.items() if path.exists() and path.is_file()}
    registry_payload = {
        "run_id": run_id,
        "script_version": VERSION,
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "upstream_historical_lock": upstream_info["lock_path"],
        "upstream_historical_run_id": lock.get("run_id"),
        "direct_input_hashes": {year: sha256_file(Path((lock["direct_inputs"])[str(year)]["path"])) for year in (2022, 2024)},
        "historical_periods": sorted(historical_psu["source_period"].unique().tolist()),
        "mapping_models": ["mapping_2022", "mapping_2024", "mapping_pooled"],
        "primary_estimand": "sum(survey_weight * probability_mapping_pooled)",
        "support_thresholds": support_thresholds,
        "selected_cluster_k": int(cluster_model.n_clusters),
        "selected_knn_k": int(best_k),
        "bootstrap_reps_requested": int(args.bootstrap_reps),
        "artifacts": artifacts,
        "artifact_hashes": hashes,
        "created_at_utc": utc_now(),
    }
    atomic_write_json(paths["registry_file"], registry_payload)
    artifacts["registry_file"] = str(paths["registry_file"])
    hashes["registry_file"] = sha256_file(paths["registry_file"])

    lock_payload = {
        "run_id": run_id,
        "script_version": VERSION,
        "schema_version": SCHEMA_VERSION,
        "validation_schema_version": VALIDATION_SCHEMA_VERSION,
        "model_schema_version": MODEL_SCHEMA_VERSION,
        "mode": "full",
        "status": status,
        "critical_failures": [asdict(t) for t in tests if t.severity == "critical" and t.status in {"FAIL", "BLOCKED"}],
        "warnings": [asdict(t) for t in tests if t.status == "WARN"],
        "upstream_historical_lock": upstream_info["lock_path"],
        "upstream_historical_run_id": lock.get("run_id"),
        "historical_periods": sorted(historical_psu["source_period"].unique().tolist()),
        "primary_model": "mapping_pooled",
        "sensitivity_models": ["mapping_2022", "mapping_2024", f"weighted_knn_k{best_k}"],
        "primary_features": FEATURES,
        "artifacts": artifacts,
        "artifact_hashes": hashes,
        "epistemic_limit": "Probabilidade histórica modelada; plataforma direta permanece não observada fora dos módulos especiais.",
        "claim_ceiling": "Reconstrução retrospectiva probabilística agregada da compatibilidade histórica com entrega por plataforma, calibrada em 2022T4/2024T3, com incerteza e suporte explicitados.",
        "created_at_utc": utc_now(),
    }
    hardening_lock = root / "00_admin" / "PNADC_HISTORICAL_BACKCAST_HARDENING_LOCK.json"
    atomic_write_json(hardening_lock, lock_payload)

    if status == "FINAL_CERTIFIED":
        copy_atomic(paths["report_file"], paths["report"] / "pnadc_historical_backcast_hardening_report.md")
        copy_atomic(paths["final_estimates"], paths["table"] / "pnadc_historical_backcast_final_estimates.csv")
        copy_atomic(paths["bundle"], paths["model"] / "pnadc_historical_backcast_hardening_bundle.joblib")
        final_lock = {
            "freeze_id": run_id,
            "status": "FINAL_CERTIFIED",
            "component": "PNADC_HISTORICAL_BACKCAST",
            "hardening_lock": str(hardening_lock),
            "upstream_core_freeze": str(root / "00_admin" / "PNADC_HISTORICAL_PROXY_CORE_FREEZE.json"),
            "final_estimates": str(paths["final_estimates"]),
            "final_estimates_sha256": sha256_file(paths["final_estimates"]),
            "model_bundle": str(paths["bundle"]),
            "model_bundle_sha256": sha256_file(paths["bundle"]),
            "read_only": True,
            "evidence_tier": "C",
            "direct_platform_observed_historical": False,
            "claim_ceiling": lock_payload["claim_ceiling"],
            "created_at_utc": utc_now(),
        }
        atomic_write_json(root / "00_admin" / "PNADC_HISTORICAL_BACKCAST_FINAL_LOCK.json", final_lock)

    print(json.dumps(lock_payload, ensure_ascii=False, indent=2))
    print(f"STATUS: {status}")
    print(f"REPORT: {paths['report_file']}")
    print(f"FINAL ESTIMATES: {paths['final_estimates']}")
    print(f"MODEL BUNDLE: {paths['bundle']}")
    logger.info("Hardening concluído | status=%s", status)
    return 0 if status == "FINAL_CERTIFIED" else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="PNADc Historical Backcast Hardening & Uncertainty Engine")
    parser.add_argument("--root", required=True, help="Raiz do SPINE-GPEv7")
    parser.add_argument("--mode", choices=["audit", "full"], default="full")
    parser.add_argument("--calibration-folds", type=int, default=5)
    parser.add_argument("--bootstrap-calibration-folds", type=int, default=3)
    parser.add_argument("--bootstrap-reps", type=int, default=30)
    parser.add_argument("--mca-components", type=int, default=8)
    parser.add_argument("--cluster-k-min", type=int, default=4)
    parser.add_argument("--cluster-k-max", type=int, default=8)
    parser.add_argument("--min-auc", type=float, default=0.80)
    parser.add_argument("--golden-max-relative-error", type=float, default=0.15)
    parser.add_argument("--knn-min-rank-correlation", type=float, default=0.70)
    parser.add_argument("--knn-max-relative-difference", type=float, default=0.25)
    parser.add_argument("--max-out-of-support-expected-share", type=float, default=0.10)
    parser.add_argument("--strict", action="store_true", help="Mantido para compatibilidade; gates críticos sempre falham fechados.")
    parser.add_argument("--verbose", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logger = setup_logger(args.verbose)
    root = Path(args.root).expanduser().resolve()
    logger.info("SPINE-GPE PNADc Backcast Hardening v%s | root=%s | mode=%s", VERSION, root, args.mode)
    if args.mode == "audit":
        return audit_mode(root, args.strict, logger)
    return full_mode(args, logger)


if __name__ == "__main__":
    raise SystemExit(main())
