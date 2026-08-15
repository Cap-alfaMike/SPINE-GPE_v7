#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SPINE-GPE v7 — Phase 0 Closure Engine v1.0.1
================================================

Executa, em sequência fail-closed:
1. RAIS Substantive Adjudication Lock;
2. RAIS Editorial Profile Decoding;
3. Phase 0 Master Harmonization & Evidence Lock.

O engine NÃO relê os 78,5 milhões de registros brutos da RAIS. Ele trabalha
sobre os locks e artefatos certificados já produzidos, em especial o Parquet
congelado dos vínculos ativos do CBO primário.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import hashlib
import json
import logging
import math
import os
import re
import shutil
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Iterable, Optional

import numpy as np
import pandas as pd

SCRIPT_VERSION = "1.0.1"
SCHEMA_VERSION = "spine-gpe-v7-phase0-closure-1.0.1"
DEFAULT_ROOT = Path("/content/drive/MyDrive/aCidadeAlgoritmica/SPINE-GPEv7")
MONTHLY_HOURS_FACTOR = 4.345

CORE_EDITORIAL_DIMENSIONS = ["sex", "race", "education", "link_type"]
ALLOWED_LAYOUT_STATUSES = {
    "LAYOUT_EQUIVALENCE_CONFIRMED",
    "DOCUMENTATION_LIMITED_OPERATIONALLY_CONSISTENT",
}


@dataclass
class Gate:
    gate_id: str
    status: str
    severity: str
    message: str
    observed: Any = None
    expected: Any = None


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def setup_logger(verbose: bool = False) -> logging.Logger:
    logger = logging.getLogger("phase0_closure")
    logger.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    return logger


def sha256_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    os.replace(tmp, path)


def atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_code(value: Any) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)) or pd.isna(value):
        return "__NA__"
    text = str(value).strip()
    if re.fullmatch(r"-?\d+\.0+", text):
        text = text.split(".", 1)[0]
    return text


def normalize_editorial_code(value: Any, dimension: str) -> str:
    """Canonicaliza códigos editoriais sem alterar códigos estruturais.

    A RAIS 2022 pode materializar sexo, raça/cor e escolaridade como strings
    zero-padded (por exemplo, ``01``), enquanto os layouts oficiais e o
    codebook usam os equivalentes inteiros (``1``). Para essas dimensões,
    removemos somente zeros à esquerda de códigos inteiros. Tipo de vínculo,
    CBO, CNAE e demais códigos preservam a representação original.
    """
    code = normalize_code(value)
    if code == "__NA__":
        return code
    if dimension in {"sex", "race", "education"} and re.fullmatch(r"[+-]?\d+", code):
        return str(int(code))
    return code


def safe_float(value: Any) -> Optional[float]:
    try:
        if pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def ensure_dirs(root: Path) -> dict[str, Path]:
    dirs = {
        "admin": root / "00_admin",
        "tables": root / "05_outputs" / "tables" / "phase0_closure",
        "reports": root / "06_reports" / "phase0_closure",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


def artifact_record(name: str, path: Path) -> dict[str, Any]:
    return {
        "name": name,
        "path": str(path),
        "exists": path.is_file(),
        "size_bytes": path.stat().st_size if path.is_file() else None,
        "sha256": sha256_file(path) if path.is_file() else None,
    }


def verify_locked_artifact(
    lock: dict[str, Any],
    artifact_name: str,
    gates: list[Gate],
    required: bool = True,
) -> Optional[Path]:
    artifacts = lock.get("artifacts", {})
    hashes = lock.get("artifact_hashes", {})
    raw_path = artifacts.get(artifact_name)
    if not raw_path:
        gates.append(Gate(
            f"artifact.{artifact_name}.declared",
            "FAIL" if required else "WARN",
            "critical" if required else "medium",
            f"Artefato {artifact_name} não declarado no lock.",
        ))
        return None
    path = Path(raw_path)
    if not path.is_file():
        gates.append(Gate(
            f"artifact.{artifact_name}.exists",
            "FAIL" if required else "WARN",
            "critical" if required else "medium",
            f"Artefato {artifact_name} não encontrado.",
            str(path),
            "existing file",
        ))
        return None
    expected = hashes.get(artifact_name)
    if expected:
        actual = sha256_file(path)
        if actual != expected:
            gates.append(Gate(
                f"artifact.{artifact_name}.hash",
                "FAIL",
                "critical",
                f"Hash divergente para {artifact_name}.",
                actual,
                expected,
            ))
            return None
        gates.append(Gate(
            f"artifact.{artifact_name}.hash",
            "PASS",
            "critical",
            f"Hash verificado para {artifact_name}.",
            actual,
            expected,
        ))
    else:
        gates.append(Gate(
            f"artifact.{artifact_name}.hash",
            "WARN",
            "medium",
            f"Lock não contém hash para {artifact_name}; existência verificada.",
            str(path),
            "sha256 in lock",
        ))
    return path


def critical_failures(gates: Iterable[Gate]) -> list[dict[str, Any]]:
    return [asdict(g) for g in gates if g.status == "FAIL" and g.severity == "critical"]


def gate_table(gates: list[Gate]) -> pd.DataFrame:
    return pd.DataFrame([asdict(g) for g in gates])


def load_rais_context(root: Path, gates: list[Gate]) -> tuple[dict[str, Any], dict[str, Any], pd.DataFrame, Path]:
    admin = root / "00_admin"
    lock_path = admin / "RAIS_FORMAL_CERTIFICATION_LOCK.json"
    freeze_path = admin / "RAIS_FORMAL_CORE_FREEZE.json"

    if not lock_path.is_file():
        gates.append(Gate("rais.lock.exists", "FAIL", "critical", "RAIS_FORMAL_CERTIFICATION_LOCK.json ausente.", str(lock_path)))
        return {}, {}, pd.DataFrame(), lock_path
    if not freeze_path.is_file():
        gates.append(Gate("rais.freeze.exists", "FAIL", "critical", "RAIS_FORMAL_CORE_FREEZE.json ausente.", str(freeze_path)))
        return {}, {}, pd.DataFrame(), lock_path

    lock = read_json(lock_path)
    freeze = read_json(freeze_path)

    if lock.get("status") != "CORE_CERTIFIED":
        gates.append(Gate("rais.lock.status", "FAIL", "critical", "RAIS não está CORE_CERTIFIED.", lock.get("status"), "CORE_CERTIFIED"))
    else:
        gates.append(Gate("rais.lock.status", "PASS", "critical", "RAIS CORE_CERTIFIED.", lock.get("status")))
    if lock.get("critical_failures"):
        gates.append(Gate("rais.lock.failures", "FAIL", "critical", "RAIS contém critical_failures.", lock.get("critical_failures"), []))
    else:
        gates.append(Gate("rais.lock.failures", "PASS", "critical", "RAIS sem critical_failures.", []))
    if freeze.get("status") != "FROZEN" or freeze.get("read_only") is not True:
        gates.append(Gate("rais.freeze.status", "FAIL", "critical", "Freeze RAIS inválido.", {"status": freeze.get("status"), "read_only": freeze.get("read_only")}, {"status": "FROZEN", "read_only": True}))
    else:
        gates.append(Gate("rais.freeze.status", "PASS", "critical", "Freeze RAIS verificado.", "FROZEN"))

    active_path = verify_locked_artifact(lock, "active_primary_parquet", gates, required=True)
    if active_path is None:
        return lock, freeze, pd.DataFrame(), lock_path

    active = pd.read_parquet(active_path)
    expected_n = int(lock.get("n_active_primary_links", -1))
    if len(active) != expected_n:
        gates.append(Gate("rais.active.row_count", "FAIL", "critical", "Contagem do Parquet ativo difere do lock.", len(active), expected_n))
    else:
        gates.append(Gate("rais.active.row_count", "PASS", "critical", "Contagem do Parquet ativo coincide com o lock.", len(active), expected_n))

    required_cols = {
        "year", "cbo2002", "active_3112", "municipality6", "uf", "region", "is_recife",
        "income_monthly_nominal", "income_hour_nominal", "contract_hours", "sex", "race",
        "education", "age", "link_type", "cnae_class", "source_sha256",
    }
    missing = sorted(required_cols - set(active.columns))
    if missing:
        gates.append(Gate("rais.active.schema", "FAIL", "critical", "Colunas obrigatórias ausentes no Parquet RAIS.", missing, sorted(required_cols)))
    else:
        gates.append(Gate("rais.active.schema", "PASS", "critical", "Schema mínimo do Parquet RAIS verificado.", sorted(required_cols)))

    return lock, freeze, active, lock_path


def prepare_active(active: pd.DataFrame) -> pd.DataFrame:
    frame = active.copy()
    for col in ["income_monthly_nominal", "income_hour_nominal", "contract_hours", "age"]:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame["valid_hours"] = frame["contract_hours"].between(1, 100)
    frame["positive_income"] = frame["income_monthly_nominal"].gt(0)
    frame["valid_hourly_income"] = frame["positive_income"] & frame["valid_hours"] & frame["income_hour_nominal"].notna()
    frame["income_status"] = np.select(
        [
            frame["income_monthly_nominal"].isna(),
            frame["income_monthly_nominal"].lt(0),
            frame["income_monthly_nominal"].eq(0),
            frame["income_monthly_nominal"].gt(0),
        ],
        ["MISSING", "NEGATIVE", "ZERO", "POSITIVE"],
        default="OTHER",
    )
    return frame


def geography_masks(active: pd.DataFrame) -> dict[str, pd.Series]:
    return {
        "Brasil": pd.Series(True, index=active.index),
        "Nordeste": active["region"].astype(str).eq("Nordeste"),
        "Pernambuco": active["uf"].astype(str).eq("PE"),
        "Recife": active["is_recife"].fillna(False).astype(bool),
    }


def q(series: pd.Series, value: float = 0.5) -> float:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    return float(clean.quantile(value)) if not clean.empty else float("nan")


def make_income_adjudication(active: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    status = (
        active["income_status"]
        .value_counts(dropna=False)
        .rename_axis("income_status")
        .reset_index(name="n_links")
    )
    status["share"] = status["n_links"] / len(active) if len(active) else np.nan

    rows: list[dict[str, Any]] = []
    for label, mask in geography_masks(active).items():
        sub = active.loc[mask].copy()
        monthly = sub[sub["positive_income"]]
        hourly = sub[sub["valid_hourly_income"]]
        rows.append({
            "year": int(pd.to_numeric(sub["year"], errors="coerce").dropna().mode().iloc[0]) if not sub.empty else None,
            "geography": label,
            "n_active_all": int(len(sub)),
            "n_income_positive": int(len(monthly)),
            "n_income_nonpositive": int((~sub["positive_income"]).sum()),
            "nonpositive_income_share": float((~sub["positive_income"]).mean()) if len(sub) else np.nan,
            "n_hourly_analytical": int(len(hourly)),
            "income_monthly_mean_all_active": float(sub["income_monthly_nominal"].mean()),
            "income_monthly_median_all_active": q(sub["income_monthly_nominal"]),
            "income_hour_mean_all_active": float(sub["income_hour_nominal"].mean()),
            "income_hour_median_all_active": q(sub["income_hour_nominal"]),
            "income_monthly_mean_positive": float(monthly["income_monthly_nominal"].mean()),
            "income_monthly_median_positive": q(monthly["income_monthly_nominal"]),
            "income_hour_mean_positive_valid_hours": float(hourly["income_hour_nominal"].mean()),
            "income_hour_median_positive_valid_hours": q(hourly["income_hour_nominal"]),
            "contract_hours_mean_analytical": float(hourly["contract_hours"].mean()),
            "contract_hours_median_analytical": q(hourly["contract_hours"]),
            "n_source_files": int(sub["source_sha256"].nunique()),
        })
    return status, pd.DataFrame(rows)


def run_rais_adjudication(root: Path, dirs: dict[str, Path], run_id: str, codebook_path: Path, logger: logging.Logger) -> tuple[dict[str, Any], pd.DataFrame]:
    gates: list[Gate] = []
    rais_lock, rais_freeze, active_raw, rais_lock_path = load_rais_context(root, gates)
    if not active_raw.empty:
        active = prepare_active(active_raw)
    else:
        active = active_raw

    if not active.empty:
        years = sorted(pd.to_numeric(active["year"], errors="coerce").dropna().astype(int).unique().tolist())
        expected_years = sorted(int(x) for x in rais_lock.get("years_certified", []))
        if years != expected_years:
            gates.append(Gate("rais.adjudication.years", "FAIL", "critical", "Anos do Parquet divergem do lock.", years, expected_years))
        else:
            gates.append(Gate("rais.adjudication.years", "PASS", "critical", "Anos do universo RAIS verificados.", years, expected_years))
        if not active["active_3112"].fillna(False).astype(bool).all():
            gates.append(Gate("rais.adjudication.active_universe", "FAIL", "critical", "Parquet ativo contém vínculos não ativos em 31/12."))
        else:
            gates.append(Gate("rais.adjudication.active_universe", "PASS", "critical", "Todos os vínculos estão ativos em 31/12."))

        primary_codes = {normalize_code(x) for x in rais_lock.get("primary_cbo_codes", [])}
        observed_codes = {normalize_code(x) for x in active["cbo2002"].dropna().unique()}
        if not observed_codes.issubset(primary_codes):
            gates.append(Gate("rais.adjudication.cbo_scope", "FAIL", "critical", "Parquet primário contém CBO fora do escopo.", sorted(observed_codes), sorted(primary_codes)))
        else:
            gates.append(Gate("rais.adjudication.cbo_scope", "PASS", "critical", "Escopo CBO primário verificado.", sorted(observed_codes)))

    income_status, adjudication = make_income_adjudication(active) if not active.empty else (pd.DataFrame(), pd.DataFrame())
    if not adjudication.empty:
        brazil = adjudication[adjudication["geography"] == "Brasil"].iloc[0]
        if int(brazil["n_active_all"]) != int(rais_lock.get("n_active_primary_links", -1)):
            gates.append(Gate("rais.adjudication.brazil_count", "FAIL", "critical", "Total Brasil diverge do lock RAIS.", int(brazil["n_active_all"]), rais_lock.get("n_active_primary_links")))
        else:
            gates.append(Gate("rais.adjudication.brazil_count", "PASS", "critical", "Total Brasil coincide com o lock RAIS.", int(brazil["n_active_all"])))
        if int(brazil["n_hourly_analytical"]) <= 0:
            gates.append(Gate("rais.adjudication.hourly_nonempty", "FAIL", "critical", "Universo analítico de renda-hora está vazio."))
        else:
            gates.append(Gate("rais.adjudication.hourly_nonempty", "PASS", "critical", "Universo analítico de renda-hora não vazio.", int(brazil["n_hourly_analytical"])))
        if int(income_status["n_links"].sum()) != len(active):
            gates.append(Gate("rais.adjudication.income_partition", "FAIL", "critical", "Partição do status da renda não fecha.", int(income_status["n_links"].sum()), len(active)))
        else:
            gates.append(Gate("rais.adjudication.income_partition", "PASS", "critical", "Partição do status da renda fecha exatamente.", len(active)))
        for geography in ["Brasil", "Nordeste", "Pernambuco", "Recife"]:
            row = adjudication[adjudication["geography"] == geography]
            if row.empty or int(row.iloc[0]["n_active_all"]) <= 0:
                gates.append(Gate(f"rais.adjudication.geo.{geography}", "FAIL", "critical", f"Geografia {geography} vazia."))
            elif int(row.iloc[0]["n_hourly_analytical"]) <= 0:
                gates.append(Gate(f"rais.adjudication.geo.{geography}.hourly", "FAIL", "critical", f"Universo analítico de renda-hora vazio em {geography}."))
            else:
                gates.append(Gate(f"rais.adjudication.geo.{geography}", "PASS", "critical", f"Geografia {geography} verificada.", {"n_active": int(row.iloc[0]["n_active_all"]), "n_hourly": int(row.iloc[0]["n_hourly_analytical"])}))

    table_paths = {
        "income_status": dirs["tables"] / f"rais_income_status_{run_id}.csv",
        "income_adjudication": dirs["tables"] / f"rais_income_universe_adjudication_{run_id}.csv",
        "gates": dirs["tables"] / f"rais_substantive_adjudication_gates_{run_id}.csv",
        "report": dirs["reports"] / f"rais_substantive_adjudication_report_{run_id}.md",
    }
    income_status.to_csv(table_paths["income_status"], index=False)
    adjudication.to_csv(table_paths["income_adjudication"], index=False)
    gate_table(gates).to_csv(table_paths["gates"], index=False)

    failures = critical_failures(gates)
    status = "ADJUDICATED" if not failures else "ADJUDICATION_BLOCKED"
    lock = {
        "run_id": run_id,
        "script_version": SCRIPT_VERSION,
        "schema_version": SCHEMA_VERSION,
        "component": "RAIS_SUBSTANTIVE_ADJUDICATION",
        "status": status,
        "critical_failures": failures,
        "warnings": [asdict(g) for g in gates if g.status == "WARN"],
        "upstream_rais_lock": str(rais_lock_path),
        "upstream_rais_lock_sha256": sha256_file(rais_lock_path) if rais_lock_path.is_file() else None,
        "upstream_rais_freeze": str(root / "00_admin" / "RAIS_FORMAL_CORE_FREEZE.json"),
        "upstream_rais_freeze_sha256": sha256_file(root / "00_admin" / "RAIS_FORMAL_CORE_FREEZE.json") if (root / "00_admin" / "RAIS_FORMAL_CORE_FREEZE.json").is_file() else None,
        "unit_of_analysis": "formal_employment_link",
        "count_universe": "all primary-CBO links active on 31/12",
        "monthly_income_universe": "active primary-CBO links with nominal monthly income > 0",
        "hourly_income_universe": "active primary-CBO links with nominal monthly income > 0, contractual weekly hours in [1,100], and nonmissing hourly income",
        "sensitivity_universe": "all active primary-CBO links, including zero income",
        "n_active_all": int(len(active)),
        "n_income_positive": int(active["positive_income"].sum()) if not active.empty else 0,
        "n_hourly_analytical": int(active["valid_hourly_income"].sum()) if not active.empty else 0,
        "income_status_counts": {str(r["income_status"]): int(r["n_links"]) for _, r in income_status.iterrows()},
        "geography_results": adjudication.to_dict(orient="records"),
        "claim_ceiling": "Baseline administrativo de vínculos formais ativos no CBO-alvo. Totais referem-se a vínculos, não pessoas únicas; plataforma e informalidade não são observadas. Estatísticas principais de remuneração excluem renda nominal não positiva e, para renda-hora, exigem jornada contratual válida.",
        "artifacts": {k: str(v) for k, v in table_paths.items()},
        "artifact_hashes": {},
        "created_at_utc": utc_now(),
    }

    report_lines = [
        "# RAIS 2022 — Substantive Adjudication",
        "",
        f"- Run ID: `{run_id}`",
        f"- Status: **{status}**",
        f"- Vínculos ativos: **{lock['n_active_all']:,}**",
        f"- Renda positiva: **{lock['n_income_positive']:,}**",
        f"- Universo renda-hora: **{lock['n_hourly_analytical']:,}**",
        "",
        "## Regra adjudicada",
        "",
        "- Contagens: todos os vínculos ativos em 31/12.",
        "- Remuneração mensal principal: renda nominal positiva.",
        "- Renda-hora principal: renda positiva e horas contratuais válidas.",
        "- Sensibilidade: todos os ativos, inclusive renda zero.",
        "",
        "## Resultados territoriais",
        "",
        adjudication.to_markdown(index=False) if not adjudication.empty else "Sem dados.",
        "",
        "## Teto de afirmação",
        "",
        lock["claim_ceiling"],
        "",
        "## Gates",
        "",
        gate_table(gates).to_markdown(index=False) if gates else "Sem gates.",
    ]
    atomic_text(table_paths["report"], "\n".join(report_lines))
    lock["artifact_hashes"] = {k: sha256_file(v) for k, v in table_paths.items() if v.is_file()}

    lock_path = dirs["admin"] / "RAIS_SUBSTANTIVE_ADJUDICATION_LOCK.json"
    freeze_path = dirs["admin"] / "RAIS_SUBSTANTIVE_ADJUDICATION_FREEZE.json"
    atomic_json(lock_path, lock)
    if status == "ADJUDICATED":
        freeze = {
            "freeze_id": run_id,
            "status": "FROZEN",
            "component": "RAIS_SUBSTANTIVE_ADJUDICATION",
            "lock": str(lock_path),
            "lock_sha256": sha256_file(lock_path),
            "read_only": True,
            "upstream_rais_freeze_sha256": lock["upstream_rais_freeze_sha256"],
            "income_adjudication": str(table_paths["income_adjudication"]),
            "income_adjudication_sha256": sha256_file(table_paths["income_adjudication"]),
            "claim_ceiling": lock["claim_ceiling"],
            "created_at_utc": utc_now(),
        }
        atomic_json(freeze_path, freeze)
    logger.info("RAIS substantive adjudication | status=%s", status)
    return lock, active


def default_codebook_path() -> Path:
    return Path(__file__).resolve().with_name("rais_editorial_codebook_ptbr_v1.0.1.csv")


def read_codebook(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(f"Codebook editorial ausente: {path}")
    codebook = pd.read_csv(path, dtype=str).fillna("")
    required = {"dimension", "code", "label_ptbr", "sort_order", "is_missing", "source_url", "source_status"}
    missing = required - set(codebook.columns)
    if missing:
        raise ValueError(f"Codebook não contém colunas obrigatórias: {sorted(missing)}")
    codebook["code_norm"] = codebook.apply(lambda row: normalize_editorial_code(row["code"], row["dimension"]), axis=1)
    codebook["sort_order_num"] = pd.to_numeric(codebook["sort_order"], errors="coerce")
    return codebook


def age_band(age: pd.Series) -> pd.Categorical:
    numeric = pd.to_numeric(age, errors="coerce")
    labels = ["Até 17 anos", "18–24 anos", "25–29 anos", "30–39 anos", "40–49 anos", "50–59 anos", "60–64 anos", "65 anos ou mais"]
    bins = [-np.inf, 17, 24, 29, 39, 49, 59, 64, np.inf]
    result = pd.cut(numeric, bins=bins, labels=labels, right=True)
    return result.cat.add_categories(["Não informado"]).fillna("Não informado")


def decode_dimension(frame: pd.DataFrame, dimension: str, codebook: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series]:
    codes = frame[dimension].map(lambda value: normalize_editorial_code(value, dimension))
    subset = codebook[codebook["dimension"] == dimension]
    label_map = dict(zip(subset["code_norm"], subset["label_ptbr"]))
    order_map = dict(zip(subset["code_norm"], subset["sort_order_num"]))
    labels = codes.map(label_map)
    orders = codes.map(order_map)
    status = np.where(labels.notna(), "MAPPED", "UNMAPPED")
    labels = labels.fillna(codes.map(lambda x: "Não informado" if x == "__NA__" else f"Código não mapeado: {x}"))
    return labels.astype(str), pd.to_numeric(orders, errors="coerce"), pd.Series(status, index=frame.index)


def editorial_profile(frame: pd.DataFrame, dimension: str, label_col: str, code_col: str, order_col: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    total = len(frame)
    for keys, sub in frame.groupby([code_col, label_col, order_col], dropna=False):
        code, label, order = keys
        hourly = sub[sub["valid_hourly_income"]]
        monthly = sub[sub["positive_income"]]
        rows.append({
            "year": int(pd.to_numeric(sub["year"], errors="coerce").dropna().mode().iloc[0]) if not sub.empty else None,
            "dimension": dimension,
            "category_code": code,
            "category_label_ptbr": label,
            "sort_order": safe_float(order),
            "n_links_active": int(len(sub)),
            "share_within_active_year": float(len(sub) / total) if total else np.nan,
            "n_monthly_income_analytical": int(len(monthly)),
            "n_hourly_income_analytical": int(len(hourly)),
            "income_monthly_nominal_mean_positive": float(monthly["income_monthly_nominal"].mean()),
            "income_monthly_nominal_median_positive": q(monthly["income_monthly_nominal"]),
            "income_hour_nominal_mean_positive_valid_hours": float(hourly["income_hour_nominal"].mean()),
            "income_hour_nominal_median_positive_valid_hours": q(hourly["income_hour_nominal"]),
            "contract_hours_mean_analytical": float(hourly["contract_hours"].mean()),
            "contract_hours_median_analytical": q(hourly["contract_hours"]),
        })
    return pd.DataFrame(rows)


def run_rais_editorial(root: Path, dirs: dict[str, Path], run_id: str, active: pd.DataFrame, codebook_path: Path, logger: logging.Logger) -> dict[str, Any]:
    gates: list[Gate] = []
    adjudication_lock_path = dirs["admin"] / "RAIS_SUBSTANTIVE_ADJUDICATION_LOCK.json"
    adjudication_freeze_path = dirs["admin"] / "RAIS_SUBSTANTIVE_ADJUDICATION_FREEZE.json"
    if not adjudication_lock_path.is_file():
        gates.append(Gate("editorial.adjudication_lock", "FAIL", "critical", "RAIS_SUBSTANTIVE_ADJUDICATION_LOCK.json ausente."))
        adjudication_lock = {}
    else:
        adjudication_lock = read_json(adjudication_lock_path)
        if adjudication_lock.get("status") != "ADJUDICATED":
            gates.append(Gate("editorial.adjudication_status", "FAIL", "critical", "Adjudicação RAIS não está ADJUDICATED.", adjudication_lock.get("status"), "ADJUDICATED"))
        else:
            gates.append(Gate("editorial.adjudication_status", "PASS", "critical", "Adjudicação RAIS verificada."))
    if not adjudication_freeze_path.is_file():
        gates.append(Gate("editorial.adjudication_freeze", "FAIL", "critical", "Freeze da adjudicação RAIS ausente."))

    try:
        codebook = read_codebook(codebook_path)
        gates.append(Gate("editorial.codebook.schema", "PASS", "critical", "Codebook editorial carregado.", str(codebook_path)))
    except Exception as exc:
        gates.append(Gate("editorial.codebook.schema", "FAIL", "critical", f"Falha ao carregar codebook: {exc}"))
        codebook = pd.DataFrame()

    frame = prepare_active(active) if not active.empty else active.copy()
    profiles: list[pd.DataFrame] = []
    coverage_rows: list[dict[str, Any]] = []

    for dimension in CORE_EDITORIAL_DIMENSIONS:
        if dimension not in frame.columns:
            gates.append(Gate(f"editorial.{dimension}.column", "FAIL", "critical", f"Coluna {dimension} ausente."))
            continue
        label_col = f"{dimension}_label_ptbr"
        code_col = f"{dimension}_code"
        order_col = f"{dimension}_sort_order"
        status_col = f"{dimension}_mapping_status"
        frame[code_col] = frame[dimension].map(lambda value: normalize_editorial_code(value, dimension))
        labels, orders, statuses = decode_dimension(frame, dimension, codebook)
        frame[label_col] = labels
        frame[order_col] = orders
        frame[status_col] = statuses
        observed_codes = sorted(frame[code_col].unique().tolist())
        unmapped_codes = sorted(frame.loc[frame[status_col] == "UNMAPPED", code_col].unique().tolist())
        mapped_share = float((frame[status_col] == "MAPPED").mean()) if len(frame) else np.nan
        coverage_rows.append({
            "dimension": dimension,
            "n_active": int(len(frame)),
            "observed_codes": json.dumps(observed_codes, ensure_ascii=False),
            "unmapped_codes": json.dumps(unmapped_codes, ensure_ascii=False),
            "mapped_share": mapped_share,
            "status": "COMPLETE" if not unmapped_codes else "INCOMPLETE",
        })
        if unmapped_codes:
            gates.append(Gate(f"editorial.{dimension}.coverage", "FAIL", "critical", f"Códigos não mapeados em {dimension}.", unmapped_codes, "all observed codes mapped"))
        else:
            gates.append(Gate(f"editorial.{dimension}.coverage", "PASS", "critical", f"Cobertura editorial completa em {dimension}.", mapped_share, 1.0))
        profiles.append(editorial_profile(frame, dimension, label_col, code_col, order_col))

    # Faixas etárias são derivadas da idade numérica, evitando dependência do código agregado.
    if "age" in frame.columns:
        frame["age_band_label_ptbr"] = age_band(frame["age"]).astype(str)
        age_order = {
            "Até 17 anos": 1, "18–24 anos": 2, "25–29 anos": 3, "30–39 anos": 4,
            "40–49 anos": 5, "50–59 anos": 6, "60–64 anos": 7, "65 anos ou mais": 8,
            "Não informado": 99,
        }
        frame["age_band_code"] = frame["age_band_label_ptbr"]
        frame["age_band_sort_order"] = frame["age_band_label_ptbr"].map(age_order)
        profiles.append(editorial_profile(frame, "age_band", "age_band_label_ptbr", "age_band_code", "age_band_sort_order"))
        gates.append(Gate("editorial.age_band", "PASS", "critical", "Faixas etárias derivadas da idade numérica."))
    else:
        gates.append(Gate("editorial.age_band", "FAIL", "critical", "Idade ausente para derivação de faixas etárias."))

    profile = pd.concat(profiles, ignore_index=True, sort=False) if profiles else pd.DataFrame()
    if not profile.empty:
        profile = profile.sort_values(["dimension", "sort_order", "category_code"], na_position="last").reset_index(drop=True)

    # CNAE permanece preservada por código; rótulo integral pode ser enriquecido por tabela oficial externa depois.
    cnae_rows: list[dict[str, Any]] = []
    if "cnae_class" in frame.columns:
        for code, sub in frame.groupby(frame["cnae_class"].map(normalize_code), dropna=False):
            hourly = sub[sub["valid_hourly_income"]]
            cnae_rows.append({
                "year": int(pd.to_numeric(sub["year"], errors="coerce").dropna().mode().iloc[0]),
                "cnae_class_code": code,
                "cnae_class_label_ptbr": "Não informado" if code == "__NA__" else f"CNAE 2.0 classe {code}",
                "label_status": "CODE_ONLY",
                "n_links_active": int(len(sub)),
                "share_within_active_year": float(len(sub) / len(frame)),
                "n_hourly_income_analytical": int(len(hourly)),
                "income_hour_nominal_mean_positive_valid_hours": float(hourly["income_hour_nominal"].mean()),
            })
    cnae = pd.DataFrame(cnae_rows).sort_values("n_links_active", ascending=False).reset_index(drop=True) if cnae_rows else pd.DataFrame()
    gates.append(Gate("editorial.cnae.labels", "WARN", "medium", "CNAE preservada por código; rótulos integrais dependem de tabela oficial externa e não bloqueiam o fechamento da Fase 0."))

    coverage = pd.DataFrame(coverage_rows)
    table_paths = {
        "editorial_profile": dirs["tables"] / f"rais_editorial_profile_{run_id}.csv",
        "editorial_coverage": dirs["tables"] / f"rais_editorial_code_coverage_{run_id}.csv",
        "cnae_code_profile": dirs["tables"] / f"rais_cnae_code_profile_{run_id}.csv",
        "decoded_active_parquet": dirs["tables"] / f"rais_active_primary_editorial_{run_id}.parquet",
        "gates": dirs["tables"] / f"rais_editorial_gates_{run_id}.csv",
        "report": dirs["reports"] / f"rais_editorial_profile_report_{run_id}.md",
    }
    profile.to_csv(table_paths["editorial_profile"], index=False)
    coverage.to_csv(table_paths["editorial_coverage"], index=False)
    cnae.to_csv(table_paths["cnae_code_profile"], index=False)
    frame.to_parquet(table_paths["decoded_active_parquet"], index=False)
    gate_table(gates).to_csv(table_paths["gates"], index=False)

    failures = critical_failures(gates)
    status = "EDITORIAL_CERTIFIED" if not failures else "EDITORIAL_BLOCKED"
    lock = {
        "run_id": run_id,
        "script_version": SCRIPT_VERSION,
        "schema_version": SCHEMA_VERSION,
        "component": "RAIS_EDITORIAL_PROFILE",
        "status": status,
        "critical_failures": failures,
        "warnings": [asdict(g) for g in gates if g.status == "WARN"],
        "upstream_adjudication_lock": str(adjudication_lock_path),
        "upstream_adjudication_lock_sha256": sha256_file(adjudication_lock_path) if adjudication_lock_path.is_file() else None,
        "codebook": str(codebook_path),
        "codebook_sha256": sha256_file(codebook_path) if codebook_path.is_file() else None,
        "core_dimensions": CORE_EDITORIAL_DIMENSIONS,
        "age_band_rule": ["Até 17", "18–24", "25–29", "30–39", "40–49", "50–59", "60–64", "65+", "Não informado"],
        "cnae_policy": "Código preservado; rótulo integral opcional e não bloqueante.",
        "n_active_links": int(len(frame)),
        "coverage": coverage.to_dict(orient="records"),
        "claim_ceiling": "Perfis descritivos de vínculos formais ativos no CBO-alvo. Categorias codificadas são decodificadas sem colapsar Não informado; remuneração usa o universo substantivamente adjudicado.",
        "artifacts": {k: str(v) for k, v in table_paths.items()},
        "artifact_hashes": {},
        "created_at_utc": utc_now(),
    }
    report_lines = [
        "# RAIS 2022 — Editorial Profile",
        "",
        f"- Run ID: `{run_id}`",
        f"- Status: **{status}**",
        f"- Vínculos ativos: **{len(frame):,}**",
        "",
        "## Cobertura do codebook",
        "",
        coverage.to_markdown(index=False) if not coverage.empty else "Sem cobertura.",
        "",
        "## Perfil editorial",
        "",
        profile.to_markdown(index=False) if not profile.empty else "Sem perfil.",
        "",
        "## Nota CNAE",
        "",
        "A CNAE é preservada por código. A ausência de rótulo integral não bloqueia a Fase 0, pois a unidade setorial permanece identificável e auditável.",
        "",
        "## Gates",
        "",
        gate_table(gates).to_markdown(index=False),
    ]
    atomic_text(table_paths["report"], "\n".join(report_lines))
    lock["artifact_hashes"] = {k: sha256_file(v) for k, v in table_paths.items() if v.is_file()}

    lock_path = dirs["admin"] / "RAIS_EDITORIAL_PROFILE_LOCK.json"
    freeze_path = dirs["admin"] / "RAIS_EDITORIAL_PROFILE_FREEZE.json"
    atomic_json(lock_path, lock)
    if status == "EDITORIAL_CERTIFIED":
        atomic_json(freeze_path, {
            "freeze_id": run_id,
            "status": "FROZEN",
            "component": "RAIS_EDITORIAL_PROFILE",
            "lock": str(lock_path),
            "lock_sha256": sha256_file(lock_path),
            "decoded_active_parquet": str(table_paths["decoded_active_parquet"]),
            "decoded_active_parquet_sha256": sha256_file(table_paths["decoded_active_parquet"]),
            "read_only": True,
            "claim_ceiling": lock["claim_ceiling"],
            "created_at_utc": utc_now(),
        })
    logger.info("RAIS editorial profile | status=%s", status)
    return lock


def status_of(lock: dict[str, Any]) -> Any:
    return lock.get("status") or lock.get("certification_status") or lock.get("grade")


def validate_component_lock(
    component_id: str,
    path: Path,
    allowed_statuses: set[str],
    gates: list[Gate],
    require_no_failures: bool = True,
) -> dict[str, Any]:
    if not path.is_file():
        gates.append(Gate(f"master.{component_id}.exists", "FAIL", "critical", f"Lock ausente: {path.name}", str(path)))
        return {}
    lock = read_json(path)
    status = str(status_of(lock))
    if status not in allowed_statuses:
        gates.append(Gate(f"master.{component_id}.status", "FAIL", "critical", f"Status inválido para {component_id}.", status, sorted(allowed_statuses)))
    else:
        gates.append(Gate(f"master.{component_id}.status", "PASS", "critical", f"Status de {component_id} verificado.", status))
    failures = lock.get("critical_failures") or []
    if require_no_failures and failures:
        gates.append(Gate(f"master.{component_id}.failures", "FAIL", "critical", f"{component_id} contém falhas críticas.", failures, []))
    else:
        gates.append(Gate(f"master.{component_id}.failures", "PASS", "critical", f"{component_id} sem falhas críticas.", failures))
    return lock


def make_evidence_matrix() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "component_id": "pnadc_direct",
            "dataset": "PNAD Contínua — módulo especial de trabalho por plataformas",
            "period": "2022T4; 2024T3",
            "unit_of_analysis": "pessoa",
            "identification": "direta: SD14001==1 & S140093==1",
            "evidence_tier": "A",
            "platform_direct_observed": True,
            "primary_estimand": "total e participação de entrega por plataforma; perfis survey-weighted",
            "weights": "V1028 e desenho amostral",
            "income_basis": "bruta como principal; real como secundária quando certificada",
            "claim_ceiling": "Entrega por plataforma diretamente observada nos cortes certificados.",
        },
        {
            "component_id": "pnad_covid",
            "dataset": "PNAD COVID19",
            "period": "2020",
            "unit_of_analysis": "pessoa-mês",
            "identification": "ocupação observada: C007C em {16,17}",
            "evidence_tier": "B",
            "platform_direct_observed": False,
            "primary_estimand": "total e perfil do trabalho de entrega no choque pandêmico",
            "weights": "peso mensal da PNAD COVID",
            "income_basis": "conforme lock certificado",
            "claim_ceiling": "Trabalho de entrega observado; uso de plataforma não observado diretamente.",
        },
        {
            "component_id": "pnadc_historical_backcast",
            "dataset": "PNAD Contínua histórica",
            "period": "2019T1–2021T4",
            "unit_of_analysis": "pessoa-trimestre",
            "identification": "probabilística por V4010, V4013 e VD4009",
            "evidence_tier": "C",
            "platform_direct_observed": False,
            "primary_estimand": "soma survey-weighted das probabilidades de compatibilidade histórica",
            "weights": "V1028; survey + model uncertainty",
            "income_basis": "não integra o modelo primário do backcast",
            "claim_ceiling": "Reconstrução agregada da compatibilidade histórica; não é difusão observada de plataformas.",
        },
        {
            "component_id": "rais_formal",
            "dataset": "RAIS vínculos",
            "period": "2022",
            "unit_of_analysis": "vínculo formal",
            "identification": "CBO 519110 ativo em 31/12",
            "evidence_tier": "D",
            "platform_direct_observed": False,
            "primary_estimand": "estoque formal ativo; remuneração positiva; renda-hora com jornada válida",
            "weights": "registro administrativo, sem peso amostral",
            "income_basis": "nominal; real somente com deflator explícito",
            "claim_ceiling": "Baseline do segmento formal regulado; não representa informalidade, plataforma ou pessoas únicas.",
        },
    ])


def make_estimand_registry() -> pd.DataFrame:
    return pd.DataFrame([
        ["pnadc_direct", "direct_platform_total", "total survey-weighted de pessoas", "SD14001==1 & S140093==1", "2022T4/2024T3", "A"],
        ["pnadc_direct", "direct_platform_share", "participação survey-weighted", "denominador elegível certificado", "2022T4/2024T3", "A"],
        ["pnad_covid", "pandemic_delivery_total", "total survey-weighted de pessoas-mês", "C007C em {16,17}", "2020", "B"],
        ["pnadc_historical_backcast", "historical_compatibility_total", "soma w_i p_i", "mapping pooled principal", "2019T1–2021T4", "C"],
        ["rais_formal", "formal_active_links", "contagem de vínculos", "CBO 519110 ativo em 31/12", "2022", "D"],
        ["rais_formal", "formal_income_monthly", "média/mediana nominal", "ativos com renda > 0", "2022", "D"],
        ["rais_formal", "formal_income_hourly", "média/mediana nominal por hora", "renda > 0 e horas válidas", "2022", "D"],
    ], columns=["component_id", "estimand_id", "measure", "universe", "period", "evidence_tier"])


def make_comparison_rules() -> pd.DataFrame:
    rules = [
        ["NO_RAW_POOLING", "PROHIBITED", "Não empilhar linhas de RAIS, PNAD COVID, PNADc regular/especial em uma regressão única.", "Universos, unidades e identificações são distintos."],
        ["PNADC_2022_2024", "ALLOWED_WITH_LIMIT", "Comparar cortes transversais repetidos e gaps ajustados.", "Não denominar DiD causal."],
        ["BACKCAST_DIRECT_SERIES", "ALLOWED_WITH_LIMIT", "Exibir sequência 2019–2024 com marcação explícita de mudança de regime de evidência.", "Não tratar como série observacional homogênea."],
        ["COVID_PLATFORM", "PROHIBITED", "Não rotular entrega da PNAD COVID como plataforma diretamente observada.", "Plataforma não é identificada em C007C."],
        ["RAIS_PNADC_COUNTS", "DESCRIPTIVE_ONLY", "Contrastar baseline formal e universo domiciliar em painéis separados.", "Não calcular taxa formal/plataforma sem denominador harmonizado e unidade comum."],
        ["RAIS_LINKS_PERSONS", "PROHIBITED", "Não interpretar vínculos como pessoas únicas.", "Um indivíduo pode possuir múltiplos vínculos."],
        ["RAIS_HOURS", "ALLOWED_WITH_LIMIT", "Usar horas contratuais para renda-hora formal.", "Não interpretar como horas efetivamente trabalhadas."],
        ["NOMINAL_INTERTEMPORAL", "PROHIBITED_UNLESS_DEFLATED", "Comparação monetária entre anos exige deflator explícito.", "Valores nominais não são intertemporalmente comparáveis."],
        ["BACKCAST_MICRO_CLASSIFICATION", "PROHIBITED", "Não usar ranking/threshold para classificar indivíduos históricos.", "Validação sustenta uso agregado."],
        ["CAUSALITY", "PROHIBITED_UNLESS_IDENTIFIED", "Não atribuir gaps ao algoritmo sem desenho identificador defensável.", "Resultados principais são descritivos, comparativos ou parcialmente identificados."],
    ]
    return pd.DataFrame(rules, columns=["rule_id", "permission", "rule", "rationale"])


def make_claim_registry() -> pd.DataFrame:
    return pd.DataFrame([
        ["platform_direct_observed", "PNADc 2022/2024", "SUPPORTED", "Tier A"],
        ["delivery_pandemic_observed", "PNAD COVID 2020", "SUPPORTED", "Tier B"],
        ["historical_platform_diffusion_observed", "PNADc backcast", "NOT_IDENTIFIABLE", "Tier C não é observação direta"],
        ["formal_baseline_observed", "RAIS 2022", "SUPPORTED", "Tier D administrativo"],
        ["rais_identifies_platform", "RAIS 2022", "NOT_SUPPORTED", "CBO não identifica plataforma"],
        ["rais_counts_unique_workers", "RAIS 2022", "NOT_SUPPORTED", "unidade é vínculo"],
        ["platform_causes_all_gaps", "multibase", "NOT_IDENTIFIABLE", "ausência de desenho causal forte"],
        ["pit_stops_reduce_precarity", "policy engine futuro", "NOT_YET_TESTED", "hipótese ex ante"],
    ], columns=["claim_id", "evidence_source", "adjudication", "basis"])


def run_master(root: Path, dirs: dict[str, Path], run_id: str, logger: logging.Logger) -> dict[str, Any]:
    gates: list[Gate] = []
    admin = dirs["admin"]
    component_paths = {
        "pnadc_direct": admin / "PNADC_CERTIFICATION_LOCK.json",
        "pnad_covid": admin / "PNAD_COVID_CERTIFICATION_LOCK.json",
        "pnad_covid_freeze": admin / "PNAD_COVID_CORE_FREEZE.json",
        "historical_backcast": admin / "PNADC_HISTORICAL_BACKCAST_FINAL_LOCK.json",
        "layout_closure": admin / "PNADC_LAYOUT_EQUIVALENCE_FINAL_LOCK.json",
        "layout_adjudication": admin / "PNADC_LAYOUT_CLOSURE_ADJUDICATION.json",
        "rais_formal": admin / "RAIS_FORMAL_CERTIFICATION_LOCK.json",
        "rais_freeze": admin / "RAIS_FORMAL_CORE_FREEZE.json",
        "rais_adjudication": admin / "RAIS_SUBSTANTIVE_ADJUDICATION_LOCK.json",
        "rais_editorial": admin / "RAIS_EDITORIAL_PROFILE_LOCK.json",
    }
    locks: dict[str, dict[str, Any]] = {}
    locks["pnadc_direct"] = validate_component_lock("pnadc_direct", component_paths["pnadc_direct"], {"CORE_CERTIFIED", "CERTIFIED"}, gates)
    locks["pnad_covid"] = validate_component_lock("pnad_covid", component_paths["pnad_covid"], {"CORE_CERTIFIED", "CERTIFIED", "FROZEN"}, gates)
    locks["pnad_covid_freeze"] = validate_component_lock("pnad_covid_freeze", component_paths["pnad_covid_freeze"], {"FROZEN"}, gates, require_no_failures=False)
    locks["historical_backcast"] = validate_component_lock("historical_backcast", component_paths["historical_backcast"], {"FINAL_CERTIFIED"}, gates)
    locks["layout_closure"] = validate_component_lock("layout_closure", component_paths["layout_closure"], ALLOWED_LAYOUT_STATUSES, gates)
    locks["rais_formal"] = validate_component_lock("rais_formal", component_paths["rais_formal"], {"CORE_CERTIFIED"}, gates)
    locks["rais_freeze"] = validate_component_lock("rais_freeze", component_paths["rais_freeze"], {"FROZEN"}, gates, require_no_failures=False)
    locks["rais_adjudication"] = validate_component_lock("rais_adjudication", component_paths["rais_adjudication"], {"ADJUDICATED"}, gates)
    locks["rais_editorial"] = validate_component_lock("rais_editorial", component_paths["rais_editorial"], {"EDITORIAL_CERTIFIED"}, gates)

    # A adjudicação do layout é um artefato de reforço obrigatório no desenho atual.
    if not component_paths["layout_adjudication"].is_file():
        gates.append(Gate("master.layout_adjudication.exists", "FAIL", "critical", "PNADC_LAYOUT_CLOSURE_ADJUDICATION.json ausente."))
    else:
        adjudication = read_json(component_paths["layout_adjudication"])
        if adjudication.get("status") not in ALLOWED_LAYOUT_STATUSES:
            gates.append(Gate("master.layout_adjudication.status", "FAIL", "critical", "Status inválido na adjudicação de layout.", adjudication.get("status"), sorted(ALLOWED_LAYOUT_STATUSES)))
        elif adjudication.get("design_variables_complete_in_parquet_schema") is not True:
            gates.append(Gate("master.layout_adjudication.design", "FAIL", "critical", "Estrato/UPA não estão completos na adjudicação operacional."))
        else:
            gates.append(Gate("master.layout_adjudication.status", "PASS", "critical", "Adjudicação operacional de layout verificada."))

    evidence = make_evidence_matrix()
    estimands = make_estimand_registry()
    rules = make_comparison_rules()
    claims = make_claim_registry()

    inventory_rows: list[dict[str, Any]] = []
    for component, path in component_paths.items():
        inventory_rows.append(artifact_record(component, path))
    inventory = pd.DataFrame(inventory_rows)

    table_paths = {
        "evidence_matrix": dirs["tables"] / f"phase0_evidence_matrix_{run_id}.csv",
        "estimand_registry": dirs["tables"] / f"phase0_estimand_registry_{run_id}.csv",
        "comparison_rules": dirs["tables"] / f"phase0_comparison_rules_{run_id}.csv",
        "claim_registry": dirs["tables"] / f"phase0_claim_registry_{run_id}.csv",
        "artifact_inventory": dirs["tables"] / f"phase0_artifact_inventory_{run_id}.csv",
        "gates": dirs["tables"] / f"phase0_master_gates_{run_id}.csv",
        "report": dirs["reports"] / f"phase0_master_harmonization_report_{run_id}.md",
    }
    evidence.to_csv(table_paths["evidence_matrix"], index=False)
    estimands.to_csv(table_paths["estimand_registry"], index=False)
    rules.to_csv(table_paths["comparison_rules"], index=False)
    claims.to_csv(table_paths["claim_registry"], index=False)
    inventory.to_csv(table_paths["artifact_inventory"], index=False)
    gate_table(gates).to_csv(table_paths["gates"], index=False)

    failures = critical_failures(gates)
    status = "PHASE0_CERTIFIED" if not failures else "PHASE0_BLOCKED"
    lock = {
        "run_id": run_id,
        "script_version": SCRIPT_VERSION,
        "schema_version": SCHEMA_VERSION,
        "component": "PHASE0_MASTER_HARMONIZATION_AND_EVIDENCE",
        "status": status,
        "phase": "FASE_0_DATA_IDENTIFIABILITY_REPRODUCIBILITY_LOCK",
        "critical_failures": failures,
        "warnings": [asdict(g) for g in gates if g.status == "WARN"],
        "component_locks": {
            component: {
                "path": str(path),
                "sha256": sha256_file(path) if path.is_file() else None,
                "status": status_of(locks.get(component, {})) if component in locks else None,
            }
            for component, path in component_paths.items()
        },
        "evidence_hierarchy": {"A": "direta", "B": "ocupacional observada", "C": "probabilística histórica", "D": "administrativa formal"},
        "no_pooling_rule": True,
        "direct_platform_components": ["pnadc_direct"],
        "non_direct_components": ["pnad_covid", "pnadc_historical_backcast", "rais_formal"],
        "phase0_completion": {
            "pnadc_direct": "CORE_CERTIFIED",
            "pnad_covid": "FROZEN",
            "historical_backcast": "FINAL_CERTIFIED/PUBLICATION",
            "layout_closure": "DOCUMENTATION_LIMITED_OPERATIONALLY_CONSISTENT or CONFIRMED",
            "rais_formal": "CORE_CERTIFIED/FROZEN",
            "rais_income_adjudication": "ADJUDICATED",
            "rais_editorial": "EDITORIAL_CERTIFIED",
        },
        "global_claim_ceiling": "A Fase 0 certifica fontes, universos, estimandos e limites de comparabilidade. Não produz por si só identificação causal. As quatro bases permanecem separadas por unidade, período e regime de evidência.",
        "next_phase": "FASE_1_EVIDENCE_FOUNDATION",
        "artifacts": {k: str(v) for k, v in table_paths.items()},
        "artifact_hashes": {},
        "created_at_utc": utc_now(),
    }
    report_lines = [
        "# SPINE-GPE v7 — Phase 0 Master Harmonization & Evidence Lock",
        "",
        f"- Run ID: `{run_id}`",
        f"- Status: **{status}**",
        "",
        "## Matriz de evidência",
        "",
        evidence.to_markdown(index=False),
        "",
        "## Registro de estimandos",
        "",
        estimands.to_markdown(index=False),
        "",
        "## Regras de comparação",
        "",
        rules.to_markdown(index=False),
        "",
        "## Adjudicação de claims",
        "",
        claims.to_markdown(index=False),
        "",
        "## Gates",
        "",
        gate_table(gates).to_markdown(index=False),
        "",
        "## Teto global",
        "",
        lock["global_claim_ceiling"],
    ]
    atomic_text(table_paths["report"], "\n".join(report_lines))
    lock["artifact_hashes"] = {k: sha256_file(v) for k, v in table_paths.items() if v.is_file()}

    lock_path = dirs["admin"] / "SPINE_GPE_PHASE0_MASTER_LOCK.json"
    freeze_path = dirs["admin"] / "SPINE_GPE_PHASE0_MASTER_FREEZE.json"
    atomic_json(lock_path, lock)
    if status == "PHASE0_CERTIFIED":
        atomic_json(freeze_path, {
            "freeze_id": run_id,
            "status": "FROZEN",
            "component": "PHASE0_MASTER_HARMONIZATION_AND_EVIDENCE",
            "lock": str(lock_path),
            "lock_sha256": sha256_file(lock_path),
            "read_only": True,
            "next_phase": "FASE_1_EVIDENCE_FOUNDATION",
            "global_claim_ceiling": lock["global_claim_ceiling"],
            "created_at_utc": utc_now(),
        })
    logger.info("Phase 0 master lock | status=%s", status)
    return lock


def run_audit(root: Path, dirs: dict[str, Path], run_id: str, codebook_path: Path, logger: logging.Logger) -> dict[str, Any]:
    gates: list[Gate] = []
    rais_lock, rais_freeze, active, rais_lock_path = load_rais_context(root, gates)
    expected_paths = [
        root / "00_admin" / "PNADC_CERTIFICATION_LOCK.json",
        root / "00_admin" / "PNAD_COVID_CERTIFICATION_LOCK.json",
        root / "00_admin" / "PNAD_COVID_CORE_FREEZE.json",
        root / "00_admin" / "PNADC_HISTORICAL_BACKCAST_FINAL_LOCK.json",
        root / "00_admin" / "PNADC_LAYOUT_EQUIVALENCE_FINAL_LOCK.json",
        root / "00_admin" / "PNADC_LAYOUT_CLOSURE_ADJUDICATION.json",
        root / "00_admin" / "RAIS_FORMAL_CERTIFICATION_LOCK.json",
        root / "00_admin" / "RAIS_FORMAL_CORE_FREEZE.json",
        codebook_path,
    ]
    for path in expected_paths:
        if path.is_file():
            gates.append(Gate(f"audit.file.{path.name}", "PASS", "critical", "Arquivo requerido localizado.", str(path)))
        else:
            gates.append(Gate(f"audit.file.{path.name}", "FAIL", "critical", "Arquivo requerido ausente.", str(path)))
    if not active.empty:
        prepared = prepare_active(active)
        status_counts = prepared["income_status"].value_counts().to_dict()
        gates.append(Gate("audit.rais.income_status", "PASS", "critical", "Status da renda calculável.", status_counts))
        for dimension in CORE_EDITORIAL_DIMENSIONS:
            if dimension not in prepared.columns:
                gates.append(Gate(f"audit.editorial.{dimension}", "FAIL", "critical", f"Coluna {dimension} ausente."))
    paths = {
        "gates": dirs["tables"] / f"phase0_closure_audit_gates_{run_id}.csv",
        "report": dirs["reports"] / f"phase0_closure_audit_report_{run_id}.md",
    }
    gate_table(gates).to_csv(paths["gates"], index=False)
    failures = critical_failures(gates)
    status = "AUDIT_PASSED" if not failures else "AUDIT_BLOCKED"
    payload = {
        "run_id": run_id,
        "script_version": SCRIPT_VERSION,
        "schema_version": SCHEMA_VERSION,
        "mode": "audit",
        "status": status,
        "critical_failures": failures,
        "warnings": [asdict(g) for g in gates if g.status == "WARN"],
        "root": str(root),
        "rais_active_rows": int(len(active)),
        "artifacts": {k: str(v) for k, v in paths.items()},
        "artifact_hashes": {},
        "created_at_utc": utc_now(),
    }
    atomic_text(paths["report"], "# Phase 0 Closure Audit\n\n" + gate_table(gates).to_markdown(index=False))
    payload["artifact_hashes"] = {k: sha256_file(v) for k, v in paths.items() if v.is_file()}
    atomic_json(dirs["admin"] / "PHASE0_CLOSURE_AUDIT_LOCK.json", payload)
    logger.info("Phase 0 closure audit | status=%s", status)
    return payload


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SPINE-GPE v7 Phase 0 Closure Engine")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--mode", choices=["audit", "full"], default="audit")
    parser.add_argument("--stage", choices=["rais_adjudication", "editorial", "master", "all"], default="all")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--codebook", type=Path, default=None)
    parser.add_argument("--strict", action="store_true", help="Mantido por compatibilidade; o engine já é fail-closed.")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    logger = setup_logger(args.verbose)
    root = args.root.expanduser().resolve()
    dirs = ensure_dirs(root)
    run_id = args.run_id or f"phase0_closure_{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_v101"
    codebook_path = (args.codebook or default_codebook_path()).expanduser().resolve()
    logger.info("SPINE-GPE Phase 0 Closure v%s | mode=%s | stage=%s | root=%s", SCRIPT_VERSION, args.mode, args.stage, root)

    if args.mode == "audit":
        payload = run_audit(root, dirs, run_id, codebook_path, logger)
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0 if payload["status"] == "AUDIT_PASSED" else 2

    active = pd.DataFrame()
    outputs: dict[str, Any] = {}
    if args.stage in {"rais_adjudication", "all"}:
        adjudication, active = run_rais_adjudication(root, dirs, run_id, codebook_path, logger)
        outputs["rais_adjudication"] = adjudication
        if adjudication["status"] != "ADJUDICATED":
            print(json.dumps(outputs, ensure_ascii=False, indent=2))
            return 2

    if args.stage in {"editorial", "all"}:
        if active.empty:
            gates: list[Gate] = []
            _, _, active, _ = load_rais_context(root, gates)
            if critical_failures(gates):
                print(json.dumps({"status": "EDITORIAL_BLOCKED", "critical_failures": critical_failures(gates)}, ensure_ascii=False, indent=2))
                return 2
        editorial = run_rais_editorial(root, dirs, run_id, active, codebook_path, logger)
        outputs["rais_editorial"] = editorial
        if editorial["status"] != "EDITORIAL_CERTIFIED":
            print(json.dumps(outputs, ensure_ascii=False, indent=2))
            return 2

    if args.stage in {"master", "all"}:
        master = run_master(root, dirs, run_id, logger)
        outputs["master"] = master
        if master["status"] != "PHASE0_CERTIFIED":
            print(json.dumps(outputs, ensure_ascii=False, indent=2))
            return 2

    print(json.dumps(outputs, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
