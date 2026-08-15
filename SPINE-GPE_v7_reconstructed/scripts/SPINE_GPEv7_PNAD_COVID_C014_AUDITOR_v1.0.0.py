#!/usr/bin/env python3
"""
SPINE-GPE v7 — PNAD COVID C014 Universe Auditor v1.0.0

Lightweight, read-only audit over the already certified immutable PNAD COVID
Parquet. It does NOT read raw microdata and does NOT generate analytical data.

Audit dimensions:
    C014 valid/missing
    × C007 position code
    × C007C delivery type (16 motoboy / 17 goods delivery)
    × reference month (May–November 2020)

The script freezes the certified PNAD COVID core after checking provenance,
cell conservation, domains, denominator rules, and the absence of direct
platform identification.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

SCRIPT_VERSION = "1.0.0"
AUDIT_SCHEMA_VERSION = "spine-gpe-v7-pnad-covid-c014-audit-1.0.0"
EXPECTED_DATA_SCHEMA = "spine-gpe-v7-pnad-covid-delivery-1.0.1"
EXPECTED_MONTHS = [5, 6, 7, 8, 9, 10, 11]
DELIVERY_CODES = {"16": "motoboy", "17": "goods_delivery"}
VALID_C014_CODES = {"1", "2"}

REQUIRED_COLUMNS = [
    "source_year",
    "reference_month",
    "survey_weight",
    "C007",
    "C007C",
    "C014",
    "pandemic_delivery_observed",
    "pandemic_delivery_self_employed",
    "social_security_response_valid",
    "social_security_contributor",
    "platform_delivery_direct",
    "platform_direct_available",
    "evidence_tier",
]


@dataclass
class AuditResult:
    test_id: str
    status: str
    severity: str
    message: str
    observed: Any = None
    expected: Any = None
    evidence: dict[str, Any] | None = None


def now_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def atomic_copy(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_name(dst.name + ".tmp")
    shutil.copy2(src, tmp)
    os.replace(tmp, dst)


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2, default=json_default) + "\n")


def json_default(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if pd.isna(value):
        return None
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def canonical_code(series: pd.Series) -> pd.Series:
    """Canonicalize integer-like categorical codes without inventing values."""
    s = series.astype("string").str.strip()
    s = s.mask(s.isin(["", "nan", "None", "<NA>"]))
    numeric = pd.to_numeric(s, errors="coerce")
    integer_like = numeric.notna() & np.isclose(numeric % 1, 0)
    out = s.copy()
    out.loc[integer_like] = numeric.loc[integer_like].astype("Int64").astype("string")
    return out


def bool_series(series: pd.Series) -> pd.Series:
    if str(series.dtype) == "boolean" or series.dtype == bool:
        return series.astype("boolean")
    s = series.astype("string").str.strip().str.lower()
    mapping = {
        "true": True,
        "1": True,
        "yes": True,
        "sim": True,
        "false": False,
        "0": False,
        "no": False,
        "nao": False,
        "não": False,
    }
    return s.map(mapping).astype("boolean")


def safe_pct(num: float, den: float) -> float | None:
    if den is None or den == 0 or not np.isfinite(den):
        return None
    return float(num / den * 100.0)


def cramer_v_from_table(table: pd.DataFrame) -> float | None:
    obs = table.to_numpy(dtype=float)
    n = obs.sum()
    if n <= 0 or obs.shape[0] < 2 or obs.shape[1] < 2:
        return None
    row = obs.sum(axis=1, keepdims=True)
    col = obs.sum(axis=0, keepdims=True)
    exp = row @ col / n
    mask = exp > 0
    if not mask.any():
        return None
    chi2 = float((((obs - exp) ** 2) / np.where(mask, exp, 1.0))[mask].sum())
    denom = n * min(obs.shape[0] - 1, obs.shape[1] - 1)
    return float(math.sqrt(chi2 / denom)) if denom > 0 else None


def load_upstream_lock(root: Path) -> tuple[Path, dict[str, Any]]:
    lock_path = root / "00_admin" / "PNAD_COVID_CERTIFICATION_LOCK.json"
    if not lock_path.exists():
        raise FileNotFoundError(f"Lock PNAD COVID ausente: {lock_path}")
    return lock_path, json.loads(lock_path.read_text(encoding="utf-8"))


def resolve_input(root: Path, upstream: dict[str, Any], override: str | None) -> Path:
    if override:
        path = Path(override).expanduser().resolve()
    else:
        value = upstream.get("output") or upstream.get("output_latest")
        if not value:
            raise RuntimeError("O lock upstream não informa output imutável nem alias latest.")
        path = Path(value)
    if not path.exists():
        raise FileNotFoundError(f"Parquet certificado não encontrado: {path}")
    return path


def read_certified_parquet(path: Path) -> pd.DataFrame:
    try:
        return pd.read_parquet(path, columns=REQUIRED_COLUMNS)
    except Exception as exc:
        raise RuntimeError(f"Falha ao ler colunas certificadas do Parquet {path}: {exc}") from exc


def prepare_delivery(frame: pd.DataFrame) -> pd.DataFrame:
    df = frame.copy()
    df["reference_month"] = pd.to_numeric(df["reference_month"], errors="coerce").astype("Int64")
    df["source_year"] = pd.to_numeric(df["source_year"], errors="coerce").astype("Int64")
    df["survey_weight"] = pd.to_numeric(df["survey_weight"], errors="coerce")
    for col in ("C007", "C007C", "C014"):
        df[col] = canonical_code(df[col])
    for col in (
        "pandemic_delivery_observed",
        "pandemic_delivery_self_employed",
        "social_security_response_valid",
        "social_security_contributor",
        "platform_delivery_direct",
        "platform_direct_available",
    ):
        df[col] = bool_series(df[col])

    delivery = df[df["pandemic_delivery_observed"].fillna(False)].copy()
    delivery["delivery_type"] = delivery["C007C"].map(DELIVERY_CODES).astype("string")
    delivery["position_code"] = delivery["C007"].fillna("MISSING")
    delivery["position_self_employed"] = delivery["C007"].eq("7")

    valid = delivery["C014"].isin(VALID_C014_CODES)
    delivery["c014_valid"] = valid
    delivery["c014_status"] = np.select(
        [
            delivery["C014"].eq("1").fillna(False).to_numpy(dtype=bool),
            delivery["C014"].eq("2").fillna(False).to_numpy(dtype=bool),
        ],
        ["valid_contributor", "valid_noncontributor"],
        default="missing",
    )
    return delivery


def cell_table(delivery: pd.DataFrame) -> pd.DataFrame:
    keys = [
        "reference_month",
        "delivery_type",
        "position_code",
        "position_self_employed",
        "c014_status",
    ]
    out = (
        delivery.groupby(keys, dropna=False, observed=True)
        .agg(
            n_unweighted=("C014", "size"),
            weighted_total=("survey_weight", "sum"),
        )
        .reset_index()
        .sort_values(keys)
    )
    return out


def grouped_summary(delivery: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    grouped = delivery.groupby(keys, dropna=False, observed=True)
    for key, g in grouped:
        key_tuple = key if isinstance(key, tuple) else (key,)
        row = dict(zip(keys, key_tuple))
        w = g["survey_weight"].fillna(0.0).astype(float)
        valid = g["c014_valid"].fillna(False).astype(bool)
        contrib = g["C014"].eq("1") & valid
        noncontrib = g["C014"].eq("2") & valid
        total_w = float(w.sum())
        valid_w = float(w[valid].sum())
        contrib_w = float(w[contrib].sum())
        row.update(
            {
                "n_delivery": int(len(g)),
                "weighted_delivery": total_w,
                "n_c014_valid": int(valid.sum()),
                "n_c014_missing": int((~valid).sum()),
                "weighted_c014_valid": valid_w,
                "weighted_c014_missing": float(w[~valid].sum()),
                "c014_coverage_percent_unweighted": safe_pct(float(valid.sum()), float(len(g))),
                "c014_coverage_percent_weighted": safe_pct(valid_w, total_w),
                "n_contributor": int(contrib.sum()),
                "n_noncontributor": int(noncontrib.sum()),
                "weighted_contributor": contrib_w,
                "weighted_noncontributor": float(w[noncontrib].sum()),
                "contribution_percent_among_valid_unweighted": safe_pct(float(contrib.sum()), float(valid.sum())),
                "contribution_percent_among_valid_weighted": safe_pct(contrib_w, valid_w),
            }
        )
        rows.append(row)
    return pd.DataFrame(rows).sort_values(keys).reset_index(drop=True)


def association_summary(delivery: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for month, g in delivery.groupby("reference_month", observed=True):
        tab = pd.crosstab(g["position_code"], g["c014_valid"], dropna=False)
        coverage = (
            g.groupby("position_code", dropna=False, observed=True)["c014_valid"]
            .agg(["size", "sum"])
            .assign(coverage=lambda x: x["sum"] / x["size"] * 100.0)
        )
        zero_codes = coverage.index[coverage["coverage"].eq(0)].astype(str).tolist()
        full_codes = coverage.index[coverage["coverage"].eq(100)].astype(str).tolist()
        rows.append(
            {
                "reference_month": int(month),
                "cramers_v_position_vs_c014_valid": cramer_v_from_table(tab),
                "n_position_codes": int(coverage.shape[0]),
                "position_codes_zero_coverage": ",".join(zero_codes),
                "position_codes_full_coverage": ",".join(full_codes),
                "min_position_coverage_percent": float(coverage["coverage"].min()),
                "max_position_coverage_percent": float(coverage["coverage"].max()),
            }
        )
    return pd.DataFrame(rows).sort_values("reference_month")


def run_tests(
    frame: pd.DataFrame,
    delivery: pd.DataFrame,
    cells: pd.DataFrame,
    upstream: dict[str, Any],
    input_path: Path,
    actual_hash: str,
) -> list[AuditResult]:
    tests: list[AuditResult] = []
    upstream_status = upstream.get("status")
    tests.append(
        AuditResult(
            "upstream.pnad_covid.certified",
            "PASS" if upstream_status == "CERTIFIED" else "FAIL",
            "critical",
            "PNAD COVID upstream está CERTIFIED." if upstream_status == "CERTIFIED" else "PNAD COVID upstream não está CERTIFIED.",
            upstream_status,
            "CERTIFIED",
        )
    )

    expected_hash = (upstream.get("artifact_hashes") or {}).get("output")
    hash_ok = expected_hash is not None and expected_hash == actual_hash
    tests.append(
        AuditResult(
            "input.immutable_hash",
            "PASS" if hash_ok else "FAIL",
            "critical",
            "Hash do Parquet coincide com o lock upstream." if hash_ok else "Hash do Parquet não coincide ou não está registrado no lock upstream.",
            actual_hash,
            expected_hash,
            {"input": str(input_path)},
        )
    )

    schema_ok = upstream.get("schema_version") == EXPECTED_DATA_SCHEMA
    tests.append(
        AuditResult(
            "input.schema_version",
            "PASS" if schema_ok else "FAIL",
            "critical",
            "Schema certificado esperado." if schema_ok else "Schema upstream diverge do contrato do auditor.",
            upstream.get("schema_version"),
            EXPECTED_DATA_SCHEMA,
        )
    )

    months = sorted(pd.to_numeric(delivery["reference_month"], errors="coerce").dropna().astype(int).unique().tolist())
    tests.append(
        AuditResult(
            "period.months_complete",
            "PASS" if months == EXPECTED_MONTHS else "FAIL",
            "critical",
            "Meses 05–11 preservados." if months == EXPECTED_MONTHS else "Conjunto de meses incompleto ou inesperado.",
            months,
            EXPECTED_MONTHS,
        )
    )

    years = sorted(pd.to_numeric(delivery["source_year"], errors="coerce").dropna().astype(int).unique().tolist())
    tests.append(
        AuditResult(
            "period.year_2020",
            "PASS" if years == [2020] else "FAIL",
            "critical",
            "Ano da fonte é 2020." if years == [2020] else "Ano inesperado no Parquet.",
            years,
            [2020],
        )
    )

    delivery_codes = set(delivery["C007C"].dropna().astype(str).unique().tolist())
    tests.append(
        AuditResult(
            "delivery.domain",
            "PASS" if delivery_codes <= set(DELIVERY_CODES) and delivery_codes else "FAIL",
            "critical",
            "Entregadores restritos a C007C=16/17." if delivery_codes <= set(DELIVERY_CODES) and delivery_codes else "Códigos de entrega inesperados.",
            sorted(delivery_codes),
            sorted(DELIVERY_CODES),
        )
    )

    c014_codes = set(delivery["C014"].dropna().astype(str).unique().tolist())
    tests.append(
        AuditResult(
            "c014.domain",
            "PASS" if c014_codes <= VALID_C014_CODES else "FAIL",
            "critical",
            "C014 observado pertence ao domínio 1/2." if c014_codes <= VALID_C014_CODES else "C014 contém códigos fora do domínio 1/2.",
            sorted(c014_codes),
            sorted(VALID_C014_CODES),
        )
    )

    valid_consistent = bool(
        (delivery["social_security_response_valid"].fillna(False).astype(bool) == delivery["c014_valid"].astype(bool)).all()
    )
    tests.append(
        AuditResult(
            "c014.validity_consistency",
            "PASS" if valid_consistent else "FAIL",
            "critical",
            "Flag de resposta válida coincide com C014 em 1/2." if valid_consistent else "Flag de validade diverge do código C014.",
        )
    )

    derived_self = delivery["position_code"].eq("7")
    stored_self = delivery["pandemic_delivery_self_employed"].fillna(False).astype(bool)
    self_ok = bool((derived_self == stored_self).all())
    tests.append(
        AuditResult(
            "c007.self_employed_derivation",
            "PASS" if self_ok else "FAIL",
            "critical",
            "Conta-própria armazenada coincide com C007=7." if self_ok else "Conta-própria armazenada diverge de C007=7.",
        )
    )

    direct_absent = bool(delivery["platform_delivery_direct"].isna().all())
    direct_flag_false = bool((delivery["platform_direct_available"].fillna(False) == False).all())  # noqa: E712
    tests.append(
        AuditResult(
            "epistemic.platform_direct_absent",
            "PASS" if direct_absent and direct_flag_false else "FAIL",
            "critical",
            "Identificação direta de plataforma permanece ausente." if direct_absent and direct_flag_false else "Foi encontrada identificação direta indevida de plataforma.",
        )
    )

    n_delivery = len(delivery)
    n_cells = int(cells["n_unweighted"].sum())
    w_delivery = float(delivery["survey_weight"].fillna(0).sum())
    w_cells = float(cells["weighted_total"].fillna(0).sum())
    conservation = n_delivery == n_cells and math.isclose(w_delivery, w_cells, rel_tol=1e-12, abs_tol=1e-6)
    tests.append(
        AuditResult(
            "cells.conservation",
            "PASS" if conservation else "FAIL",
            "critical",
            "Células preservam contagens e pesos." if conservation else "Células não preservam totais do domínio de entrega.",
            {"n_delivery": n_delivery, "n_cells": n_cells, "weighted_delivery": w_delivery, "weighted_cells": w_cells},
        )
    )

    for month in EXPECTED_MONTHS:
        g = delivery[delivery["reference_month"].eq(month)]
        valid_n = int(g["c014_valid"].sum())
        coverage = safe_pct(valid_n, len(g))
        tests.append(
            AuditResult(
                f"2020m{month:02d}.c014.denominator",
                "PASS" if valid_n >= 30 else "FAIL",
                "critical",
                "Denominador C014 válido é suficiente para descrição nacional." if valid_n >= 30 else "Denominador C014 válido insuficiente.",
                valid_n,
                ">=30",
            )
        )
        if coverage is not None and coverage < 95:
            tests.append(
                AuditResult(
                    f"2020m{month:02d}.c014.coverage",
                    "WARN",
                    "high",
                    "Cobertura C014 abaixo de 95%; interpretação permanece condicional às respostas válidas.",
                    coverage,
                    ">=95%",
                )
            )

    return tests


def fmt(value: Any, digits: int = 4) -> str:
    if value is None or (isinstance(value, float) and not np.isfinite(value)):
        return ""
    if isinstance(value, (float, np.floating)):
        return f"{float(value):.{digits}f}"
    return str(value)


def build_report(
    run_id: str,
    status: str,
    input_path: Path,
    actual_hash: str,
    upstream: dict[str, Any],
    month_summary: pd.DataFrame,
    position_summary: pd.DataFrame,
    association: pd.DataFrame,
    tests: list[AuditResult],
    outputs: dict[str, str],
) -> str:
    lines = [
        "# SPINE-GPE v7 — Auditoria do Universo C014 na PNAD COVID 2020",
        "",
        f"- Run ID: `{run_id}`",
        f"- Versão: `{SCRIPT_VERSION}`",
        f"- Audit schema: `{AUDIT_SCHEMA_VERSION}`",
        f"- Status: **{status}**",
        f"- Parquet certificado: `{input_path}`",
        f"- SHA-256: `{actual_hash}`",
        f"- Upstream PNAD COVID: `{upstream.get('status')}` / `{upstream.get('run_id')}`",
        "",
        "## Escopo",
        "",
        "Auditoria somente-leitura de `C014 válido/ausente × C007 × C007C × mês`. Nenhum microdado bruto é relido, nenhum registro é imputado e nenhum output analítico certificado é alterado.",
        "",
        "## Limite de interpretação",
        "",
        "O percentual de contribuição previdenciária é definido apenas entre entregadores com `C014` válido (`1` ou `2`). Ausências permanecem ausências. A auditoria descreve associação com posição e tipo de entrega, mas não classifica automaticamente o missingness como MCAR, MAR ou MNAR.",
        "",
        "## Cobertura mensal geral",
        "",
        "| Mês | n entrega | n C014 válido | n C014 ausente | Cobertura não ponderada | Cobertura ponderada | Contribuição entre válidos ponderada |",
        "|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, row in month_summary.iterrows():
        lines.append(
            f"| {int(row['reference_month']):02d} | {int(row['n_delivery'])} | {int(row['n_c014_valid'])} | "
            f"{int(row['n_c014_missing'])} | {fmt(row['c014_coverage_percent_unweighted'])} | "
            f"{fmt(row['c014_coverage_percent_weighted'])} | {fmt(row['contribution_percent_among_valid_weighted'])} |"
        )

    lines.extend(
        [
            "",
            "## Associação entre posição C007 e validade de C014",
            "",
            "| Mês | Cramér V | n códigos C007 | Cobertura mínima por posição | Cobertura máxima por posição | Códigos com 0% | Códigos com 100% |",
            "|---:|---:|---:|---:|---:|---|---|",
        ]
    )
    for _, row in association.iterrows():
        lines.append(
            f"| {int(row['reference_month']):02d} | {fmt(row['cramers_v_position_vs_c014_valid'])} | "
            f"{int(row['n_position_codes'])} | {fmt(row['min_position_coverage_percent'])} | "
            f"{fmt(row['max_position_coverage_percent'])} | {row['position_codes_zero_coverage']} | "
            f"{row['position_codes_full_coverage']} |"
        )

    lines.extend(
        [
            "",
            "## Cobertura por posição e tipo de entrega",
            "",
            "A tabela detalhada está no CSV `position_delivery_month`. Abaixo são mostradas as células com pelo menos um entregador:",
            "",
            "| Mês | Tipo | C007 | Conta-própria | n | Cobertura C014 não ponderada | Cobertura C014 ponderada | Contribuição entre válidos ponderada |",
            "|---:|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    for _, row in position_summary.iterrows():
        lines.append(
            f"| {int(row['reference_month']):02d} | {row['delivery_type']} | {row['position_code']} | "
            f"{bool(row['position_self_employed'])} | {int(row['n_delivery'])} | "
            f"{fmt(row['c014_coverage_percent_unweighted'])} | {fmt(row['c014_coverage_percent_weighted'])} | "
            f"{fmt(row['contribution_percent_among_valid_weighted'])} |"
        )

    lines.extend(
        [
            "",
            "## Gates",
            "",
            "| Status | Severidade | Teste | Mensagem |",
            "|---|---|---|---|",
        ]
    )
    for t in tests:
        lines.append(f"| {t.status} | {t.severity} | `{t.test_id}` | {t.message} |")

    lines.extend(
        [
            "",
            "## Artefatos",
            "",
        ]
    )
    for key, value in outputs.items():
        lines.append(f"- {key}: `{value}`")

    lines.extend(
        [
            "",
            "## Decisão",
            "",
            "O núcleo PNAD COVID permanece congelado como `CERTIFIED`. O outcome previdenciário é utilizável apenas com a formulação explícita: contribuição entre entregadores com resposta válida em C014, acompanhada da cobertura por mês, posição e tipo de entrega.",
            "",
            "## Próximo gate",
            "",
            "Certificar a PNADc histórica e calibrar a proxy ocupação × atividade × posição contra a identificação direta PNADc 2022T4/2024T3, sem promover a proxy a plataforma observada.",
        ]
    )
    return "\n".join(lines) + "\n"


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, help="Raiz SPINE-GPEv7")
    parser.add_argument("--input", help="Parquet certificado imutável; padrão: lock PNAD COVID")
    parser.add_argument("--strict", action="store_true", help="Retorna código 2 se houver falha crítica")
    args = parser.parse_args(list(argv) if argv is not None else None)

    root = Path(args.root).expanduser().resolve()
    run_id = now_run_id()
    lock_path, upstream = load_upstream_lock(root)
    input_path = resolve_input(root, upstream, args.input)
    actual_hash = sha256_file(input_path)

    frame = read_certified_parquet(input_path)
    delivery = prepare_delivery(frame)
    cells = cell_table(delivery)
    month_summary = grouped_summary(delivery, ["reference_month"])
    type_summary = grouped_summary(delivery, ["reference_month", "delivery_type"])
    position_summary = grouped_summary(
        delivery,
        ["reference_month", "delivery_type", "position_code", "position_self_employed"],
    )
    association = association_summary(delivery)
    tests = run_tests(frame, delivery, cells, upstream, input_path, actual_hash)

    critical = [asdict(t) for t in tests if t.status in {"FAIL", "BLOCKED"} and t.severity == "critical"]
    warnings = [asdict(t) for t in tests if t.status == "WARN"]
    status = "AUDIT_PASSED" if not critical else "AUDIT_BLOCKED"

    admin = root / "00_admin"
    registry = admin / "registry"
    tables = root / "05_outputs" / "tables" / "pnad_covid"
    reports = root / "06_reports" / "pnad_covid_certification"
    for directory in (admin, registry, tables, reports):
        directory.mkdir(parents=True, exist_ok=True)

    paths = {
        "cells": tables / f"pnad_covid_c014_cells_{run_id}.csv",
        "month": tables / f"pnad_covid_c014_month_{run_id}.csv",
        "type": tables / f"pnad_covid_c014_delivery_type_month_{run_id}.csv",
        "position": tables / f"pnad_covid_c014_position_delivery_month_{run_id}.csv",
        "association": tables / f"pnad_covid_c014_position_association_{run_id}.csv",
        "audit_json": registry / f"pnad_covid_c014_universe_audit_{run_id}.json",
        "report": reports / f"pnad_covid_c014_audit_report_{run_id}.md",
        "lock": admin / "PNAD_COVID_C014_AUDIT_LOCK.json",
        "freeze": admin / "PNAD_COVID_CORE_FREEZE.json",
    }

    cells.to_csv(paths["cells"], index=False)
    month_summary.to_csv(paths["month"], index=False)
    type_summary.to_csv(paths["type"], index=False)
    position_summary.to_csv(paths["position"], index=False)
    association.to_csv(paths["association"], index=False)

    artifact_hashes = {
        key: sha256_file(path)
        for key, path in paths.items()
        if key in {"cells", "month", "type", "position", "association"}
    }

    audit_payload = {
        "run_id": run_id,
        "script_version": SCRIPT_VERSION,
        "audit_schema_version": AUDIT_SCHEMA_VERSION,
        "status": status,
        "input": str(input_path),
        "input_sha256": actual_hash,
        "upstream_lock": str(lock_path),
        "upstream_run_id": upstream.get("run_id"),
        "upstream_status": upstream.get("status"),
        "n_rows_input": int(len(frame)),
        "n_delivery": int(len(delivery)),
        "tests": [asdict(t) for t in tests],
        "critical_failures": critical,
        "warnings": warnings,
        "outputs": {key: str(value) for key, value in paths.items()},
        "artifact_hashes": artifact_hashes,
        "created_at_utc": utc_iso(),
    }
    atomic_write_json(paths["audit_json"], audit_payload)

    report = build_report(
        run_id,
        status,
        input_path,
        actual_hash,
        upstream,
        month_summary,
        position_summary,
        association,
        tests,
        {key: str(value) for key, value in paths.items()},
    )
    atomic_write_text(paths["report"], report)
    audit_payload["artifact_hashes"]["audit_json"] = sha256_file(paths["audit_json"])
    audit_payload["artifact_hashes"]["report"] = sha256_file(paths["report"])

    lock_payload = {
        "run_id": run_id,
        "script_version": SCRIPT_VERSION,
        "audit_schema_version": AUDIT_SCHEMA_VERSION,
        "status": status,
        "critical_failures": critical,
        "warnings": warnings,
        "input": str(input_path),
        "input_sha256": actual_hash,
        "report": str(paths["report"]),
        "audit_json": str(paths["audit_json"]),
        "position_delivery_month": str(paths["position"]),
        "artifact_hashes": audit_payload["artifact_hashes"],
        "created_at_utc": utc_iso(),
    }
    atomic_write_json(paths["lock"], lock_payload)

    if status == "AUDIT_PASSED":
        freeze_payload = {
            "freeze_id": run_id,
            "status": "FROZEN",
            "component": "PNAD_COVID_2020_CORE",
            "core_certification_status": upstream.get("status"),
            "core_certification_run_id": upstream.get("run_id"),
            "immutable_parquet": str(input_path),
            "immutable_parquet_sha256": actual_hash,
            "c014_audit_lock": str(paths["lock"]),
            "c014_audit_report": str(paths["report"]),
            "social_security_claim_ceiling": "Contribuição previdenciária entre entregadores com resposta válida em C014; cobertura reportada por mês, C007 e C007C.",
            "epistemic_limit": "C007C=16/17 observa ocupação de entrega; plataforma direta permanece não observada.",
            "read_only": True,
            "created_at_utc": utc_iso(),
        }
        atomic_write_json(paths["freeze"], freeze_payload)

    print(json.dumps(lock_payload, ensure_ascii=False, indent=2, default=json_default))
    print(f"STATUS: {status}")
    print(f"REPORT: {paths['report']}")
    print(f"POSITION TABLE: {paths['position']}")
    if status == "AUDIT_PASSED":
        print(f"FREEZE: {paths['freeze']}")

    if args.strict and critical:
        return 2
    return 0 if not critical else 1


if __name__ == "__main__":
    raise SystemExit(main())
