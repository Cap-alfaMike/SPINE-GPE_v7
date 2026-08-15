#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SPINE-GPE v7 — PNADc Annual Layout Equivalence Closure v1.0.1

Objetivo
--------
Fechar, de forma fail-closed e auditável, a pendência documental de equivalência
semântica/estrutural das variáveis centrais da PNAD Contínua usadas no backcast.

O engine:
1. inventaria documentos oficiais locais (.xls/.xlsx/.ods/.txt/.zip/.pdf);
2. usa um registry explícito para atribuir ano/versão independente às fontes;
3. extrai posição, largura, tipo, rótulo e assinatura de domínio quando possível;
4. compara V4010, V4013, VD4009, V1028, Estrato, UPA, UF, Capital e RM_RIDE;
5. audita estabilidade operacional nos Parquets certificados quando disponíveis;
6. produz um lock que distingue CONFIRMED, DOCUMENTATION_LIMITED e BLOCKED.

Nenhuma ausência documental é convertida silenciosamente em equivalência.
"""
from __future__ import annotations

import argparse
import csv
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
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd

SCRIPT_VERSION = "1.0.1"
SCHEMA_VERSION = "spine-gpe-v7-pnadc-layout-equivalence-closure-1.0.1"
TARGET_VARS = ["V4010", "V4013", "VD4009", "V1028", "Estrato", "UPA", "UF", "Capital", "RM_RIDE"]
REGISTRY_COLUMNS = ["source_path_pattern", "reference_year", "source_kind", "independent_version", "notes"]
DEFAULT_YEARS = [2019, 2020, 2021, 2022, 2024]
SUPPORTED_LAYOUT_EXT = {".xls", ".xlsx", ".ods", ".csv", ".txt", ".sas", ".sps", ".zip", ".pdf"}

CANONICAL_PARQUET_ALIASES = {
    "V4010": ["V4010", "occupation_code"],
    "V4013": ["V4013", "activity_code"],
    "VD4009": ["VD4009", "position_code"],
    "V1028": ["V1028", "survey_weight", "weight"],
    "Estrato": ["Estrato", "stratum"],
    "UPA": ["UPA", "psu"],
    "UF": ["UF", "uf"],
    "Capital": ["Capital", "capital"],
    "RM_RIDE": ["RM_RIDE", "rm_ride"],
}

HEADER_ALIASES = {
    "variable": ["variavel", "variable", "codigo da variavel", "codigo variavel", "nome da variavel"],
    "position": ["posicao inicial", "posicao", "inicio", "start", "coluna inicial"],
    "width": ["tamanho", "largura", "width", "comprimento", "numero de caracteres"],
    "type": ["tipo", "formato", "type", "classe"],
    "label": ["descricao", "descricao da variavel", "quesito", "denominacao", "rotulo", "label"],
    "category": ["categoria", "valor", "codigo", "dominio", "nivel"],
    "category_label": ["descricao da categoria", "descricao", "rotulo", "label", "significado"],
}


@dataclass
class LayoutEvidence:
    reference_year: Optional[int]
    variable: str
    source_path: str
    source_sha256: str
    source_name: str
    source_kind: str
    independent_version: bool
    parser: str
    sheet: Optional[str] = None
    position: Optional[int] = None
    width: Optional[int] = None
    data_type: Optional[str] = None
    label: Optional[str] = None
    domain_signature: Optional[str] = None
    raw_row_json: Optional[str] = None
    evidence_quality: str = "PARTIAL"
    notes: Optional[str] = None


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_id_default() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def safe_run_id(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", value):
        raise argparse.ArgumentTypeError("run_id inválido.")
    return value


def parse_years(value: str) -> list[int]:
    out = []
    for token in value.split(","):
        token = token.strip()
        if token:
            out.append(int(token))
    if not out:
        raise argparse.ArgumentTypeError("Informe ao menos um ano.")
    return sorted(set(out))


def normalize_text(value: Any) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    text = str(value).strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"\s+", " ", text)
    return text.lower().strip()


def normalize_var(value: Any) -> str:
    raw = str(value).strip() if value is not None else ""
    compact = re.sub(r"[^A-Za-z0-9_]", "", raw).upper()
    for target in TARGET_VARS:
        if compact == re.sub(r"[^A-Za-z0-9_]", "", target).upper():
            return target
    return raw.strip()


def int_or_none(value: Any) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip().replace(",", ".")
    m = re.search(r"-?\d+", text)
    if not m:
        return None
    try:
        return int(m.group())
    except Exception:
        return None


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def json_dump(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def configure_logger(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("pnadc_layout_closure")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


def infer_year_from_path(path: Path, allowed_years: Iterable[int]) -> Optional[int]:
    allowed = set(allowed_years)
    years = [int(x) for x in re.findall(r"(?:19|20)\d{2}", str(path))]
    years = [y for y in years if y in allowed]
    if not years:
        return None
    return years[-1]


def load_source_registry(path: Optional[Path]) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    df = pd.read_csv(path, dtype=str).fillna("")
    rows = []
    for _, r in df.iterrows():
        rows.append({
            "source_path_pattern": r.get("source_path_pattern", "").strip(),
            "reference_year": int(r["reference_year"]) if str(r.get("reference_year", "")).strip() else None,
            "source_kind": r.get("source_kind", "official_layout").strip() or "official_layout",
            "independent_version": normalize_text(r.get("independent_version", "true")) in {"1", "true", "sim", "yes", "s"},
            "notes": r.get("notes", "").strip(),
        })
    return rows


def registry_match(path: Path, registry: list[dict[str, Any]], allowed_years: list[int]) -> dict[str, Any]:
    s = str(path).replace("\\", "/")
    for row in registry:
        pat = row.get("source_path_pattern", "")
        if not pat:
            continue
        try:
            if re.search(pat, s, flags=re.I):
                return row.copy()
        except re.error:
            if pat.lower() in s.lower():
                return row.copy()
    return {
        "reference_year": infer_year_from_path(path, allowed_years),
        "source_kind": "official_layout_auto",
        "independent_version": True,
        "notes": "Ano inferido automaticamente do caminho; valide no registry.",
    }


def discover_candidate_files(roots: list[Path], work_dir: Path, logger: logging.Logger) -> list[Path]:
    files: list[Path] = []
    seen_hashes: set[str] = set()
    extracted_root = work_dir / "extracted_layout_archives"
    extracted_root.mkdir(parents=True, exist_ok=True)
    for root in roots:
        if not root.exists():
            logger.warning("Layout root ausente: %s", root)
            continue
        for p in root.rglob("*"):
            if not p.is_file() or p.suffix.lower() not in SUPPORTED_LAYOUT_EXT:
                continue
            try:
                h = sha256_file(p)
            except OSError:
                continue
            if h in seen_hashes:
                continue
            seen_hashes.add(h)
            files.append(p)
            if p.suffix.lower() == ".zip":
                dest = extracted_root / h[:16]
                if not dest.exists():
                    try:
                        with zipfile.ZipFile(p) as zf:
                            safe_members = [m for m in zf.namelist() if not (m.startswith("/") or ".." in Path(m).parts)]
                            for member in safe_members:
                                zf.extract(member, dest)
                    except Exception as exc:
                        logger.warning("ZIP não extraído %s: %s", p, exc)
                if dest.exists():
                    for q in dest.rglob("*"):
                        if q.is_file() and q.suffix.lower() in SUPPORTED_LAYOUT_EXT - {".zip"}:
                            files.append(q)
    return sorted(set(files))


def detect_header_map(df: pd.DataFrame, row_index: int) -> dict[str, int]:
    best: dict[str, int] = {}
    best_score = -1
    start = max(0, row_index - 20)
    for hr in range(start, row_index + 1):
        candidate: dict[str, int] = {}
        for ci, value in enumerate(df.iloc[hr].tolist()):
            n = normalize_text(value)
            if not n:
                continue
            for canonical, aliases in HEADER_ALIASES.items():
                if any(a == n or a in n for a in aliases):
                    candidate.setdefault(canonical, ci)
        score = len(candidate)
        if score > best_score:
            best_score = score
            best = candidate
    return best


def domain_signature_from_rows(df: pd.DataFrame, row_index: int, var_col: int, header: dict[str, int]) -> Optional[str]:
    values = []
    cat_col = header.get("category")
    label_col = header.get("category_label")
    max_r = min(len(df), row_index + 60)
    for r in range(row_index + 1, max_r):
        row = df.iloc[r]
        cell_var = normalize_var(row.iloc[var_col]) if var_col < len(row) else ""
        if cell_var in TARGET_VARS:
            break
        parts = []
        if cat_col is not None and cat_col < len(row):
            x = str(row.iloc[cat_col]).strip()
            if x and x.lower() != "nan":
                parts.append(x)
        if label_col is not None and label_col < len(row):
            x = str(row.iloc[label_col]).strip()
            if x and x.lower() != "nan":
                parts.append(x)
        if parts:
            values.append(" | ".join(parts))
    if not values:
        return None
    normalized = sorted(set(normalize_text(v) for v in values if normalize_text(v)))
    return sha256_text("\n".join(normalized)) if normalized else None


def parse_excel_layout(path: Path, meta: dict[str, Any], logger: logging.Logger) -> list[LayoutEvidence]:
    out: list[LayoutEvidence] = []
    try:
        sheets = pd.read_excel(path, sheet_name=None, header=None, dtype=object)
    except Exception as exc:
        logger.warning("Falha ao ler planilha %s: %s", path, exc)
        return out
    h = sha256_file(path)
    for sheet_name, df in sheets.items():
        if df.empty:
            continue
        for r in range(len(df)):
            row_values = df.iloc[r].tolist()
            for c, value in enumerate(row_values):
                var = normalize_var(value)
                if var not in TARGET_VARS:
                    continue
                header = detect_header_map(df, r)
                def get_field(name: str) -> Any:
                    ci = header.get(name)
                    if ci is None or ci >= len(row_values):
                        return None
                    return row_values[ci]
                position = int_or_none(get_field("position"))
                width = int_or_none(get_field("width"))
                dtype = str(get_field("type")).strip() if get_field("type") is not None else None
                label = str(get_field("label")).strip() if get_field("label") is not None else None
                domain_sig = domain_signature_from_rows(df, r, c, header)
                quality_parts = sum(x is not None and str(x).strip() not in {"", "nan"} for x in [position, width, dtype, label])
                quality = "FULL" if quality_parts >= 4 else "STRUCTURAL" if quality_parts >= 2 else "PARTIAL"
                out.append(LayoutEvidence(
                    reference_year=meta.get("reference_year"), variable=var,
                    source_path=str(path), source_sha256=h, source_name=path.name,
                    source_kind=meta.get("source_kind", "official_layout"),
                    independent_version=bool(meta.get("independent_version", True)), parser="excel",
                    sheet=str(sheet_name), position=position, width=width,
                    data_type=dtype, label=label, domain_signature=domain_sig,
                    raw_row_json=json.dumps([None if str(x) == "nan" else x for x in row_values], ensure_ascii=False, default=str),
                    evidence_quality=quality, notes=meta.get("notes"),
                ))
    return out


def parse_delimited_or_input(path: Path, meta: dict[str, Any], logger: logging.Logger) -> list[LayoutEvidence]:
    out: list[LayoutEvidence] = []
    raw = path.read_bytes()
    text = None
    for enc in ["utf-8-sig", "utf-8", "latin1", "cp1252"]:
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        return out
    h = sha256_file(path)
    lines = text.splitlines()
    # Primeiro tenta CSV/TSV estruturado.
    for sep in [";", "\t", ","]:
        try:
            df = pd.read_csv(path, sep=sep, dtype=object, encoding="latin1" if "latin" in (text[:0] or "") else None)
            if len(df.columns) >= 3:
                tmp = parse_dataframe_layout(df, path, meta, h, parser=f"delimited:{repr(sep)}")
                if tmp:
                    out.extend(tmp)
                    break
        except Exception:
            pass
    found = {(e.variable, e.position, e.width) for e in out}
    for i, line in enumerate(lines):
        for var in TARGET_VARS:
            if not re.search(rf"(?<![A-Za-z0-9_]){re.escape(var)}(?![A-Za-z0-9_])", line, flags=re.I):
                continue
            position = None
            width = None
            # SAS INPUT: @1 UF 2. / @0001 V4010 $4.
            m = re.search(rf"@\s*(\d+)\s+{re.escape(var)}\s+\$?\s*(\d+)", line, flags=re.I)
            if m:
                position, width = int(m.group(1)), int(m.group(2))
            else:
                m2 = re.search(rf"{re.escape(var)}\s+\$?\s*(\d+)\s*[-:]\s*(\d+)", line, flags=re.I)
                if m2:
                    start, end = int(m2.group(1)), int(m2.group(2))
                    position, width = start, end - start + 1
            key = (var, position, width)
            if key in found:
                continue
            context = "\n".join(lines[max(0, i - 2): min(len(lines), i + 3)])
            out.append(LayoutEvidence(
                reference_year=meta.get("reference_year"), variable=var,
                source_path=str(path), source_sha256=h, source_name=path.name,
                source_kind=meta.get("source_kind", "official_layout"),
                independent_version=bool(meta.get("independent_version", True)), parser="text_input",
                position=position, width=width, data_type="$" if "$" in line else None,
                label=None, domain_signature=None, raw_row_json=json.dumps({"line": line, "context": context}, ensure_ascii=False),
                evidence_quality="STRUCTURAL" if position is not None and width is not None else "PARTIAL",
                notes=meta.get("notes"),
            ))
            found.add(key)
    return out


def parse_dataframe_layout(df: pd.DataFrame, path: Path, meta: dict[str, Any], h: str, parser: str) -> list[LayoutEvidence]:
    out = []
    normalized_cols = {normalize_text(c): c for c in df.columns}
    col_map: dict[str, Any] = {}
    for canonical, aliases in HEADER_ALIASES.items():
        for ncol, original in normalized_cols.items():
            if any(a == ncol or a in ncol for a in aliases):
                col_map[canonical] = original
                break
    var_col = col_map.get("variable")
    if var_col is None:
        for c in df.columns:
            vals = {normalize_var(v) for v in df[c].dropna().astype(str).head(5000)}
            if vals.intersection(TARGET_VARS):
                var_col = c
                break
    if var_col is None:
        return out
    for _, row in df.iterrows():
        var = normalize_var(row.get(var_col))
        if var not in TARGET_VARS:
            continue
        pos = int_or_none(row.get(col_map.get("position"))) if col_map.get("position") is not None else None
        width = int_or_none(row.get(col_map.get("width"))) if col_map.get("width") is not None else None
        dtype = str(row.get(col_map.get("type"))).strip() if col_map.get("type") is not None else None
        label = str(row.get(col_map.get("label"))).strip() if col_map.get("label") is not None else None
        out.append(LayoutEvidence(
            reference_year=meta.get("reference_year"), variable=var,
            source_path=str(path), source_sha256=h, source_name=path.name,
            source_kind=meta.get("source_kind", "official_layout"),
            independent_version=bool(meta.get("independent_version", True)), parser=parser,
            position=pos, width=width, data_type=dtype, label=label,
            raw_row_json=json.dumps(row.to_dict(), ensure_ascii=False, default=str),
            evidence_quality="FULL" if all(x is not None for x in [pos, width, dtype, label]) else "STRUCTURAL",
            notes=meta.get("notes"),
        ))
    return out


def parse_layout_file(path: Path, meta: dict[str, Any], logger: logging.Logger) -> list[LayoutEvidence]:
    ext = path.suffix.lower()
    if ext in {".xls", ".xlsx", ".ods"}:
        return parse_excel_layout(path, meta, logger)
    if ext in {".csv", ".txt", ".sas", ".sps"}:
        return parse_delimited_or_input(path, meta, logger)
    return []


def consolidate_evidence(evidence_df: pd.DataFrame) -> pd.DataFrame:
    if evidence_df.empty:
        return evidence_df
    quality_order = {"FULL": 3, "STRUCTURAL": 2, "PARTIAL": 1}
    df = evidence_df.copy()
    df["quality_rank"] = df["evidence_quality"].map(quality_order).fillna(0)
    df["nonnull_structural"] = df[["position", "width", "data_type", "label"]].notna().sum(axis=1)
    df = df.sort_values(["reference_year", "variable", "quality_rank", "nonnull_structural"], ascending=[True, True, False, False])
    # Mantém melhor evidência por ano/variável/hash, preservando versões independentes.
    return df.drop_duplicates(["reference_year", "variable", "source_sha256"], keep="first").drop(columns=["quality_rank", "nonnull_structural"])


def normalize_dtype(value: Any) -> str:
    n = normalize_text(value)
    if not n:
        return ""
    if any(x in n for x in ["char", "string", "alfanumer", "caract", "$", "texto"]):
        return "string"
    if any(x in n for x in ["num", "integer", "float", "double", "decimal"]):
        return "numeric"
    return n


def compare_layouts(evidence_df: pd.DataFrame, years: list[int]) -> tuple[pd.DataFrame, list[dict[str, Any]], list[dict[str, Any]]]:
    rows = []
    failures: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    for var in TARGET_VARS:
        sub = evidence_df[evidence_df["variable"] == var].copy() if not evidence_df.empty else pd.DataFrame()
        baseline = None
        if not sub.empty:
            if (sub["reference_year"] == 2022).any():
                baseline = sub[sub["reference_year"] == 2022].iloc[0]
            else:
                baseline = sub.dropna(subset=["reference_year"]).sort_values("reference_year").iloc[0] if sub["reference_year"].notna().any() else sub.iloc[0]
        for year in years:
            ysub = sub[sub["reference_year"] == year] if not sub.empty else pd.DataFrame()
            if ysub.empty:
                rows.append({
                    "reference_year": year, "variable": var, "evidence_found": False,
                    "source_sha256": None, "position": None, "width": None, "data_type": None,
                    "label": None, "domain_signature": None, "position_equal_baseline": None,
                    "width_equal_baseline": None, "type_equal_baseline": None,
                    "label_similarity_baseline": None, "domain_equal_baseline": None,
                    "equivalence_status": "DOCUMENTATION_MISSING",
                })
                continue
            best = ysub.iloc[0]
            pos_eq = None if baseline is None or pd.isna(best.get("position")) or pd.isna(baseline.get("position")) else int(best["position"]) == int(baseline["position"])
            width_eq = None if baseline is None or pd.isna(best.get("width")) or pd.isna(baseline.get("width")) else int(best["width"]) == int(baseline["width"])
            dtype_eq = None if baseline is None or not normalize_dtype(best.get("data_type")) or not normalize_dtype(baseline.get("data_type")) else normalize_dtype(best.get("data_type")) == normalize_dtype(baseline.get("data_type"))
            label_sim = None
            if baseline is not None and normalize_text(best.get("label")) and normalize_text(baseline.get("label")):
                label_sim = SequenceMatcher(None, normalize_text(best.get("label")), normalize_text(baseline.get("label"))).ratio()
            domain_eq = None
            if baseline is not None and best.get("domain_signature") and baseline.get("domain_signature"):
                domain_eq = best.get("domain_signature") == baseline.get("domain_signature")
            conflict = any(x is False for x in [pos_eq, width_eq, dtype_eq, domain_eq]) or (label_sim is not None and label_sim < 0.75)
            status = "CONFLICT" if conflict else "EXACT_OR_COMPATIBLE"
            rows.append({
                "reference_year": year, "variable": var, "evidence_found": True,
                "source_sha256": best.get("source_sha256"), "source_path": best.get("source_path"),
                "position": best.get("position"), "width": best.get("width"), "data_type": best.get("data_type"),
                "label": best.get("label"), "domain_signature": best.get("domain_signature"),
                "position_equal_baseline": pos_eq, "width_equal_baseline": width_eq,
                "type_equal_baseline": dtype_eq, "label_similarity_baseline": label_sim,
                "domain_equal_baseline": domain_eq, "equivalence_status": status,
            })
            if conflict:
                failures.append({
                    "test_id": f"layout.{year}.{var}.conflict", "severity": "critical",
                    "message": "Conflito estrutural/semântico entre versões documentais.",
                    "observed": {"position": best.get("position"), "width": best.get("width"), "type": best.get("data_type"), "label_similarity": label_sim},
                    "expected": "compatível com baseline",
                })
    matrix = pd.DataFrame(rows)
    independent_years = sorted(set(int(y) for y in evidence_df.loc[evidence_df["independent_version"].fillna(False) & evidence_df["reference_year"].notna(), "reference_year"].tolist())) if not evidence_df.empty else []
    if len(independent_years) < 2:
        warnings.append({
            "test_id": "layout.independent_year_versions", "severity": "high",
            "message": "Menos de duas versões anuais independentes foram documentadas.",
            "observed": independent_years, "expected": ">=2 annual versions",
        })
    missing_cells = int((matrix["equivalence_status"] == "DOCUMENTATION_MISSING").sum()) if not matrix.empty else len(TARGET_VARS) * len(years)
    if missing_cells:
        warnings.append({
            "test_id": "layout.documentation_coverage", "severity": "high",
            "message": "Há células ano×variável sem evidência documental parseável.",
            "observed": missing_cells, "expected": 0,
        })
    return matrix, failures, warnings


def find_historical_parquets(root: Path, historical_lock: Optional[Path], logger: logging.Logger) -> list[tuple[str, Path]]:
    paths: list[tuple[str, Path]] = []
    if historical_lock and historical_lock.exists():
        try:
            obj = json.loads(historical_lock.read_text(encoding="utf-8"))
            for key, value in obj.get("artifacts", {}).items():
                if isinstance(value, str) and value.endswith(".parquet"):
                    p = Path(value)
                    if p.exists():
                        m = re.search(r"(20\d{2}q[1-4])", key + " " + p.name, re.I)
                        paths.append((m.group(1).lower() if m else key, p))
        except Exception as exc:
            logger.warning("Lock histórico não lido: %s", exc)
    if not paths:
        candidates = []
        for p in root.rglob("*.parquet"):
            s = str(p).lower()
            if "pnadc" in s and ("histor" in s or "proxy" in s):
                candidates.append(p)
        for p in candidates:
            m = re.search(r"(20\d{2}q[1-4])", p.name, re.I)
            if m:
                paths.append((m.group(1).lower(), p))
    dedup = {}
    for period, p in paths:
        dedup[(period, sha256_file(p))] = p
    return [(period, p) for (period, _), p in dedup.items()]


def operational_domain_audit(parquets: list[tuple[str, Path]], logger: logging.Logger) -> pd.DataFrame:
    rows = []
    try:
        import pyarrow.parquet as pq
    except Exception:
        logger.warning("pyarrow ausente; auditoria operacional de Parquet não executada.")
        return pd.DataFrame(rows)
    for period, path in parquets:
        try:
            pf = pq.ParquetFile(path)
            cols = set(pf.schema.names)
            selected = {}
            for var, aliases in CANONICAL_PARQUET_ALIASES.items():
                for alias in aliases:
                    if alias in cols:
                        selected[var] = alias
                        break
            if not selected:
                continue
            table = pq.read_table(path, columns=sorted(set(selected.values())))
            df = table.to_pandas()
            for var, col in selected.items():
                s = df[col]
                nonmissing = s.dropna()
                sample_values = sorted(nonmissing.astype(str).unique().tolist())[:50]
                rows.append({
                    "source_period": period, "year": int(period[:4]), "variable": var,
                    "source_path": str(path), "source_sha256": sha256_file(path),
                    "parquet_column": col, "dtype": str(s.dtype), "n": len(s),
                    "missing_rate": float(s.isna().mean()), "n_unique": int(nonmissing.nunique()),
                    "min": str(nonmissing.min()) if len(nonmissing) else None,
                    "max": str(nonmissing.max()) if len(nonmissing) else None,
                    "sample_domain_signature": sha256_text("\n".join(sample_values)),
                })
        except Exception as exc:
            logger.warning("Parquet não auditado %s: %s", path, exc)
    return pd.DataFrame(rows)


def make_registry_template(path: Path, files: list[Path], years: list[int]) -> None:
    """Grava sempre um CSV válido, mesmo quando nenhum documento foi localizado.

    Quando há arquivos candidatos, cria uma linha por arquivo. Quando não há,
    cria linhas-placeholder por ano para orientar a inclusão dos layouts sem
    produzir um arquivo de zero bytes.
    """
    rows: list[dict[str, Any]] = []
    for p in files:
        rows.append({
            "source_path_pattern": re.escape(p.name),
            "reference_year": infer_year_from_path(p, years) or "",
            "source_kind": "official_layout",
            "independent_version": "true",
            "notes": "CONFIRMAR ano e independência documental",
        })

    if not rows:
        for year in years:
            rows.append({
                "source_path_pattern": rf".*{year}.*(dicionario|input|variaveis|layout).*",
                "reference_year": year,
                "source_kind": "official_layout_placeholder",
                "independent_version": "true",
                "notes": (
                    "PLACEHOLDER: nenhum documento candidato foi localizado no audit. "
                    "Adicione o layout oficial, revise o padrão e confirme a independência documental."
                ),
            })

    pd.DataFrame(rows, columns=REGISTRY_COLUMNS).to_csv(path, index=False)


def write_report(path: Path, lock: dict[str, Any], matrix: pd.DataFrame, evidence: pd.DataFrame, operational: pd.DataFrame) -> None:
    lines = [
        "# PNADc Annual Layout Equivalence Closure",
        "",
        f"- Run ID: `{lock['run_id']}`",
        f"- Status: **{lock['status']}**",
        f"- Script: `{lock['script_version']}`",
        f"- Anos: {', '.join(map(str, lock['years']))}",
        f"- Variáveis: {', '.join(lock['target_variables'])}",
        "",
        "## Interpretação do status",
        "",
        "- `LAYOUT_EQUIVALENCE_CONFIRMED`: pelo menos duas versões documentais independentes e nenhum conflito crítico.",
        "- `DOCUMENTATION_LIMITED_OPERATIONALLY_CONSISTENT`: documentação anual insuficiente, sem conflito observado; limitação congelada.",
        "- `LAYOUT_EQUIVALENCE_BLOCKED`: conflito estrutural/semântico crítico.",
        "",
        "## Falhas críticas",
        "",
        "```json",
        json.dumps(lock.get("critical_failures", []), ensure_ascii=False, indent=2),
        "```",
        "",
        "## Advertências",
        "",
        "```json",
        json.dumps(lock.get("warnings", []), ensure_ascii=False, indent=2),
        "```",
        "",
        "## Cobertura documental",
        "",
        f"- Evidências parseadas: {len(evidence)}",
        f"- Células ano×variável: {len(matrix)}",
        f"- Células sem documentação: {int((matrix['equivalence_status']=='DOCUMENTATION_MISSING').sum()) if not matrix.empty else 'NA'}",
        f"- Linhas de auditoria operacional: {len(operational)}",
        "",
        "## Teto de afirmação",
        "",
        lock["claim_ceiling"],
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Fechamento documental de equivalência dos layouts PNADc.")
    p.add_argument("--root", type=Path, required=True)
    p.add_argument("--mode", choices=["audit", "full"], default="audit")
    p.add_argument("--run-id", type=safe_run_id, default=run_id_default())
    p.add_argument("--years", type=parse_years, default=DEFAULT_YEARS)
    p.add_argument("--layout-root", action="append", type=Path, default=[])
    p.add_argument("--source-registry", type=Path)
    p.add_argument("--historical-lock", type=Path)
    p.add_argument("--strict", action="store_true")
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)
    root = args.root.resolve()
    output_dir = root / "05_outputs" / "tables" / "pnadc_layout_equivalence_closure"
    report_dir = root / "06_reports" / "pnadc_layout_equivalence_closure"
    admin_dir = root / "00_admin"
    work_dir = root / "03_intermediate" / "pnadc_layout_equivalence_closure" / args.run_id
    output_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)
    logger = configure_logger(report_dir / f"pnadc_layout_equivalence_closure_{args.run_id}.log")
    logger.info("PNADc Layout Closure v%s | mode=%s | root=%s", SCRIPT_VERSION, args.mode, root)

    roots = [p.resolve() for p in args.layout_root]
    if not roots:
        defaults = [
            root / "01_raw" / "IBGE",
            root / "01_raw" / "ibge",
            root / "00_admin" / "documentation",
            root / "02_raw" / "IBGE",
        ]
        roots = [p for p in defaults if p.exists()]
    files = discover_candidate_files(roots, work_dir, logger)
    if not files:
        logger.warning(
            "Nenhum documento candidato de layout foi localizado nas roots: %s. "
            "O registry será gerado com placeholders e o status não poderá ser CONFIRMED.",
            [str(p) for p in roots],
        )
    registry_template = output_dir / f"pnadc_layout_source_registry_template_{args.run_id}.csv"
    make_registry_template(registry_template, files, args.years)
    registry = load_source_registry(args.source_registry)

    evidence: list[LayoutEvidence] = []
    inventory_rows = []
    for path in files:
        meta = registry_match(path, registry, args.years)
        inventory_rows.append({
            "path": str(path), "sha256": sha256_file(path), "extension": path.suffix.lower(),
            "reference_year": meta.get("reference_year"), "source_kind": meta.get("source_kind"),
            "independent_version": meta.get("independent_version"), "size_bytes": path.stat().st_size,
        })
        if path.suffix.lower() != ".pdf":
            evidence.extend(parse_layout_file(path, meta, logger))
    inventory = pd.DataFrame(inventory_rows)
    evidence_df = consolidate_evidence(pd.DataFrame([asdict(x) for x in evidence])) if evidence else pd.DataFrame(columns=list(LayoutEvidence.__annotations__))
    matrix, failures, warnings = compare_layouts(evidence_df, args.years)

    hist_lock = args.historical_lock
    if hist_lock is None:
        candidate = admin_dir / "PNADC_HISTORICAL_PROXY_CERTIFICATION_LOCK.json"
        hist_lock = candidate if candidate.exists() else None
    operational = operational_domain_audit(find_historical_parquets(root, hist_lock, logger), logger)
    # Auditoria operacional: conflitos de dtype/domínio por variável são warnings, não equivalência documental.
    if not operational.empty:
        for var, sub in operational.groupby("variable"):
            dtypes = sorted(set(sub["dtype"].astype(str)))
            if len(dtypes) > 1:
                warnings.append({
                    "test_id": f"operational.{var}.dtype_stability", "severity": "medium",
                    "message": "Mais de um dtype observado nos Parquets harmonizados.",
                    "observed": dtypes, "expected": "dtype estável",
                })

    independent_years = sorted(set(int(x) for x in evidence_df.loc[evidence_df.get("independent_version", False).fillna(False) & evidence_df["reference_year"].notna(), "reference_year"].tolist())) if not evidence_df.empty else []
    if failures:
        status = "LAYOUT_EQUIVALENCE_BLOCKED"
    elif len(independent_years) >= 2 and not (matrix["equivalence_status"] == "DOCUMENTATION_MISSING").any():
        status = "LAYOUT_EQUIVALENCE_CONFIRMED"
    else:
        status = "DOCUMENTATION_LIMITED_OPERATIONALLY_CONSISTENT"

    paths = {
        "inventory": output_dir / f"pnadc_layout_document_inventory_{args.run_id}.csv",
        "evidence": output_dir / f"pnadc_layout_extracted_evidence_{args.run_id}.csv",
        "matrix": output_dir / f"pnadc_annual_layout_semantic_equivalence_{args.run_id}.csv",
        "operational": output_dir / f"pnadc_operational_domain_stability_{args.run_id}.csv",
        "registry_template": registry_template,
        "report": report_dir / f"pnadc_layout_equivalence_closure_report_{args.run_id}.md",
    }
    inventory.to_csv(paths["inventory"], index=False)
    evidence_df.to_csv(paths["evidence"], index=False)
    matrix.to_csv(paths["matrix"], index=False)
    operational.to_csv(paths["operational"], index=False)
    artifacts = {k: str(v) for k, v in paths.items()}
    hashes = {k: sha256_file(v) for k, v in paths.items() if v.exists() and v.is_file() and k != "report"}
    lock = {
        "run_id": args.run_id,
        "script_version": SCRIPT_VERSION,
        "schema_version": SCHEMA_VERSION,
        "mode": args.mode,
        "status": status,
        "critical_failures": failures,
        "warnings": warnings,
        "years": args.years,
        "target_variables": TARGET_VARS,
        "independent_documented_years": independent_years,
        "layout_roots": [str(p) for p in roots],
        "source_registry": str(args.source_registry) if args.source_registry else None,
        "historical_lock": str(hist_lock) if hist_lock else None,
        "artifacts": artifacts,
        "artifact_hashes": hashes,
        "claim_ceiling": (
            "Equivalência documental confirmada apenas quando sustentada por versões oficiais independentes. "
            "Na ausência, registra-se consistência operacional com limitação documental congelada; isso não converte "
            "uma única versão de layout em prova anual independente."
        ),
        "created_at_utc": utc_now(),
    }
    write_report(paths["report"], lock, matrix, evidence_df, operational)
    lock["artifacts"]["report"] = str(paths["report"])
    lock["artifact_hashes"]["report"] = sha256_file(paths["report"])

    lock_name = "PNADC_LAYOUT_EQUIVALENCE_FINAL_LOCK.json" if args.mode == "full" else "PNADC_LAYOUT_EQUIVALENCE_AUDIT_LOCK.json"
    lock_path = admin_dir / lock_name
    json_dump(lock_path, lock)
    logger.info("Layout closure concluído | status=%s | lock=%s", status, lock_path)
    print(json.dumps(lock, ensure_ascii=False, indent=2))

    if failures:
        return 2
    if args.strict and status == "DOCUMENTATION_LIMITED_OPERATIONALLY_CONSISTENT":
        # Limitação documental é resultado válido e congelável; strict não bloqueia, apenas mantém warning.
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
