#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SPINE-GPE v7 — RAIS Formal Baseline Certifier v1.0.0

Certifica um núcleo administrativo de vínculos formais associados ao CBO 519110
(ou conjunto configurado), com universo primário de vínculos ativos em 31/12.

Características centrais
------------------------
- leitura em chunks de TXT/CSV/COMT e arquivos 7z;
- harmonização fail-closed de nomes antigos e novos, inclusive via de-para 2024;
- preservação da unidade administrativa: vínculo, não trabalhador único;
- outputs para Brasil, região, UF, município, Pernambuco e Recife;
- remuneração nominal e renda-hora nominal; valores reais apenas com deflator explícito;
- manifestos, hashes, quality gates, golden tests opcionais e freeze imutável;
- evidence tier D e teto de afirmação explícito: baseline formal registrado.
"""
from __future__ import annotations

import argparse
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
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional

import numpy as np
import pandas as pd

SCRIPT_VERSION = "1.0.0"
SCHEMA_VERSION = "spine-gpe-v7-rais-formal-certifier-1.0.0"
DEFAULT_YEARS = list(range(2017, 2025))
DEFAULT_PRIMARY_CBO = ["519110"]
READABLE_EXT = {".txt", ".csv", ".comt", ".7z", ".parquet"}
MONTHLY_HOURS_FACTOR = 4.345
RECIFE_MUN6 = "261160"
PE_UF_CODE = "26"

UF_CODE_TO_SIGLA = {
    "11":"RO","12":"AC","13":"AM","14":"RR","15":"PA","16":"AP","17":"TO",
    "21":"MA","22":"PI","23":"CE","24":"RN","25":"PB","26":"PE","27":"AL","28":"SE","29":"BA",
    "31":"MG","32":"ES","33":"RJ","35":"SP","41":"PR","42":"SC","43":"RS",
    "50":"MS","51":"MT","52":"GO","53":"DF",
}
REGION_BY_UF = {
    **{x:"Norte" for x in ["RO","AC","AM","RR","PA","AP","TO"]},
    **{x:"Nordeste" for x in ["MA","PI","CE","RN","PB","PE","AL","SE","BA"]},
    **{x:"Sudeste" for x in ["MG","ES","RJ","SP"]},
    **{x:"Sul" for x in ["PR","SC","RS"]},
    **{x:"Centro-Oeste" for x in ["MS","MT","GO","DF"]},
}

ALIASES: dict[str, list[str]] = {
    "year": ["ano", "ano base", "ano_base", "anobase"],
    "cbo2002": ["cbo ocupacao 2002", "cbo 2002 ocupacao", "cbo 2002", "cbo_ocupacao_2002", "cbo2002", "cbo"],
    "active_3112": ["vinculo ativo 31 12", "vinculo ativo em 31 12", "ind vinculo ativo 31 12", "indicador vinculo ativo 31 12", "vinculo_ativo_3112", "ativo 31 12", "ativo_3112"],
    "municipality_work": ["mun trab", "municipio trabalho", "municipio do trabalho", "municipio estabelecimento", "municipio", "mun_trab", "id municipio", "id_municipio"],
    "uf": ["uf", "sigla uf", "sigla_uf", "uf trab", "uf trabalho"],
    "income_avg_nominal": ["vl remun media nom", "valor remuneracao media nominal", "remuneracao media nominal", "vl_remun_media_nom", "valor_remuneracao_media", "valor remuneracao media"],
    "income_dec_nominal": ["vl remun dezembro nom", "valor remuneracao dezembro nominal", "remuneracao dezembro nominal", "vl_remun_dezembro_nom"],
    "income_avg_sm": ["vl remun media sm", "valor remuneracao media sm", "valor_remuneracao_media_sm", "vl_remun_media_sm"],
    "contract_hours": ["qtd hora contr", "quantidade horas contratuais", "horas contratuais", "horas_contratuais", "qtd_hora_contrat", "quantidade_horas_contratadas"],
    "sex": ["sexo trabalhador", "sexo", "genero"],
    "race": ["raca cor", "raca_cor", "cor raca", "raça cor"],
    "education": ["escolaridade apos 2005", "escolaridade", "grau instrucao apos 2005", "grau_instrucao_apos_2005", "grau instrucao"],
    "age": ["idade", "idade trabalhador"],
    "age_group": ["faixa etaria", "faixa_etaria"],
    "link_type": ["tipo vinculo", "tipo_vinculo", "tipo de vinculo"],
    "cnae_class": ["cnae 2 0 classe", "cnae 2.0 classe", "cnae_2_0_classe", "cnae classe", "cnae2 classe"],
    "admission_type": ["tipo admissao", "tipo_admissao"],
    "establishment_size": ["tamanho estabelecimento", "tamanho_estabelecimento"],
    "legal_nature": ["natureza juridica", "natureza_juridica"],
    "worker_id_hash": ["id trabalhador", "id_trabalhador", "identificador trabalhador", "pis trabalhador hash", "cpf trabalhador hash"],
}

CRITICAL_FIELDS = ["cbo2002", "active_3112", "municipality_work", "contract_hours"]
INCOME_FIELDS = ["income_avg_nominal", "income_dec_nominal", "income_avg_sm"]


@dataclass
class SourceAudit:
    source_path: str
    source_sha256: str
    source_extension: str
    source_size_bytes: int
    inferred_year: Optional[int]
    encoding: Optional[str]
    delimiter: Optional[str]
    header_columns: list[str]
    canonical_mapping: dict[str, str]
    rows_total: int = 0
    rows_active_all_occupations: int = 0
    rows_target_all: int = 0
    rows_target_active: int = 0
    status: str = "PENDING"
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
    years = sorted(set(int(x.strip()) for x in value.split(",") if x.strip()))
    if not years:
        raise argparse.ArgumentTypeError("Informe ao menos um ano.")
    return years


def parse_codes(value: str) -> list[str]:
    out = []
    for token in value.split(","):
        digits = re.sub(r"\D", "", token)
        if digits:
            out.append(digits.zfill(6)[-6:])
    if not out:
        raise argparse.ArgumentTypeError("Informe ao menos um CBO.")
    return sorted(set(out))


def normalize_text(value: Any) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    text = unicodedata.normalize("NFKD", str(value).strip())
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def canonical_header(value: Any) -> str:
    return normalize_text(value).replace(" ", "_")


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def json_dump(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def configure_logger(path: Path) -> logging.Logger:
    logger = logging.getLogger("rais_formal_certifier")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    path.parent.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(path, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


def infer_year(path: Path, allowed: Iterable[int]) -> Optional[int]:
    allowed_set = set(allowed)
    found = [int(x) for x in re.findall(r"(?:19|20)\d{2}", str(path))]
    found = [x for x in found if x in allowed_set]
    return found[-1] if found else None


def normalize_cbo(series: pd.Series) -> pd.Series:
    raw = series.astype(str).str.strip().str.replace(r"\.0+$", "", regex=True)
    digits = raw.str.replace(r"\D", "", regex=True)
    digits = digits.where(~((digits.str.len() == 7) & digits.str.endswith("0")), digits.str[:6])
    return digits.str.zfill(6).str[-6:]


def normalize_municipality(series: pd.Series) -> pd.Series:
    digits = series.astype(str).str.replace(r"\D", "", regex=True)
    # RAIS tradicional usa seis dígitos. Caso venha código IBGE de sete dígitos, remove dígito verificador.
    return digits.where(digits.str.len() != 7, digits.str[:6]).str.zfill(6).str[-6:]


def parse_numeric(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    # Formato brasileiro: 1.234,56. Se só houver ponto decimal, preserva.
    has_comma = s.str.contains(",", regex=False)
    s = s.where(~has_comma, s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False))
    s = s.replace({"": np.nan, "nan": np.nan, "None": np.nan, "-1": np.nan})
    return pd.to_numeric(s, errors="coerce")


def normalize_active(series: pd.Series) -> pd.Series:
    n = series.map(normalize_text)
    return n.isin({"1", "sim", "s", "true", "ativo", "yes"})


def detect_encoding_delimiter(path: Path) -> tuple[str, str]:
    raw = path.read_bytes()[:200000]
    candidates = ["utf-8-sig", "utf-8", "latin1", "cp1252"]
    best = None
    for enc in candidates:
        try:
            text = raw.decode(enc)
        except UnicodeDecodeError:
            continue
        first = text.splitlines()[0] if text.splitlines() else text
        counts = {sep: first.count(sep) for sep in [";", "\t", ",", "|"]}
        sep = max(counts, key=counts.get)
        score = counts[sep]
        if best is None or score > best[0]:
            best = (score, enc, sep)
    if best is None:
        return "latin1", ";"
    return best[1], best[2]


def read_depara(path: Optional[Path], logger: logging.Logger) -> dict[str, str]:
    """Retorna mapa normalized_new_name -> normalized_old_name."""
    if path is None or not path.exists():
        return {}
    out: dict[str, str] = {}
    try:
        sheets = pd.read_excel(path, sheet_name=None, dtype=str)
    except Exception as exc:
        logger.warning("De-para não lido: %s", exc)
        return out
    for _, df in sheets.items():
        if df.empty:
            continue
        cols = list(df.columns)
        old_col = None
        new_col = None
        for c in cols:
            n = normalize_text(c)
            if old_col is None and any(x in n for x in ["antigo", "anterior", "origem", "nome rais 2023", "variavel antiga"]):
                old_col = c
            if new_col is None and any(x in n for x in ["novo", "atual", "destino", "nome rais 2024", "variavel nova"]):
                new_col = c
        if old_col is None or new_col is None:
            # Fallback: primeiras duas colunas com muitos textos distintos.
            if len(cols) >= 2:
                old_col, new_col = cols[0], cols[1]
            else:
                continue
        for _, row in df[[old_col, new_col]].dropna(how="all").iterrows():
            old = normalize_text(row.get(old_col))
            new = normalize_text(row.get(new_col))
            if old and new:
                out[new] = old
    return out


def resolve_columns(headers: list[str], depara_new_to_old: dict[str, str]) -> dict[str, str]:
    norm_to_original = {normalize_text(h): h for h in headers}
    resolved: dict[str, str] = {}
    alias_norm = {k: [normalize_text(x) for x in vals] for k, vals in ALIASES.items()}
    for canonical, aliases in alias_norm.items():
        for n, original in norm_to_original.items():
            if n in aliases:
                resolved[canonical] = original
                break
        if canonical in resolved:
            continue
        # Nomenclatura 2024: converte novo->antigo e testa aliases antigos.
        for n, original in norm_to_original.items():
            old = depara_new_to_old.get(n)
            if old and old in aliases:
                resolved[canonical] = original
                break
        if canonical in resolved:
            continue
        # Compatibilidade controlada: alias contido no nome, exigindo >=5 caracteres.
        for n, original in norm_to_original.items():
            if any(len(a) >= 5 and (a in n or n in a) for a in aliases):
                resolved[canonical] = original
                break
    return resolved


def discover_sources(roots: list[Path], explicit: list[Path], years: list[int], logger: logging.Logger) -> list[Path]:
    candidates = []
    for p in explicit:
        if p.exists() and p.is_file() and p.suffix.lower() in READABLE_EXT:
            candidates.append(p.resolve())
    for root in roots:
        if not root.exists():
            logger.warning("RAIS root ausente: %s", root)
            continue
        for p in root.rglob("*"):
            if not (p.is_file() and p.suffix.lower() in READABLE_EXT):
                continue
            name_n = normalize_text(p.name)
            if any(token in name_n for token in ["estabelecimento", "estab", "dicionario", "de para", "layout", "leiaute", "manual", "readme"]):
                continue
            if infer_year(p, years) is not None or p.suffix.lower() == ".7z":
                candidates.append(p.resolve())
    unique = {}
    for p in candidates:
        try:
            unique[sha256_file(p)] = p
        except OSError:
            continue
    return sorted(unique.values())


def extract_7z(path: Path, dest_root: Path, logger: logging.Logger) -> list[Path]:
    try:
        import py7zr
    except Exception as exc:
        raise RuntimeError("py7zr é obrigatório para arquivos .7z") from exc
    h = sha256_file(path)
    dest = dest_root / h[:16]
    dest.mkdir(parents=True, exist_ok=True)
    marker = dest / ".complete"
    if not marker.exists():
        logger.info("Extraindo %s", path)
        with py7zr.SevenZipFile(path, mode="r") as z:
            z.extractall(dest)
        marker.write_text(utc_now(), encoding="utf-8")
    return [p for p in dest.rglob("*") if p.is_file() and p.suffix.lower() in {".txt", ".csv", ".comt", ".parquet"}]


def expand_sources(sources: list[Path], work_dir: Path, logger: logging.Logger) -> list[Path]:
    out = []
    seen = set()
    for p in sources:
        expanded = extract_7z(p, work_dir / "extracted_7z", logger) if p.suffix.lower() == ".7z" else [p]
        for q in expanded:
            h = sha256_file(q)
            if h not in seen:
                seen.add(h)
                out.append(q)
    return sorted(out)


def read_header(path: Path) -> tuple[list[str], Optional[str], Optional[str]]:
    if path.suffix.lower() == ".parquet":
        import pyarrow.parquet as pq
        return pq.ParquetFile(path).schema.names, None, None
    enc, sep = detect_encoding_delimiter(path)
    df = pd.read_csv(path, sep=sep, encoding=enc, nrows=0, dtype=str, engine="python")
    return list(df.columns), enc, sep


def iter_chunks(path: Path, usecols: list[str], encoding: Optional[str], delimiter: Optional[str], chunksize: int) -> Iterator[pd.DataFrame]:
    if path.suffix.lower() == ".parquet":
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(path)
        for batch in pf.iter_batches(batch_size=chunksize, columns=usecols):
            yield batch.to_pandas()
        return
    yield from pd.read_csv(
        path, sep=delimiter or ";", encoding=encoding or "latin1", dtype=str,
        usecols=usecols, chunksize=chunksize, low_memory=False, engine="c",
        on_bad_lines="warn",
    )


def load_deflator(path: Optional[Path], base_year: int) -> dict[int, float]:
    if path is None or not path.exists():
        return {}
    df = pd.read_csv(path)
    cols = {normalize_text(c): c for c in df.columns}
    year_col = cols.get("year") or cols.get("ano")
    factor_col = cols.get("factor to base") or cols.get("fator para base") or cols.get("factor_to_base")
    if year_col is None or factor_col is None:
        raise ValueError("Deflator deve conter year/ano e factor_to_base/fator para base.")
    out = {}
    for _, row in df.iterrows():
        y = int(row[year_col])
        f = float(row[factor_col])
        if f <= 0:
            raise ValueError(f"Fator inválido em {y}: {f}")
        out[y] = f
    if base_year in out and abs(out[base_year] - 1.0) > 1e-6:
        raise ValueError("O ano-base deve ter factor_to_base=1.")
    return out


def harmonize_target_chunk(chunk: pd.DataFrame, mapping: dict[str, str], file_year: Optional[int], target_codes: set[str],
                           sm_values: dict[int, float], deflator: dict[int, float], base_year: int) -> tuple[pd.DataFrame, dict[str, int]]:
    out = pd.DataFrame(index=chunk.index)
    for canonical, original in mapping.items():
        if original in chunk.columns:
            out[canonical] = chunk[original]
    if "year" in out:
        year_num = pd.to_numeric(out["year"], errors="coerce")
        out["year"] = year_num.fillna(file_year).astype("Int64")
    elif file_year is not None:
        out["year"] = file_year
    else:
        out["year"] = pd.Series(pd.NA, index=out.index, dtype="Int64")

    out["cbo2002"] = normalize_cbo(out["cbo2002"])
    active = normalize_active(out["active_3112"])
    rows_total = len(out)
    rows_active_all = int(active.sum())
    is_target = out["cbo2002"].isin(target_codes)
    target = out.loc[is_target].copy()
    target["active_3112"] = active.loc[is_target].astype(bool)
    target["municipality_work_raw"] = target["municipality_work"].astype(str)
    target["municipality6"] = normalize_municipality(target["municipality_work"])
    target["uf_code"] = target["municipality6"].str[:2]
    target["uf"] = target.get("uf", pd.Series(index=target.index, dtype=object)).astype(str).str.upper().str.strip()
    target["uf"] = target["uf"].where(target["uf"].isin(REGION_BY_UF), target["uf_code"].map(UF_CODE_TO_SIGLA))
    target["region"] = target["uf"].map(REGION_BY_UF)
    target["is_pe"] = target["uf"].eq("PE")
    target["is_recife"] = target["municipality6"].eq(RECIFE_MUN6)
    target["contract_hours"] = parse_numeric(target["contract_hours"])

    nominal = pd.Series(np.nan, index=target.index, dtype=float)
    income_source = pd.Series("", index=target.index, dtype=object)
    if "income_avg_nominal" in target:
        x = parse_numeric(target["income_avg_nominal"])
        nominal = nominal.fillna(x)
        income_source = income_source.where(x.isna(), "income_avg_nominal")
    if "income_dec_nominal" in target:
        x = parse_numeric(target["income_dec_nominal"])
        nominal = nominal.fillna(x)
        income_source = income_source.where(x.isna() | income_source.ne(""), "income_dec_nominal")
    if "income_avg_sm" in target:
        x = parse_numeric(target["income_avg_sm"])
        years = target["year"].astype("Int64")
        brl = pd.Series([xv * sm_values.get(int(y), np.nan) if pd.notna(xv) and pd.notna(y) else np.nan for xv, y in zip(x, years)], index=target.index)
        nominal = nominal.fillna(brl)
        income_source = income_source.where(brl.isna() | income_source.ne(""), "income_avg_sm_converted")
    target["income_monthly_nominal"] = nominal
    target["income_source"] = income_source
    valid_hours = target["contract_hours"].between(1, 100)
    target["income_hour_nominal"] = target["income_monthly_nominal"] / (target["contract_hours"] * MONTHLY_HOURS_FACTOR)
    target.loc[~valid_hours, "income_hour_nominal"] = np.nan
    if deflator:
        factors = target["year"].map(deflator)
        target[f"income_monthly_real_{base_year}"] = target["income_monthly_nominal"] * factors
        target[f"income_hour_real_{base_year}"] = target["income_hour_nominal"] * factors
    target["record_quality_valid_hours"] = valid_hours
    target["record_quality_positive_income"] = target["income_monthly_nominal"].gt(0)
    target["record_quality_valid_municipality"] = target["uf"].notna() & target["municipality6"].str.match(r"^\d{6}$")
    stats = {
        "rows_total": rows_total,
        "rows_active_all_occupations": rows_active_all,
        "rows_target_all": int(is_target.sum()),
        "rows_target_active": int(target["active_3112"].sum()),
    }
    return target.reset_index(drop=True), stats


def process_source(path: Path, year: Optional[int], depara: dict[str, str], target_codes: set[str], chunksize: int,
                   sm_values: dict[int, float], deflator: dict[int, float], base_year: int,
                   logger: logging.Logger) -> tuple[pd.DataFrame, SourceAudit, list[dict[str, Any]]]:
    headers, encoding, delimiter = read_header(path)
    mapping = resolve_columns(headers, depara)
    missing_critical = [x for x in CRITICAL_FIELDS if x not in mapping]
    if not any(x in mapping for x in INCOME_FIELDS):
        missing_critical.append("income_any")
    audit = SourceAudit(
        source_path=str(path), source_sha256=sha256_file(path), source_extension=path.suffix.lower(),
        source_size_bytes=path.stat().st_size, inferred_year=year, encoding=encoding, delimiter=delimiter,
        header_columns=headers, canonical_mapping=mapping,
    )
    failures = []
    if missing_critical:
        audit.status = "BLOCKED_SCHEMA"
        failures.append({
            "test_id": f"source.{audit.source_sha256[:12]}.critical_columns", "severity": "critical",
            "message": "Variáveis críticas RAIS não resolvidas.", "observed": missing_critical,
            "expected": CRITICAL_FIELDS + ["one income field"], "source": str(path),
        })
        return pd.DataFrame(), audit, failures
    usecols = sorted(set(mapping.values()))
    parts = []
    total_stats = {"rows_total":0, "rows_active_all_occupations":0, "rows_target_all":0, "rows_target_active":0}
    for idx, chunk in enumerate(iter_chunks(path, usecols, encoding, delimiter, chunksize), start=1):
        harmonized, stats = harmonize_target_chunk(chunk, mapping, year, target_codes, sm_values, deflator, base_year)
        for k, v in stats.items():
            total_stats[k] += int(v)
        if not harmonized.empty:
            harmonized["source_sha256"] = audit.source_sha256
            harmonized["source_file"] = path.name
            parts.append(harmonized)
        if idx % 20 == 0:
            logger.info("%s | chunks=%s | rows=%s | target=%s", path.name, idx, total_stats["rows_total"], total_stats["rows_target_all"])
    for k, v in total_stats.items():
        setattr(audit, k, v)
    audit.status = "PARSED" if total_stats["rows_target_all"] > 0 else "NO_TARGET_ROWS"
    # Uma partição regional pode legitimamente não conter o CBO-alvo; o gate é anual/consolidado.
    return (pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()), audit, failures


def load_simple_mapping(path: Optional[Path], key_candidates: list[str], value_candidates: list[str]) -> dict[int, float]:
    if path is None or not path.exists():
        return {}
    df = pd.read_csv(path)
    norm = {normalize_text(c): c for c in df.columns}
    key = next((norm.get(normalize_text(k)) for k in key_candidates if norm.get(normalize_text(k))), None)
    val = next((norm.get(normalize_text(v)) for v in value_candidates if norm.get(normalize_text(v))), None)
    if key is None or val is None:
        raise ValueError(f"Arquivo {path} não contém colunas esperadas.")
    return {int(r[key]): float(r[val]) for _, r in df.dropna(subset=[key, val]).iterrows()}


def weighted_quantile_unweighted(s: pd.Series, q: float) -> float:
    return float(s.quantile(q)) if s.notna().any() else np.nan


def summary_stats(df: pd.DataFrame, group_cols: list[str], base_year: int, has_real: bool) -> pd.DataFrame:
    rows = []
    for keys, sub in df.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = dict(zip(group_cols, keys))
        row.update({
            "n_links_active": int(len(sub)),
            "n_source_files": int(sub["source_sha256"].nunique()),
            "income_monthly_nominal_mean": float(sub["income_monthly_nominal"].mean()),
            "income_monthly_nominal_median": weighted_quantile_unweighted(sub["income_monthly_nominal"], .5),
            "income_hour_nominal_mean": float(sub["income_hour_nominal"].mean()),
            "income_hour_nominal_median": weighted_quantile_unweighted(sub["income_hour_nominal"], .5),
            "contract_hours_mean": float(sub["contract_hours"].mean()),
            "contract_hours_median": weighted_quantile_unweighted(sub["contract_hours"], .5),
            "missing_income_rate": float(sub["income_monthly_nominal"].isna().mean()),
            "missing_hours_rate": float(sub["contract_hours"].isna().mean()),
        })
        if has_real:
            row.update({
                f"income_monthly_real_{base_year}_mean": float(sub[f"income_monthly_real_{base_year}"].mean()),
                f"income_monthly_real_{base_year}_median": weighted_quantile_unweighted(sub[f"income_monthly_real_{base_year}"], .5),
                f"income_hour_real_{base_year}_mean": float(sub[f"income_hour_real_{base_year}"].mean()),
                f"income_hour_real_{base_year}_median": weighted_quantile_unweighted(sub[f"income_hour_real_{base_year}"], .5),
            })
        rows.append(row)
    return pd.DataFrame(rows)


def geography_summary(active: pd.DataFrame, base_year: int, has_real: bool) -> pd.DataFrame:
    outputs = []
    specs = [
        ("BRASIL", ["year"], active.assign(geography="Brasil"), ["year", "geography"]),
        ("REGION", ["year", "region"], active, ["year", "region"]),
        ("UF", ["year", "uf"], active, ["year", "uf"]),
        ("MUNICIPALITY", ["year", "uf", "municipality6"], active, ["year", "uf", "municipality6"]),
    ]
    for level, _, frame, groups in specs:
        s = summary_stats(frame, groups, base_year, has_real)
        s.insert(0, "geography_level", level)
        outputs.append(s)
    return pd.concat(outputs, ignore_index=True, sort=False)


def demographic_summary(active: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for dim in ["sex", "race", "education", "age_group", "link_type", "cnae_class"]:
        if dim not in active.columns:
            continue
        grouped = active.groupby(["year", dim], dropna=False)
        for (year, category), sub in grouped:
            total = len(active[active["year"] == year])
            rows.append({
                "year": int(year), "dimension": dim, "category": category,
                "n_links_active": int(len(sub)), "share_within_year": float(len(sub) / total if total else np.nan),
                "income_hour_nominal_mean": float(sub["income_hour_nominal"].mean()),
                "contract_hours_mean": float(sub["contract_hours"].mean()),
            })
    return pd.DataFrame(rows)


def quality_summary(all_target: pd.DataFrame, active: pd.DataFrame, audits: list[SourceAudit]) -> pd.DataFrame:
    rows = []
    years = sorted(set(int(y) for y in all_target["year"].dropna().unique())) if not all_target.empty else []
    for y in years:
        sub_all = all_target[all_target["year"] == y]
        sub = active[active["year"] == y]
        rows.append({
            "year": y,
            "n_target_links_all_year": int(len(sub_all)),
            "n_target_links_active_3112": int(len(sub)),
            "active_share_within_target": float(len(sub) / len(sub_all) if len(sub_all) else np.nan),
            "missing_municipality_rate_active": float(sub["municipality6"].isna().mean()) if len(sub) else np.nan,
            "missing_income_rate_active": float(sub["income_monthly_nominal"].isna().mean()) if len(sub) else np.nan,
            "nonpositive_income_rate_active": float((sub["income_monthly_nominal"] <= 0).mean()) if len(sub) else np.nan,
            "invalid_hours_rate_active": float((~sub["contract_hours"].between(1, 100)).mean()) if len(sub) else np.nan,
            "recife_links_active": int(sub["is_recife"].sum()) if len(sub) else 0,
            "pe_links_active": int(sub["is_pe"].sum()) if len(sub) else 0,
            "n_source_files": int(sub["source_sha256"].nunique()) if len(sub) else 0,
        })
    return pd.DataFrame(rows)


def evaluate_golden(golden_path: Optional[Path], active: pd.DataFrame, all_target: pd.DataFrame,
                    audits: list[SourceAudit]) -> tuple[pd.DataFrame, list[dict[str, Any]], list[dict[str, Any]]]:
    failures, warnings, rows = [], [], []
    if golden_path is None or not golden_path.exists():
        warnings.append({
            "test_id": "golden.external", "severity": "medium",
            "message": "Golden externo oficial não fornecido; certificação baseada em schema, universo e consistência interna.",
            "observed": None, "expected": "official golden CSV optional",
        })
        return pd.DataFrame(), failures, warnings
    g = pd.read_csv(golden_path)
    for _, r in g.iterrows():
        year = int(r["year"])
        metric = str(r["metric"])
        geo_type = str(r.get("geography_type", "BRASIL")).upper()
        geo_code = str(r.get("geography_code", "BRASIL"))
        expected = float(r["value"])
        tol = float(r.get("tolerance_relative", 0.01))
        if metric == "active_target_links":
            sub = active[active["year"] == year]
            if geo_type == "UF": sub = sub[sub["uf"] == geo_code]
            elif geo_type == "MUNICIPALITY": sub = sub[sub["municipality6"] == re.sub(r"\D", "", geo_code)[:6]]
            observed = float(len(sub))
        elif metric == "all_target_links":
            sub = all_target[all_target["year"] == year]
            if geo_type == "UF": sub = sub[sub["uf"] == geo_code]
            elif geo_type == "MUNICIPALITY": sub = sub[sub["municipality6"] == re.sub(r"\D", "", geo_code)[:6]]
            observed = float(len(sub))
        elif metric == "active_all_links":
            observed = float(sum(a.rows_active_all_occupations for a in audits if a.inferred_year == year))
        else:
            warnings.append({"test_id": f"golden.{year}.{metric}", "severity": "low", "message": "Métrica golden não suportada.", "observed": metric, "expected": "supported metric"})
            continue
        rel = (observed - expected) / expected if expected else np.nan
        passed = abs(rel) <= tol if expected else observed == expected
        rows.append({"year":year, "metric":metric, "geography_type":geo_type, "geography_code":geo_code,
                     "expected":expected, "observed":observed, "difference":observed-expected,
                     "difference_relative":rel, "tolerance_relative":tol, "passed":passed})
        if not passed:
            failures.append({
                "test_id": f"golden.{year}.{metric}.{geo_type}.{geo_code}", "severity": "critical",
                "message": "Golden oficial fora da tolerância.", "observed": observed,
                "expected": expected, "difference_relative": rel,
            })
    return pd.DataFrame(rows), failures, warnings


def write_report(path: Path, lock: dict[str, Any], quality: pd.DataFrame, geography: pd.DataFrame) -> None:
    lines = [
        "# RAIS Formal Baseline Certification Report",
        "",
        f"- Run ID: `{lock['run_id']}`",
        f"- Status: **{lock['status']}**",
        f"- Anos solicitados: {', '.join(map(str, lock['years_requested']))}",
        f"- Anos certificados: {', '.join(map(str, lock['years_certified']))}",
        f"- CBO primário: {', '.join(lock['primary_cbo_codes'])}",
        f"- Unidade: {lock['unit_of_analysis']}",
        "",
        "## Universo principal",
        "",
        "Vínculos formais do CBO-alvo marcados como ativos em 31/12. Registros anuais não são tratados como trabalhadores únicos.",
        "",
        "## Falhas críticas",
        "",
        "```json", json.dumps(lock.get("critical_failures", []), ensure_ascii=False, indent=2), "```",
        "",
        "## Advertências",
        "",
        "```json", json.dumps(lock.get("warnings", []), ensure_ascii=False, indent=2), "```",
        "",
        "## Qualidade por ano",
        "",
        quality.to_markdown(index=False) if not quality.empty else "Sem dados.",
        "",
        "## Teto de afirmação",
        "",
        lock["claim_ceiling"],
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Certificador do baseline formal RAIS.")
    p.add_argument("--root", type=Path, required=True)
    p.add_argument("--mode", choices=["audit", "full"], default="audit")
    p.add_argument("--run-id", type=safe_run_id, default=run_id_default())
    p.add_argument("--years", type=parse_years, default=DEFAULT_YEARS)
    p.add_argument("--rais-root", type=Path, action="append", default=[])
    p.add_argument("--source-file", type=Path, action="append", default=[])
    p.add_argument("--depara-2024", type=Path)
    p.add_argument("--primary-cbo", type=parse_codes, default=DEFAULT_PRIMARY_CBO)
    p.add_argument("--sensitivity-cbo", type=parse_codes, default=[])
    p.add_argument("--chunksize", type=int, default=300000)
    p.add_argument("--minimum-years", type=int, default=1)
    p.add_argument("--golden-csv", type=Path)
    p.add_argument("--deflator-csv", type=Path)
    p.add_argument("--real-base-year", type=int, default=2022)
    p.add_argument("--minimum-wage-csv", type=Path)
    p.add_argument("--strict", action="store_true")
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    root = args.root.resolve()
    table_dir = root / "05_outputs" / "tables" / "rais_formal_certification"
    model_dir = root / "05_outputs" / "models" / "rais_formal_certification"
    report_dir = root / "06_reports" / "rais_formal_certification"
    admin_dir = root / "00_admin"
    work_dir = root / "03_intermediate" / "rais_formal_certification" / args.run_id
    for d in [table_dir, model_dir, report_dir, admin_dir, work_dir]: d.mkdir(parents=True, exist_ok=True)
    logger = configure_logger(report_dir / f"rais_formal_certification_{args.run_id}.log")
    logger.info("RAIS Formal Certifier v%s | mode=%s | root=%s", SCRIPT_VERSION, args.mode, root)

    roots = [p.resolve() for p in args.rais_root]
    if not roots:
        roots = [p for p in [root/"01_raw"/"MTE"/"RAIS", root/"01_raw"/"mte"/"RAIS", root/"01_raw"/"RAIS", root/"02_raw"/"MTE"/"RAIS"] if p.exists()]
    sources = discover_sources(roots, [p.resolve() for p in args.source_file], args.years, logger)
    expanded = expand_sources(sources, work_dir, logger)
    depara = read_depara(args.depara_2024, logger)
    target_codes = set(args.primary_cbo) | set(args.sensitivity_cbo)
    deflator = load_deflator(args.deflator_csv, args.real_base_year)
    sm_values = load_simple_mapping(args.minimum_wage_csv, ["year","ano"], ["minimum wage","salario minimo","valor"])

    inventory_rows = []
    audits: list[SourceAudit] = []
    failures: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    target_parts = []
    seen_year_source_scope: dict[tuple[int, str], str] = {}

    for path in expanded:
        year = infer_year(path, args.years)
        if year is None:
            warnings.append({
                "test_id": f"source.{sha256_file(path)[:12]}.year", "severity": "high",
                "message": "Ano não inferido do caminho; fonte ignorada.", "observed": str(path), "expected": args.years,
            })
            continue
        inventory_rows.append({"path":str(path), "sha256":sha256_file(path), "year":year, "size_bytes":path.stat().st_size, "extension":path.suffix.lower()})
        try:
            df, audit, source_failures = process_source(path, year, depara, target_codes, args.chunksize, sm_values, deflator, args.real_base_year, logger)
            audits.append(audit)
            failures.extend(source_failures)
            if not df.empty:
                target_parts.append(df)
        except Exception as exc:
            failures.append({
                "test_id": f"source.{sha256_file(path)[:12]}.parse", "severity": "critical",
                "message": "Falha de leitura/processamento da fonte RAIS.", "observed": repr(exc), "expected": "successful parse", "source": str(path),
            })
            logger.exception("Falha ao processar %s", path)

    inventory = pd.DataFrame(inventory_rows)
    audit_df = pd.DataFrame([asdict(a) for a in audits])

    if args.mode == "audit":
        # Auditoria não exige carregar e consolidar uma série completa, mas registra disponibilidade/schema.
        status = "AUDIT_PASSED" if not failures else "AUDIT_BLOCKED"
        if 2024 in args.years and not depara:
            warnings.append({
                "test_id": "rais2024.depara", "severity": "high",
                "message": "De-para 2024 não foi fornecido; a RAIS 2024 mudou nomenclatura/formatação.",
                "observed": None, "expected": "--depara-2024",
            })
        paths = {
            "inventory": table_dir / f"rais_source_inventory_{args.run_id}.csv",
            "source_audit": table_dir / f"rais_source_schema_audit_{args.run_id}.csv",
        }
        inventory.to_csv(paths["inventory"], index=False)
        audit_df.to_csv(paths["source_audit"], index=False)
        lock = {
            "run_id": args.run_id, "script_version": SCRIPT_VERSION, "schema_version": SCHEMA_VERSION,
            "mode": "audit", "status": status, "critical_failures": failures, "warnings": warnings,
            "years_requested": args.years, "primary_cbo_codes": args.primary_cbo,
            "sensitivity_cbo_codes": args.sensitivity_cbo, "rais_roots": [str(p) for p in roots],
            "sources_discovered": len(sources), "files_expanded": len(expanded),
            "depara_2024": str(args.depara_2024) if args.depara_2024 else None,
            "artifacts": {k:str(v) for k,v in paths.items()},
            "artifact_hashes": {k:sha256_file(v) for k,v in paths.items()},
            "created_at_utc": utc_now(),
        }
        json_dump(admin_dir / "RAIS_FORMAL_AUDIT_LOCK.json", lock)
        print(json.dumps(lock, ensure_ascii=False, indent=2))
        return 0 if not failures else 2

    all_target = pd.concat(target_parts, ignore_index=True) if target_parts else pd.DataFrame()
    if all_target.empty:
        failures.append({"test_id":"rais.target_nonempty", "severity":"critical", "message":"Nenhum registro-alvo consolidado.", "observed":0, "expected":">0"})
        active = pd.DataFrame()
    else:
        all_target["year"] = pd.to_numeric(all_target["year"], errors="coerce").astype("Int64")
        all_target = all_target[all_target["year"].isin(args.years)].copy()
        active = all_target[all_target["active_3112"] == True].copy()  # noqa: E712

    years_certified = sorted(int(y) for y in active["year"].dropna().unique()) if not active.empty else []
    missing_years = sorted(set(args.years) - set(years_certified))
    if len(years_certified) < args.minimum_years:
        failures.append({
            "test_id":"rais.minimum_years", "severity":"critical", "message":"Número insuficiente de anos com vínculos ativos do CBO-alvo.",
            "observed":years_certified, "expected":f">={args.minimum_years} years",
        })
    if missing_years:
        warnings.append({
            "test_id":"rais.year_coverage", "severity":"high", "message":"Nem todos os anos solicitados foram certificados.",
            "observed":missing_years, "expected":args.years,
        })
    if 2024 in years_certified and not depara:
        warnings.append({
            "test_id":"rais2024.depara", "severity":"high", "message":"RAIS 2024 processada sem de-para oficial explícito; aliases resolveram schema, mas documentação está incompleta.",
            "observed":None, "expected":"--depara-2024",
        })
    if not deflator:
        warnings.append({
            "test_id":"rais.real_income", "severity":"medium", "message":"Deflator não fornecido; remuneração real não foi criada.",
            "observed":None, "expected":"--deflator-csv for real income",
        })

    quality = quality_summary(all_target, active, audits) if not all_target.empty else pd.DataFrame()
    if not quality.empty:
        for _, r in quality.iterrows():
            y = int(r["year"])
            if r["missing_income_rate_active"] > 0.10:
                failures.append({"test_id":f"quality.{y}.income_missing", "severity":"critical", "message":"Missing de renda acima de 10%.", "observed":float(r["missing_income_rate_active"]), "expected":"<=0.10"})
            if r["invalid_hours_rate_active"] > 0.05:
                failures.append({"test_id":f"quality.{y}.hours_invalid", "severity":"critical", "message":"Horas inválidas acima de 5%.", "observed":float(r["invalid_hours_rate_active"]), "expected":"<=0.05"})
            if r["n_target_links_active_3112"] <= 0:
                failures.append({"test_id":f"quality.{y}.active_nonempty", "severity":"critical", "message":"Sem vínculos ativos do CBO-alvo.", "observed":0, "expected":">0"})

    has_real = bool(deflator)
    geography = geography_summary(active, args.real_base_year, has_real) if not active.empty else pd.DataFrame()
    demographics = demographic_summary(active) if not active.empty else pd.DataFrame()
    special = pd.DataFrame()
    if not active.empty:
        frames = []
        for label, mask in [
            ("Brasil", pd.Series(True, index=active.index)),
            ("Nordeste", active["region"].eq("Nordeste")),
            ("Pernambuco", active["uf"].eq("PE")),
            ("Recife", active["is_recife"]),
        ]:
            s = summary_stats(active[mask].assign(special_geography=label), ["year","special_geography"], args.real_base_year, has_real)
            frames.append(s)
        special = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    golden_df, golden_fail, golden_warn = evaluate_golden(args.golden_csv, active, all_target, audits)
    failures.extend(golden_fail)
    warnings.extend(golden_warn)

    # Primary CBO flag; sensibilidades ficam no mesmo Parquet, mas tabelas centrais são CBO primário.
    if not all_target.empty:
        all_target["cbo_scope"] = np.where(all_target["cbo2002"].isin(set(args.primary_cbo)), "PRIMARY", "SENSITIVITY")
        active["cbo_scope"] = np.where(active["cbo2002"].isin(set(args.primary_cbo)), "PRIMARY", "SENSITIVITY")
        active_primary = active[active["cbo_scope"] == "PRIMARY"].copy()
    else:
        active_primary = active

    # Recalcula tabelas centrais apenas para CBO primário.
    if not active_primary.empty:
        geography = geography_summary(active_primary, args.real_base_year, has_real)
        demographics = demographic_summary(active_primary)
        frames = []
        for label, mask in [("Brasil", pd.Series(True,index=active_primary.index)), ("Nordeste",active_primary["region"].eq("Nordeste")), ("Pernambuco",active_primary["uf"].eq("PE")), ("Recife",active_primary["is_recife"])]:
            frames.append(summary_stats(active_primary[mask].assign(special_geography=label), ["year","special_geography"], args.real_base_year, has_real))
        special = pd.concat(frames, ignore_index=True)

    paths = {
        "inventory": table_dir / f"rais_source_inventory_{args.run_id}.csv",
        "source_audit": table_dir / f"rais_source_schema_audit_{args.run_id}.csv",
        "quality": table_dir / f"rais_formal_quality_profile_{args.run_id}.csv",
        "geography": table_dir / f"rais_formal_annual_geography_{args.run_id}.csv",
        "special_geographies": table_dir / f"rais_formal_brasil_regiao_pe_recife_{args.run_id}.csv",
        "demographics": table_dir / f"rais_formal_demographic_profile_{args.run_id}.csv",
        "golden": table_dir / f"rais_formal_golden_tests_{args.run_id}.csv",
        "target_all_parquet": table_dir / f"rais_formal_target_links_all_{args.run_id}.parquet",
        "active_primary_parquet": table_dir / f"rais_formal_active_primary_links_{args.run_id}.parquet",
        "report": report_dir / f"rais_formal_certification_report_{args.run_id}.md",
    }
    inventory.to_csv(paths["inventory"], index=False)
    audit_df.to_csv(paths["source_audit"], index=False)
    quality.to_csv(paths["quality"], index=False)
    geography.to_csv(paths["geography"], index=False)
    special.to_csv(paths["special_geographies"], index=False)
    demographics.to_csv(paths["demographics"], index=False)
    golden_df.to_csv(paths["golden"], index=False)
    if not all_target.empty:
        all_target.to_parquet(paths["target_all_parquet"], index=False)
    else:
        pd.DataFrame().to_parquet(paths["target_all_parquet"], index=False)
    if not active_primary.empty:
        active_primary.to_parquet(paths["active_primary_parquet"], index=False)
    else:
        pd.DataFrame().to_parquet(paths["active_primary_parquet"], index=False)

    status = "CORE_CERTIFIED" if not failures else "CERTIFICATION_BLOCKED"
    lock = {
        "run_id": args.run_id, "script_version": SCRIPT_VERSION, "schema_version": SCHEMA_VERSION,
        "mode":"full", "status":status, "critical_failures":failures, "warnings":warnings,
        "years_requested":args.years, "years_certified":years_certified,
        "primary_cbo_codes":args.primary_cbo, "sensitivity_cbo_codes":args.sensitivity_cbo,
        "unit_of_analysis":"formal_employment_link",
        "primary_universe":"target CBO links active on 31/12",
        "n_target_links_all":int(len(all_target)), "n_active_primary_links":int(len(active_primary)),
        "evidence_tier":"D", "platform_direct_observed":False,
        "deflator_csv":str(args.deflator_csv) if args.deflator_csv else None,
        "real_base_year":args.real_base_year if deflator else None,
        "depara_2024":str(args.depara_2024) if args.depara_2024 else None,
        "golden_csv":str(args.golden_csv) if args.golden_csv else None,
        "claim_ceiling":"Baseline administrativo de vínculos formais registrados no CBO-alvo; não representa informalidade, não identifica uso de plataforma e não equivale a trabalhadores únicos quando uma pessoa possui múltiplos vínculos.",
        "artifacts":{k:str(v) for k,v in paths.items()},
        "artifact_hashes":{},
        "created_at_utc":utc_now(),
    }
    write_report(paths["report"], lock, quality, geography)
    lock["artifact_hashes"] = {k:sha256_file(v) for k,v in paths.items() if v.exists()}
    hardening_lock = admin_dir / "RAIS_FORMAL_CERTIFICATION_LOCK.json"
    json_dump(hardening_lock, lock)
    if status == "CORE_CERTIFIED":
        freeze = {
            "freeze_id":args.run_id, "status":"FROZEN", "component":"RAIS_FORMAL_BASELINE",
            "certification_lock":str(hardening_lock), "certification_lock_sha256":sha256_file(hardening_lock),
            "active_primary_parquet":str(paths["active_primary_parquet"]),
            "active_primary_parquet_sha256":sha256_file(paths["active_primary_parquet"]),
            "special_geographies":str(paths["special_geographies"]),
            "special_geographies_sha256":sha256_file(paths["special_geographies"]),
            "read_only":True, "evidence_tier":"D", "platform_direct_observed":False,
            "claim_ceiling":lock["claim_ceiling"], "created_at_utc":utc_now(),
        }
        json_dump(admin_dir / "RAIS_FORMAL_CORE_FREEZE.json", freeze)
    logger.info("RAIS certification concluída | status=%s", status)
    print(json.dumps(lock, ensure_ascii=False, indent=2))
    return 0 if status == "CORE_CERTIFIED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
