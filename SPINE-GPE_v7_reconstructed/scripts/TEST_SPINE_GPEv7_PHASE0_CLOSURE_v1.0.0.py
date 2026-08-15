#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import json
import sys
import importlib.util
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
ENGINE = HERE / "SPINE_GPEv7_PHASE0_CLOSURE_v1.0.0.py"
CODEBOOK = HERE / "rais_editorial_codebook_ptbr_v1.0.0.csv"

# Fallback de teste quando pyarrow/fastparquet não estão disponíveis no ambiente
# de validação do pacote. No Colab, o requirements instala pyarrow e o engine usa
# Parquet real. Aqui o mesmo caminho é serializado por pickle para validar lógica.
_orig_to_parquet = pd.DataFrame.to_parquet
_orig_read_parquet = pd.read_parquet
pd.DataFrame.to_parquet = lambda self, path, index=False, **kwargs: self.to_pickle(path)
pd.read_parquet = lambda path, **kwargs: pd.read_pickle(path)

spec = importlib.util.spec_from_file_location("phase0_engine", ENGINE)
module = importlib.util.module_from_spec(spec)
assert spec and spec.loader
sys.modules[spec.name] = module
spec.loader.exec_module(module)


def sha256_file(path: Path) -> str:
    import hashlib
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def make_root(root: Path, unmapped_race: bool = False) -> None:
    admin = root / "00_admin"
    tables = root / "05_outputs" / "tables" / "rais_formal_certification"
    admin.mkdir(parents=True, exist_ok=True)
    tables.mkdir(parents=True, exist_ok=True)

    n = 100
    active = pd.DataFrame({
        "cbo2002": ["519110"] * n,
        "active_3112": [True] * n,
        "municipality_work": [261160] * 20 + [355030] * 80,
        "municipality6": ["261160"] * 20 + ["355030"] * 80,
        "uf_code": [26] * 20 + [35] * 80,
        "uf": ["PE"] * 20 + ["SP"] * 80,
        "region": ["Nordeste"] * 20 + ["Sudeste"] * 80,
        "is_pe": [True] * 20 + [False] * 80,
        "is_recife": [True] * 10 + [False] * 90,
        "income_monthly_nominal": [1800.0] * 10 + [0.0] * 10 + [1800.0] * 80,
        "income_hour_nominal": [1800.0 / (44 * 4.345)] * 10 + [0.0] * 10 + [1800.0 / (44 * 4.345)] * 80,
        "contract_hours": [44.0] * 99 + [0.0],
        "sex": [1] * 95 + [2] * 5,
        "race": ([7] if unmapped_race else [8]) + [2] * 49 + [4] * 50,
        "education": [7] * 80 + [9] * 20,
        "age": list(range(18, 68)) * 2,
        "age_group": [4] * n,
        "link_type": [10] * n,
        "cnae_class": [53202] * 90 + [49302] * 10,
        "source_sha256": ["a"] * 50 + ["b"] * 50,
        "source_file": ["one.txt"] * 50 + ["two.txt"] * 50,
        "cbo_scope": ["PRIMARY"] * n,
        "year": [2022] * n,
    })
    active_path = tables / "active.parquet"
    active.to_parquet(active_path, index=False)
    special_path = tables / "special.csv"
    pd.DataFrame({"year": [2022], "special_geography": ["Brasil"], "n_links_active": [n]}).to_csv(special_path, index=False)

    rais_lock = {
        "run_id": "rais_test",
        "status": "CORE_CERTIFIED",
        "critical_failures": [],
        "years_certified": [2022],
        "primary_cbo_codes": ["519110"],
        "n_target_links_all": 120,
        "n_active_primary_links": n,
        "evidence_tier": "D",
        "platform_direct_observed": False,
        "artifacts": {
            "active_primary_parquet": str(active_path),
            "special_geographies": str(special_path),
        },
        "artifact_hashes": {
            "active_primary_parquet": sha256_file(active_path),
            "special_geographies": sha256_file(special_path),
        },
    }
    write_json(admin / "RAIS_FORMAL_CERTIFICATION_LOCK.json", rais_lock)
    write_json(admin / "RAIS_FORMAL_CORE_FREEZE.json", {
        "status": "FROZEN", "read_only": True,
        "active_primary_parquet": str(active_path),
        "active_primary_parquet_sha256": sha256_file(active_path),
    })

    write_json(admin / "PNADC_CERTIFICATION_LOCK.json", {"status": "CORE_CERTIFIED", "critical_failures": []})
    write_json(admin / "PNAD_COVID_CERTIFICATION_LOCK.json", {"status": "CORE_CERTIFIED", "critical_failures": []})
    write_json(admin / "PNAD_COVID_CORE_FREEZE.json", {"status": "FROZEN", "read_only": True})
    write_json(admin / "PNADC_HISTORICAL_BACKCAST_FINAL_LOCK.json", {"status": "FINAL_CERTIFIED", "critical_failures": []})
    write_json(admin / "PNADC_LAYOUT_EQUIVALENCE_FINAL_LOCK.json", {"status": "DOCUMENTATION_LIMITED_OPERATIONALLY_CONSISTENT", "critical_failures": []})
    write_json(admin / "PNADC_LAYOUT_CLOSURE_ADJUDICATION.json", {
        "status": "DOCUMENTATION_LIMITED_OPERATIONALLY_CONSISTENT",
        "design_variables_complete_in_parquet_schema": True,
    })


def run_engine(root: Path) -> int:
    return module.main([
        "--root", str(root),
        "--mode", "full",
        "--stage", "all",
        "--run-id", "synthetic_v100",
        "--codebook", str(CODEBOOK),
        "--strict",
    ])


def test_success() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "SPINE-GPEv7"
        make_root(root)
        result = run_engine(root)
        assert result == 0
        admin = root / "00_admin"
        adjudication = json.loads((admin / "RAIS_SUBSTANTIVE_ADJUDICATION_LOCK.json").read_text())
        editorial = json.loads((admin / "RAIS_EDITORIAL_PROFILE_LOCK.json").read_text())
        master = json.loads((admin / "SPINE_GPE_PHASE0_MASTER_LOCK.json").read_text())
        assert adjudication["status"] == "ADJUDICATED"
        assert adjudication["n_active_all"] == 100
        assert adjudication["n_income_positive"] == 90
        assert adjudication["n_hourly_analytical"] == 89
        assert editorial["status"] == "EDITORIAL_CERTIFIED"
        assert master["status"] == "PHASE0_CERTIFIED"
        assert (admin / "SPINE_GPE_PHASE0_MASTER_FREEZE.json").exists()


def test_unmapped_fail_closed() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "SPINE-GPEv7"
        make_root(root, unmapped_race=True)
        result = run_engine(root)
        assert result == 2
        editorial_path = root / "00_admin" / "RAIS_EDITORIAL_PROFILE_LOCK.json"
        assert editorial_path.exists()
        editorial = json.loads(editorial_path.read_text())
        assert editorial["status"] == "EDITORIAL_BLOCKED"
        assert any("race" in x["gate_id"] for x in editorial["critical_failures"])


def test_codebook_integrity() -> None:
    df = pd.read_csv(CODEBOOK, dtype=str)
    assert not df.empty
    assert set(["sex", "race", "education", "link_type"]).issubset(set(df["dimension"]))
    assert not df.duplicated(["dimension", "code"]).any()


if __name__ == "__main__":
    test_codebook_integrity()
    test_success()
    test_unmapped_fail_closed()
    print("ALL TESTS PASSED")
