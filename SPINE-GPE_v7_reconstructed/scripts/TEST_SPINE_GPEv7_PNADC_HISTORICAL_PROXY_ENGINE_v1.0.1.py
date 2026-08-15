#!/usr/bin/env python3
from __future__ import annotations
import json
import importlib.util
from types import SimpleNamespace
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

ENGINE = Path(__file__).with_name("SPINE_GPEv7_PNADC_HISTORICAL_PROXY_ENGINE_v1.0.1.py")
UPSTREAM = Path(__file__).with_name("SPINE_GPEv7_PNADC_CERTIFIER_v1.2.0.py")


def direct_frame(year: int, n: int = 5000) -> pd.DataFrame:
    rng = np.random.default_rng(year)
    occ_options = np.array(["8321", "8322", "9331", "9621", "4110", "5223", "9111"])
    act_options = np.array(["53002", "49030", "56011", "47010", "84011"])
    pos_options = np.array(["1", "2", "3", "4", "8", "9", "10"])
    occ = rng.choice(occ_options, n, p=[0.05, 0.04, 0.03, 0.04, 0.30, 0.28, 0.26])
    act = rng.choice(act_options, n, p=[0.08, 0.18, 0.16, 0.38, 0.20])
    pos = rng.choice(pos_options, n)
    score = -5.0 + 3.2 * np.isin(occ, ["8321", "8322", "9331", "9621"]) + 2.4 * (act == "53002") + 0.4 * np.isin(pos, ["8", "9"])
    prob = 1 / (1 + np.exp(-score))
    target = rng.random(n) < prob
    # garante amostra positiva robusta
    if target.sum() < 150:
        target[:150] = True
        occ[:150] = "8321"
        act[:150] = "53002"
    return pd.DataFrame({
        "source_year": year,
        "reference_quarter": 4 if year == 2022 else 3,
        "survey_weight": rng.uniform(100, 1200, n),
        "survey_stratum": rng.integers(1, 80, n).astype(str),
        "survey_psu": rng.integers(1, 1500, n).astype(str),
        "eligible_platform_module": True,
        "platform_delivery_direct": target,
        "V4010": occ,
        "V4013": act,
        "VD4009": pos,
        "V2007": rng.choice(["1", "2"], n),
        "V2009": rng.integers(18, 66, n),
        "V2010": rng.choice(["1", "2", "4", "8"], n),
        "VD3004": rng.choice(["1", "2", "3", "4", "5", "6", "7"], n),
        "UF": rng.choice(["26", "33", "35", "41"], n),
    })


def historical_frame(n: int = 6000) -> pd.DataFrame:
    rng = np.random.default_rng(2020)
    return pd.DataFrame({
        "Ano": "2020",
        "Trimestre": "1",
        "UF": rng.choice(["26", "33", "35", "41"], n),
        "Capital": rng.choice(["26", "33", "35", "41", pd.NA], n),
        "RM_RIDE": pd.NA,
        "UPA": rng.integers(1, 1800, n).astype(str),
        "Estrato": rng.integers(1, 100, n).astype(str),
        "V1028": rng.uniform(100, 1400, n),
        "V2007": rng.choice(["1", "2"], n),
        "V2009": rng.integers(14, 78, n),
        "V2010": rng.choice(["1", "2", "4", "8"], n),
        "VD3004": rng.choice(["1", "2", "3", "4", "5", "6", "7"], n),
        "VD4002": "1",
        "VD4009": rng.choice(["1", "2", "3", "4", "8", "9", "10"], n),
        "V4010": rng.choice(["8321", "8322", "9331", "9621", "4110", "5223", "9111"], n),
        "V4013": rng.choice(["53002", "49030", "56011", "47010", "84011"], n),
        "VD4012": rng.choice(["1", "2", pd.NA], n),
        "VD4019": rng.uniform(600, 5000, n),
        "V4039": rng.uniform(10, 70, n),
    })



def layout_selector_contract_test() -> None:
    spec = importlib.util.spec_from_file_location("hist_engine_v101", ENGINE)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    with tempfile.TemporaryDirectory() as temp:
        txt = Path(temp) / "PNADC_042022.txt"
        txt.write_text("0" * 20 + "\n", encoding="ascii")
        def field(name: str):
            return SimpleNamespace(variable=name)
        regular = SimpleNamespace(
            path="regular_layout.xls", width=20, has_s140093=False,
            year_score={"2022": 10},
            fields=[field("V4010"), field("V4013"), field("VD4009"), field("V1028"), field("Estrato"), field("UPA")],
        )
        direct = SimpleNamespace(
            path="direct_layout.xls", width=20, has_s140093=True,
            year_score={"2022": 100},
            fields=[field("V4010"), field("V4013"), field("VD4009"), field("S140093"), field("SD14001")],
        )
        selected, width = module.choose_layout_for_source(None, txt, module.Period(2022, 4), [direct, regular])
        assert selected.path == "regular_layout.xls"
        assert width == 20
        try:
            module.choose_layout_for_source(None, txt, module.Period(2022, 4), [direct])
        except RuntimeError as exc:
            assert "Nenhum layout regular" in str(exc)
        else:
            raise AssertionError("Layout direto não pode ser aceito como regular")
    print("layout selector contract: OK")

def main() -> None:
    layout_selector_contract_test()
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "SPINE-GPEv7"
        (root / "scripts").mkdir(parents=True)
        (root / "00_admin").mkdir(parents=True)
        (root / "03_processed/10_pnadc_certified").mkdir(parents=True)
        (root / "data_pnadc").mkdir(parents=True)
        shutil.copy2(UPSTREAM, root / "scripts" / UPSTREAM.name)
        (root / "00_admin/PHASE0_LOCK.json").write_text(json.dumps({"status": "RELEASED"}))
        (root / "00_admin/PNAD_COVID_CORE_FREEZE.json").write_text(json.dumps({"status": "FROZEN"}))
        outputs = {}
        for year in (2022, 2024):
            path = root / f"03_processed/10_pnadc_certified/certified_pnadc_platform_{year}.parquet"
            direct_frame(year).to_parquet(path, index=False)
            outputs[str(year)] = str(path)
        (root / "00_admin/PNADC_CERTIFICATION_LOCK.json").write_text(json.dumps({"status": "CORE_CERTIFIED", "outputs": outputs}))
        hist = root / "data_pnadc/PNADC_012020.parquet"
        historical_frame().to_parquet(hist, index=False)

        cmd = [sys.executable, str(ENGINE), "--root", str(root), "--mode", "full", "--periods", "auto", "--strict"]
        completed = subprocess.run(cmd, text=True, capture_output=True, check=False)
        print(completed.stdout)
        print(completed.stderr)
        assert completed.returncode == 0, completed.stdout + completed.stderr
        lock = json.loads((root / "00_admin/PNADC_HISTORICAL_PROXY_CERTIFICATION_LOCK.json").read_text())
        assert lock["status"] == "CERTIFIED", lock
        assert lock["certified_periods"] == ["2020q1"], lock
        output = Path(lock["historical_outputs"]["2020q1"])
        frame = pd.read_parquet(output)
        assert frame["platform_delivery_direct"].isna().all()
        assert not frame["platform_direct_available"].any()
        assert frame["platform_delivery_probability_calibrated"].between(0, 1).all()
        assert (root / "00_admin/PNADC_HISTORICAL_PROXY_CORE_FREEZE.json").exists()
        print("fixture end-to-end: OK")


if __name__ == "__main__":
    main()
