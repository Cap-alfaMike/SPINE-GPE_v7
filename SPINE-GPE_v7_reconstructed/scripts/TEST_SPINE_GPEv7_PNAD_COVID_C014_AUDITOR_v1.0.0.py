"""Deterministic software tests. Temporary fixture only; never analytical data."""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd

SCRIPT = Path(__file__).with_name("SPINE_GPEv7_PNAD_COVID_C014_AUDITOR_v1.0.0.py")


def test_end_to_end() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "00_admin").mkdir(parents=True)
        output_dir = root / "03_processed" / "20_pnad_covid_certified"
        output_dir.mkdir(parents=True)

        rows = []
        for month in range(5, 12):
            for idx in range(80):
                delivery_code = "16" if idx % 3 == 0 else "17"
                position = "7" if idx % 2 == 0 else "3"
                c014 = "1" if idx % 4 == 0 else ("2" if idx % 4 == 1 else None)
                valid = c014 in {"1", "2"}
                rows.append(
                    {
                        "source_year": 2020,
                        "reference_month": month,
                        "survey_weight": 100.0 + idx,
                        "C007": position,
                        "C007C": delivery_code,
                        "C014": c014,
                        "pandemic_delivery_observed": True,
                        "pandemic_delivery_self_employed": position == "7",
                        "social_security_response_valid": valid,
                        "social_security_contributor": True if c014 == "1" else (False if c014 == "2" else pd.NA),
                        "platform_delivery_direct": pd.NA,
                        "platform_direct_available": False,
                        "evidence_tier": "B",
                    }
                )

        parquet = output_dir / "certified_pnad_covid_delivery_2020_m05_m11_TEST.parquet"
        pd.DataFrame(rows).to_parquet(parquet, index=False)
        digest = hashlib.sha256(parquet.read_bytes()).hexdigest()
        upstream = {
            "run_id": "TEST",
            "script_version": "1.0.2",
            "schema_version": "spine-gpe-v7-pnad-covid-delivery-1.0.1",
            "status": "CERTIFIED",
            "output": str(parquet),
            "artifact_hashes": {"output": digest},
        }
        (root / "00_admin" / "PNAD_COVID_CERTIFICATION_LOCK.json").write_text(
            json.dumps(upstream), encoding="utf-8"
        )

        result = subprocess.run(
            [sys.executable, str(SCRIPT), "--root", str(root), "--strict"],
            text=True,
            capture_output=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        lock = json.loads((root / "00_admin" / "PNAD_COVID_C014_AUDIT_LOCK.json").read_text())
        freeze = json.loads((root / "00_admin" / "PNAD_COVID_CORE_FREEZE.json").read_text())
        assert lock["status"] == "AUDIT_PASSED"
        assert freeze["status"] == "FROZEN"
        assert freeze["immutable_parquet_sha256"] == digest


if __name__ == "__main__":
    test_end_to_end()
    print("All deterministic tests passed.")
