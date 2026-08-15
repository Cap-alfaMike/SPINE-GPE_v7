from pathlib import Path
import importlib.util
import json
import subprocess
import sys
import tempfile

import pandas as pd

SCRIPT = Path(__file__).with_name("SPINE_GPEv7_PNAD_COVID_CERTIFIER_v1.0.2.py")

spec = importlib.util.spec_from_file_location("pnad_covid_engine", SCRIPT)
engine = importlib.util.module_from_spec(spec)
sys.modules["pnad_covid_engine"] = engine
assert spec.loader is not None
spec.loader.exec_module(engine)

# Regressão principal: 07, 7.0 e 7 devem ser a mesma categoria oficial.
series = pd.Series(["07", "7.0", "7", " 007 ", None, "A7"], dtype="string")
normalized = engine.canonical_category_code(series).tolist()
assert normalized[:4] == ["7", "7", "7", "7"], normalized
assert pd.isna(normalized[4])
assert normalized[5] == "A7"

with tempfile.TemporaryDirectory() as copy_td:
    copy_root = Path(copy_td)
    source = copy_root / "immutable.bin"
    latest = copy_root / "latest.bin"
    source.write_bytes(b"immutable-v1")
    engine.atomic_copy(source, latest)
    assert latest.read_bytes() == b"immutable-v1"
    source.write_bytes(b"immutable-v2")
    engine.atomic_copy(source, latest)
    assert latest.read_bytes() == b"immutable-v2"

with tempfile.TemporaryDirectory() as td:
    root = Path(td) / "SPINE-GPEv7"
    (root / "00_admin").mkdir(parents=True)
    (root / "00_admin/PHASE0_LOCK.json").write_text(json.dumps({"status": "RELEASED"}))
    (root / "00_admin/PNADC_CERTIFICATION_LOCK.json").write_text(
        json.dumps({"status": "CORE_CERTIFIED", "outputs": {}})
    )
    raw_dir = root / "01_raw/10_ibge/pnad_covid_2020/2020_09"
    raw_dir.mkdir(parents=True)

    rows = []
    code_variants = ["07", "7.0", "7"]
    for i in range(120):
        occupation = "16" if i < 20 else ("17" if i < 45 else "36")
        is_delivery = i < 45
        rows.append(
            {
                "Ano": "2020",
                "UF": "26",
                "CAPITAL": "26",
                "RM_RIDE": "26",
                "UPA": str(1000 + i // 4),
                "Estrato": str(10 + i // 20),
                "V1008": str(i % 14 + 1),
                "V1012": "1",
                "V1013": "09",
                "V1016": "5",
                "V1022": "1",
                "V1023": "1",
                "V1030": "211000000",
                "V1031": "1750000",
                "V1032": "1750000",
                "posest": "2611",
                "A001": str(i % 5 + 1),
                "A001A": "01",
                "A001B1": "01",
                "A001B2": "01",
                "A001B3": "1990",
                "A002": "30",
                "A003": "1",
                "A004": "4",
                "A005": "5",
                "C001": "1",
                "C002": None,
                "C006": "2",
                "C007": code_variants[i % 3] if is_delivery else "4",
                "C007A": None,
                "C007B": None if is_delivery else "1",
                "C007C": occupation,
                "C007D": "10",
                "C008": "45",
                "C009": "44",
                "C010": "1",
                "C0101": "1",
                "C01012": "1800",
                "C0102": None,
                "C01022": None,
                "C011A": "1",
                "C011A1": "1",
                "C011A12": "1700",
                "C011A2": None,
                "C011A22": None,
                "C012": "1",
                "C013": "2",
                # 30 válidos e 15 ausentes entre os 45 entregadores.
                "C014": ("01" if i % 2 == 0 else "2") if i < 30 else (None if is_delivery else "1"),
            }
        )

    raw_frame = pd.DataFrame(rows)
    raw_frame.to_csv(raw_dir / "PNAD_COVID_092020.csv", index=False)
    pd.DataFrame(
        [
            ["C007C", "16", "Motoboy"],
            [
                "C007C",
                "17",
                "Entregador de mercadorias (de restaurante, de farmácia, de loja, Uber Eats, IFood, Rappy etc.)",
            ],
            ["C007", "7", "Conta própria"],
            ["C014", "1", "Sim"],
            ["C014", "2", "Não"],
        ]
    ).to_excel(raw_dir / "Dicionario_PNAD_COVID_092020_TEST.xlsx", index=False, header=False)

    transformed = engine.transform_chunk(raw_frame, 9, "abc", 0)
    assert int(transformed["pandemic_delivery_observed"].fillna(False).sum()) == 45
    assert int(transformed["pandemic_delivery_self_employed"].fillna(False).sum()) == 45
    assert set(transformed.loc[:44, "C007"].dropna()) == {"7"}
    assert set(transformed["C014"].dropna().unique()) <= {"1", "2"}

    audit = engine.monthly_coverage_audit(transformed, 9)
    assert audit["n_delivery"] == 45
    assert audit["n_self_employed"] == 45
    assert audit["n_C014_valid"] == 30
    assert audit["n_C014_missing"] == 15

    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--root",
            str(root),
            "--mode",
            "certify",
            "--months",
            "9",
            "--chunk-rows",
            "30",
            "--strict",
        ],
        text=True,
        capture_output=True,
    )
    print(proc.stdout)
    print(proc.stderr)
    # A fixture pequena deve bloquear apenas gates de escala amostral/populacional.
    assert proc.returncode == 2
    lock = json.loads((root / "00_admin/PNAD_COVID_CERTIFICATION_LOCK.json").read_text())
    assert lock["status"] == "BLOCKED"
    ids = {item["test_id"]: item for item in lock["critical_failures"]}
    assert "2020m09.self_employed.derivation" not in ids
    assert lock["coverage_audits"]["9"]["n_self_employed"] == 45

    immutable = Path(lock["output"])
    assert immutable.exists()
    assert "_BLOCKED.parquet" in immutable.name
    # Um run bloqueado jamais deve promover o alias latest.
    assert lock["output_latest"] is None

print("TESTS v1.0.2: OK")
