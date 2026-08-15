#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd

SCRIPT = Path(__file__).with_name("SPINE_GPEv7_PNADC_CERTIFIER_v1.2.0.py")
spec = importlib.util.spec_from_file_location("spine_pnadc_v120", SCRIPT)
if spec is None or spec.loader is None:
    raise RuntimeError(f"Não foi possível carregar {SCRIPT}")
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)


def row(year: int, value: str, var: str, unit: str, platform: str = "59653") -> dict[str, str]:
    return {
        "Nível Territorial (Código)": "1",
        "Nível Territorial": "Brasil",
        "Unidade de Medida (Código)": unit,
        "Unidade de Medida": "teste",
        "Valor": value,
        "Brasil (Código)": "1",
        "Brasil": "Brasil",
        "Variável (Código)": var,
        "Variável": "teste",
        "Ano (Código)": str(year),
        "Ano": str(year),
        "Trabalho por meio de plataforma digital de serviço no trabalho principal (Código)": platform,
    }


def main() -> None:
    df = pd.DataFrame([
        row(2022, "1319", "12900", "1572"),
        row(2024, "1654", "12900", "1572"),
        row(2022, "1.5", "12902", "2"),
        row(2024, "1.9", "12902", "2"),
    ])
    total_contract = module.SIDRA_CODE_CONTRACTS["platform_any_total"]
    total_2022, evidence = module.select_sidra_contract_value(
        df, 2022, "platform_any_total", total_contract
    )
    assert total_2022 == 1_319_000.0, evidence

    pct_contract = module.SIDRA_CODE_CONTRACTS["platform_any_percent"]
    pct_2024, evidence = module.select_sidra_contract_value(
        df, 2024, "platform_any_percent", pct_contract
    )
    assert pct_2024 == 1.9, evidence

    passed = module.compare_golden(
        "test", 2024, 1.86845, 1.9, 0.06, {},
        tolerance_kind="absolute", severity="critical"
    )
    assert passed.status == "PASS"

    income = module.compare_golden(
        "income", 2024, 3081.5903, 2996.0, 0.02, {},
        tolerance_kind="relative", severity="high"
    )
    assert income.status == "FAIL"
    assert income.severity == "high"

    print("Todos os testes determinísticos v1.2.0 passaram.")


if __name__ == "__main__":
    main()
