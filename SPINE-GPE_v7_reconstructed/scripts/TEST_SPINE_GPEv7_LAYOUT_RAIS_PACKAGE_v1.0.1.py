#!/usr/bin/env python3
from __future__ import annotations
import importlib.util
import tempfile
from pathlib import Path
import pandas as pd

BASE = Path(__file__).resolve().parent


def load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, BASE / filename)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    import sys
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod

layout = load("layout_mod", "SPINE_GPEv7_PNADC_LAYOUT_EQUIVALENCE_CLOSURE_v1.0.1.py")
rais = load("rais_mod", "SPINE_GPEv7_RAIS_FORMAL_CERTIFIER_v1.0.0.py")


def test_layout_excel_parser():
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "dicionario_2022.xlsx"
        df = pd.DataFrame([
            ["Variável", "Posição inicial", "Tamanho", "Tipo", "Descrição"],
            ["V4010", 100, 4, "num", "Ocupação no trabalho principal"],
            ["V4013", 104, 5, "num", "Atividade do empreendimento"],
            ["VD4009", 109, 2, "num", "Posição na ocupação"],
            ["V1028", 111, 12, "num", "Peso final"],
            ["Estrato", 123, 7, "char", "Estrato"],
            ["UPA", 130, 9, "num", "UPA"],
            ["UF", 139, 2, "num", "UF"],
            ["Capital", 141, 2, "num", "Capital"],
            ["RM_RIDE", 143, 2, "num", "RM/RIDE"],
        ])
        df.to_excel(p, header=False, index=False)
        ev = layout.parse_excel_layout(p, {"reference_year":2022,"source_kind":"test","independent_version":True}, __import__('logging').getLogger("test"))
        found = {x.variable for x in ev}
        assert set(layout.TARGET_VARS).issubset(found), found
        v = next(x for x in ev if x.variable == "V4010")
        assert v.position == 100 and v.width == 4



def test_empty_registry_template_is_valid_csv():
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "registry.csv"
        layout.make_registry_template(path, [], [2019, 2020, 2021, 2022, 2024])
        assert path.exists() and path.stat().st_size > 0
        df = pd.read_csv(path, dtype=str)
        assert df.columns.tolist() == layout.REGISTRY_COLUMNS
        assert len(df) == 5
        assert set(df["reference_year"].astype(int)) == {2019, 2020, 2021, 2022, 2024}
        assert (df["source_kind"] == "official_layout_placeholder").all()


def test_layout_comparison():
    rows = []
    for year in [2019, 2022]:
        for var in layout.TARGET_VARS:
            rows.append({
                "reference_year":year,"variable":var,"source_sha256":str(year),"source_path":f"x{year}",
                "position":1,"width":2,"data_type":"numeric","label":var,"domain_signature":None,
                "independent_version":True,
            })
    matrix, failures, warnings = layout.compare_layouts(pd.DataFrame(rows), [2019,2022])
    assert not failures
    assert (matrix["equivalence_status"] == "EXACT_OR_COMPATIBLE").all()


def test_rais_column_resolution_and_harmonization():
    headers = ["Ano", "CBO Ocupação 2002", "Vínculo Ativo 31/12", "Mun Trab", "Vl Remun Média Nom", "Qtd Hora Contr", "Sexo Trabalhador", "Raça Cor"]
    mapping = rais.resolve_columns(headers, {})
    for field in ["year","cbo2002","active_3112","municipality_work","income_avg_nominal","contract_hours"]:
        assert field in mapping, (field, mapping)
    chunk = pd.DataFrame({
        "Ano":["2022","2022","2022"],
        "CBO Ocupação 2002":["5191-10","519110","999999"],
        "Vínculo Ativo 31/12":["1","0","1"],
        "Mun Trab":["261160","355030","261160"],
        "Vl Remun Média Nom":["1.778,00","2000,00","1000,00"],
        "Qtd Hora Contr":["42","40","44"],
        "Sexo Trabalhador":["1","2","1"],
        "Raça Cor":["8","4","2"],
    })
    out, stats = rais.harmonize_target_chunk(chunk, mapping, 2022, {"519110"}, {}, {}, 2022)
    assert len(out) == 2
    assert stats["rows_target_active"] == 1
    assert out.loc[0,"municipality6"] == "261160"
    assert abs(out.loc[0,"income_monthly_nominal"] - 1778.0) < 1e-6
    assert out.loc[0,"is_recife"]


def test_rais_depara_resolution():
    headers = ["ANO_BASE_NOVO", "CBO_NOVO", "ATIVO_NOVO", "MUN_NOVO", "RENDA_NOVA", "HORAS_NOVAS"]
    depara = {
        rais.normalize_text("ANO_BASE_NOVO"): rais.normalize_text("Ano"),
        rais.normalize_text("CBO_NOVO"): rais.normalize_text("CBO Ocupação 2002"),
        rais.normalize_text("ATIVO_NOVO"): rais.normalize_text("Vínculo Ativo 31/12"),
        rais.normalize_text("MUN_NOVO"): rais.normalize_text("Mun Trab"),
        rais.normalize_text("RENDA_NOVA"): rais.normalize_text("Vl Remun Média Nom"),
        rais.normalize_text("HORAS_NOVAS"): rais.normalize_text("Qtd Hora Contr"),
    }
    mapping = rais.resolve_columns(headers, depara)
    assert set(["year","cbo2002","active_3112","municipality_work","income_avg_nominal","contract_hours"]).issubset(mapping)


if __name__ == "__main__":
    test_layout_excel_parser()
    test_empty_registry_template_is_valid_csv()
    test_layout_comparison()
    test_rais_column_resolution_and_harmonization()
    test_rais_depara_resolution()
    print("ALL TESTS PASSED")
