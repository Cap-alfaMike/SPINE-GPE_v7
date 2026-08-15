from pathlib import Path
import json, subprocess, sys, tempfile
import pandas as pd

SCRIPT=Path(__file__).with_name("SPINE_GPEv7_PNAD_COVID_CERTIFIER_v1.0.0.py")
with tempfile.TemporaryDirectory() as td:
    root=Path(td)/"SPINE-GPEv7"
    (root/"00_admin").mkdir(parents=True)
    (root/"00_admin/PHASE0_LOCK.json").write_text(json.dumps({"status":"RELEASED"}))
    (root/"00_admin/PNADC_CERTIFICATION_LOCK.json").write_text(json.dumps({"status":"CORE_CERTIFIED","outputs":{}}))
    raw=root/"01_raw/10_ibge/pnad_covid_2020/2020_09"
    raw.mkdir(parents=True)
    rows=[]
    for i in range(120):
        occ="16" if i<20 else ("17" if i<45 else "36")
        rows.append({
            "Ano":"2020","UF":"26","CAPITAL":"26","RM_RIDE":"26","UPA":str(1000+i//4),
            "Estrato":str(10+i//20),"V1008":str(i%14+1),"V1012":"1","V1013":"09","V1016":"5",
            "V1022":"1","V1023":"1","V1030":"211000000","V1031":"1750000","V1032":"1750000",
            "posest":"2611","A001":str(i%5+1),"A001A":"01","A001B1":"01","A001B2":"01","A001B3":"1990",
            "A002":"30","A003":"1","A004":"4","A005":"5","C001":"1","C002":None,"C006":"2",
            "C007":"7" if i<45 else "4","C007A":None,"C007B":None if i<45 else "1","C007C":occ,
            "C007D":"10","C008":"45","C009":"44","C010":"1","C0101":"1","C01012":"1800",
            "C0102":None,"C01022":None,"C011A":"1","C011A1":"1","C011A12":"1700","C011A2":None,
            "C011A22":None,"C012":"1","C013":"2","C014":"1" if i%3==0 else "2"
        })
    pd.DataFrame(rows).to_csv(raw/"PNAD_COVID_092020.csv",index=False)
    # dicionário mínimo legível
    pd.DataFrame([
        ["C007C","16","Motoboy"],
        ["C007C","17","Entregador de mercadorias (de restaurante, de farmácia, de loja, Uber Eats, IFood, Rappy etc.)"],
        ["C007","7","Conta própria"],
        ["C014","1","Sim"],
    ]).to_excel(raw/"Dicionario_PNAD_COVID_092020_TEST.xlsx",index=False,header=False)
    proc=subprocess.run([sys.executable,str(SCRIPT),"--root",str(root),"--mode","certify","--months","9","--chunk-rows","30","--strict"],text=True,capture_output=True)
    print(proc.stdout); print(proc.stderr)
    assert proc.returncode==2, "fixture usa 120 linhas e deve falhar somente no gate de tamanho plausível"
    lock=json.loads((root/"00_admin/PNAD_COVID_CERTIFICATION_LOCK.json").read_text())
    assert lock["status"]=="BLOCKED"
    # teste direto da transformação por import
    import importlib.util
    spec=importlib.util.spec_from_file_location("engine",SCRIPT); mod=importlib.util.module_from_spec(spec); sys.modules["engine"]=mod; spec.loader.exec_module(mod)
    transformed=mod.transform_chunk(pd.DataFrame(rows),9,"abc",0)
    assert int(transformed["pandemic_delivery_observed"].fillna(False).sum())==45
    assert int(transformed["pandemic_motoboy_observed"].fillna(False).sum())==20
    assert int(transformed["pandemic_goods_delivery_observed"].fillna(False).sum())==25
    assert transformed["platform_delivery_direct"].isna().all()
    assert (~transformed["platform_direct_available"]).all()
print("TESTS: OK")
