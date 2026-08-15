# SPINE-GPE v7.1 PIPELINE RECONSTRUCTION PLAN

## EXECUTIVE SUMMARY

**Status:** CRITICAL - Pipeline Fragmented  
**Root Cause:** 70 notebooks com duplicatas massivas (57% órfãos), dependência exclusiva de dados remotos no GDrive, ausência de scripts Python modulares, e zero visualizações salvas em disco.

**Evidence Cube Status:** BLOQUEADO - Arquivo `evidence_cube.parquet` existe apenas em `/content/drive/MyDrive/aCidadeAlgoritmica/SPINE-GPEv7/` (remoto)

---

## 1. INVENTORY & DIAGNOSIS

### 1.1 Notebook Inventory (70 arquivos totais)

#### CANONICAL NOTEBOOKS (15 únicos funcionais):
| Notebook | Fase | Células | Código | Outputs | Status |
|----------|------|---------|--------|---------|--------|
| `SPINE_GPEv7_FASE0_COLAB_v7.0.3.ipynb` | 0 | 12 | 6 | ✅ | OK |
| `SPINE_GPEv7_LAYOUT_CLOSURE_AND_RAIS_FORMAL_COLAB_v1.0.1.ipynb` | 0 | 17 | 11 | ❌ | Sem outputs |
| `SPINE_GPEv7_PHASE0_CLOSURE_COLAB_v1.0.1.ipynb` | 0 | 13 | 9 | ❌ | Sem outputs |
| `SPINE_GPEv7_PHASE0_REPRODUCIBILITY_DOSSIER_COLAB_v1.0.0.ipynb` | 0 | 8 | 6 | ❌ | Sem outputs |
| `SPINE_GPEv7_PHASE1_EVIDENCE_FOUNDATION_COLAB_v1.0.0.ipynb` | 1 | 7 | 6 | ✅ | OK |
| `SPINE_GPEv7_PHASE1_EXTENDED_EVIDENCE_COLAB_v1.0.0.ipynb` | 1 | 11 | 9 | ❌ | Sem outputs |
| `SPINE_GPEv7_PNADC_CERTIFIER_COLAB_v1.2.0.ipynb` | 1 | 10 | 9 | ✅ | OK |
| `SPINE_GPEv7_PNADC_HISTORICAL_BACKCAST_HARDENING_COLAB_v1.2.0.ipynb` | 1 | 18 | 9 | ❌ | Sem outputs |
| `SPINE_GPEv7_PNADC_HISTORICAL_PROXY_ENGINE_COLAB_v1.0.1.ipynb` | 1 | 16 | 9 | ✅ | OK |
| `SPINE_GPEv7_PNAD_COVID_C014_AUDITOR_COLAB_v1.0.0.ipynb` | 1 | 6 | 5 | ❌ | Sem outputs |
| `SPINE_GPEv7_PNAD_COVID_CERTIFIER_COLAB_v1.0.2.ipynb` | 1 | 14 | 8 | ❌ | Sem outputs |
| `FASE_2_.ipynb` | 2 | 48 | 48 | ✅ | OK (mas 48 células = sobrecarregado) |
| `FASE_3.ipynb` | 3 | 10 | 10 | ✅ | OK |
| `FASE_3_FINALE.ipynb` | 3 | 10 | 10 | ✅ | OK |
| `FASE_4.ipynb` | 4 | 7 | 7 | ✅ | OK |

#### ORPHAN/DUPLICATE NOTEBOOKS (55 arquivos - 79% do repo):
- **Duplicatas numeradas:** `(1)`, `(2)`, `(3)`, `(4)` - 40+ arquivos
- **Untitled notebooks:** `Untitled2.ipynb`, `Untitled3.ipynb`, `Untitled4.ipynb` - 3 arquivos
- **Versões desatualizadas:** Múltiplas versões v1.0.0, v1.0.1, v1.1.0 do mesmo notebook

### 1.2 Scripts Python (.py)
**Status:** AUSENTES no repositório local  
**Localização:** Provavelmente apenas no GDrive (`/scripts/`)

### 1.3 Dados e Manifests
**Status:** 100% REMOTOS no GDrive

Arquivos críticos ausentes localmente:
- `evidence_cube.parquet` (95% das figuras bloqueadas)
- `certified_pnadc_platform_2022.parquet` / `2024.parquet`
- `spatial_syntax_metrics.parquet`
- `PHASE2_MASTER_FREEZE.json` / `MASTER_CERTIFICATE.json`
- `rais_formal_baseline.parquet`
- `backcast_proxy.parquet`
- `MASTER_CONSOLIDATION_FASES_0_3_20260728T195922Z.md`

---

## 2. CRITICAL GAPS IDENTIFIED

### 2.1 Data Layer (FASE 0)
- ❌ **Sem downloader/versionamento/hash** dos arquivos
- ❌ **Sem dicionário semântico oficial** por variável
- ❌ **Sem contratos de dados** por fonte
- ❌ **Sem schema registry e golden tests**
- ❌ **Sem auditoria de cobertura** temporal e espacial
- ❌ **Sem classificação:** observado / proxy / imputado / simulado
- ❌ **Sem DAGs, estimandos e limites de identificação** documentados
- ❌ **Sem gates fail-closed** contra leakage e identificação falsa

### 2.2 Evidence Foundation (FASE 1)
- ❌ **Backcasting não calibrado** (sem validação cruzada 2022/2024)
- ❌ **Sem diagrama Sankey** de attrition de amostra
- ❌ **Sem tabela de balanceamento** (SMD pre/post-weighting)

### 2.3 Mechanism Engine (FASE 2)
- ❌ **Multiverse analysis ausente** (100+ iterações)
- ❌ **Decomposição Oaxaca-Blinder** não implementada como waterfall
- ❌ **DFL, RIF regressions** ausentes
- ❌ **TMLE/AIPW/DML** com desenho survey não validados

### 2.4 Spatial Engines (FASE 3A/3B)
- ❌ **Grafo angular segment-based** não auditado
- ❌ **NAIN/NACH canônicos** sem validação com DepthmapX
- ❌ **Small Area Estimation / MRP** não implementado
- ❌ **Superfícies dinâmicas** (fricção h,t, acessibilidade h,t) ausentes

### 2.5 Visualizations (Q1 SOTA Standard)
**Missing Critical Visualizations:**
1. ❌ ETL Sankey Diagram (Sample Attrition)
2. ❌ Multiverse Forest Plot (POF vs AMOBITEC sensitivity)
3. ❌ Raincloud Plots (Income-Hour distributions)
4. ❌ Heterogeneity Forest Plot (Sex/Race/UF penalties)
5. ❌ Oaxaca-Blinder Waterfall Chart
6. ❌ Friction vs Rugosity Scatterplot
7. ❌ Policy IPE Priority Map
8. ❌ SAE TFD Intensity Map (UFs 21,23,29)
9. ❌ ROC-PR Curves (Precision-Recall for imbalanced classes)

**Existing Code But Not Executed:**
- ⚠️ Mapa de Cobertura UF
- ⚠️ Heatmap de Missingness
- ⚠️ CATE Heterogeneity Bar Plot
- ⚠️ Ranking UF Gap Salarial
- ⚠️ Scatter ICA vs Renda
- ⚠️ Mapa TFD Digital Land Rent

**Status Real:** ZERO arquivos `.png`, `.pdf`, ou `.svg` gerados em `/workspace`.

---

## 3. RECONSTRUCTION PLAN (v7.1)

### PHASE 0.5: DATA INFRASTRUCTURE LOCK (Priority: CRITICAL)
**Goal:** Create local frozen data layer with full reproducibility

**Deliverables:**
1. `scripts/00_ingest_all.py` - Master ingestion script
   - Download from GDrive paths confirmed by user
   - Compute SHA-256 hashes
   - Store in `/workspace/data/frozen_v7/`
   - Generate `data_inventory.json`

2. `data/data_dict.json` - Official semantic dictionary
   - Variable mappings per source (RAIS, PNADc, PNAD COVID)
   - Type annotations (observed/proxy/imputed/simulated)
   - Temporal/spatial coverage metadata

3. `contracts/data_contracts.yaml` - Data contracts per source
   - Schema validation rules
   - Golden test cases
   - Fail-closed gates

4. `docs/DAG_identification.md` - DAGs and identification limits
   - Estimands per claim
   - Causal boundaries (CAUSAL_BLOCKED flags)
   - Leakage prevention rules

**Estimated Time:** 4-6 hours

---

### PHASE 1.5: EVIDENCE FOUNDATION REBUILD (Priority: HIGH)
**Goal:** Rebuild Phase 1 with calibrated backcasting and sample auditing

**Deliverables:**
1. `scripts/01_backcast_calibration.py`
   - Train 2022 → Validate 2024
   - Train 2024 → Validate 2022
   - Report sensitivity, specificity, PPV, NPV
   - Propagate classification error

2. `scripts/02_sample_attrition_sankey.py`
   - Flow: Raw RAIS/PNAD → Cleaned → Matched → Final Analytic Sample
   - Highlight common support loss
   - Output: `figures/MG-01_sankey_attrition.png` (300 DPI, CMYK-safe)

3. `scripts/03_balance_table_heatmap.py`
   - SMD pre/post-weighting for TMLE
   - Threshold visualization (SMD < 0.1)
   - Output: `figures/MG-02_balance_heatmap.png`

**Estimated Time:** 6-8 hours

---

### PHASE 2.5: MECHANISM ROBUSTIFICATION (Priority: HIGH)
**Goal:** Implement multiverse analysis and advanced decomposition

**Deliverables:**
1. `scripts/04_multiverse_forest_plot.py`
   - 100+ iterations varying specifications
   - X-Axis: Cost Model (POF vs AMOBITEC)
   - Y-Axis: TFD Coefficient
   - Show 59.19% gap stability vs IV instability
   - Output: `figures/CM-01_multiverse_forest.png`

2. `scripts/05_oaxaca_blinder_waterfall.py`
   - Explained vs Unexplained components
   - Label: "Structural Penalty (Unobserved Factors)"
   - Output: `figures/CM-02_oaxaca_waterfall.png`

3. `scripts/06_heterogeneity_forest.py`
   - Plot penalties: Sex '4' (+45.9%), UF '16' (-39.5%), etc.
   - 95% CI error bars
   - Title: "Conditional Associative Penalties"
   - Output: `figures/SD-02_heterogeneity_forest.png`

4. `scripts/07_raincloud_plots.py`
   - Half-violin + raw points + box
   - Faceted by Sex/Race
   - Highlight ~20% Informality Penalty
   - Output: `figures/SD-01_raincloud_income_hour.png`

**Estimated Time:** 8-10 hours

---

### PHASE 3.5: SPATIAL INTEGRATION (Priority: MEDIUM)
**Goal:** Fuse social and spatial layers via MRP/SAE

**Deliverables:**
1. `scripts/08_friction_rugosity_scatter.py`
   - Hexbin density with regression line
   - X=Rugosity (NAIN), Y=Friction (Time/Cost proxy)
   - Label: "Structural Association"
   - Output: `figures/SP-02_friction_rugosity_scatter.png`

2. `scripts/09_policy_ipe_map.py`
   - Choropleth with hatched overlay for "Proposed Intervention"
   - Caption: "Exploratory Policy Target (Tier D)"
   - Output: `figures/SP-03_policy_ipe_map.png`

3. `scripts/10_sae_tfd_intensity_map.py`
   - Highlight UFs 21, 23, 29 (~50% TFD intensity)
   - Include uncertainty shading (SE of SAE prediction)
   - Output: `figures/SP-04_sae_tfd_map.png`

4. `scripts/11_space_syntax_dual_layer.py`
   - NAIN/Choice vs Pit-Stop Density
   - Rename: "Configurational Integration" NOT "Algorithmic Demand"
   - Output: `figures/SP-01_space_syntax_map.png`

**Estimated Time:** 10-12 hours

---

### PHASE 4+: Q1 VISUALIZATION PACKAGE (Priority: MEDIUM)
**Goal:** Generate all missing SOTA visualizations

**Deliverables:**
1. `scripts/12_roc_pr_curves.py`
   - Precision-Recall for imbalanced classes
   - Label: "Model Discriminative Power (Associative)"
   - Output: `figures/ML-02_pr_curve.png`

2. `scripts/13_vip_shap_plot.py`
   - Random Forest Variable Importance (SHAP/Permutation)
   - Highlight "Hours" and "Income" as primary cleavage
   - Warning against RAIS/PNADc leakage
   - Output: `figures/ML-01_vip_plot.png`

3. `scripts/14_categorical_distribution_panels.py`
   - Demographic panels (Sex, Race, Education, Region)
   - Proportions, descriptive statistics
   - Output: `figures/SD-03_demographic_panels.png`

4. `scripts/15_temporal_coverage_audit.py`
   - Timeline visualization of all data sources
   - RAIS 2017–latest, PNAD COVID 2020, PNADc 2022/2024
   - Output: `figures/MG-03_temporal_coverage.png`

5. `scripts/16_spatial_coverage_audit.py`
   - Brazil → Macroregions → NE focus → States → PE focus → RMR → Recife
   - Hierarchical choropleths
   - Output: `figures/MG-04_spatial_coverage_{scale}.png` (7 files)

**Estimated Time:** 8-10 hours

---

## 4. IMPLEMENTATION ROADMAP

### Week 1: Data Infrastructure & Evidence Foundation
- **Day 1-2:** Phase 0.5 (Data Lock) - `scripts/00_ingest_all.py`
- **Day 3-4:** Phase 1.5 (Evidence Rebuild) - Backcast calibration + Sankey
- **Day 5:** Balance tables + Temporal/Spatial coverage audits

### Week 2: Mechanism & Spatial Engines
- **Day 6-7:** Phase 2.5 (Multiverse + Decomposition)
- **Day 8-9:** Phase 3.5 (Spatial Integration)
- **Day 10:** Buffer/Debug

### Week 3: Q1 Visualization Package
- **Day 11-12:** Phase 4+ (Core SOTA figures)
- **Day 13-14:** Polish, caption enforcement, epistemic ceiling labels
- **Day 15:** Final integration test

---

## 5. IMMEDIATE NEXT STEPS

1. **Execute `scripts/00_ingest_all.py`** to download all GDrive artifacts
   - Requires: GDrive API credentials or manual download
   - Output: `/workspace/data/frozen_v7/` with hashes

2. **Delete orphan notebooks** (55 files)
   - Keep only 15 canonical notebooks
   - Move to `/workspace/notebooks_archive/` before deletion

3. **Create modular scripts directory** (`/workspace/scripts/`)
   - Start with `00_ingest_all.py`
   - Followed by visualization scripts in priority order

4. **Generate first SOTA figure** (Sankey Diagram)
   - Test end-to-end pipeline: Data → Script → Figure
   - Validate epistemic ceiling labeling

---

## 6. EPISTEMIC GOVERNANCE ENFORCEMENT

All scripts must include:
```python
# EPISTEMIC CEILING: Tier B (Associative, Non-Causal)
# CLAIM CEILING ENFORCEMENT: 
#   - Do NOT label as "Causal Effect"
#   - Do NOT imply observed algorithmic dispatch
#   - Label as "Configurational Friction" NOT "Algorithmic Pressure"
#   - Policy maps are Tier D (Normative/Exploratory)
```

Figure captions must include:
- Evidence Tier (A/B/C/D)
- Epistemic boundary statement
- Uncertainty intervals (95% CI)
- Data source provenance

---

## 7. SUCCESS METRICS

- [ ] 100% of data artifacts downloaded and hashed locally
- [ ] 55 orphan notebooks archived/deleted
- [ ] 15 canonical notebooks executable without errors
- [ ] 15+ SOTA visualizations generated (300 DPI, CMYK-safe)
- [ ] All figures include epistemic ceiling labels
- [ ] Reproducible pipeline: `make all` runs end-to-end
- [ ] Claim-Evidence cross-reference matrix complete

---

**Prepared by:** SPINE-GPE v7.1 Reconstruction Committee  
**Date:** 2026-08-15  
**Next Review:** Upon completion of Phase 0.5
