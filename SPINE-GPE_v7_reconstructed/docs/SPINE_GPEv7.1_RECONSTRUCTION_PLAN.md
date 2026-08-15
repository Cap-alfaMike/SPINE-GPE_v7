# SPINE-GPE v7.1 — RECONSTRUCTION PLAN

## Executive Summary

**Status:** READY FOR EXECUTION  
**Source Material:** MASTER PROMPT v2.txt + scripts/ directory analysis  
**Target:** Complete pipeline reconstruction with 8 phases (0-6), Q1 SOTA visualizations, and epistemic governance enforcement.

---

## Phase 0: Data, Identifiability & Reproducibility Lock

### Inputs from Existing Codebase
- `SPINE_GPEv7_PHASE0_CLOSURE_v1.0.1.py` (58KB)
- `SPINE_GPEv7_LAYOUT_CLOSURE_AND_RAIS_FORMAL_COLAB_v1.0.1.ipynb`
- `OFFICIAL_SOURCE_REGISTRY_SPINE_GPEv7_PHASE0_CLOSURE_v1.0.1.json`
- `PACKAGE_MANIFEST_SPINE_GPEv7_PHASE0_CLOSURE_v1.0.1.json`

### Reconstruction Actions
1. **Automated Inventory**: Port existing Phase 0 closure script to modular Python package
2. **Hash Verification**: Implement SHA-256 hashing for all frozen artifacts
3. **Semantic Dictionary**: Extract variable mappings from existing JSON registries
4. **Data Contracts**: Formalize contracts for RAIS, PNADc, PNAD COVID
5. **DAGs & Estimands**: Document identification limits (CAUSAL_BLOCKED gate)
6. **Fail-Closed Gates**: Implement leakage prevention logic

### Outputs
- `phase0_master_freeze.json` (hash-anchored)
- `data_dictionary_official.json`
- `source_registry_v7.1.json`
- `reproducibility_dossier.md`

---

## Phase 1: Evidence Foundation

### Inputs from Existing Codebase
- `SPINE_GPEv7_PNADC_CERTIFIER_v1.2.0.py` (125KB) - FINAL CERTIFIED
- `SPINE_GPEv7_PNADC_HISTORICAL_BACKCAST_HARDENING_v1.2.0.py` (109KB) - FINAL CERTIFIED
- `SPINE_GPEv7_PNADC_HISTORICAL_PROXY_ENGINE_v1.0.1.py` (81KB)
- `SPINE_GPEv7_PNAD_COVID_CERTIFIER_v1.0.2.py` (70KB)
- `SPINE_GPEv7_RAIS_FORMAL_CERTIFIER_v1.0.0.py` (47KB)

### Critical Empirical Targets (MUST MATCH)
1. **Backcast v1.2.0 Totals** (Tier C):
   - 2019T1: 317,479 [288,225 - 346,733]
   - 2021T4: 452,058 [413,303 - 490,813]
   - ROC AUC: 0.9788 (2022→2024), 0.9794 (2024→2022)

2. **PNADc Direct 2022/2024** (Tier A):
   - 2022 Q4: 445,867 identified (SD14001 & S140093)
   - 2024 Q3: 487,285 identified
   - Golden tests: PASS (except income secondary)

3. **PNAD COVID 2020** (Tier B):
   - Sep 2020: n=973 deliverers (C007C=17)
   - Hourly wage: R$ 12.14 (weighted)
   - C014 coverage: 65-68% (WARNING documented)

4. **RAIS 2022** (Tier D):
   - Active links (Dec 31): 212,330
   - Hourly wage: R$ 10.08 (BR), R$ 9.35 (Recife)
   - FEOLS: log(hours) β=0.4388, male β=0.0551

### Reconstruction Actions
1. **READ-ONLY Execution**: Run certifiers on frozen data (DO NOT re-estimate Backcast)
2. **Claim Ledger**: Build structured claim-evidence book
3. **Sample Attrition Sankey**: Visualize RAIS/PNAD → final analytic sample
4. **Calibration Tables**: Document sensitivity/specificity of backcast

### Outputs
- `phase1_evidence_freeze.json` (hash-anchored to bb11164...)
- `backcast_series_certified.csv`
- `pnadc_direct_2022_2024.parquet`
- `pnad_covid_2020_certified.parquet`
- `rais_formal_baseline.parquet`
- `claim_ledger_phase1.json`
- **Figure MG-01**: ETL Sankey Diagram (SVG/PDF/PNG 300dpi)
- **Figure MG-02**: Backcast Calibration Plot

---

## Phase 2: Mechanism & Partial Identification Engine

### Critical Empirical Targets (MUST MATCH)
1. **TFD Gap 59.19 pp** (Tier B):
   - Gross gap (DoubleML PLR): -0.0081 (p=0.3733)
   - Gross premium (TMLE): +0.0155 (p<0.001)
   - Net gap (after POF/AMOBITEC costs): -50.65%
   - TFD Gap: 59.19 percentage points

2. **Heterogeneity CATEs** (Tier B):
   - Sex '4' (male): +45.9% (p=0.006, n=646)
   - Sex '2' (female): +15.7% (p=0.007, n=154)
   - Race '029': -17.6% (p=0.015, n=47)
   - UF 16 (PA): -39.5% (p=0.000, n=12)
   - UF 35 (SP): -24.6% (p=0.000, n=237)

3. **CAUSAL_BLOCKED Gate**:
   - IV: BLOCKED (weak instrument)
   - GMM: BLOCKED (low precision)
   - DML-IV: BLOCKED
   - Allowed: DoubleML PLR, TMLE, AIPW, Causal Forest (diagnostic only)

### Reconstruction Actions
1. **Multiverse Analysis**: 100+ specifications varying cost models (POF vs AMOBITEC)
2. **TMLE/AIPW**: Execute with survey weights preserved
3. **Oaxaca-Blinder Decomposition**: Explained vs unexplained components
4. **CATE Estimation**: Causal Forest + R-Learner (label as diagnostic)
5. **Sensitivity Analysis**: Oster, E-values, tipping points

### Outputs
- `phase2_mechanism_freeze.json`
- `tfd_partial_identification.parquet`
- `cate_distributions.parquet`
- `decomposition_results.json`
- **Figure CM-01**: Multiverse Forest Plot (TFD sensitivity to cost models)
- **Figure SD-01**: Raincloud Plots (income-hour distributions)
- **Figure SD-02**: Heterogeneity Forest Plot (CATEs by subgroup)
- **Figure CM-02**: Oaxaca-Blinder Waterfall Chart

---

## Phase 3A: Spatial Configuration Engine

### Inputs Required
- `spatial_syntax_metrics.parquet` (from GDrive)
- Recife road network GeoPackage (from GDrive)
- Phase 2 freeze

### Reconstruction Actions
1. **Angular Graph Construction**: Segment-based dual graph
2. **NAIN/NACH Computation**: 400m to global radii
3. **SAE/MRP**: Small Area Estimation for platform worker prevalence
4. **Static Surfaces**: Centrality, accessibility, friction, land value

### Outputs
- `phase3a_spatial_freeze.json`
- `angular_graph.gpkg`
- `nain_nach_metrics.parquet`
- `sae_mrp_results.parquet`
- `static_surfaces.parquet`
- **Figure SP-01**: Dual-Layer Space Syntax Map (NAIN + Infrastructure)
- **Figure SP-04**: SAE TFD Intensity Map (UFs 21, 23, 29 highlighted)

---

## Phase 3B: Dynamic Urban Graph Engine

### Status: PENDING DATA
Requires traffic sensor time series (15-min resolution) from GDrive.

---

## Phase 4: Structural Algorithmic Emulator

### Status: SIMULATION ONLY (Tier D)
Must be labeled as simulation, NOT observed evidence.

---

## Phase 5: Distributional Policy Engine

### Status: NORMATIVE ONLY (Tier D)
Must be labeled as policy proposal, NOT proven impact.

---

## Phase 6: Integrated Falsification & Claim Adjudication

### Reconstruction Actions
1. **Triangulation**: Cross-validate findings across Phases 0-5
2. **Placebo Tests**: Temporal, spatial, pseudo-treatments
3. **Claim Adjudication**: Update status (SUPPORTED/PARTIALLY/BLOCKED/FALSIFIED)
4. **Evidence Graph**: Structured representation of claim-evidence links

### Outputs
- `claim_evidence_book_final.json`
- `evidence_graph.json`
- `final_synthesis.md`

---

## Visualization Portfolio (Q1 SOTA Standard)

### Methodological Governance (Tier A/B)
- ✅ FIG-MG-01: ETL Sankey (sample attrition)
- ✅ FIG-MG-02: Backcast Calibration Plot
- ⏳ FIG-MG-03: Balance Table Heatmap (SMD)

### Structural Descriptive (Tier B)
- ✅ FIG-SD-01: Raincloud Plots (income-hour distributions)
- ✅ FIG-SD-02: Heterogeneity Forest Plot (CATEs)
- ⏳ FIG-SD-03: Moran Scatter (spatial autocorrelation)

### Causal & Mechanism (Tier B/C)
- ✅ FIG-CM-01: **Multiverse Forest Plot** (TFD sensitivity)
- ✅ FIG-CM-02: Oaxaca-Blinder Waterfall
- ⏳ FIG-CM-03: CATE Distribution Histogram

### Spatial & Policy (Tier D)
- ✅ FIG-SP-01: Dual-Layer Space Syntax Map
- ⏳ FIG-SP-02: Friction vs Rugosity Scatterplot
- ✅ FIG-SP-03: Policy IPE Priority Map
- ✅ FIG-SP-04: SAE TFD Intensity Map

### Machine Learning (Tier B/C)
- ⏳ FIG-ML-01: Random Forest VIP (SHAP)
- ⏳ FIG-ML-02: ROC-PR Curves

---

## Execution Constraints

1. **NO HALLUCINATIONS**: All results must derive from frozen data
2. **CAUSAL_BLOCKED Gate**: Enforce fail-closed logic
3. **Claim Ceilings**: Label every output with evidence tier (A/B/C/D)
4. **Reproducibility**: Fixed seeds, SHA-256 hashes, immutable freezes
5. **Q1 Aesthetics**: 300 DPI, colorblind-safe palettes, vector exports (SVG/PDF)

---

## Next Steps

1. **Immediate**: Copy certified Python scripts from `/workspace/SPINE-GPE_v7_fresh/scripts/` to reconstructed structure
2. **Data Access**: Download frozen parquet files from GDrive to `/workspace/SPINE-GPE_v7_reconstructed/data/frozen/`
3. **Phase 0-1 Execution**: Run certifiers on frozen data, generate Claim Ledger
4. **Phase 2 Execution**: Implement Multiverse Analysis, TFD identification, CATE estimation
5. **Visualization Pipeline**: Generate all 15 Q1 SOTA figures with proper labeling

**Estimated Time**: 12-18 hours for full reconstruction (excluding data download time)
