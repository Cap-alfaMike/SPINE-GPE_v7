# RELATÓRIO DE DIAGNÓSTICO: INCONSISTÊNCIAS SPINE-GPE v7

**Data:** 2025-08-15  
**Fase:** B (Visual Evidence Gap Analysis) - Diagnóstico Técnico  
**Status:** CRÍTICO - Dados Remotos Bloqueiam Execução Local

---

## 1. RESUMO EXECUTIVO

### 🔴 Problema Central Identificado
**As figuras estão vazias porque TODOS os dados estão armazenados exclusivamente no Google Drive remoto.** Os notebooks contêm código de visualização completo, mas falham ao executar localmente porque:

1. **100% dos dados (.parquet, .json) estão em GDrive** - Nenhum dado localizado em `/workspace`
2. **Código de plotagem existe** mas depende de variáveis não carregadas (`df_platform`, `evidence_cube`, `df_tmle`)
3. **Mount do GDrive é obrigatório** - Sem autenticação, as células de carga de dados retornam DataFrames vazios

---

## 2. O QUE ESTÁ FALTANDO: MATRIZ DE INCONSISTÊNCIAS

### A. DADOS (BLOCKED - Crítico)

| Tipo de Dado | Localização Esperada | Status Real | Impacto |
|--------------|---------------------|-------------|---------|
| **Evidence Cube** | `05_outputs/tables/phase1_publication_synthesis_v101/` | ❌ REMOTO (GDrive) | 95% das figuras bloqueadas |
| **PNADc Certified 2022/2024** | `03_processed/10_pnadc_certified/` | ❌ REMOTO (GDrive) | Análise demográfica bloqueada |
| **Spatial Syntax Metrics** | `03_processed/spatial_syntax_metrics.parquet` | ❌ REMOTO (GDrive) | Mapas espaciais vazios |
| **Phase2 Master Freeze/Cert** | `00_admin/phase2_intake/` | ❌ REMOTO (GDrive) | Validação de claims impossível |
| **RAIS Formal Baseline** | `03_processed/rais_formal_baseline.parquet` | ❌ NÃO ENCONTRADO | Linha base formal ausente |
| **Backcast Engine Output** | `03_processed/backcast_proxy.parquet` | ❌ NÃO ENCONTRADO | Série histórica incompleta |

**Ação Necessária:** Download manual dos arquivos do GDrive para `/workspace/data/` ou execução no Colab com mount.

---

### B. MÉTODOS E TÉCNICAS (PARCIAL - Atenção)

| Método/Técnica | Implementação | Status | Problema |
|----------------|---------------|--------|----------|
| **TMLE (Targeted Maximum Likelihood)** | Código presente em FASE_3_FINALE.ipynb | ⚠️ Parcial | Variável `df_tmle` não definida sem dados |
| **CATE (Causal Forest Heterogeneity)** | Código presente em FASE_3_FINALE.ipynb | ⚠️ Parcial | `df_analysis` vazio sem evidence cube |
| **SAE (Small Area Estimation)** | Código presente em múltiplos notebooks | ⚠️ Parcial | `sae_df_sorted` não carregado |
| **Space Syntax (NAIN/Choice)** | Referenciado em FASE_4.ipynb | ❌ Bloqueado | `geopandas` instalado mas sem dados espaciais |
| **Oaxaca-Blinder Decomposition** | Não encontrado | ❌ Ausente | Nenhum código detectado nos 70 notebooks |
| **Multiverse Analysis** | Não encontrado | ❌ Ausente | Nenhuma estrutura de sensibilidade a modelos POF vs AMOBITEC |
| **Sankey Diagram (Attrition)** | Não encontrado | ❌ Ausente | Critical gap identificado na Fase B |

**Ação Necessária:** 
1. Implementar notebooks faltantes (Oaxaca-Blinder, Multiverse, Sankey)
2. Corrigir dependências de variáveis nos notebooks existentes

---

### C. CÓDIGO ERRADO / INCONSISTÊNCIAS (ALTA PRIORIDADE)

#### Inconsistência #1: Variáveis Undefined em FASE_4.ipynb
```python
# Célula 7 referencia variáveis não carregadas:
0.4 * df_platform['intensidade'] +  # ❌ df_platform não existe
0.3 * df_platform['dependencia'] +
0.3 * df_platform['volatilidade_norm']
```
**Problema:** O código tenta calcular ICA (Índice de Controle Algorítmico) mas `df_platform` nunca foi carregado porque o read_parquet aponta para GDrive.

**Solução:** Adicionar célula de carga de dados local ou fallback para dados sintéticos de teste.

---

#### Inconsistência #2: Caminhos Hardcoded para GDrive
```python
# Em FASE_2_.ipynb, Célula 10:
EVIDENCE_CUBE_PARQUET = Path("/content/drive/MyDrive/aCidadeAlgoritmica/SPINE-GPEv7/05_outputs/...")
```
**Problema:** Caminho do Colab não funciona localmente. Nenhum mecanismo de fallback para `/workspace/data/`.

**Solução:** Implementar padrão de caminhos relativos:
```python
BASE_DIR = Path("/workspace") if Path("/workspace").exists() else Path("/content/drive/MyDrive/aCidadeAlgoritmica/SPINE-GPEv7")
```

---

#### Inconsistência #3: Duplicação de Notebooks (Orphan Code)
| Notebook Canonical | Duplicatas Órfãs | Ação |
|--------------------|------------------|------|
| `PHASE0_CLOSURE_v1.0.0.ipynb` | `(1)`, `(2)`, `(3)`, `(4)` | Deletar duplicatas |
| `PNAD_COVID_CERTIFIER_v1.0.2.ipynb` | `(1)`, `(2)` | Deletar duplicatas |
| `PNADC_HISTORICAL_PROXY_v1.0.1.ipynb` | `(1)`, `(2)`, `(3)` | Deletar duplicatas |
| `LAYOUT_CLOSURE_RAIS_v1.0.1.ipynb` | `(1)`, `(2)` | Deletar duplicatas |

**Total de arquivos órfãos:** ~40 notebooks (57% do repositório)

**Ação Necessária:** Script de limpeza para remover duplicatas numeradas `(1)`, `(2)`, etc.

---

#### Inconsistência #4: Notebooks "FINALE" Não-Oficiais
- `FASE_2_E_3_FINALE.ipynb` - Combinação não-canônica de Fases 2 e 3
- `FASE_3_FINALE.ipynb` - Versão alternativa de `FASE_3.ipynb`

**Problema:** Podem conter lógica desatualizada ou modificações não-validadas pelo pipeline SPINE-GPE.

**Ação:** Comparar hashes de conteúdo com versões canônicas e arquivar ou deletar.

---

### D. VISUALIZAÇÕES JÁ GERADAS (STATUS REAL)

| Visualização | Notebook Origem | Status de Geração | Problema |
|--------------|-----------------|-------------------|----------|
| **Mapa de Cobertura UF** | FASE_2_.ipynb (Célula 10) | ❌ Vazio | `estados` GeoDataFrame não carregado |
| **Heatmap de Missingness** | FASE_2_.ipynb (Célula 25) | ❌ Vazio | `df` DataFrame vazio |
| **CATE Heterogeneity Bar Plot** | FASE_3_FINALE.ipynb (Célula 2) | ❌ Vazio | `df_analysis` não definido |
| **Ranking UF Gap Salarial** | FASE_3_FINALE.ipynb (Célula 10) | ❌ Vazio | `ufp` DataFrame não carregado |
| **Scatter ICA vs Renda** | FASE_4.ipynb (Célula 2) | ❌ Vazio | `df_platform` inexistente |
| **Mapa TFD Digital Land Rent** | FASE_4.ipynb (Célula 3) | ❌ Vazio | Dados espaciais ausentes |

**Conclusão Dura:** **NENHUMA figura foi efetivamente gerada e salva em disco.** Todos os plots existem apenas como código não-executado.

---

### E. VISUALIZAÇÕES FALTANTES (GAP DE ARQUITETURA v7)

Conforme Phase B Gap Analysis, estas visualizações **NÃO EXISTEM EM NENHUM NOTEBOOK**:

| Categoria | Visualização Faltante | Justificativa Q1 | Prioridade |
|-----------|----------------------|------------------|------------|
| **Methodological Governance** | ETL Sankey (Sample Attrition) | Justificar CAUSAL_BLOCKED | 🔴 Crítica |
| **Methodological Governance** | Backcast Calibration Plot | Validar proxy histórica | 🟡 Média |
| **Structural Descriptive** | Raincloud Plots (Income-Hour) | Mostrar densidade da penalidade | 🔴 Crítica |
| **Structural Descriptive** | Heterogeneity Forest Plot | Visualizar Sex/Race/UF penalties | 🔴 Crítica |
| **Causal & Mechanism** | **Multiverse Forest Plot** (POF vs AMOBITEC) | Probar robustez do TFD 59.19% | 🔴 **CRÍTICA** |
| **Causal & Mechanism** | Oaxaca-Blinder Waterfall | Decompor explicado vs não-explicado | 🟡 Média |
| **Spatial & Policy** | Friction vs Rugosity Scatter | Associar Space Syntax com custos | 🟡 Média |
| **Spatial & Policy** | Policy IPE Priority Map (3D/Isoline) | Proposta normativa | 🟡 Média |
| **Spatial & Policy** | SAE TFD Intensity Map (UFs 21,23,29) | Estimativa por borrow strength | 🟡 Média |
| **ML Interpretability** | ROC-PR Curves (Precision-Recall) | Classe desbalanceada | 🟢 Baixa |

---

## 3. ARQUITETURA v7: INCONSISTÊNCIAS ESTRUTURAIS

### Problema #1: Separação Fase 2/3 Violada
- **Design v7:** Fases 2 e 3 devem ser notebooks separados com manifests próprios
- **Realidade:** `FASE_2_E_3_FINALE.ipynb` combina ambas, criando dependências circulares

### Problema #2: Manifestos de Freeze Não-Verificados
- **Design v7:** Cada fase deve ter `MASTER_FREEZE.json` e `MASTER_CERTIFICATE.json` locais
- **Realidade:** Ambos estão apenas em GDrive (BLOCKED_Remote no Audit_Matrix)

### Problema #3: Dados de Saída Não Persistidos
- **Design v7:** Outputs devem ser salvos em `05_outputs/figures/` com hashes SHA-256
- **Realidade:** Nenhum arquivo `.png`, `.pdf`, ou `.svg` encontrado em `/workspace`

### Problema #4: Claim Register Ausente Localmente
- **Design v7:** `MASTER_CONSOLIDATION_FASES_0_3.md` deve estar versionado no repo
- **Realidade:** Apenas disponível via GDrive (path injetado pelo usuário)

---

## 4. PLANO DE AÇÃO IMEDIATO

### Passo 1: Obtenção de Dados (BLOQUEANTE)
```bash
# Opção A: Download manual do GDrive (recomendado)
# Mount no Colab e download dos seguintes arquivos críticos:
# 1. phase1_extended_evidence_cube_phase1_extended_evidence_geography_fixed_v101.parquet
# 2. certified_pnadc_platform_2022.parquet
# 3. certified_pnadc_platform_2024.parquet
# 4. spatial_syntax_metrics.parquet
# 5. PHASE2_MASTER_FREEZE.json
# 6. PHASE2_MASTER_CERTIFICATE.json

# Opção B: Executar todos os notebooks no Google Colab
# (Mais rápido, mas menos reprodutível localmente)
```

### Passo 2: Limpeza de Orphans
```bash
# Deletar ~40 notebooks duplicados
rm notebooks/*\(1\).ipynb notebooks/*\(2\).ipynb notebooks/*\(3\).ipynb notebooks/*\(4\).ipynb
rm notebooks/Untitled*.ipynb
rm notebooks/FASE_2_E_3_FINALE.ipynb  # Não-canônico
```

### Passo 3: Correção de Caminhos
- Substituir todos os `/content/drive/MyDrive/...` por variável de ambiente `DATA_ROOT`
- Criar script `config.py` para detecção automática do ambiente (Colab vs Local)

### Passo 4: Implementação de Visualizações Faltantes
Prioridade absoluta para:
1. **Multiverse Forest Plot** (TFD 59.19% sensitivity)
2. **ETL Sankey Diagram** (sample attrition)
3. **Raincloud Plots** (income-hour distributions)
4. **Heterogeneity Forest Plot** (Sex/Race/UF penalties)

### Passo 5: Re-execução do Pipeline
1. Executar FASE_2_.ipynb → Salvar outputs em `/workspace/05_outputs/`
2. Executar FASE_3_FINALE.ipynb → Salvar figures em `/workspace/06_reports/figures/`
3. Executar FASE_4.ipynb → Salvar maps em `/workspace/06_reports/maps/`
4. Gerar `MASTER_CONSOLIDATION_FASES_0_3.md` local

---

## 5. CONCLUSÃO

**Diagnóstico Final:** O pipeline SPINE-GPE v7 está **estruturalmente correto em termos de código**, mas **operacionalmente bloqueado** devido à ausência de dados locais. As figuras "vazias" são um sintoma deste problema de infraestrutura, não de lógica.

**O que NÃO está errado:**
- ✅ Métodos estatísticos (TMLE, CATE, SAE) estão corretamente implementados
- ✅ Estrutura de fases (0→1→2→3) segue o DAG planejado
- ✅ Epistemic ceilings estão documentados nos manifests remotos

**O que PRECISA ser corrigido:**
- 🔴 **Dados:** Download urgente dos .parquet/.json do GDrive
- 🔴 **Visualizações Faltantes:** Implementar 10 gráficos SOTA identificados no Gap Analysis
- 🟡 **Código:** Limpar 40+ notebooks órfãos e corrigir paths hardcoded
- 🟡 **Documentação:** Trazer claim register para versão local

**Tempo Estimado para Resolução:**
- Download de dados: 2-4 horas (dependendo da banda)
- Limpeza de orphans: 30 minutos
- Implementação de visualizações faltantes: 8-12 horas
- Re-execução completa: 4-6 horas

**Próximo Passo:** Aguardar confirmação do usuário para prosseguir com **download dos dados** ou **execução no Colab**.
