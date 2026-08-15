#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

SCRIPT = Path(__file__).with_name("SPINE_GPEv7_PNADC_HISTORICAL_BACKCAST_HARDENING_v1.2.0.py")
spec = importlib.util.spec_from_file_location("hardening_v120", SCRIPT)
assert spec and spec.loader
hardening = importlib.util.module_from_spec(spec)
sys.modules["hardening_v120"] = hardening
spec.loader.exec_module(hardening)


def make_cells(year: int, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows = []
    occs = ["8322", "9333", "5223", "9112", "9621", "5120"]
    acts = ["53002", "56112", "49302", "47113", "82997"]
    positions = ["1", "2", "5", "6", "9"]
    for stratum in range(1, 16):
        for psu in range(1, 25):
            for _ in range(4):
                occ = rng.choice(occs)
                act = rng.choice(acts)
                pos = rng.choice(positions)
                weight = rng.uniform(20, 120)
                score = -5.2 + 2.1 * (occ in {"8322", "9333", "9621"}) + 1.7 * (act in {"53002", "56112"}) + .8 * (pos in {"5", "6"}) + (.15 if year == 2024 else 0)
                rate = 1 / (1 + np.exp(-score))
                positive = weight * rate
                rows.append({
                    "year": year,
                    "survey_stratum": str(stratum),
                    "survey_psu": str(psu),
                    "occupation_code": occ,
                    "activity_code": act,
                    "position_code": pos,
                    "weight_total": weight,
                    "weight_positive": positive,
                    "n": 10,
                    "n_positive": max(0, int(round(rate * 10))),
                })
    return pd.DataFrame(rows)


def make_historical(seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows = []
    for period in ["2019q1", "2020q2", "2021q4"]:
        for stratum in range(1, 12):
            for psu in range(1, 18):
                for _ in range(3):
                    rows.append({
                        "source_period": period,
                        "survey_stratum": str(stratum),
                        "survey_psu": str(psu),
                        "occupation_code": rng.choice(["8322", "9333", "5223", "9112", "9621", "5120"]),
                        "activity_code": rng.choice(["53002", "56112", "49302", "47113", "82997"]),
                        "position_code": rng.choice(["1", "2", "5", "6", "9"]),
                        "weight_total": rng.uniform(20, 120),
                        "old_probability_weighted": 0.0,
                        "n": 8,
                    })
    return pd.DataFrame(rows)


def main() -> int:
    cells22 = make_cells(2022, 22)
    cells24 = make_cells(2024, 24)
    pooled = pd.concat([cells22, cells24], ignore_index=True)

    b22 = hardening.fit_mapping_bundle(cells22, "mapping_2022", n_splits=3)
    b24 = hardening.fit_mapping_bundle(cells24, "mapping_2024", n_splits=3)
    bp = hardening.fit_mapping_bundle(pooled, "mapping_pooled", n_splits=3)
    p24 = hardening.bundle_predict(b22, cells24)
    metric = hardening.evaluate_predictions("mapping_2022", 2022, 2024, cells24, p24)
    assert metric.roc_auc is not None and 0 <= metric.roc_auc <= 1
    assert metric.calibration_status.startswith("ESTIMATED"), metric
    assert metric.calibration_slope is not None and np.isfinite(metric.calibration_slope)
    assert np.all((p24 >= 0) & (p24 <= 1))

    f22 = hardening.feature_cells_from_psu(cells22, direct=True)
    f24 = hardening.feature_cells_from_psu(cells24, direct=True)
    fp = hardening.feature_cells_from_psu(pooled, direct=True)
    hist = make_historical(99)
    fh = hardening.feature_cells_from_psu(hist, direct=False)

    mca = hardening.fit_weighted_mca(fp, n_components=4)
    support, thresholds = hardening.support_mapping(f22, f24, fp, fh, mca)
    assert len(support) == len(fh)
    assert set(support["support_class"].astype(str)).issubset(set(hardening.SUPPORT_ORDER))
    assert thresholds["q75"] <= thresholds["q95"] <= thresholds["q99"]

    cluster, selection, stability = hardening.choose_and_fit_clusters(fp, mca["coordinates"], 3, 4)
    assert cluster.n_clusters in {3, 4}
    assert not selection.empty and not stability.empty

    best_k, knn_metrics = hardening.temporal_knn_benchmark({2022: cells22, 2024: cells24}, mca, (3, 5, 10))
    assert not knn_metrics.empty
    knn = hardening.fit_knn_pooled(pooled, mca, best_k)
    fmap, estimates, support_summary, divergence = hardening.historical_predictions_and_estimates(
        hist,
        {"mapping_2022": b22, "mapping_2024": b24, "mapping_pooled": bp},
        support,
        cluster,
        mca,
        knn,
    )
    assert not fmap.empty
    assert len(estimates) == 3
    assert len(support_summary) == 3 * len(hardening.SUPPORT_ORDER)
    assert {"weighted_pearson_probability", "top10_weighted_overlap"}.issubset(divergence.columns)
    aggregate = hardening.aggregate_knn_stability(divergence)
    assert len(aggregate) == 1

    period_weights = hist.groupby(["source_period", *hardening.FEATURES], observed=True).agg(weight_total=("weight_total", "sum")).reset_index()
    with tempfile.TemporaryDirectory() as temp:
        checkpoint = Path(temp) / "bootstrap.csv"
        raw1, summary1 = hardening.bootstrap_model_uncertainty(
            pooled, fh, period_weights, reps=2, n_splits=2,
            logger=hardening.setup_logger(False), checkpoint_path=checkpoint,
            resume=False, checkpoint_every=1, seed=123,
        )
        assert raw1["replicate"].nunique() == 2
        raw2, summary2 = hardening.bootstrap_model_uncertainty(
            pooled, fh, period_weights, reps=3, n_splits=2,
            logger=hardening.setup_logger(False), checkpoint_path=checkpoint,
            resume=True, checkpoint_every=1, seed=123,
        )
        assert raw2["replicate"].nunique() == 3
        assert not summary1.empty and not summary2.empty
        assert {"model_total_cv", "model_share_cv"}.issubset(summary2.columns)

    final = hardening.combine_uncertainty(estimates, summary2)
    assert "total_ci95_lower_survey_plus_model" in final
    args = hardening.apply_profile_defaults(hardening.build_parser().parse_args(["--root", "/tmp", "--profile", "publication"]))
    assert args.bootstrap_reps == 500
    print("TEST PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
