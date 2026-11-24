"""Distributed XGBoost training on the UCI Cover Type dataset with Ray Train.

Usage: python train_covtype.py <storage root> <training workers> <CPUs per training worker>

The storage root is a directory that every Ray node's task mounts at the same
path. The script writes the dataset to <storage root>/covtype/ once and Ray
Train checkpoints to <storage root>/checkpoints/.

The driver is this process; it connects to the cluster named by RAY_ADDRESS,
which a Ray job has set. The dataset preparation and the validation scoring
run in Ray tasks, so the driver itself reads nothing from the storage root.
"""
import os
import sys
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import ray
import ray.data as rd
import xgboost as xgb
from ray.data import ActorPoolStrategy
from ray.train import (CheckpointConfig, FailureConfig, RunConfig, ScalingConfig, get_checkpoint, get_context,
                       get_dataset_shard)
from ray.train.xgboost import RayTrainReportCallback, XGBoostTrainer

STORAGE_ROOT = sys.argv[1]
NUM_WORKERS = int(sys.argv[2])
CPUS_PER_WORKER = int(sys.argv[3])
PARQUET_DIR = os.path.join(STORAGE_ROOT, "covtype")
TRAIN_PARQUET = os.path.join(PARQUET_DIR, "train.parquet")
VAL_PARQUET = os.path.join(PARQUET_DIR, "val.parquet")
INDEX_COLS = {"__index_level_0__"}


@ray.remote(num_cpus=1)
def prepare_dataset():
    """Download Cover Type and write a stratified 80/20 split as Parquet."""
    from sklearn.datasets import fetch_covtype
    from sklearn.model_selection import train_test_split

    if os.path.exists(TRAIN_PARQUET) and os.path.exists(VAL_PARQUET):
        return "already present"
    df = fetch_covtype(as_frame=True).frame
    df.rename(columns={"Cover_Type": "label"}, inplace=True)
    df["label"] = df["label"] - 1  # classes 1-7 become 0-6
    assert df["label"].between(0, 6).all()
    train_df, val_df = train_test_split(df, test_size=0.2, random_state=42, stratify=df["label"])
    os.makedirs(PARQUET_DIR, exist_ok=True)
    train_df.to_parquet(TRAIN_PARQUET, index=False)
    val_df.to_parquet(VAL_PARQUET, index=False)
    return f"wrote {len(train_df):,} train rows and {len(val_df):,} validation rows"


def _arrow_table_from_shard(name):
    tables = [ray.get(r) for r in get_dataset_shard(name).materialize().to_arrow_refs()]
    tbl = pa.concat_tables(tables, promote_options="none") if tables else pa.table({})
    return tbl.select([c for c in tbl.column_names if c not in INDEX_COLS])


def _dmat_from_arrow(table, feature_cols, label_col):
    X = np.column_stack([table[c].to_numpy(zero_copy_only=False) for c in feature_cols])
    return xgb.DMatrix(X, label=table[label_col].to_numpy(zero_copy_only=False), feature_names=feature_cols)


def train_func(config):
    label_col = config["label_column"]
    train_arrow = _arrow_table_from_shard("train")
    eval_arrow = _arrow_table_from_shard("evaluation")
    feature_cols = [c for c in train_arrow.column_names if c != label_col]
    dtrain = _dmat_from_arrow(train_arrow, feature_cols, label_col)
    deval = _dmat_from_arrow(eval_arrow, feature_cols, label_col)

    booster = None
    ckpt = get_checkpoint()
    if ckpt:
        with ckpt.as_directory() as d:
            booster = xgb.Booster()
            booster.load_model(os.path.join(d, RayTrainReportCallback.CHECKPOINT_NAME))
            print(f"[Rank {get_context().get_world_rank()}] Resumed from checkpoint")

    xgb.train(
        params=config["params"],
        dtrain=dtrain,
        evals=[(dtrain, "train"), (deval, "validation")],
        num_boost_round=config["num_boost_round"],
        xgb_model=booster,
        # Ray 2.60.0 takes the checkpoint frequency here, not in CheckpointConfig.
        callbacks=[RayTrainReportCallback(frequency=10)],
    )


class XGBPredictor:
    """Loads the Booster once per actor and scores batches."""

    def __init__(self, ckpt, feature_cols):
        self.model = RayTrainReportCallback.get_model(ckpt)
        self.feature_cols = feature_cols

    def __call__(self, batch: pd.DataFrame) -> pd.DataFrame:
        preds = np.argmax(self.model.predict(xgb.DMatrix(batch[self.feature_cols])), axis=1)
        return pd.DataFrame({"pred": preds.astype(np.int32), "label": batch["label"].astype(np.int32)})


def validation_accuracy(val_ds, ckpt, feature_columns):
    pred_ds = val_ds.map_batches(XGBPredictor, fn_constructor_args=(ckpt, feature_columns), batch_format="pandas",
                                 compute=ActorPoolStrategy(), num_cpus=1)
    stats = pred_ds.map_batches(
        lambda df: pd.DataFrame({"correct": [int((df["pred"].to_numpy() == df["label"].to_numpy()).sum())],
                                 "n": [int(len(df))]}),
        batch_format="pandas")
    return int(stats.sum("correct")) / int(stats.sum("n"))


def main():
    started = time.time()
    ray.init(logging_level="ERROR")
    print("RAY", ray.__version__, "CLUSTER", {k: v for k, v in ray.cluster_resources().items() if k in ("CPU", "memory")})
    print("PREPARE", ray.get(prepare_dataset.remote()), f"{time.time() - started:.0f}s", flush=True)

    train_ds = rd.read_parquet(TRAIN_PARQUET).random_shuffle()
    val_ds = rd.read_parquet(VAL_PARQUET)
    print("ROWS train", train_ds.count(), "validation", val_ds.count(), flush=True)
    feature_columns = [c for c in val_ds.schema().names if c != "label"]

    trainer = XGBoostTrainer(
        train_func,
        scaling_config=ScalingConfig(num_workers=NUM_WORKERS, use_gpu=False,
                                     resources_per_worker={"CPU": CPUS_PER_WORKER}),
        datasets={"train": train_ds, "evaluation": val_ds},
        train_loop_config={
            "label_column": "label",
            "params": {"objective": "multi:softprob", "num_class": 7, "eval_metric": "mlogloss",
                       "tree_method": "hist", "eta": 0.3, "max_depth": 8, "nthread": CPUS_PER_WORKER},
            "num_boost_round": 50,
        },
        run_config=RunConfig(
            name="covtype_xgb_cpu",
            storage_path=os.path.join(STORAGE_ROOT, "checkpoints"),
            checkpoint_config=CheckpointConfig(num_to_keep=1,
                                               checkpoint_score_attribute="validation-mlogloss",
                                               checkpoint_score_order="min"),
            failure_config=FailureConfig(max_failures=1),
        ),
    )
    for label in ("FIRST_FIT", "SECOND_FIT"):  # the second fit resumes from the first one's checkpoint
        t = time.time()
        result = trainer.fit()
        print(label, f"{time.time() - t:.0f}s", "validation-mlogloss", result.metrics.get("validation-mlogloss"),
              "checkpoint", result.checkpoint.path, flush=True)
        print(label, "VALIDATION_ACCURACY", f"{validation_accuracy(val_ds, result.checkpoint, feature_columns):.3f}",
              flush=True)
    print("TOTAL", f"{time.time() - started:.0f}s")


if __name__ == "__main__":
    main()
