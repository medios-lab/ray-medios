# Distributed XGBoost Cover Type Example

This example trains a 7-class XGBoost classifier on the UCI Cover Type dataset (581,012 rows, 54 features) using Ray Train and Ray Data on a `ray-medios` cluster, then evaluates and resumes from checkpoints stored in a shared Medios store.

## Contents

- `Dockerfile` — Container image containing Ray `2.60.0`, XGBoost `3.0.5`, scikit-learn, PyArrow, pandas, matplotlib, seaborn, and JupyterLab (pinned with hashes in `requirements.txt` / `uv.lock`).
- `train_covtype.py` — Script runnable via the Ray Jobs API (`/api/jobs/`).
- `xgboost_covtype.ipynb` — Interactive Jupyter notebook running the same workflow.

## Build the Image

```sh
docker build -t ray-medios-xgboost-covtype:latest notebooks/xgboost-covtype
```

Push the image to a registry accessible by your Medios nodes and pass it to `ray-medios -image ...` along with a shared `-store <name>=/mnt/cluster_storage` mount (mounted read-write on both head and workers).

## Run as a Ray Job

Submit `train_covtype.py` to the cluster's dashboard URL logged by `ray-medios`:

```sh
RAY_ADDRESS=http://<head-address>:<dashboard-port> \
RAY_AUTH_MODE=token \
RAY_AUTH_TOKEN="$(cat auth.token)" \
ray job submit --working-dir notebooks/xgboost-covtype -- \
  python train_covtype.py /mnt/cluster_storage/ray-demo 2 2
```
