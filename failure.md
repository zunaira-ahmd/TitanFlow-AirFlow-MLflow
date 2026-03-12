# FAILURE.md — Issues Encountered & Fixes Applied

---

## Failure 1 – `docker-compose.yaml` YAML Parse Error

**Error:** `yaml: construct errors: mapping key "container_name" already defined`
**Cause:** Accidental edit merged `mlflow:` service key + `image:` into the comment line.
**Fix:** Restored the two missing lines: `mlflow:` and `image: python:3.10-slim`.

---

## Failure 2 – `data_validation` Intentional Retry (Task 3 Requirement)

**Error:** `RuntimeError: [INTENTIONAL FAILURE] First attempt deliberately failed.`
**Cause:** Intentional — code raises on `try_number == 1` to demonstrate Airflow retry.
**Fix:** None needed. Airflow auto-retried after 20 s; attempt 2 passed.

---

## Failure 3 – `train_model` `ModuleNotFoundError: No module named 'mlflow'` (Runs 1–3)

**Error:** `ModuleNotFoundError: No module named 'mlflow'` inside Airflow task worker
**Root Cause:** `_PIP_ADDITIONAL_REQUIREMENTS` silently conflicted with pinned Airflow deps → `import mlflow` failed in every task. Also: MLflow container used `python:3.10-slim` and was still `pip install`-ing when tasks started.
**Fix:** Created `Dockerfile` extending `apache/airflow:2.9.2` that bakes `mlflow`, `scikit-learn`, `pandas`, `numpy` into the image at **build time** (`docker compose build`). Removed `_PIP_ADDITIONAL_REQUIREMENTS`. Switched MLflow service to official `ghcr.io/mlflow/mlflow:v2.11.1` image with a Python healthcheck; Airflow `depends_on: mlflow: service_healthy`.

---

## Failure 4 – `evaluate_model` `MlflowException: Run is already FINISHED` (Run 4)

**Error:** `MlflowException: Run with ID '...' is in 'FINISHED' status, expected 'RUNNING'`
**Root Cause:** `evaluate_model` called `mlflow.start_run(run_id=run_id)` to log metrics into the run created by `train_model`. Once `train_model`'s `with mlflow.start_run()` block exits, MLflow marks the run as FINISHED. Re-opening raises an exception.
**Fix (attempt 1):** Replaced `with mlflow.start_run(run_id=)` with `MlflowClient.log_metric()` calls — but this also failed because the MLflow **server** rejects writes to FINISHED runs at the REST API level.
**Fix (attempt 2 – final):** Call `client.update_run(run_id, "RUNNING")` first to reactivate the run, log metrics, then `client.update_run(run_id, "FINISHED")` to close it again. Same pattern applied to `reject_model`.

---

## Failure 5 – Pandas Version Conflict (non-fatal, build warning)

**Warning:** `apache-airflow-providers-google requires pandas<2.2, but pandas==2.2.1 installed`
**Fix:** Downgraded to `pandas==2.1.4` in `Dockerfile` (satisfies both MLflow and Airflow providers).

---

## Failure 6 — `train_model` still failing: `PermissionError /mlruns` persists after `--serve-artifacts`

**Error (from task logs — full traceback):**
```
File "mlflow/store/artifact/local_artifact_repo.py", line 60, in log_artifacts
    mkdir(artifact_dir)
PermissionError: [Errno 13] Permission denied: '/mlruns'
```

**Root Cause — Stale experiment `artifact_location` in MLflow database:**

Even after enabling `--serve-artifacts`, the existing `TitanFlow-Titanic-Survival` experiment in the MLflow database retained its original `artifact_location`:
```
artifact_location: /mlruns/1   ← file URI, set before --serve-artifacts was added
```

MLflow clients inherit the experiment's `artifact_location` for every new run. Since the URI was `file:///mlruns/...`, the client selected `LocalArtifactRepo` and attempted to write directly to `/mlruns`, **bypassing the HTTP proxy entirely**. The `--serve-artifacts` flag only applies to experiments created **after** it was enabled.

**Confirmed by:**
- MLflow traceback shows `local_artifact_repo.py` is being used (not the HTTP repo)
- The MLflow UI screenshot showed the run was created (connectivity works), but it failed in 5 seconds — exactly when `log_model` tried to write artifacts locally

**Fix:** Stopped the MLflow container, wiped the `./mlruns/` directory on the host to remove all stale experiment metadata, then restarted. When the DAG runs again, `mlflow.set_experiment()` will create a **fresh experiment** whose `artifact_location` is `mlflow-artifacts:/...` (the proxy URI), so all artifact uploads go through the server via HTTP.

```powershell
docker compose stop mlflow
Remove-Item -Recurse -Force mlruns\*
docker compose up -d --force-recreate mlflow
```

---

## Failure 7 – `KeyError: 'random_state'` in `train_model`

**Error** `KeyError: 'random_state'` — task crashes before training |
**Cause** Manual edit of `HYPERPARAMS` dict accidentally changed `"random_state"` → `"_state"`. |
**Fix** Corrected typo back to `"random_state": 42` in the `HYPERPARAMS` dict. |

---

## Failure 8 – `ValueError` in `evaluate_model`

**Error** `ValueError: Classification metrics can't handle a mix of unknown and binary targets` |
**Cause** `y_test` (from the CSV's `Survived` column) was loaded as a generic/object type, which sklearn's `accuracy_score` rejected when compared against `LogisticRegression`'s binary integer predictions. |
**Fix** Added `df["Survived"] = df["Survived"].astype(int)` to the `encode_data` task to ensure the target column is strictly an integer before splitting. |

---

## Failure 9 – Persistent `ValueError` in `evaluate_model`


**Error** `ValueError: Classification metrics can't handle a mix of unknown and binary targets` |
**Cause** `y_test` was evaluated as a pandas Series, which when pickled and compared against the numpy array `y_pred`, confused sklearn's `type_of_target()`, raising the error. |
**Fix** Forced both `y_test` and `y_pred` into flat numpy integer arrays using `np.array(y).ravel().astype(int)` before passing them to the metric functions.

## Failure 10 — `evaluate_model` failed: sklearn type mismatch

**Error (from task logs):**
```
ValueError: Classification metrics can't handle a mix of unknown and binary targets
  at: accuracy_score(y_test, y_pred)
```

**Root Cause:**

`y_test` is a **pandas Series** with a non-contiguous integer index (a subset of the original DataFrame rows after `train_test_split`). `y_pred` is a **numpy array**. sklearn's internal `type_of_target()` function inspects the object type and index and can classify one as `"binary"` and the other as `"unknown"` — triggering the ValueError even though both contain only `{0, 1}` values.

**Fix:** Explicitly cast both arrays to flat 1-D numpy int arrays before computing any metric:

```python
# Before (broken)
accuracy = accuracy_score(y_test, y_pred)

# After (fixed)
y_test_arr = np.array(y_test).ravel().astype(int)
y_pred_arr = np.array(y_pred).ravel().astype(int)
accuracy   = accuracy_score(y_test_arr, y_pred_arr)
```

`.ravel()` ensures 1-D shape. `.astype(int)` ensures consistent integer dtype. Diagnostic logging was also added to print `dtype`, `shape`, and `unique values` of both arrays so future type issues are immediately visible in the task logs.



