# Best Model Analysis

This document details the selection of the best performing model within the TitanFlow pipeline. We ran three variations of the `LogisticRegression` model by adjusting hyperparameter combinations (Run 1, 2, and 3).

## Experiment Variations

1.  **Run 1 (C=0.01, max_iter=100, solver='lbfgs'):** High penalty (strong regularization).
2.  **Run 2 (C=1.0, max_iter=200, solver='lbfgs'):** Default, balanced regularization.
3.  **Run 3 (C=10.0, max_iter=300, solver='saga'):** Low penalty (weak regularization), different solver.

## Selection Process

The `evaluate_model` task in the Airflow DAG computes several metrics (`Accuracy`, `Precision`, `Recall`, `F1-Score`) and logs them directly to the MLflow tracking server under the `TitanFlow-Experiment`. 

By comparing these runs in the MLflow UI (at `http://localhost:5000`), we generally observe that **Run 2** or **Run 3** achieves the highest accuracy on the given Titanic dataset slice. 
*   The high regularization in **Run 1** (`C=0.01`) tends to underfit the data. 
*   **Run 2** (the standard balanced model) and **Run 3** perform significantly better by fully utilizing the engineered features (like `FamilySize` and categorical encodings).

### Pipeline Registration Logic

Regardless of manual metric comparisons, the automated pipeline utilizes an explicit quantitative threshold for "best" model selection:
*   If a run's `Accuracy >= 0.80`, the pipeline routes to the `register_model` task, actively promoting the model to the MLflow Model Registry under the name **TitanFlow-Model**.
*   If the model scores below `0.80`, it is routed to `reject_model` and explicitly tagged as "REJECTED" within the MLflow run metadata.
