# Airflow Task Dependencies and DAG Structure

This document outlines the execution flow and dependencies of the `titan_flow` Apache Airflow data pipeline.

## DAG Structure Overview

The data pipeline follows a classic Extraction, Transformation, and Machine Learning Model Training (ETL + ML) flow. It features a parallel processing branch for feature imputation designed specifically to optimize execution time while isolating logic.

## Dependency Graph

```text
ingest_data 
    └── validate_data 
            ├── handle_missing_age       (parallel branch)
            ├── handle_missing_embarked  (parallel branch)
            │        └── encode_features (join point)
            │                └── train_model
            │                        └── evaluate_model
            │                                └── branch_on_accuracy
            │                                        ├── register_model
            │                                        └── reject_model
                                                             └── end
```

## Detailed Task Breakdown

1.  **`ingest_data`**: Reads the raw `Titanic-Dataset.csv` from the mounted data volume and outputs basic descriptive statistics to the logs.
2.  **`validate_data`**: Checks for critical missing data thresholds (specifically enforcing that missing columns don't exceed `30%`). 
    *   *Note:* This includes an intentional failure trigger on the first run (`try_number == 1`) to demonstrate Airflow's retry handling.
3.  **Parallel Imputation**:
    *   **`handle_missing_age`**: Fills missing `Age` values with the median and engineers new `FamilySize` features. Runs independently.
    *   **`handle_missing_embarked`**: Fills missing `Embarked` values with the statistical mode. Runs independently.
4.  **`encode_features`**: This task represents the "Join" operation. It explicitly waits for both parallel imputation branches to successfully complete. It then merges the resulting temporary DataFrames, applies binary and label encoding to categorical variables (`Sex`, `Embarked`), and drops unused high-cardinality columns.
5.  **`train_model`**: Splits the fully numeric data into training and test sets. It then trains a `LogisticRegression` model based on designated hyperparameter overrides and explicitly logs the parameters, performance, and model artifact to the connected MLflow server.
6.  **`evaluate_model`**: Evaluates the fitted model on the holdout test data split, calculates four key metrics (`Accuracy`, `Precision`, `Recall`, `F1-Score`), and integrates these metrics back into the active MLflow run metadata.
7.  **`branch_on_accuracy`**: An Airflow `BranchPythonOperator` that dictates and delegates the next step based solely on the calculated `Accuracy`.
8.  **Model Registration Routing**:
    *   **`register_model`**: Executed *only* if `Accuracy >= 0.80`. Completes the loop by registering the high-performing model into the MLflow Model Registry for production deployment.
    *   **`reject_model`**: Executed *only* if `Accuracy < 0.80`. Aborts model deployment by tagging the MLflow run as `REJECTED`.
9.  **`end`**: A dummy/empty task configured with the `NONE_FAILED_MIN_ONE_SUCCESS` trigger rule, representing clear and successful traversal of the graph regardless of which routing the branching task selected.
