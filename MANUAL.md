## Instructions

- Combine all your work in one compressed folder. The folder must contain **only deliverable files** (no binaries, no exe files, etc.).
- All submissions will be done on **Google Classroom** within the deadline. Submissions other than Google Classroom (e.g., email, etc.) will **not** be accepted.
- The student is solely responsible for checking files for issues like corrupt files, viruses, and mistakenly executed sends. If the file cannot be downloaded from Google Classroom for any reason, it will lead to **zero marks**.
- Displayed output should be well-mannered and well-presented. Use appropriate **comments and indentation** in your source code.
- Be prepared for a **viva or anything else** after the submission of the assignment.
- If there is a **syntax error** in the code, zero marks will be awarded in that part.
- **Understanding the assignment** is also part of the assignment.
- **Zero marks** will be awarded to students involved in plagiarism. (Copying from the internet is the easiest way to get caught.)
- **Late submissions** will not be entertained, and no retake request will be accepted as per the course policy.

> **Tip:** For timely completion of the assignment, start as early as possible.  
> **Note:** Follow the given instructions; failing to do so will result in a zero.

---

## Q#1 (100 Marks)

Students will design and implement an **end-to-end Machine Learning pipeline** where:
- **Apache Airflow** orchestrates the workflow using a DAG
- **MLflow** manages experiment tracking and model registry

The pipeline will predict **survival** using the **Titanic dataset**:  
🔗 https://www.kaggle.com/datasets/yasserh/titanic-dataset/data

---

## Task 1 – DAG Design (10 Marks)

- Create a DAG with **parallel tasks** and **branching logic**.
- Demonstrate **no cyclic dependencies**.
- Submit a **DAG Graph View screenshot**.
- Explain **task dependencies** in a short description.

---

## Task 2 – Data Ingestion (10 Marks)

- Load the **Titanic CSV file**.
- Print the **dataset shape**.
- Log the **missing values count**.
- Push the **dataset path** using **XCom**.

---

## Task 3 – Data Validation (10 Marks)

- Check **missing percentage** in `Age` and `Embarked` columns.
- Raise an **exception** if missing percentage > **30%**.
- Demonstrate **retry behavior** with an intentional failure.

---

## Task 4 – Parallel Processing (10 Marks)

- Handle **missing values** for `Age` and `Embarked`.
- Perform **feature engineering**:
  - `FamilySize`
  - `IsAlone`
- Ensure the above tasks **execute in parallel**.

---

## Task 5 – Data Encoding (5 Marks)

- **Encode categorical variables**: `Sex`, `Embarked`.
- **Drop irrelevant columns**.

---

## Task 6 – Model Training with MLflow (15 Marks)

- Start an **MLflow run**.
- Log **model type** and **hyperparameters**.
- Train a **Logistic Regression** or **Random Forest** model.
- Log the **model artifact** and **dataset size**.

---

## Task 7 – Model Evaluation (10 Marks)

- Compute the following metrics:
  - Accuracy
  - Precision
  - Recall
  - F1-score
- **Log all metrics** to MLflow.
- **Push accuracy** using XCom.

---

## Task 8 – Branching Logic (10 Marks)

- If **Accuracy ≥ 0.80** → **Register model**
- Else → **Reject model**
- Use **`BranchPythonOperator`** for this logic.

---

## Task 9 – Model Registration (10 Marks)

- **Register** the approved model in the **MLflow Model Registry**.
- **Log rejection reason** if accuracy is below threshold.

---

## Task 10 – Experiment Comparison (10 Marks)

- Run the DAG **at least 3 times** with **different hyperparameters**.
- **Compare runs** in the MLflow UI.
- **Identify the best-performing model** and justify your choice.

---

## Deliverables

| # | Deliverable |
|---|-------------|
| 1 | DAG Python file: `mlops_airflow_mlflow_pipeline.py` |
| 2 | Titanic dataset (if not linked) |
| 3 | `requirements.txt` file |
| 4 | Screenshots of **Airflow UI** (Graph view, logs, retry evidence) |
| 5 | Screenshots of **MLflow UI** (experiments, runs comparison, model registry) |
| 6 | **Technical Report** (3–4 pages) — see details below |
| 7 | **GitHub repository link** with README instructions |

### Technical Report Must Include:
- Architecture explanation (Airflow + MLflow interaction)
- DAG structure and dependency explanation
- Experiment comparison analysis
- Failure and retry explanation
- Reflection questions on production deployment

---

## Submission Format

Submit a **ZIP folder** containing all the above deliverables.

---

## Marks Breakdown Summary

| Task | Description | Marks |
|------|-------------|-------|
| Task 1 | DAG Design | 10 |
| Task 2 | Data Ingestion | 10 |
| Task 3 | Data Validation | 10 |
| Task 4 | Parallel Processing | 10 |
| Task 5 | Data Encoding | 5 |
| Task 6 | Model Training with MLflow | 15 |
| Task 7 | Model Evaluation | 10 |
| Task 8 | Branching Logic | 10 |
| Task 9 | Model Registration | 10 |
| Task 10 | Experiment Comparison | 10 |
| **Total** | | **100** |
