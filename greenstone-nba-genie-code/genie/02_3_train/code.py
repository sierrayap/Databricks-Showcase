# Databricks notebook source
# MAGIC %md
# MAGIC # 02_3_train — Genie Code writes this file
# MAGIC
# MAGIC Do not paste code into `02_Build_NBA_with_Genie_Code`. Ask Genie Code to implement **this** notebook, then re-run `%run ./genie/02_3_train/code` in `02`.

# COMMAND ----------

# DBTITLE 1,Train NBA classifier and log to MLflow
# 2.3 Train a fast CPU sklearn NBA classifier and log it to MLflow.
# Expects X, y, cat_cols, num_cols (from 02_2_features) and MODEL_NAME, EXPERIMENT_PATH, RANDOM_SEED (from _lab_config).
import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

for _name in ["X", "y", "cat_cols", "num_cols", "MODEL_NAME", "EXPERIMENT_PATH", "RANDOM_SEED"]:
    if _name not in globals():
        raise NameError(f"`{_name}` not found - run _lab_config and 02_2_features first.")

# Train / test split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=RANDOM_SEED, stratify=y
)

# Dense preprocessing (HistGradientBoostingClassifier does not accept sparse input)
preprocess = ColumnTransformer(
    transformers=[
        ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), cat_cols),
        ("num", "passthrough", num_cols),
    ],
    sparse_threshold=0,
)

model = Pipeline(
    steps=[
        ("preprocess", preprocess),
        ("clf", HistGradientBoostingClassifier(
            max_depth=4, max_iter=80, learning_rate=0.1, random_state=RANDOM_SEED
        )),
    ]
)

model.fit(X_train, y_train)

# Evaluate on the held-out test split
y_pred = model.predict(X_test)
y_proba = model.predict_proba(X_test)[:, 1]
test_auc = roc_auc_score(y_test, y_proba)

print(f"Train rows: {len(X_train):,} | Test rows: {len(X_test):,}")
print("\n=== Classification report (test) ===")
print(classification_report(y_test, y_pred, digits=3))
print(f"ROC AUC (test): {test_auc:.4f}")

# Log to MLflow
mlflow.set_tracking_uri("databricks")
mlflow.set_experiment(EXPERIMENT_PATH)

_input_example = X_train.head(5)
_signature = infer_signature(_input_example, model.predict(_input_example))

with mlflow.start_run(run_name="nba_hgb_classifier") as run:
    mlflow.log_params({
        "model_type": "HistGradientBoostingClassifier",
        "max_depth": 4,
        "max_iter": 80,
        "learning_rate": 0.1,
        "test_size": 0.2,
        "random_state": RANDOM_SEED,
    })
    mlflow.log_metric("test_roc_auc", test_auc)
    try:
        model_info = mlflow.sklearn.log_model(
            model,
            name="model",
            signature=_signature,
            input_example=_input_example,
            registered_model_name=MODEL_NAME,
        )
        print(f"\nRegistered model: {MODEL_NAME} (version {model_info.registered_model_version})")
    except Exception as e:
        print(f"\nRegistration under {MODEL_NAME} failed ({type(e).__name__}: {e}); logging without registering.")
        model_info = mlflow.sklearn.log_model(
            model, name="model", signature=_signature, input_example=_input_example
        )

run_id = run.info.run_id
model_uri = model_info.model_uri
print(f"MLflow run_id: {run_id}")
print(f"Model URI: {model_uri}")