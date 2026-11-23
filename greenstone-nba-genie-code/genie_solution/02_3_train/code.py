# Databricks notebook source
# MAGIC %md
# MAGIC # 02_3_train (presenter solution)

# COMMAND ----------

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=RANDOM_SEED, stratify=y
)

preprocess = ColumnTransformer(
    transformers=[
        ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), cat_cols),
        ("num", "passthrough", num_cols),
    ],
    sparse_threshold=0,
)

model = Pipeline(
    steps=[
        ("prep", preprocess),
        (
            "clf",
            HistGradientBoostingClassifier(
                max_depth=4,
                max_iter=80,
                learning_rate=0.1,
                random_state=RANDOM_SEED,
            ),
        ),
    ]
)

model.fit(X_train, y_train)
proba = model.predict_proba(X_test)[:, 1]
preds = (proba >= 0.5).astype(int)
print(classification_report(y_test, preds))
print("ROC AUC:", round(roc_auc_score(y_test, proba), 3))

mlflow.set_tracking_uri("databricks")
mlflow.set_experiment(EXPERIMENT_PATH)
with mlflow.start_run(run_name="nba_hist_gradient_boosting") as run:
    mlflow.log_param("model_type", "HistGradientBoostingClassifier")
    mlflow.log_param("max_iter", 80)
    mlflow.log_metric("roc_auc", float(roc_auc_score(y_test, proba)))
    mlflow.sklearn.log_model(model, artifact_path="model", registered_model_name=MODEL_NAME)
    print("Run ID:", run.info.run_id)
    print("Registered model:", MODEL_NAME)
