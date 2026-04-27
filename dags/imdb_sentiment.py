"""
IMDB Sentiment — Airflow DAG
=============================
Converts mlflow_ex1.py into a TaskFlow DAG.

Stages:
  1. prepare_data      – build inline dataset, train/test split
  2. train_*           – one @task per candidate model (parallel)
  3. find_best         – pick highest F1 across all three runs
  4. promote_champion  – set 'champion' alias in MLflow Model Registry
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

import mlflow
import mlflow.sklearn
from mlflow import MlflowClient
from mlflow.models.signature import infer_signature
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.svm import LinearSVC

from airflow.decorators import dag, task
from airflow.models import Variable

MLFLOW_TRACKING_URI = Variable.get("MLFLOW_TRACKING_URI", "http://mlflow:5001")
EXPERIMENT_NAME     = "imdb-sentiment-traditional"
MODEL_NAME          = "imdb-sentiment_Shetty"

logger = logging.getLogger(__name__)


@dag(
    dag_id       = "imdb_sentiment",
    description  = "Train three TF-IDF sentiment models and promote the best one",
    schedule     = None,                  # trigger manually
    start_date   = datetime(2026, 4, 1),
    catchup      = False,
    default_args = {
        "owner":        "ml-team",
        "retries":      1,
        "retry_delay":  timedelta(minutes=2),
    },
    tags = ["mlops", "nlp", "mlflow"],
)
def imdb_sentiment_pipeline():

    # ── 1. Prepare data ───────────────────────────────────────────────────────

    @task()
    def prepare_data() -> dict:
        reviews = [
            "This movie was absolutely fantastic! I loved every minute.",
            "Brilliant performances and a gripping storyline. Highly recommend.",
            "One of the best films I have ever seen. A masterpiece.",
            "Great direction and superb acting. Will watch again.",
            "Incredible movie with a beautiful ending. Loved it.",
            "Terrible film. Boring, slow, and a complete waste of time.",
            "Awful acting and a nonsensical plot. I want my money back.",
            "One of the worst movies ever made. Do not watch.",
            "Dull and predictable. Fell asleep halfway through.",
            "Disappointing in every way. The script was a disaster.",
        ]
        labels = [1, 1, 1, 1, 1, 0, 0, 0, 0, 0]

        X_train, X_test, y_train, y_test = train_test_split(
            reviews, labels, test_size=0.3, random_state=42
        )
        return {
            "X_train": X_train,
            "X_test":  X_test,
            "y_train": y_train,
            "y_test":  y_test,
        }

    # ── 2. Train helpers ──────────────────────────────────────────────────────

    def _run_candidate(run_name: str, pipeline: Pipeline, params: dict, data: dict) -> dict:
        """Shared training logic — called by each model-specific task."""
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_registry_uri(MLFLOW_TRACKING_URI)

        # Route artifact uploads through the server (avoids direct filesystem access).
        # Required when MLflow runs with --serve-artifacts.
        os.environ["MLFLOW_TRACKING_URI"] = MLFLOW_TRACKING_URI
        os.environ["MLFLOW_ARTIFACT_UPLOAD_DOWNLOAD_TIMEOUT"] = "300"
        mlflow.set_experiment(EXPERIMENT_NAME)

        X_train = data["X_train"]
        X_test  = data["X_test"]
        y_train = data["y_train"]
        y_test  = data["y_test"]

        with mlflow.start_run(run_name=run_name) as run:
            pipeline.fit(X_train, y_train)
            preds = pipeline.predict(X_test)

            metrics = {
                "accuracy": accuracy_score(y_test, preds),
                "f1_score": f1_score(y_test, preds, zero_division=0),
            }

            mlflow.log_params(params)
            mlflow.log_metrics(metrics)

            signature = infer_signature(X_train, pipeline.predict(X_train))
            mlflow.sklearn.log_model(
                sk_model=pipeline,
                artifact_path="model",
                registered_model_name=MODEL_NAME,
                signature=signature,
            )

            logger.info("%s | acc=%.4f | f1=%.4f", run_name, metrics["accuracy"], metrics["f1_score"])
            return {"run_id": run.info.run_id, "run_name": run_name, **metrics}

    # ── 3. One @task per candidate (runs in parallel) ─────────────────────────

    @task()
    def train_logistic(data: dict) -> dict:
        return _run_candidate(
            run_name = "tfidf-logistic",
            pipeline = Pipeline([
                ("tfidf", TfidfVectorizer(max_features=5000, ngram_range=(1, 2))),
                ("clf",   LogisticRegression(max_iter=1000)),
            ]),
            params = {"vectorizer": "tfidf", "model": "LogisticRegression",
                      "max_features": 5000, "ngram_range": "(1,2)"},
            data = data,
        )

    @task()
    def train_linearsvc(data: dict) -> dict:
        return _run_candidate(
            run_name = "tfidf-linearsvc",
            pipeline = Pipeline([
                ("tfidf", TfidfVectorizer(max_features=5000, ngram_range=(1, 2))),
                ("clf",   LinearSVC()),
            ]),
            params = {"vectorizer": "tfidf", "model": "LinearSVC",
                      "max_features": 5000, "ngram_range": "(1,2)"},
            data = data,
        )

    @task()
    def train_randomforest(data: dict) -> dict:
        return _run_candidate(
            run_name = "tfidf-randomforest",
            pipeline = Pipeline([
                ("tfidf", TfidfVectorizer(max_features=3000)),
                ("clf",   RandomForestClassifier(n_estimators=100, random_state=42)),
            ]),
            params = {"vectorizer": "tfidf", "model": "RandomForest",
                      "max_features": 3000, "n_estimators": 100},
            data = data,
        )

    # ── 4. Find best ──────────────────────────────────────────────────────────

    @task()
    def find_best(results: list[dict]) -> dict:
        """
        Receives a list of {run_id, run_name, accuracy, f1_score} dicts
        from all three training tasks and returns the one with the highest F1.
        """
        best = max(results, key=lambda r: r["f1_score"])
        logger.info("Best model: %s  (F1=%.4f)", best["run_name"], best["f1_score"])
        return best

    # ── 5. Promote champion ───────────────────────────────────────────────────

    @task()
    def promote_champion(best: dict) -> None:
        """
        Finds the registered model version that matches the winning run_id
        and assigns the 'champion' alias so serving code can always load
        models.load_model("models:/imdb-sentiment@champion").
        """
        client = MlflowClient(MLFLOW_TRACKING_URI)

        versions = client.search_model_versions(f"name='{MODEL_NAME}'")
        best_version = next(
            v.version for v in versions if v.run_id == best["run_id"]
        )

        client.set_registered_model_alias(MODEL_NAME, "champion", version=best_version)
        logger.info(
            "Alias 'champion' -> version %s (%s)",
            best_version, best["run_name"],
        )

    # ── Wire the DAG ──────────────────────────────────────────────────────────

    data = prepare_data()

    result_lr  = train_logistic(data)
    result_svc = train_linearsvc(data)
    result_rf  = train_randomforest(data)

    best = find_best([result_lr, result_svc, result_rf])
    promote_champion(best)


imdb_sentiment_pipeline()

