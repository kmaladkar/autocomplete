"""Train a ranking model on pipeline features."""

from pathlib import Path

import joblib
import pandas as pd
import yaml
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split


class TrainingPipeline:
    """Loads features, trains a ranking model, and persists the model and metadata."""

    def __init__(self, config_path: str | Path) -> None:
        self._config_path = Path(config_path)
        self._config = self._load_config()

    def _load_config(self) -> dict:
        with open(self._config_path) as f:
            return yaml.safe_load(f)

    def run(self) -> None:
        """Load data, train model, save model and metadata."""
        path = Path(self._config["train_data_path"])
        df = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_csv(path)

        target = self._config["target_column"]
        feature_cols = self._config.get("feature_columns")
        if feature_cols is None:
            feature_cols = [
                c
                for c in df.columns
                if c not in (target, "query", "clicked_suggestion")
            ]

        X = df[feature_cols].fillna(0)
        y = df[target]

        X_train, X_val, y_train, y_val = train_test_split(
            X,
            y,
            test_size=self._config.get("validation_split", 0.2),
            random_state=self._config.get("random_state", 42),
        )

        params = self._config.get("model_params", {}) or {
            "n_estimators": 100,
            "random_state": 42,
        }
        model = RandomForestClassifier(**params)
        model.fit(X_train, y_train)
        score = model.score(X_val, y_val)

        out = Path(self._config["model_output_path"])
        out.mkdir(parents=True, exist_ok=True)
        joblib.dump(model, out / "model.joblib")
        meta = {"target_column": target, "feature_columns": feature_cols}
        with open(out / "metadata.yaml", "w") as f:
            yaml.dump(meta, f)

        print(f"Validation score: {score:.4f}, model saved to {out}")


def run_training(config_path: str | Path) -> None:
    """Convenience function: run the training pipeline with the given config."""
    pipeline = TrainingPipeline(config_path)
    pipeline.run()


def main() -> None:
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="configs/train.yaml")
    run_training(p.parse_args().config)
