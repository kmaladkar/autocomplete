"""Run the click logs pipeline: load -> features -> vocabulary -> save."""

from pathlib import Path

import pandas as pd
import yaml

from autocomplete.pipeline.loader import ClickLogsLoader
from autocomplete.pipeline.features import FeatureEngineer


class FeaturePipeline:
    """Orchestrates loading click logs, building features and vocabulary, and writing outputs."""

    def __init__(self, config_path: str | Path) -> None:
        self._config_path = Path(config_path)
        self._config = self._load_config()

    def _load_config(self) -> dict:
        with open(self._config_path) as f:
            return yaml.safe_load(f)

    def run(self) -> None:
        """Load config, run pipeline, write features and vocabulary."""
        inp = self._config["input"]
        out = self._config["output"]
        opts = self._config.get("feature_options", {})

        loader = ClickLogsLoader(
            query_col=inp.get("query_col", "query"),
            clicked_col=inp.get("clicked_col", "clicked_suggestion"),
            position_col=inp.get("position_col", "position"),
            timestamp_col=inp.get("timestamp_col", "timestamp"),
        )
        logs = loader.load(inp["path"])

        time_decay = opts.get("time_decay_days", 30)
        engineer = FeatureEngineer(
            time_decay_days=time_decay,
            neg_per_pos=opts.get("neg_per_pos", 1),
            random_state=opts.get("random_state", 42),
        )
        query_stats = engineer.build_query_stats(logs)
        features = engineer.build_features(logs, query_stats)
        vocabulary = engineer.filter_vocabulary(
            query_stats,
            min_queries=opts.get("min_queries_for_trie", 2),
            top_k=opts.get("top_vocabulary_size"),
        )

        self._write_outputs(out, features, query_stats, vocabulary)
        print(f"Features: {out['features_path']}, vocabulary: {out['vocabulary_path']}")

    def _write_outputs(
        self,
        out: dict,
        features: pd.DataFrame,
        query_stats: pd.DataFrame,
        vocabulary: pd.DataFrame,
    ) -> None:
        Path(out["features_path"]).parent.mkdir(parents=True, exist_ok=True)
        Path(out["stats_path"]).parent.mkdir(parents=True, exist_ok=True)
        Path(out["vocabulary_path"]).parent.mkdir(parents=True, exist_ok=True)
        features.to_parquet(out["features_path"], index=False)
        query_stats.to_parquet(out["stats_path"], index=False)
        vocabulary.to_parquet(out["vocabulary_path"], index=False)


def run_pipeline(config_path: str | Path) -> None:
    """Convenience function: run the feature pipeline with the given config."""
    pipeline = FeaturePipeline(config_path)
    pipeline.run()


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="configs/pipeline.yaml", help="Pipeline config YAML")
    args = parser.parse_args()
    run_pipeline(args.config)
