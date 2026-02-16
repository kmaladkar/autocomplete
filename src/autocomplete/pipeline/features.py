"""Generate features from click logs for ML and trie vocabulary."""

import random
from datetime import datetime, timedelta, timezone

import pandas as pd


class FeatureEngineer:
    """Builds query stats, ML features, and trie vocabulary from click logs."""

    FEATURE_COLUMNS = [
        "query_len",
        "suggestion_len",
        "prefix_match_len",
        "position",
        "clicked_query_count",
        "clicked_sum_position",
        "clicked_mean_position",
        "clicked_ctr_approx",
        "prefix_query_count",
        "prefix_sum_position",
        "prefix_mean_position",
        "prefix_ctr_approx",
    ]

    def __init__(
        self,
        time_decay_days: int = 30,
        neg_per_pos: int = 1,
        random_state: int | None = None,
    ) -> None:
        self._time_decay_days = time_decay_days
        self._neg_per_pos = neg_per_pos
        self._random_state = random_state

    def _cutoff_timestamp(self) -> pd.Timestamp | None:
        if not self._time_decay_days:
            return None
        cutoff = datetime.now(timezone.utc) - timedelta(days=self._time_decay_days)
        return pd.Timestamp(cutoff)

    def build_query_stats(self, logs: pd.DataFrame) -> pd.DataFrame:
        """Aggregate per-query stats: count, clicks, CTR, position stats."""
        logs = logs.copy()
        cutoff = self._cutoff_timestamp()
        if cutoff is not None:
            logs = logs[logs["timestamp"] >= cutoff]

        agg = (
            logs.groupby("clicked_suggestion", as_index=False)
            .agg(
                query_count=("query", "count"),
                sum_position=("position", "sum"),
                mean_position=("position", "mean"),
            )
            .rename(columns={"clicked_suggestion": "query"})
        )
        agg["ctr_approx"] = 1.0 / (agg["mean_position"].clip(lower=1) + 1)
        return agg

    def _add_feature_columns(
        self,
        logs: pd.DataFrame,
        right_clicked: pd.DataFrame,
        right_prefix: pd.DataFrame,
        clicked_value: int,
    ) -> pd.DataFrame:
        """Add merged stats and derived features; set clicked to clicked_value."""
        df = logs.merge(
            right_clicked,
            left_on="clicked_suggestion",
            right_on="query",
            how="left",
        )
        df = df.drop(columns=["query_y"], errors="ignore")
        df = df.rename(columns={"query_x": "query"})
        df = df.merge(right_prefix, on="query", how="left")
        df["clicked"] = clicked_value
        df["query_len"] = df["query"].str.len()
        df["suggestion_len"] = df["clicked_suggestion"].str.len()
        df["prefix_match_len"] = df.apply(
            lambda r: sum(
                1
                for a, b in zip(str(r["query"]), str(r["clicked_suggestion"]))
                if a == b
            ),
            axis=1,
        )
        return df

    def build_features(
        self,
        logs: pd.DataFrame,
        query_stats: pd.DataFrame,
    ) -> pd.DataFrame:
        """Build training features: positives (clicked=1) and negative samples (clicked=0)."""
        logs = logs.copy()
        cutoff = self._cutoff_timestamp()
        if cutoff is not None:
            logs = logs[logs["timestamp"] >= cutoff]

        right_clicked = query_stats.rename(
            columns={c: f"clicked_{c}" for c in query_stats.columns if c != "query"}
        )
        right_prefix = query_stats.rename(
            columns={c: f"prefix_{c}" for c in query_stats.columns if c != "query"}
        )

        positives = self._add_feature_columns(
            logs[["query", "clicked_suggestion", "position"]].copy(),
            right_clicked,
            right_prefix,
            clicked_value=1,
        )

        vocabulary = query_stats["query"].tolist()
        if len(vocabulary) < 2 or self._neg_per_pos < 1:
            existing = [c for c in self.FEATURE_COLUMNS if c in positives.columns]
            out_cols = ["query", "clicked_suggestion", "clicked"] + existing
            return positives[[c for c in out_cols if c in positives.columns]].fillna(0)

        rng = random.Random(self._random_state)
        neg_rows = []
        for _, row in positives.iterrows():
            others = [s for s in vocabulary if s != row["clicked_suggestion"]]
            if not others:
                continue
            for _ in range(self._neg_per_pos):
                other = rng.choice(others)
                neg_rows.append({
                    "query": row["query"],
                    "clicked_suggestion": other,
                    "position": 0,
                })
        if not neg_rows:
            existing = [c for c in self.FEATURE_COLUMNS if c in positives.columns]
            out_cols = ["query", "clicked_suggestion", "clicked"] + existing
            return positives[[c for c in out_cols if c in positives.columns]].fillna(0)

        negatives = self._add_feature_columns(
            pd.DataFrame(neg_rows),
            right_clicked,
            right_prefix,
            clicked_value=0,
        )
        combined = pd.concat([positives, negatives], ignore_index=True)
        combined = combined.sample(frac=1, random_state=self._random_state)

        existing = [c for c in self.FEATURE_COLUMNS if c in combined.columns]
        out_cols = ["query", "clicked_suggestion", "clicked"] + existing
        return combined[[c for c in out_cols if c in combined.columns]].fillna(0)

    def filter_vocabulary(
        self,
        query_stats: pd.DataFrame,
        min_queries: int = 2,
        top_k: int | None = None,
    ) -> pd.DataFrame:
        """Filter query_stats to vocabulary for trie."""
        vocab = query_stats[query_stats["query_count"] >= min_queries].copy()
        if top_k is not None and top_k > 0:
            vocab = vocab.nlargest(top_k, "query_count")
        return vocab
