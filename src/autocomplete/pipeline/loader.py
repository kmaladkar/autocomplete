"""Load and normalize click logs."""

from pathlib import Path

import pandas as pd


class ClickLogsLoader:
    """Loads click logs from CSV and normalizes columns to a standard schema."""

    DEFAULT_QUERY_COL = "query"
    DEFAULT_CLICKED_COL = "clicked_suggestion"
    DEFAULT_POSITION_COL = "position"
    DEFAULT_TIMESTAMP_COL = "timestamp"

    def __init__(
        self,
        query_col: str = DEFAULT_QUERY_COL,
        clicked_col: str = DEFAULT_CLICKED_COL,
        position_col: str = DEFAULT_POSITION_COL,
        timestamp_col: str = DEFAULT_TIMESTAMP_COL,
    ) -> None:
        self._query_col = query_col
        self._clicked_col = clicked_col
        self._position_col = position_col
        self._timestamp_col = timestamp_col

    def load(self, path: str | Path) -> pd.DataFrame:
        """Load click logs from path and return normalized DataFrame."""
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Click logs not found: {path}")

        df = pd.read_csv(path)
        required = {
            self._query_col,
            self._clicked_col,
            self._position_col,
            self._timestamp_col,
        }
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns: {missing}")

        df = df.rename(
            columns={
                self._query_col: "query",
                self._clicked_col: "clicked_suggestion",
                self._position_col: "position",
                self._timestamp_col: "timestamp",
            }
        )

        df["query"] = df["query"].astype(str).str.strip().str.lower()
        df["clicked_suggestion"] = (
            df["clicked_suggestion"].astype(str).str.strip().str.lower()
        )
        df["position"] = pd.to_numeric(df["position"], errors="coerce").fillna(0).astype(int)
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["query", "timestamp"])
        return df


def load_click_logs(
    path: str | Path,
    query_col: str = "query",
    clicked_col: str = "clicked_suggestion",
    position_col: str = "position",
    timestamp_col: str = "timestamp",
) -> pd.DataFrame:
    """Convenience function: load click logs with given column names."""
    loader = ClickLogsLoader(
        query_col=query_col,
        clicked_col=clicked_col,
        position_col=position_col,
        timestamp_col=timestamp_col,
    )
    return loader.load(path)
