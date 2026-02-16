#!/usr/bin/env python3
"""Feature engineering pipeline: load raw click logs -> build features + vocabulary -> save to processed."""
import argparse
import os
import sys
from pathlib import Path

root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run feature engineering pipeline (click logs -> features + vocabulary)")
    parser.add_argument(
        "--config",
        type=Path,
        default=root / "configs" / "pipeline.yaml",
        help="Pipeline config YAML path",
    )
    args = parser.parse_args()

    os.chdir(root)

    (root / "data" / "raw").mkdir(parents=True, exist_ok=True)
    (root / "data" / "processed").mkdir(parents=True, exist_ok=True)

    from autocomplete.pipeline import FeaturePipeline

    FeaturePipeline(args.config).run()
    print("Feature pipeline done. Next: run_training_pipeline.py")


if __name__ == "__main__":
    main()
