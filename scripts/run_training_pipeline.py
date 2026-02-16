#!/usr/bin/env python3
"""Training pipeline: load processed features -> train ranking model -> save model."""
import argparse
import os
import sys
from pathlib import Path

root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run training pipeline (features -> trained model)")
    parser.add_argument(
        "--config",
        type=Path,
        default=root / "configs" / "train.yaml",
        help="Training config YAML path",
    )
    args = parser.parse_args()

    os.chdir(root)

    (root / "models").mkdir(parents=True, exist_ok=True)

    from autocomplete.training import TrainingPipeline

    TrainingPipeline(args.config).run()
    print("Training pipeline done. Load trie with: python scripts/run_load_trie.py (with Redis up)")


if __name__ == "__main__":
    main()
