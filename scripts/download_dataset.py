"""Download the airline delay dataset (2009-2018) from Kaggle into ./data.

Usage: python scripts/download_dataset.py
"""
import os
from pathlib import Path

# Keep the download inside the project instead of ~/.cache/kagglehub
PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.environ["KAGGLEHUB_CACHE"] = str(PROJECT_ROOT / "data")

import kagglehub

path = kagglehub.dataset_download(
    "yuanyuwendymu/airline-delay-and-cancellation-data-2009-2018"
)
print(f"Dataset downloaded to: {path}")
