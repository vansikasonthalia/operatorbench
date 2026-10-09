"""
OperatorBench — Dataset Preparation
Run once to freeze both samples to versioned CSV.
Never resample after this.

Usage: python scripts/prep_datasets.py
"""

import pandas as pd
import hashlib
import json
import datetime
from pathlib import Path
from datasets import load_dataset

ROOT = Path(__file__).resolve().parent.parent
SEED = 42
N_PER_CLASS = 250  # 500 total per domain


def prep_reviews():
    """IMDB test set -> 500 balanced reviews (250 neg + 250 pos)."""
    print("Loading IMDB dataset from HuggingFace...")
    ds = load_dataset("imdb", split="test")
    df = pd.DataFrame(ds)
    df["sentiment_label"] = df["label"].map({0: "negative", 1: "positive"})

    neg = df[df["label"] == 0].sample(n=N_PER_CLASS, random_state=SEED)
    pos = df[df["label"] == 1].sample(n=N_PER_CLASS, random_state=SEED)
    reviews = (
        pd.concat([neg, pos])
        .sample(frac=1, random_state=SEED)
        .reset_index(drop=True)
    )
    reviews = reviews[["text", "sentiment_label"]].rename(
        columns={"text": "review_text"}
    )
    reviews.index.name = "row_id"

    out_dir = ROOT / "data" / "reviews"
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "reviews_v1.csv"
    reviews.to_csv(csv_path)

    sha = hashlib.sha256(csv_path.read_bytes()).hexdigest()[:16]
    manifest = {
        "dataset": "imdb_test",
        "source": "huggingface/imdb",
        "sample_size": len(reviews),
        "sampling_seed": SEED,
        "frozen_date": datetime.date.today().isoformat(),
        "sha256_prefix": sha,
        "columns": list(reviews.columns),
        "balance": {
            "negative": int((reviews.sentiment_label == "negative").sum()),
            "positive": int((reviews.sentiment_label == "positive").sum()),
        },
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"  Reviews: {len(reviews)} rows, sha256={sha}")
    print(f"  Saved to {csv_path}")
    return reviews


def prep_abstracts():
    """
    arXiv abstracts -> 500 rows (250 ML-related + 250 non-ML).

    Uses the HuggingFace 'ccdv/arxiv-classification' dataset which has
    category labels. We map CS categories to ML vs non-ML.

    Alternative: download from Kaggle (arxiv-metadata-oai-snapshot).
    If this dataset isn't available, fall back to the Kaggle version.
    """
    print("Loading arXiv abstracts...")

    try:
        # Try HuggingFace arxiv classification dataset
        ds = load_dataset("ccdv/arxiv-classification", split="test")
        df = pd.DataFrame(ds)

        # The dataset has 'text' (abstract) and 'label' (category index)
        # Map to ML vs non-ML
        ml_categories = {0, 1, 2, 3}  # cs.AI, cs.CL, cs.CV, cs.LG typically
        df["is_ml"] = df["label"].isin(ml_categories)

    except Exception:
        print("  ccdv/arxiv-classification not available, trying alternative...")
        # Fallback: use a different arxiv dataset
        ds = load_dataset("gfissore/arxiv-abstracts-2021", split="train")
        df = pd.DataFrame(ds).head(10000)  # work with first 10k

        ml_tags = {"cs.LG", "cs.AI", "cs.CL", "cs.CV", "stat.ML", "cs.NE"}
        df["is_ml"] = df["categories"].apply(
            lambda cats: bool(set(cats.split()) & ml_tags)
        )

    ml_rows = df[df["is_ml"]].sample(n=N_PER_CLASS, random_state=SEED)
    non_ml = df[~df["is_ml"]].sample(n=N_PER_CLASS, random_state=SEED)

    abstracts = (
        pd.concat([ml_rows, non_ml])
        .sample(frac=1, random_state=SEED)
        .reset_index(drop=True)
    )

    # Normalize column names
    text_col = "text" if "text" in abstracts.columns else "abstract"
    abstracts = abstracts[[text_col, "is_ml"]].rename(
        columns={text_col: "abstract_text"}
    )
    abstracts["topic_label"] = abstracts["is_ml"].map(
        {True: "machine_learning", False: "other"}
    )
    abstracts = abstracts[["abstract_text", "topic_label"]]
    abstracts.index.name = "row_id"

    out_dir = ROOT / "data" / "abstracts"
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "abstracts_v1.csv"
    abstracts.to_csv(csv_path, escapechar="\\", doublequote=False)

    sha = hashlib.sha256(csv_path.read_bytes()).hexdigest()[:16]
    manifest = {
        "dataset": "arxiv_abstracts",
        "source": "huggingface/arxiv",
        "sample_size": len(abstracts),
        "sampling_seed": SEED,
        "frozen_date": datetime.date.today().isoformat(),
        "sha256_prefix": sha,
        "columns": list(abstracts.columns),
        "balance": {
            "machine_learning": int((abstracts.topic_label == "machine_learning").sum()),
            "other": int((abstracts.topic_label == "other").sum()),
        },
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"  Abstracts: {len(abstracts)} rows, sha256={sha}")
    print(f"  Saved to {csv_path}")
    return abstracts


if __name__ == "__main__":
    print("=" * 60)
    print("OperatorBench - Dataset Preparation")
    print("=" * 60)

    reviews = prep_reviews()
    print()
    abstracts = prep_abstracts()

    print()
    print("=" * 60)
    print("Both datasets frozen. DO NOT re-run this script.")
    print("If you need changes, create a v2 CSV instead.")
    print("=" * 60)
