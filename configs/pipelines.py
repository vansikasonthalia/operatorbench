"""
OperatorBench — Pipeline Configurations

All pipeline definitions in one place. Matches the spec exactly:
  - Reviews: filter negative → classify sentiment
  - Abstracts: filter ML-related → classify topic

Contradiction definitions are frozen here.
"""

PIPELINES = {
    "reviews": {
        "data_file": "data/reviews/reviews_v1.csv",
        "text_col": "review_text",
        "ground_truth_col": "sentiment_label",  # for reference only, not used in metrics
        "filter": {
            "predicate": "Is this review negative? {review_text}",
            "expected_pass": "negative",  # what filter-positive means semantically
        },
        "map": {
            "instruction": "Respond with exactly one word. Classify the sentiment of this review as: positive, negative, or neutral. Review: {review_text}",
            "output_col": "mapped_sentiment",
            "valid_labels": {"positive", "negative", "neutral"},
        },
        "contradiction": {
            # A row is contradictory if: passed negative filter BUT mapped positive
            "definition": "filter admits row as negative, map labels it positive",
            "is_contradiction": lambda filter_passed, map_label: (
                filter_passed and map_label.strip().lower() == "positive"
            ),
        },
    },
    "abstracts": {
        "data_file": "data/abstracts/abstracts_v1.csv",
        "text_col": "abstract_text",
        # ⚠ F9-DEFECTIVE, KNOWINGLY LEFT AS-IS (D-W3-05, 28 Sept 2026).
        # This column is wrong on 176 of 449 labelled rows. NOTHING in any
        # metric reads it -- the contradiction predicate takes only filter
        # and map output; filter_precision.py, run_rq4.py and
        # run_f7_ablation.py all use topic_label_v2 instead. The single
        # reader is make_handcheck_sheet.py:186, which printed it as the
        # "dataset label" field on the already-completed coding sheet.
        # Do NOT "fix" this: the hand-check is coded and silently changing
        # what the instrument displayed is worse than the disclosure in
        # paper/reproducibility.tex. If the sheet is ever regenerated,
        # switch to topic_label_v2 FIRST and record that you did.
        "ground_truth_col": "topic_label",
        "filter": {
            "predicate": "Is this paper about machine learning? {abstract_text}",
            "expected_pass": "machine_learning",
        },
        "map": {
            "instruction": "Respond with exactly one word. Classify the primary topic of this paper as: ML, systems, theory, or other. Abstract: {abstract_text}",
            "output_col": "mapped_topic",
            "valid_labels": {"ML", "systems", "theory", "other"},  # case-sensitive
        },
        "contradiction": {
            "definition": "filter admits row as ML-related, map labels it non-ML",
            "is_contradiction": lambda filter_passed, map_label: (
                filter_passed and map_label.strip().upper() != "ML"
            ),
        },
    },
}

# Backend configurations
BACKENDS = {
    "hosted": {
        "model": "gpt-4o-mini",  # via LiteLLM -> OpenAI
        "seed": None,            # no seed was configured for the hosted study
        "description": "Hosted API - includes serving-stack variance",
    },
    "local": {
        "model": "ollama/llama3.1:8b",  # via LiteLLM → Ollama
        "seed": 42,
        "description": "Local open-weights, pinned seed — operator-level variance only",
    },
}

# Run configurations for each RQ
RQ_CONFIGS = {
    "rq1": {
        "description": "Individual-operator repeatability",
        "n_runs": 5,
        "backends": ["hosted"],  # the full five-run study used hosted only
    },
    "rq3": {
        "description": "Cross-operator criterion-disagreement rate",
        "n_runs": 3,
        "backends": ["hosted"],
    },
    "rq4": {
        "description": "Redundant structured date-cue perturbation on abstracts (paired)",
        "n_runs": 2,
        "backends": ["hosted"],
        "injection_cols": {
            "abstracts": "submission_month",  # only source with real metadata
        },
    },
}
