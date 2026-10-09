# OperatorBench

**Understanding disagreement and coverage in LLM filter–map pipelines.**

OperatorBench is the code and evidence companion to *Lower Disagreement Through
Exclusion: A Case Study of Semantic Pipelines*, by **Vansika Sonthalia**.
It studies LOTUS pipelines that first select documents with a natural-language
filter and then assign labels with a natural-language map operation.

The central question is simple: **if a pipeline reports fewer disagreements,
did its labels improve, or did it stop admitting the difficult documents?**

[Paper](paper/) · [Results and evidence](docs/RESULTS.md) ·
[Reproduction guide](docs/ARTIFACT-README.md) · [Code guide](docs/CODE-GUIDE.md)

## Main result

On 500 selected arXiv documents, changing the filter from “about machine
learning” to “machine learning is the primary topic” reduced disagreements
while leaving every label on the retained documents unchanged.

| Filter revision | Admitted documents, before → after | Disagreements, before → after | Label changes on common admissions |
| --- | ---: | ---: | ---: |
| Replicate 1 | 154 → 94 | 36 → 4 | 0 / 94 |
| Replicate 2 | 156 → 94 | 38 → 4 | 0 / 94 |

All 32 and 34 removed disagreements came from excluding documents. Recall
against the author-category proxy fell from **87.83% to 61.74%** in both
replicates. That proxy is available for 449 of the 500 documents; it is not
an independent correctness judgment.

The study also includes an 8,000-request matched operator control and a
2,800-request revision evaluation on 100 reused documents. The
[results guide](docs/RESULTS.md) explains their separate purposes and links
these claims to the saved reports.

## What the code does

- Accounts for retained, excluded, newly admitted and never-admitted rows.
- Separates label changes from selection changes and invalid output formats.
- Checks declared revision requirements, such as preserving admissions or
  allowing a narrower filter while preserving common-row labels.
- Provides experiment runners, saved-output analyses and audit utilities.

Complete paired reporting recovers the ledger's aggregate terms. The paper
claims an empirical case and diagnostic reporting procedure, not a new measure
of semantic correctness or an advantage over complete paired reporting.

## Try the diagnostic example

Clone the repository and enter its root directory. With Python 3.10–3.12:

```bash
git clone https://github.com/vansikasonthalia/operatorbench.git
cd operatorbench
python -m pip install -r requirements-artifact.txt
python -m scripts.admission_ledger_example
```

This prints a constructed example: both versions admit five rows and have
three disagreements, despite exclusions, new admissions and opposing label
changes. It uses the small analysis environment and requires no API key or model calls.
Use `--json` for the complete records and ledger.

To run the public unit tests in a virtual environment:

```bash
python -m pip install -r requirements-test.txt
python -m pytest
```

The default tests use constructed inputs. Framework integration tests and
checks requiring the withheld research data are selected separately; see the
[reproduction guide](docs/ARTIFACT-README.md).

## Scope and availability

The current manuscript is an **author preprint, not peer reviewed**. Its
[standalone LaTeX source](paper/reproducibility.tex) is available here; a compiled
PDF and arXiv identifier are not yet included.

This repository contains the software, experiment specifications, result
summaries and selected audits. Input document text, per-run CSVs containing
that text and raw request logs are withheld because redistribution permissions
have not been established. **A public clone cannot reproduce the full
experimental analysis.** [NOTICE.md](NOTICE.md) explains the boundary and
credits the dataset sources.

The evidence is limited to LOTUS, one hosted model family and selected samples.
The revision evaluation reuses documents from the main study. The historical
logs retain 178 malformed lines and provenance gaps; successful reanalysis
does not establish semantic correctness or repair those gaps.

## Citation and license

Use [CITATION.cff](CITATION.cff), or the [BibTeX entry](paper/README.md#citation).
Original software and usage documentation are [MIT licensed](LICENSE).
The manuscript retains its author's copyright; third-party materials are
excluded from that license.
