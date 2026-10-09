# OperatorBench

**Lower Disagreement Through Exclusion: A Case Study of Semantic Pipelines**  
Vansika Sonthalia · Research preprint · 9 October 2026

This repository accompanies a study of how filtering changes the population
on which an LLM pipeline is evaluated. A lower disagreement rate can result
from excluding documents while leaving the labels on retained documents unchanged.

**Publication status:** author preprint; not peer reviewed. arXiv submission
is being prepared; no arXiv identifier has been assigned.

- [Paper source](paper/reproducibility.tex)
- [Versioned release](https://github.com/vansikasonthalia/operatorbench/releases/tag/v0.1.0)
- [Results and scope](#main-findings)
- [Evidence and reproduction guide](docs/ARTIFACT-README.md)
- [Licensing and data availability](NOTICE.md)

## Main findings

On a selected sample of 500 arXiv documents, changing a LOTUS filter from
ML aboutness to primary-topic membership produced the following results:

| Measure | Before | After |
| --- | ---: | ---: |
| Admitted documents, two intervention comparisons | 154 / 156 | 94 / 94 |
| Disagreements | 36 / 38 | 4 / 4 |
| Changed labels on the 94 common admissions | — | 0 |
| Recall against author-category proxy | 87.83% | 61.74% |

Exclusion accounts for all 32 and 34 removed disagreements. These figures
measure criterion disagreement and a category-proxy coverage tradeoff; they
do not establish improved semantic correctness.

A matched binary control records 8,000 requests across four replicates.
A separately fixed revision evaluation records 2,800 requests on 100 reused
documents. A filter paraphrase introduces the same membership exception in
each replicate; narrowing excludes two documents as permitted by its declared
requirement. Complete paired reporting reconstructs the ledger's aggregate
terms, and separately implemented core assertions recover the same exceptions.

## What is available

This public snapshot includes the current paper source, original software,
fixed experiment specifications, aggregate results and selected audit reports.
Dataset text, result CSVs containing input documents, and request logs with
copied documents are retained in the local frozen archive while redistribution
terms are resolved. **The public snapshot alone is not a complete reproduction
package.** See [NOTICE.md](NOTICE.md) for sources and the precise boundary.

The original public pilot has been moved to `legacy/pilot/` for provenance.
Its broad cross-system and logical-contradiction claims are superseded by this
preprint. It is not part of the current paper's frozen evaluation.

## Explore the accounting without model calls

Python 3.10 or newer:

```bash
python -m pip install -r requirements-artifact.txt
python -m scripts.admission_ledger_example
```

The example is constructed, not an additional experiment. For local tests:

```bash
python -m pip install pytest
python -m pytest tests/test_admission_ledger.py tests/test_revision_contracts.py tests/test_package_artifact.py
```

## Reanalyse the complete frozen archive

These commands require the original data, result CSVs and raw logs in addition
to this public snapshot. They do not make model calls:

```bash
python -m scripts.reproduce_artifact
python -m scripts.run_revision_evaluation --aggregate-only
```

The first command reanalyses the historical and matched studies. The second
verifies and aggregates the completed revision study using its frozen plan.
The [artifact guide](docs/ARTIFACT-README.md) describes their requirements.
Do not regenerate the frozen inputs with `scripts/prep_datasets.py`.

## Limits of the evidence

The main finding concerns selected documents, one framework and one hosted
model family. Category declarations are a proxy, not independent correctness
labels. The revision study reuses previously studied documents. The historical
archive retains 178 malformed log lines and incomplete provenance; successful
reanalysis does not repair those gaps. No detection or information advantage
over complete paired reporting is claimed.

## Citation

```bibtex
@unpublished{sonthalia2026exclusion,
  author = {Vansika Sonthalia},
  title = {Lower Disagreement Through Exclusion: A Case Study of Semantic Pipelines},
  year = {2026},
  note = {Author preprint; not peer reviewed},
  url = {https://github.com/vansikasonthalia/operatorbench}
}
```

## License

Original software and usage documentation: [MIT](LICENSE). The paper retains
its author's copyright; its intended arXiv license is perpetual and
non-exclusive. Third-party data is not covered by the MIT grant. See [NOTICE.md](NOTICE.md).
