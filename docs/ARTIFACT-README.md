# Reproduction guide

There are three distinct activities: testing the software, inspecting the
published reports, and reproducing the experiments' saved-output analysis.
Only the first two are possible from this public clone alone.

## Public example and unit tests

Run from the repository root with Python 3.10–3.12. Install the small analysis environment for the constructed example:

```bash
python -m pip install -r requirements-artifact.txt
python -m scripts.admission_ledger_example
python -m scripts.admission_ledger_example --json
```

For tests, create and activate a virtual environment, then install:

```bash
python -m pip install -r requirements-test.txt
python -m pytest
```

The default suite checks accounting, revision requirements, parsing, statistics,
audit logic and packaging with constructed inputs. It needs no corpus, API key
or model calls. `pytest.ini` limits collection to `tests/` and excludes the
explicit `framework` and `archive` groups.

The public CI runs this same suite on Windows and Linux with Python 3.10 and
3.12. A passing software test is not a reproduced experimental result.

## Optional framework integration tests

These tests exercise native LOTUS prompt construction and runner behavior with
mocked provider responses. Install the larger model-runner environment first:

```bash
python -m pip install -r requirements.txt -r requirements-test.txt
python -m pytest -m framework
```

To include both public unit and framework tests, use
`python -m pytest -m "not archive"`. No paid provider calls are made by these
tests. Installing or importing third-party dependencies can access the network.

## Inspect the published evidence

Start with [Results and evidence](RESULTS.md). The saved reports include the
admission ledger, proxy comparisons, matched control, sensitivity analyses,
reporting comparison and revision evaluation. [The audit index](../audits/README.md)
distinguishes software tests from historical and later experiment audits.

Dataset text, per-run result CSVs and raw request/native-prompt archives are
not distributed here. See [NOTICE.md](../NOTICE.md) for the input sources and
licensing boundary. Downloading the upstream datasets again does not establish
that their rows and bytes match the frozen inputs.

## Complete local archive only

The commands in this section require the original frozen inputs and logs at
their original paths. **They will not work on the public repository alone.**
No complete review archive is attached to the public releases.

Given that archive, install `requirements-artifact.txt` and run:

```bash
python -m scripts.reproduce_artifact
python -m scripts.run_revision_evaluation --aggregate-only
```

The first command reanalyses eight historical outputs, the admission ledger,
matched control, sensitivities, reporting comparison and prototype revision
requirements. It blocks socket connections and writes to `audits/reproduction/`.
It does not run the revision experiment's aggregation.

The second verifies and aggregates that separate revision archive, including
its frozen preparation plan, native prompts and request-to-row bindings.
Neither command calls a model or establishes answer correctness.

The [9 October reanalysis report](../audits/preprint-reanalysis-20261009/report.json)
records the earlier successful local run. The
[revision audit](../audits/2026-10-08/revision_evaluation_check.json) covers the
separate completed evaluation. The historical 178 malformed log lines and
provenance gaps remain limitations.

Two additional tests inspect the actual frozen category labels and stamp
removal; with those inputs present, run `python -m pytest -m archive`.
Missing or changed archive inputs are errors, not silently skipped checks.

The local packaging utility can build a review ZIP after successful reanalysis:

```bash
python -m scripts.package_artifact --output artifacts/new-review.zip
python -m scripts.package_artifact --verify artifacts/new-review.zip
```

It refuses to overwrite a ZIP and verifies payload hashes. This ZIP includes
copied document text; it is not a public-release bundle and is not uploaded
by the command. Do not regenerate frozen inputs or bypass source/hash checks
to make an archive command pass.
