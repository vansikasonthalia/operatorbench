# Evidence audits

These are saved audit reports with different scopes and dates. They are not
certificates of semantic correctness, peer review or complete public reproducibility.

| Folder | Scope |
| --- | --- |
| [2026-10-07](2026-10-07/) | Historical row/log consistency and recomputation of eight historical analyses |
| [2026-10-08](2026-10-08/) | Admission accounting, matched-control coverage and sensitivities, and the separate revision evaluation |
| [preprint-reanalysis-20261009](preprint-reanalysis-20261009/) | Selected outputs from successful saved-output reanalysis of the complete local archive |

Useful entry points:

- [Readable tables](preprint-reanalysis-20261009/TABLES.md)
- [Reporting comparison](preprint-reanalysis-20261009/REPORTING-COMPARISON.md)
- [Prototype requirement checks](preprint-reanalysis-20261009/REVISION-CONTRACTS.md)
- [Nine-check reanalysis report](preprint-reanalysis-20261009/report.json)
- [Matched-control request audit](2026-10-08/matched_control_full_check.json)
- [Revision-evaluation audit](2026-10-08/revision_evaluation_check.json)

The historical audit records 178 malformed log lines and provenance gaps in
30 runs. Later complete request coverage does not retroactively repair those
historical gaps. The saved hashes identify particular bytes; they do not prove
correct answers or the timing of files whose execution-time provenance is absent.

Source hashes in a dated report refer to the source at that audit, not every
later repository commit. Hash-bound matched/revision sources remain unchanged.
The [reproduction guide](../docs/ARTIFACT-README.md) states which underlying
files are absent from the public snapshot and which checks require them.

Repository CI tests software with constructed inputs. Its results must not be
presented as a rerun of these scientific audits.
