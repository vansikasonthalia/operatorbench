# Revision requirements: offline prototype

Post-hoc prototype demonstration. Requirements were chosen after seeing the saved changes; these verdicts are not detection accuracy, prospective validation, or evidence of novelty.

Preserve-results requires equal admission sets and unchanged common saved labels. A declared filter change permits narrowing and requires unchanged common labels. A declared map change preserves admissions but permits label changes. A declared joint change permits narrowing and label changes. All require valid observed raw and saved outputs. Full-input assessments require complete evidence; observed-cohort assessments explicitly waive that requirement.

| Case | Preserve results | Declared change, full input | Declared change, observed cohort | Unassessed rows |
|---|---|---|---|---:|
| historical_r1 | violated | satisfied | satisfied | 0 |
| historical_r2 | violated | satisfied | satisfied | 0 |
| matched_r1_filter_change | violated | not_evaluable | satisfied | 21 |
| matched_r1_map_change | violated | not_evaluable | satisfied | 21 |
| matched_r1_both_change | violated | not_evaluable | satisfied | 21 |
| matched_r2_filter_change | violated | not_evaluable | satisfied | 21 |
| matched_r2_map_change | violated | not_evaluable | satisfied | 21 |
| matched_r2_both_change | violated | not_evaluable | satisfied | 21 |
| matched_r3_filter_change | violated | not_evaluable | satisfied | 21 |
| matched_r3_map_change | violated | not_evaluable | satisfied | 21 |
| matched_r3_both_change | violated | not_evaluable | satisfied | 21 |
| matched_r4_filter_change | violated | not_evaluable | satisfied | 26 |
| matched_r4_map_change | violated | not_evaluable | satisfied | 26 |
| matched_r4_both_change | violated | not_evaluable | satisfied | 26 |

Requirements make intent explicit but do not discover intent or judge whether a changed question is appropriate. These checks use information already available in a complete paired report. Observed-cohort satisfaction does not imply a full-input verdict; input omissions remain visible. For filter-only cases, the common four-cell validity policy omits rows even though their required filter/map answers are valid. Not evaluable concerns this supplied view, not the absence of all original responses; a separately derived full-input filter-only view could be evaluated.

JSON retains the exact contract, each requirement's status, and exception row IDs. No performance score is calculated from these post-hoc examples.
