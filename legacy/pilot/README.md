# OperatorBench

**Characterising Reliability Failure Modes in LLM-Powered Semantic SQL Operators**

[![Status](https://img.shields.io/badge/status-active%20research-brightgreen)]()
[![Target venue](https://img.shields.io/badge/target-VLDB%202027%20EA%26B-blue)]()
[![arXiv](https://img.shields.io/badge/arXiv-coming%20Aug%202026-red)]()

> *A cross-system empirical study of structural reliability failure modes in LLM-powered semantic SQL operators across Snowflake Cortex AISQL, DocETL, and LOTUS.*

---

## Motivation

Modern enterprise data systems expose operators like `AI_FILTER`, `AI_CLASSIFY`, and `AI_JOIN` that embed LLM calls inside SQL queries. These operators violate core SQL guarantees:

- The same query on the same data returns **different outputs across runs**
- Outputs **violate declared schemas** silently, with no error raised
- Chained operators produce **logically contradictory results**
- Irrelevant schema columns **change operator outputs** unpredictably

No systematic empirical study of these failure modes exists. This project fills that gap.

## Research Questions

| RQ | Question |
|---|---|
| RQ1 | How stable are semantic operator outputs across repeated executions on identical inputs? |
| RQ2 | How frequently do operators produce outputs that violate their declared output types? |
| RQ3 | When operators are chained, how often do their outputs exhibit logical contradictions? |
| RQ4 | How sensitive are operator outputs to schema-irrelevant columns? |
| RQ5 | Can observed failure modes be detected at query execution time through lightweight validation primitives? |

## Systems Under Study

| System | Type | Operators |
|---|---|---|
| [Snowflake Cortex AISQL](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions) | Commercial cloud warehouse | `AI_FILTER`, `AI_CLASSIFY`, `AI_SUMMARIZE`, `AI_EXTRACT` |
| [DocETL](https://github.com/ucbepic/docetl) | Open-source pipeline framework | `filter`, `map`, `reduce`, `resolve` |
| [LOTUS](https://github.com/stanford-futuredata/lotus) | Open-source dataframe library | `sem_filter`, `sem_map`, `sem_join`, `sem_agg` |

## Datasets

All datasets are public. No proprietary data is used.

- [Data Agent Benchmark (DAB)](https://github.com/DABench/DABench) — 54 queries across 12 datasets, 9 domains
- TPC-H — standard database benchmark
- BIRD — text-to-SQL benchmark
- Spider — cross-domain text-to-SQL

## Proposed Validation Primitives

Based on observed failure patterns, the project will design and evaluate:

```sql
-- Checks output conforms to declared type/category
ASSERT_TYPE(ai_classify(text, ['pos','neg','neu']), ['pos','neg','neu'])

-- Checks output stability across re-execution
ASSERT_CONSISTENT(ai_filter(text, 'mentions billing'), n_runs=3)

-- Checks logical coherence across chained operators  
ASSERT_COHERENT(filter_result, classify_result, rule='if filtered then classified')

-- Checks irrelevant columns don't affect output
ASSERT_CONTEXT_INDEPENDENT(ai_classify(text, labels), irrelevant_cols=['id','date'])
```

## Prior Work

This project extends the methodology of:

- **SemBench** (Lao et al., 2025) — evaluates accuracy, cost, efficiency of semantic query engines; does not measure cross-run consistency or cross-operator coherence
- **DAB** (Ma et al., 2026) — benchmarks data agent performance; does not systematically categorise failure modes
- **RGEval** (Sonthalia et al., ICWS 2025) — benchmarks LLM-generated GraphQL queries; establishes semantic equivalence evaluation methodology extended here · [IEEE Xplore](https://ieeexplore.ieee.org/document/11169687)

## Status

| Phase | Status | Target date |
|---|---|---|
| Infrastructure setup | 🔄 In progress | May 2026 |
| Phase 1: DAB pilot on Snowflake Cortex | ⬜ Planned | Jun 2026 |
| Phase 2: Extended benchmark + DocETL | ⬜ Planned | Jul 2026 |
| Week 10 decision gate | ⬜ Planned | Jul 2026 |
| Phase 3: LOTUS + cross-system comparison | ⬜ Planned | Aug 2026 |
| Phase 4: Primitive design + evaluation | ⬜ Planned | Sep 2026 |
| arXiv preprint | ⬜ Planned | Aug 2026 |
| VLDB 2027 EA&B submission | ⬜ Planned | Oct 2026 |

## Author

**Vansika Sonthalia** — Data Engineer, Tredence Inc.

[Google Scholar](https://scholar.google.com/citations?user=L5Jdou8AAAAJ) · [LinkedIn](https://linkedin.com/in/vansikasonthalia) · [Website](https://vansikasonthalia.github.io)

Related publication: [Robust Evaluation of LLM-Generated GraphQL Queries for Web Services](https://ieeexplore.ieee.org/document/11169687), IEEE ICWS 2025

## Citation

If you find this work useful, please cite:

```bibtex
@misc{sonthalia2026operatorbench,
  title={Characterising Reliability Failure Modes in LLM-Powered Semantic SQL Operators},
  author={Sonthalia, Vansika},
  year={2026},
  note={Work in progress. \url{https://github.com/vansikasonthalia/operatorbench}}
}
```

---

*Research proposal available on request.*
