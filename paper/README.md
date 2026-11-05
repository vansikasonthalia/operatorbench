# Paper

**Lower Disagreement Through Exclusion: A Case Study of Semantic Pipelines**
Vansika Sonthalia · 9 October 2026

Author preprint; not peer reviewed. No arXiv identifier has been assigned.

[Read the paper (PDF)](reproducibility.pdf) ·
[Read the study summary and evidence](../docs/RESULTS.md) ·
[LaTeX source](reproducibility.tex)

## Reading and building

`reproducibility.tex` is the complete manuscript, including the bibliography
and appendices. It has no external figure or bibliography files. Upload this
file to an Overleaf project, select pdfLaTeX and recompile. A local LaTeX
installation can also build the source with pdfLaTeX; run it twice to resolve
cross-references.

The [compiled PDF](reproducibility.pdf) was built from this source with
Tectonic 0.17.0 and checked for resolved references and page layout.
To rebuild it with Tectonic, run `tectonic --untrusted reproducibility.tex`.

The manuscript covers the admission and coverage tradeoff, matched operator
control, offline sensitivity analyses, revision evaluation, related work and
evidence limitations. The appendices retain the historical repeatability,
schema-conformance, context and auxiliary-control analyses.

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

The code's MIT license does not apply to the manuscript. See
[NOTICE.md](../NOTICE.md).
