# Rights and public-release scope

Copyright (c) 2026 Vansika Sonthalia.

The MIT license in `LICENSE` applies to original OperatorBench software in
`configs/`, `harness/`, `scripts/` and `tests/`, and its usage documentation.
It does not license the research manuscript, third-party corpora, copied
document text, third-party software, or other authors' material.

The manuscript is an author preprint, not a peer-reviewed publication.
The intended arXiv license for this version is arXiv's perpetual,
non-exclusive distribution license. That license takes effect through the
author's arXiv submission; this notice does not claim an arXiv posting or ID.
The author retains copyright in the manuscript.

## Third-party inputs

The experiments use a selected sample from
[ccdv/arxiv-classification](https://huggingface.co/datasets/ccdv/arxiv-classification)
and the [Large Movie Review Dataset](https://ai.stanford.edu/~amaas/data/sentiment/).
The arXiv classification dataset credits He et al., *Long Document
Classification From Local Word Glimpses via Recurrent Attention Learning*
(IEEE Access, 2019), and its
[upstream collection](https://github.com/LiqunW/Long-document-dataset).
The movie-review dataset credits Maas et al., *Learning Word Vectors for
Sentiment Analysis* (ACL, 2011).

The arXiv dataset card and upstream repository inspected on 9 October 2026
did not establish a blanket redistribution license for the collected papers.
The IMDB dataset card labels its license `other`. Public accessibility does
not establish an MIT license for either corpus.

This initial public release therefore contains the paper, original software,
experiment specifications, aggregate results and selected audit reports.
It excludes dataset text, result CSVs containing input documents, raw request
logs, and native prompts containing copied documents. The original frozen
files remain unchanged in the local research archive. This public snapshot
alone cannot run the complete saved-output reproduction; it is not the full
reproducibility package required by a future experimental-analysis submission.

Dependency packages retain their own licenses. Installing dependencies does
not change their licenses. No third-party package source is vendored here.
