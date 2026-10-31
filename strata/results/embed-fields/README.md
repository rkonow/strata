# Which fields to embed: title, text, or both

`strata-embed-eval` (`strata/src/bin/strata-embed-eval.rs`) loads a BEIR
collection into a Strata table with three embedded vector fields made by one
model, from the title (`emb_title`), the text (`emb_text`) and the title and
text joined by a newline (`emb_both`), and scores each, with exact (flat)
vector search, against the collection's relevance judgments. References: BM25
over title + text, RRF of that BM25 and `emb_both`, and the collection's own
Cohere embed-english-v3 vectors of title + text.

Measured on Roberto's MacBook Air (Apple M2, 8 cores, 24 GB, macOS 26.0.1),
2026-10-27. Scores do not depend on the machine; times do.

## nDCG@10

The best of the three single vectors in each row is in bold.

| collection | model | title | text | title + text | title + text with BM25 (RRF) |
|---|---|---|---|---|---|
| scifact | Model2Vec potion-retrieval-32M | 0.487 | 0.613 | **0.633** | 0.697 |
| scifact | BGE-small v1.5 | 0.598 | 0.697 | **0.713** | 0.724 |
| scifact | Arctic Embed M v1.5 | 0.576 | 0.704 | **0.716** | 0.729 |
| scifact | *references: BM25 0.689; Cohere embed-english-v3 of title + text 0.718* | | | | |
| nfcorpus | Model2Vec potion-retrieval-32M | 0.283 | 0.295 | **0.308** | 0.340 |
| nfcorpus | BGE-small v1.5 | 0.318 | **0.349** | 0.344 | 0.365 |
| nfcorpus | Arctic Embed M v1.5 | 0.321 | 0.361 | **0.363** | 0.370 |
| nfcorpus | *references: BM25 0.327; Cohere embed-english-v3 of title + text 0.387* | | | | |
| scidocs | Model2Vec potion-retrieval-32M | 0.128 | 0.120 | **0.137** | 0.159 |
| scidocs | BGE-small v1.5 | 0.195 | 0.198 | **0.205** | 0.195 |
| scidocs | *references: BM25 0.157; Cohere embed-english-v3 of title + text 0.203* | | | | |
| trec-covid | Model2Vec potion-retrieval-32M | **0.504** | 0.482 | 0.446 | 0.654 |
| trec-covid | *references: BM25 0.658; Cohere embed-english-v3 of title + text 0.818* | | | | |

## What it says

- **Title + text is the best single vector** in most runs. Where it is not,
  it is within 0.005 (NFCorpus with BGE-small) or the collection is an
  outlier (TREC-COVID with Model2Vec).
- **The title alone is the weakest** on SciFact (0.12 to 0.15 below title +
  text: its queries are claims whose evidence is in the abstract) and on
  NFCorpus, and close on SciDocs, whose queries are themselves paper titles.
- **TREC-COVID with Model2Vec reverses the order** (title 0.504, text 0.482,
  both 0.446). Its 50 queries are short COVID questions and its titles are on
  topic, while a static model's average over a long abstract drifts toward
  generic words. A contextual model was not run there: at the BERT models'
  8 to 27 texts a second on this CPU, TREC-COVID's 514K texts would take
  5 to 18 hours.
- **Adding BM25 helps unless the vector is far ahead of it**: RRF of BM25 and
  the title + text vector beats the best single vector in every run but one,
  and beats BM25 alone everywhere except TREC-COVID (0.654 vs 0.658), where
  the vector is much weaker than BM25. The one is the reverse case, SciDocs
  with BGE-small: BM25 (0.157) is far behind the vector (0.205), and fusing
  them gives 0.195.
- **BGE-small matches the Cohere reference on SciDocs** (0.205 vs 0.203),
  and comes within 0.005 to 0.04 of it on SciFact and NFCorpus.
- **Model2Vec plus BM25 comes close to a BERT model alone** on SciFact (0.697
  vs 0.713 to 0.716), at about 1,000 times the embedding speed.

So: embed title + description into one vector field, search it fused with
BM25 (and check fusion against the vector alone on your own queries: it
helped in 8 of 9 runs here), and, when titles are informative and
descriptions long (above all with a static model), add a title-only field and
measure both.

## recall@100

| collection | model | title | text | title + text | title + text with BM25 (RRF) | BM25 | Cohere v3 |
|---|---|---|---|---|---|---|---|
| scifact | Model2Vec potion-retrieval-32M | 0.791 | 0.928 | 0.934 | 0.955 | 0.925 | 0.963 |
| scifact | BGE-small v1.5 | 0.876 | 0.948 | 0.945 | 0.965 | 0.925 | 0.963 |
| scifact | Arctic Embed M v1.5 | 0.878 | 0.945 | 0.943 | 0.957 | 0.925 | 0.963 |
| nfcorpus | Model2Vec potion-retrieval-32M | 0.271 | 0.283 | 0.287 | 0.309 | 0.249 | 0.351 |
| nfcorpus | BGE-small v1.5 | 0.290 | 0.313 | 0.311 | 0.314 | 0.249 | 0.351 |
| nfcorpus | Arctic Embed M v1.5 | 0.289 | 0.324 | 0.325 | 0.326 | 0.249 | 0.351 |
| scidocs | Model2Vec potion-retrieval-32M | 0.306 | 0.318 | 0.342 | 0.377 | 0.359 | 0.450 |
| scidocs | BGE-small v1.5 | 0.444 | 0.460 | 0.459 | 0.442 | 0.359 | 0.450 |
| trec-covid | Model2Vec potion-retrieval-32M | 0.070 | 0.072 | 0.075 | 0.110 | 0.127 | 0.159 |

## Collections

`scripts/prepare_beir.py OUT scifact nfcorpus scidocs trec-covid` (the
documents, queries, judgments and Cohere vectors of
[CohereLabs/beir-embed-english-v3](https://huggingface.co/datasets/CohereLabs/beir-embed-english-v3);
NFCorpus and SciDocs were prepared for this run with
`uv run --no-project --with pyarrow --with numpy python3 scripts/prepare_beir.py`).

| collection | documents | test queries with a judgment | what the queries are |
|---|---|---|---|
| scifact | 5,183 abstracts | 300 | scientific claims |
| nfcorpus | 3,633 PubMed abstracts | 323 | NutritionFacts.org video and article titles, and questions |
| scidocs | 25,657 paper abstracts | 1,000 | paper titles (citation prediction) |
| trec-covid | 171,332 CORD-19 abstracts (7 without a title) | 50 | COVID-19 questions |

## Models

`strata-server fetch-model NAME` into `STRATA_MODELS=~/code2/strata-models`:

| model | kind | dimensions | model.safetensors |
|---|---|---|---|
| potion-retrieval-32M | Model2Vec (static) | 512 | 129.2 MB |
| bge-small-en-v1.5 | BERT, CLS pooling, query instruction | 384 | 133.5 MB |
| snowflake-arctic-embed-m-v1.5 | BERT, CLS pooling, query instruction | 768 | 435.6 MB |

## Commands and code

From `strata/`, one run at a time:

```bash
export STRATA_MODELS=~/code2/strata-models STRATA_DATA=~/code2/bench-data/beir
cargo build --release --features accelerate --bin strata-embed-eval   # `bert` for the first two runs
caffeinate -i target/release/strata-embed-eval --dataset D --model M --out results/embed-fields/D-M.json > results/embed-fields/D-M.txt 2>&1
```

The first two runs (SciFact with Model2Vec and with BGE-small) were built at
`0c0ee30` with `--features bert` (candle's own CPU kernels). The rest were
built at `547d474` (`0c0ee30` plus `7595959`, the `accelerate` feature) with
`--features accelerate`, which sends the matrix products to Apple's
Accelerate. SciFact with BGE-small was run with both builds and scored the
same to the fourth decimal (`scifact-bge-small-en-v1.5-accelerate.*`). A
profile of the Accelerate build (macOS `sample`) put about 57% of the CPU
time in candle's exact GELU (`libm::erff`) and 29% in the matrix products.

`runs.log` has each run's start and end, the load averages and wall times.
The Mac was shared: a Docker VM, other agents' builds and a Kubernetes
thread's cluster tests ran during several runs (load averages up to 124 were seen), and
the runs paused twice (17:43 to 17:54 and 18:19 to 18:26 PDT) to let those
tests run alone. Times are therefore upper bounds.

## Times

"Load + embed" is Strata loading the collection and embedding all three
fields (`load_and_embed_seconds`); texts a second counts the title, text and
title + text of every document.

| collection | documents | queries | model | build | load + embed 3 fields | texts a second |
|---|---|---|---|---|---|---|
| scifact | 5,183 | 300 | Model2Vec potion-retrieval-32M | `bert` | 1.2 s | 12,659 |
| scifact | 5,183 | 300 | BGE-small v1.5 | `bert` | 1,893.4 s | 8 |
| scifact | 5,183 | 300 | BGE-small v1.5 | `accelerate` | 1,009.4 s | 15 |
| scifact | 5,183 | 300 | Arctic Embed M v1.5 | `accelerate` | 1,920.7 s | 8 |
| nfcorpus | 3,633 | 323 | Model2Vec potion-retrieval-32M | `accelerate` | 0.6 s | 17,562 |
| nfcorpus | 3,633 | 323 | BGE-small v1.5 | `accelerate` | 773.5 s | 14 |
| nfcorpus | 3,633 | 323 | Arctic Embed M v1.5 | `accelerate` | 1,437.1 s | 8 |
| scidocs | 25,657 | 1,000 | Model2Vec potion-retrieval-32M | `accelerate` | 3.5 s | 22,229 |
| scidocs | 25,657 | 1,000 | BGE-small v1.5 | `accelerate` | 2,848.4 s | 27 |
| trec-covid | 171,332 | 50 | Model2Vec potion-retrieval-32M | `accelerate` | 25.5 s | 20,120 |
