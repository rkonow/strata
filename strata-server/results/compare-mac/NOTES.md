# How these numbers were taken

Mac + kind simulation, shared 4 CPUs, on 2026-10-28 (UTC).

- Machine: MacBook Air, Apple M2 (4P + 4E), 24 GB, macOS 26.0.1. Docker
  Desktop engine 24.0.5, VM 4 CPUs / 8.7 GB, LinuxKit 5.15.49; kind 0.33,
  Kubernetes 1.37.0 (1 control plane, 3 workers), the cluster of
  `deploy/kind-test.sh up` with its `/data` and `/beir` mounts. Its own
  Strata pods (namespace `strata`) were scaled to 0 to free the VM's memory.
- Code: `main` at 6b85211 plus this branch's `engine-compare` (the image
  was rebuilt as the client changed: the facet fix and count check before
  the second Elasticsearch and Solr runs, p90/p99.9/max before the second
  Elasticsearch run and the final Strata runs). Strata's databases were
  built once and served by both Strata runs.
- Images: `elasticsearch:9.5.5`, `solr:10.1.0`, `strata-server:compare`
  (`deploy/Dockerfile`).
- Nothing else heavy ran on the Mac: the embedding-fields queue had finished
  at 02:14 UTC and `bench-work/embed-pause` was held during the runs. Each
  step waited for the 1-minute load average to drop below 6.

## Order of the runs (UTC; load average before each step)

First pass (`first-pass/`):

| step | start | load |
|---|---|---|
| Strata build (shard 0, then 1) | 02:31:51 | 3.37 |
| Strata run, shards 1.25 + 1.25, coordinator 0.5 | 02:35:45 | 4.69 |
| Elasticsearch load | 02:39:08 | 4.95 |
| Elasticsearch run | 02:47:52 | 3.05 |
| Solr load | 02:50:51 | 4.02 |
| Solr run | 03:09:01 | 3.73 |
| Strata run, 1 + 1 + 1 | 03:12:48 | 3.80 |

Second pass (this directory):

| step | start | load |
|---|---|---|
| Elasticsearch load (an image build on the VM in its first minutes) | 03:18:56 | 4.62 |
| Elasticsearch run | 03:28:10 | 4.10 |
| Solr load | 03:31:09 | 4.43 |
| Solr run | 03:49:23 | 3.96 |
| Strata run, 1.25 + 1.25 + 0.5 | 03:53:08 | 3.13 |
| Strata run, 1 + 1 + 1 | 03:56:15 | 3.49 |

## Commands

```bash
export STRATA_WORK=~/code2/bench-work/k8s-quora   # /data in the cluster
C=strata-server/deploy/compare/compare.sh
$C image
$C build                                  # Strata: 2 shards, one after the other
SHARD_CPU=1250m COORD_CPU=500m $C up strata; $C run strata --label strata-coord500m; $C down strata
SHARD_CPU=1 COORD_CPU=1 $C up strata;      $C run strata;                           $C down strata
$C up es;   $C load es;   $C run es;   $C down es
$C up solr; $C load solr; $C run solr; $C down solr
python3 strata-server/deploy/compare/analyze.py results/compare-mac /path/to/beir/quora > tables.md
```

The drivers that ran these with the waits above were
`bench-work/scripts/compare-all.sh`, `compare-es-solr.sh` and
`compare-strata-p90.sh` (outside the repository); `analyze.py` reads the
`run-*.json.gz` reports here as they are.
