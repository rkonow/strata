# Engram

Automatic long-term context for Claude Code, stored in [Strata](../strata).

Engram records everything a coding session does into one Strata file per
project: prompts, answers, tool calls and results, errors, and compaction
summaries. When it helps, Engram brings the right pieces back on its own,
and it gives the model tools to search its history. Nothing depends on the
model remembering to save. The design is in [DESIGN.md](DESIGN.md).

```
 Claude Code ──hooks──▶ engram hook <event> ─┐   (~15 ms per hook)
     └──MCP (stdio)──▶ engram mcp ───────────┤   unix socket
                                              ▼
                         engram serve: one daemon per project, started on demand
                         capture → embed → Strata (BM25 + vectors + filters)
                         recall  → live-window exclusion → priors → gate → pack
```

## What happens automatically

| when | Engram |
|---|---|
| session start, resume, clear, compaction | injects a briefing: planned work and its requirements, rules, decisions, facts, where the last session stopped, and a reminder to search memory before design decisions |
| every prompt | stores it; injects the earlier decisions, rules and requirements that match it; searches older history (never what is still in context) and injects the best records when the evidence is strong (up to 4,000 characters), three records when it is weak (1,200), nothing otherwise |
| before `Edit`/`Write` of a file | injects up to two earlier errors or decisions about that file, once per context window |
| every tool call | stores the call and its result (errors flagged, long output trimmed to head and tail, secrets redacted) |
| end of a turn | stores the answer, the commentary written between tool calls, and an episode card (prompt, files changed, commands, errors, outcome); then a small model writes down the decisions, rules, planned work and facts the turn stated, updating or closing earlier ones |
| compaction | stores the summary by section; the live window restarts |

The model also gets five MCP tools:
- `memory_search`: hybrid search;
- `memory_timeline`: what surrounded a record;
- `memory_sql`: read-only SQL over the `memory` table;
- `memory_remember` and `memory_forget`.

## Install

From the repository root, `./install.sh` does everything below for every
session (`--project DIR` for one project) and runs `engram doctor`; see
[Install on your machine](../docs/engram/setup.md). By hand:

```sh
cargo build --release                      # in engram/
target/release/engram fetch-model          # potion-retrieval-32M (static embedder, 129 MB)
target/release/engram install --project /path/to/your/project
target/release/engram doctor               # checks hooks, MCP server, model, CLI login and the daemon
```

`install` writes four things:
- the hooks, into `.claude/settings.local.json` (personal and git-ignored; `--shared` writes `.claude/settings.json` instead);
- the MCP server, into `.mcp.json`;
- its approval, through `enabledMcpjsonServers`;
- a permission rule for the memory tools.

The store lives in `~/.local/share/engram/projects/<project>/`, outside the repository.

### Every session

```sh
cargo install --path . --bin engram --root ~/.local   # a stable path for the hooks: ~/.local/bin/engram
engram fetch-model
engram install --user --distill-model haiku
```

`install --user` puts the hooks and the memory tools' permission into
`~/.claude/settings.json` (saving the previous version next to it) and
registers the MCP server at user scope with `claude mcp add-json`. Every
Claude Code session then records into the store of its project directory,
one Strata file per project, so sessions in the same project share memory
and sessions in other projects never see it. Within a file each session
keeps its own id. A project with its own `engram install` keeps using that
one: the user-level hooks step aside there, so nothing is recorded twice.
`--distill-model` sets `ENGRAM_DISTILL_MODEL` in the settings' `env`.

`engram uninstall --user` removes all of it again and keeps the stores
(`engram uninstall` does the same for a project install). `ENGRAM_DISABLE=1`
in a session's environment switches Engram off for that session only.

Optional: `cargo build --release --features bert` and `engram fetch-model bert`
enable BGE-small vectors in the daemon (`"embedder": "bert"` in the store's
`config.json`). They are better for paraphrased questions, but each record took about 50 ms to embed on the 4-core cloud VM's CPU.

## Use by hand

```sh
engram search "why did the planner pick post-filtering"
engram sql "SELECT ts, text FROM memory WHERE kind = 'error' AND MATCH(files, 'exec') ORDER BY ts DESC LIMIT 5"
engram remember decision "Prices are integer cents"
engram ingest-transcript ~/.claude/projects/<project>/<session>.jsonl   # back-fill old sessions
engram distill                                                          # write down knowledge for turns stored before distillation existed
engram stats | briefing | stop [--all]
engram ui                                                               # the inspector over every local store
```

`ENGRAM_DISABLE=1` turns the hooks off. `ENGRAM_HOME` and `ENGRAM_MODELS`
relocate the store and the models.

Distillation calls the `claude` CLI in print mode after each turn, off the
prompt's path; a small, fast model is enough (about $0.001 and 8 s per
turn with one). Choose it with `ENGRAM_DISTILL_MODEL` or `"distill":
{"model": ...}` in the store's `config.json`; without one, the CLI's
default model is used. `ENGRAM_DISTILL=0` or `"distill": {"enabled":
false}` turns it off. The call skips the user's settings and MCP servers
(their plugins and tool definitions would add tens of thousands of tokens
to each one); set `"distill": {"isolated": false}` when the CLI's
credentials come from the user settings.

## Live inspector

```sh
engram ui                                                   # your own stores, per project and session
demo/run.sh --model <model> --distill-model <small model>   # a chat in a scratch project
```

`engram ui` serves the inspector from the binary over every store under
`$ENGRAM_HOME/projects` (`--home` for another directory): pick a store and a
session, read the session rebuilt from its records with the briefing, each
recall decision and the distilled notes, and use every panel below on that
store. It only reads, through each project's daemon, listens on 127.0.0.1
and wants the token in the URL it prints.

The demo:

A local web app for watching Engram work: chat with Claude Code in a scratch
project (`~/engram-demo/project`, with its own store; `--data` moves it) and
see, as each turn runs, what the hooks store, what the distiller writes down
and what recall injects. The inspector reads the Strata file through the
project's daemon: every record and its links, the knowledge notes, the graph
(with `REACHABLE` traversals), the vectors (projected on their first two
principal components, with nearest neighbours), the table's schema and
segments, and an SQL console with text-embedded vector parameters.
Nothing is simulated: chat turns are `claude -p` runs and every view is a query
against the store. `--embedder hash:256` runs without the embedding model.

## Evaluation

```sh
python3 scripts/download_memory_benchmarks.py && python3 scripts/convert_memory_benchmarks.py
target/release/engram-bench retrieval --data /home/user/data/memory/longmemeval_s.jsonl --embedder static --ctx request
target/release/engram-bench retrieval --data /home/user/data/memory/locomo.jsonl --embedder static --ctx request
target/release/engram-bench probes --transcript <session.jsonl> --probes probes/lost_probes.json --points 1952:4632,4633:5392
target/release/engram-bench scale --data /home/user/data/memory/longmemeval_s.jsonl --sizes 10000,100000,1000000
target/release/engram-bench latency --data /home/user/data/memory/longmemeval_s.jsonl --sizes 10000,100000,1000000
python3 scripts/fit_gate.py results/retrieval/longmemeval_s_static_request_c400.rows.jsonl --out results/gate_static_c400.json
```

What a record should be (keys, values, presentation, model-written facts)
and whether it changes the answers a model gives:

```sh
target/release/engram-structure --data /home/user/data/memory/longmemeval_s.jsonl --out results/structure
target/release/engram-structure --transcript <session.jsonl> --probes probes/session_probes.json,probes/lost_probes.json \
    --points 1952:4632,4633:5392 --out results/structure
python3 scripts/extract_facts.py --data /home/user/data/memory/longmemeval_s.jsonl --every 5 --out facts.jsonl
python3 scripts/reader_eval.py --data /home/user/data/memory/longmemeval_s.jsonl --judge <model> \
    --config today=results/structure/context_<...>.jsonl --config oracle=oracle --out results/reader
```

The session transcript and `probes/session_probes.json` come from a private
development session and are not in the repository.

Claude Code with and without Engram, on a project whose earlier sessions
stated rules, decisions and planned work (`eval/ab`; the held-out
scenarios are in `eval/ab/scenarios`):

```sh
python3 scripts/ab_eval.py history --scenario stockroom --model <model>   # record the earlier sessions
python3 scripts/ab_eval.py run --scenario stockroom --model <model> --repeats 5
python3 scripts/ab_stats.py stockroom=results/ab/stockroom/runs.jsonl slotwise=results/ab/slotwise/runs.jsonl --out results/ab/stats.json
```

The query engine (planner, searches, ranker) on a benchmark, and Claude
Code with each memory system (none, Claude Code's auto memory, past
transcripts, claude-mem, mem0, Engram) on the same scenarios; the
competitors' setup is in `eval/competitors`:

```sh
target/release/engram-bench deep --data /home/user/data/memory/longmemeval_s.jsonl --out results/v2/deep --tag haiku
target/release/engram-bench deep --data /home/user/data/memory/longmemeval_s.jsonl --every 5 --hard-model sonnet --tag haiku_sonnet_hard
python3 scripts/memory_bench.py all --scenario bakehouse --repeats 3   # every lane; see eval/competitors/README.md
python3 scripts/v2_charts.py
```

Results are in `results/`. `report/engram.html` covers the system and
`report/structure.html` the record structure (`scripts/build_report.py`,
`scripts/build_structure_report.py`).
