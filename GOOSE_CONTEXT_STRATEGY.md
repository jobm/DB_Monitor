# Goose Context Strategy for DB Monitor

A dual-layer context management system designed for running DB Monitor's 33,000-line codebase on Goose with Qwen 2.5 (9B parameters) — a model that suffers from context inflation and short-term memory loss.

## Problem

The DB Monitor codebase is too large for a 9B model to hold in active context. Loading all source files into a single prompt causes:
- **Context inflation**: the model wastes tokens on irrelevant code, diluting attention
- **Short-term memory loss**: critical architectural constraints from early in the conversation are forgotten by later turns
- **Hallucinated file paths**: the model invents module names or imports that don't exist

## Solution: Two Files

| File | Size | Purpose | When Loaded |
|------|------|---------|-------------|
| `.goosehints` | ~1,300 tokens (8 KB) | Behavioral guardrails injected into **every turn** | Always — per-turn system prompt |
| `.goosemem` | ~9,300 tokens (73 KB) | 20 semantic memory blocks retrieved **on demand** | Only when keywords match the user's request |

### Total budget: ~10,600 tokens (vs. 33,000+ lines of raw source)

---

## Layer 1: `.goosehints` — The Per-Turn Guardrail

This file is injected into the system prompt at the start of every conversation turn. It contains **zero raw code** and **zero massive directory listings**. Instead, it provides:

- **Project identity**: what DB Monitor is, what stack it uses
- **Directory layout**: which directories contain real code vs. compatibility shims
- **Import rules**: the #1 source of hallucinated paths — always import from `db_monitor.*`, never from bare module names
- **Data flow**: the 11-step ingestion pipeline sequence
- **Common pitfalls**: mistakes the 9B model is known to make (e.g., confusing `AsyncSessionLocal` as a session)
- **Code style**: Python 3.11+, async/await, line length 79, SQLAlchemy 2.0 style

**Design principle**: If a constraint applies to every possible task (editing, reading, generating), it belongs here. The file must be small enough to fit in the system prompt without crowding out the user's actual request.

---

## Layer 2: `.goosemem` — The Semantic Memory Catalog

This file contains **20 isolated memory blocks**, each prefixed with space-separated keywords/hashtags. Goose's Memory Extension retrieves only the blocks matching the user's keywords, keeping the active context window clear of irrelevant code.

### Memory Blocks

| # | Keywords | Domain |
|---|----------|--------|
| 1 | `#configuration #config #env` | Environment Variables & Config |
| 2 | `#database #db #sql #orm` | Database Layer, ORM Models, Migrations |
| 3 | `#ingestion #consumer #kafka` | Ingestion Pipeline & Consumer Service |
| 4 | `#events #debezium #cdc` | Event Parsing & Source Metadata |
| 5 | `#repositories #readmodel #query` | Data Repositories, Query Patterns & Response Models |
| 6 | `#api #routes #rest #endpoints` | API Routes & HTTP Endpoints |
| 7 | `#auth #rbac #jwt` | Authentication, RBAC & API Key Lifecycle |
| 8 | `#websocket #ws #broadcast` | WebSocket Broadcasting & Backplane |
| 9 | `#metrics #prometheus #otel` | Prometheus Metrics, Observability & Tracing |
| 10 | `#webhooks #hmac #circuitbreaker` | Outbound Webhook Delivery |
| 11 | `#retention #partition #cleanup` | Retention, Cleanup & Partition Maintenance |
| 12 | `#provisioning #customer #controlplane` | Customer Provisioning & Control Plane |
| 13 | `#quotas #rate #limiting` | Ingestion Quota Enforcement |
| 14 | `#cli #tui #sdk` | CLI, TUI, Terminal Interface & SDKs |
| 15 | `#deploy #helm #terraform #k8s` | Deployment Infrastructure |
| 16 | `#lifecycle #startup #shutdown` | Application Lifecycle & Background Tasks |
| 17 | `#conftest #fixtures #mock` | Test Fixtures & Mock Patterns |
| 18 | `#migration #sql #partitioning` | Database Migrations & Schema Evolution |
| 19 | `#tracing #otel #opentelemetry` | OpenTelemetry Distributed Tracing |
| 20 | `#tests #pytest #integration` | Testing & Quality Gates |

### How Keyword Retrieval Works

When a user asks: *"How does the auth system work?"*

1. Goose's Memory Extension scans `.goosemem` for blocks matching `#auth`
2. It retrieves block #7 (Authentication, RBAC & API Key Lifecycle)
3. The other 19 blocks are **not loaded** into context
4. The 9B model receives: `.goosehints` (guardrails) + block #7 (auth details) + user question

When a user asks: *"Add a new retention policy"*

1. Keywords matched: `#retention`, `#database`, `#config`, `#migration`
2. Blocks #2, #11, #18 are retrieved
3. The model has exactly the context it needs: database schema, retention logic, and migration patterns

---

## Setup for Goose (Qwen 2.5 9B)

### Option 1: System Prompt Injection (Recommended)

Configure Goose to load `.goosehints` as a system prompt prefix and `.goosemem` as its memory file:

```yaml
# .goose.yaml or equivalent
system_prompt_file: .goosehints
memory_file: .goosemem
```

> Note: exact directive names depend on your Goose version. If `system_prompt_file`/`memory_file` are not recognized, fall back to Option 2 and check the Goose docs for your build.

### Option 2: Manual Injection

If Goose doesn't support automatic file loading, prepend `.goosehints` to your system prompt manually, and use the Memory Extension to load `.goosemem`.

### Option 3: Per-Task Manual Loading

For one-off tasks, manually read the relevant `.goosemem` block and paste it into context:

```bash
# Find the block matching your keyword
grep -A 50 "#auth #rbac" .goosemem | head -60
```

---

## Design Principles

### 1. `.goosehints` Contains No Raw Code

The per-turn file must be pure constraints and architecture. If the model reads it 100 times per session, it should never encounter implementation details that change between versions.

### 2. `.goosemem` Blocks Are Self-Contained

Each block includes its own FILE list, ENTRY point, and PITFALLS section. A block retrieved in isolation must give the model enough context to work without reading other blocks.

### 3. Keywords Are Space-Separated Hashtags

Blocks use `#keyword` prefixes so Goose's memory retrieval can match on any subset. A block tagged `#auth #rbac #jwt` will be retrieved when the user mentions any of those three terms.

### 4. Cross-References Are Explicit, Not Duplicated

When one block depends on another (e.g., the ingestion pipeline references auth), it uses a short `see #keyword block` pointer instead of repeating the other block's content. This keeps each block dense and avoids paying twice for the same information when several blocks are retrieved together.

### 5. Pitfalls Are Prioritized

Every block ends with a PITFALLS section listing the mistakes the 9B model is most likely to make in that domain. This is the highest-value content for preventing hallucinations.

---

## Token Budget Analysis

| Layer | Tokens | % of Context | Purpose |
|-------|--------|-------------|---------|
| `.goosehints` | ~1,300 | 5% | Always-on guardrails |
| 1 retrieved block | ~400–700 | 2–3% | Domain-specific knowledge |
| User prompt + response | ~2,000–4,000 | 8–16% | Actual task |
| **Total per turn** | **~4,000–6,000** | **16–24%** | — |

For a 9B model with ~32K context window, this leaves **~26K tokens headroom** for:
- Conversation history (previous turns)
- File content retrieved during the current turn
- Generated code output

Compare to loading all 33,000 lines (~130K tokens) which would immediately exceed the context window.

---

## Maintenance

### Adding a New Memory Block

1. Choose keywords that describe the domain (e.g., `#cache #redis #caching`)
2. Add the block to `.goosemem` with `================================================================================` separators
3. Include: DOMAIN, FILES, ENTRY, key content, PITFALLS
4. Update the block table above with the new block's keywords

### Updating an Existing Block

1. Edit the relevant block in `.goosemem`
2. Keep the keyword prefix unchanged to avoid breaking retrieval
3. Run `wc -c .goosemem` to verify the file hasn't grown beyond budget

### Validating Keyword Coverage

Coverage is measured per **module**, and scoped to a **single block**. A module counts as referenced when one block either contains its `package/file.py` path, or names its package path and its filename together. A directory mention in one block plus a filename in another does not count, because Goose only ever has the retrieved block in context — a split reference is a reference the agent cannot see.

`__init__.py` is exempt: it marks a package, and the package directory is covered by the same rule. Every other module under `src/db_monitor/` must be named.

```bash
bash scripts/validate_goosemem_coverage.sh   # exit 1 on gaps, 2 if the tree is missing
```

The shell script is the stable entrypoint (CI and pre-commit call it); the block parsing and matching live in `scripts/validate_goosemem_coverage.py`.

**Interpreting the output**
- Each `GAP:` line is a module no single block references. Add it to the `FILES:` line of the block that owns its package (or a nearby section that names both), keeping the parenthetical role hint short.
- `Directories` and `__init__ files` are informational; the module count is the gate.
- The validator requires `src/db_monitor/` to exist, so it cannot report a vacuous pass on a stale checkout.
- New modules fail the check until they are added to a block, which is the intended pressure — the memory is only useful if it names what exists.
- `app/`-prefixed references in `.goosemem` would become stale if the legacy tree returned; there are none today.

### Re-verifying Retrieval and Grounding

`scripts/verify_goosemem_retrieval.py` is the committed harness for the strategy itself. It runs with the standard library only and checks three things:

1. **Structure** — every block parses, has a `DOMAIN:` line, and no header has invalid or duplicate tags. A tag containing anything outside `[a-z0-9]` (e.g. `#event_detail`) silently makes the whole block unmatchable, so this is a hard failure.
2. **Grounding** — every `src/`, `tests/`, `scripts/`, `migrations/`, `tui/`, `sdk/`, or `docs/` path named in a block exists in the working tree.
3. **Retrieval** — 34 natural-language questions (no tags appended) and 14 keyworded questions must route to the expected domain. Each block has at least two natural-language cases; `#configuration` and `#cli`/TUI have eight each. Scoring matches Goose: exact tag hit = 4, body word-prefix hit = 1; the expected domain must appear in the top 3 (all 48 currently land at rank 1).

```bash
python3 scripts/verify_goosemem_retrieval.py            # all checks, exit 1 on failure
python3 scripts/verify_goosemem_retrieval.py -v         # show rank 1-3 for every query
python3 scripts/verify_goosemem_retrieval.py --only retrieval --top 1
python3 scripts/verify_goosemem_retrieval.py --list-blocks
```

When you add or rename a block, add one question per new domain to `NATURAL_QUERIES` and `KEYWORD_QUERIES` in the script. Natural-language questions are the important ones: if they only pass with tags appended, the block's aliases are too thin.

Both scripts are also wrapped by `make monitor-check-context` (see `CONTRIBUTING.md`), and run automatically:

- **CI** — `.github/workflows/context-memory.yml` is path-filtered on `.goosemem`, `.goosehints`, this document, and the two scripts, so it does not add time to unrelated PRs.
- **Pre-commit** — the `goosemem-retrieval` and `goosemem-coverage` local hooks in `.pre-commit-config.yaml` run whenever `.goosemem` is staged.

---

## File Statistics

| Metric | `.goosehints` | `.goosemem` | Total |
|--------|--------------|-------------|-------|
| Lines | 111 | 1,046 | 1,157 |
| Words | 997 | 7,454 | 8,451 |
| Bytes | 8,222 | 74,439 | 82,661 |
| Est. Tokens | ~1,300 | ~9,300 | ~10,600 |
| Memory Blocks | — | 20 | — |
