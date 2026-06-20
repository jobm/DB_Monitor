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
| `.goosemem` | ~8,900 tokens (66 KB) | 20 semantic memory blocks retrieved **on demand** | Only when keywords match the user's request |

### Total budget: ~10,200 tokens (vs. 33,000+ lines of raw source)

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

| # | Keywords | Domain | Approx Tokens |
|---|----------|--------|---------------|
| 1 | `#configuration #config #env` | Environment Variables & Config | ~600 |
| 2 | `#database #db #sql #orm` | Database Layer, ORM Models, Migrations | ~700 |
| 3 | `#ingestion #consumer #kafka` | Ingestion Pipeline & Consumer Service | ~500 |
| 4 | `#events #debezium #cdc` | Event Parsing & Source Metadata | ~500 |
| 5 | `#api #routes #rest #endpoints` | API Routes & HTTP Endpoints | ~700 |
| 6 | `#auth #rbac #jwt` | Authentication, RBAC & API Key Lifecycle | ~400 |
| 7 | `#websocket #ws #broadcast` | WebSocket Broadcasting & Backplane | ~300 |
| 8 | `#metrics #prometheus #otel` | Prometheus Metrics, Observability & Tracing | ~400 |
| 9 | `#webhooks #hmac #circuitbreaker` | Outbound Webhook Delivery | ~300 |
| 10 | `#retention #partition #cleanup` | Retention, Cleanup & Partition Maintenance | ~300 |
| 11 | `#provisioning #customer #controlplane` | Customer Provisioning & Control Plane | ~400 |
| 12 | `#quotas #rate #limiting` | Ingestion Quota Enforcement | ~300 |
| 13 | `#cli #tui #sdk` | CLI, TUI, Terminal Interface & SDKs | ~300 |
| 14 | `#deploy #helm #terraform #k8s` | Deployment Infrastructure | ~600 |
| 15 | `#lifecycle #startup #shutdown` | Application Lifecycle & Background Tasks | ~400 |
| 16 | `#conftest #fixtures #mock` | Test Fixtures & Mock Patterns | ~500 |
| 17 | `#migration #sql #partitioning` | Database Migrations & Schema Evolution | ~500 |
| 18 | `#tracing #otel #opentelemetry` | OpenTelemetry Distributed Tracing | ~400 |
| 19 | `#tests #pytest #integration` | Testing & Quality Gates | ~300 |
| 20 | `#repositories #readmodel #query #keyset` | Data Repositories, Query Patterns & Response Models | ~500 |

### How Keyword Retrieval Works

When a user asks: *"How does the auth system work?"*

1. Goose's Memory Extension scans `.goosemem` for blocks matching `#auth`
2. It retrieves block #6 (Authentication, RBAC & API Key Lifecycle) — ~400 tokens
3. Blocks #1–5, #7–20 are **not loaded** into context
4. The 9B model receives: `.goosehints` (guardrails) + block #6 (auth details) + user question

When a user asks: *"Add a new retention policy"*

1. Keywords matched: `#retention`, `#database`, `#config`, `#migration`
2. Blocks #2, #10, #17 are retrieved — ~1,500 tokens total
3. The model has exactly the context it needs: database schema, retention logic, and migration patterns

When a user asks: *"What does EventsRepository do?"*

1. Keywords matched: `#repositories`, `#events`, `#readmodel`, `#query`
2. Block #20 (Data Repositories, Query Patterns & Response Models) is retrieved — ~500 tokens
3. Block #4 (Event Parsing & Source Metadata) may also match on `#events` — ~500 tokens

---

## Setup for Goose (Qwen 2.5 9B)

### Option 1: System Prompt Injection (Recommended)

Configure Goose to load `.goosehints` as a system prompt prefix. In your Goose configuration:

```yaml
# .goose.yaml or equivalent
system_prompt_file: .goosehints
memory_file: .goosemem
```

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

### 4. Cross-References Are Explicit

When one block references another (e.g., the ingestion pipeline references auth), it uses the block's keyword tags so the model knows which additional block to retrieve.

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
4. Update the table above with the new block's approx token count

### Updating an Existing Block

1. Edit the relevant block in `.goosemem`
2. Keep the keyword prefix unchanged to avoid breaking retrieval
3. Run `wc -c .goosemem` to verify the file hasn't grown beyond budget

### Validating Keyword Coverage

```bash
# Check that all source files are referenced in at least one block
grep -roh "app/[a-z_]*\.py" .goosemem | sort -u > referenced.txt
find app/ -name "*.py" -not -path "*/__pycache__/*" | sed 's|^|app/|' | sort -u > actual.txt
comm -23 actual.txt referenced.txt
```

---

## File Statistics

| Metric | `.goosehints` | `.goosemem` | Total |
|--------|--------------|-------------|-------|
| Lines | 111 | 1,054 | 1,165 |
| Bytes | 8,222 | 65,676 | 73,898 |
| Est. Tokens | ~1,300 | ~8,900 | ~10,200 |
| Memory Blocks | — | 20 | — |
