#!/usr/bin/env python3
"""verify_goosemem_retrieval.py — Re-verify the .goosemem context strategy.

Runs three independent checks:

  1. STRUCTURE   every block is parseable, has a DOMAIN line, and no header
                 contains invalid or duplicate tags (an underscore in a tag
                 makes the whole block invisible to keyword tokenizers).
  2. GROUNDING   every file path named in a block exists in the working tree.
  3. RETRIEVAL   natural-language and explicitly keyworded questions are routed
                 to the expected domain, using the same scoring Goose applies:
                 exact tag hit = 4, body word-prefix hit = 1.

Exit codes:
  0  all checks passed
  1  at least one check failed

Usage:
  python3 scripts/verify_goosemem_retrieval.py
  python3 scripts/verify_goosemem_retrieval.py --mem .goosemem -v
  python3 scripts/verify_goosemem_retrieval.py --only retrieval --top 1
  python3 scripts/verify_goosemem_retrieval.py --list-blocks
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

TAG_LINE = re.compile(r"^(?:#[a-z0-9]+)(?: #[a-z0-9]+)*$")
SEPARATOR = re.compile(r"^={20,}$")
PATH_RE = re.compile(
    r"(?<![\w/-])"
    r"((?:src/db_monitor|tests|scripts|migrations|tui|sdk|docs)/"
    r"[A-Za-z0-9_./-]+\.(?:py|sql|md|sh|ya?ml|toml))"
)

TAG_WEIGHT = 4
BODY_WEIGHT = 1

STOPWORDS = frozenset(
    "a an the is are was were do does did how i in on of to with for and or "
    "what where when which why this that it its from by as at be been being "
    "can could should would will there their they we you your my me".split()
)

# (question, expected domain substring). Questions keep their natural phrasing;
# none of them append tags, so they exercise the alias tags alone.
NATURAL_QUERIES: tuple[tuple[str, str], ...] = (
    ("How are API keys verified?", "Authentication"),
    ("How are duplicate Kafka events avoided on insert?", "Database"),
    ("How do I deploy this with Helm?", "Deployment"),
    ("What happens to old partitions during cleanup?", "Retention"),
    ("How is distributed tracing configured?", "OpenTelemetry"),
    ("Where is the consumer loop that processes messages?", "Ingestion"),
    ("How does the advisory lock work during migrations?", "Migrations"),
    ("How are webhook signatures computed?", "Webhook"),
    ("How does websocket fanout work across replicas?", "WebSocket"),
    ("How are customer provisioning jobs executed?", "Provisioning"),
    ("What happens on graceful shutdown?", "Lifecycle"),
    ("Where are Prometheus metrics defined?", "Metrics"),
    ("How do I configure the DB pool size?", "Configuration"),
    ("How do I run the unit tests?", "Testing"),
    ("How is a table schema discovered?", "Event Parsing"),
    ("How do I rotate the JWT secret?", "Authentication"),
    ("What happens to a failed message in the dead letter queue?", "Ingestion"),
    ("How does pagination with cursor work?", "Repositories"),
    # --- configuration -----------------------------------------------------
    ("What is the default database pool size?", "Configuration"),
    ("Which environment variables control retention?", "Configuration"),
    ("How do I supply an env var value from a file?", "Configuration"),
    ("What hardening applies when APP_ENV is production?", "Configuration"),
    ("Are environment variables read at import time?", "Configuration"),
    ("Which broker clusters setting wins, BROKER_CLUSTERS or KAFKA_CLUSTERS?", "Configuration"),
    ("Which env var holds the webhook URL list?", "Configuration"),
    ("How do I pass secrets to the container without env vars?", "Configuration"),
    # --- CLI / TUI ---------------------------------------------------------
    ("How do I open the terminal dashboard?", "CLI"),
    ("Which screens does the TUI include?", "CLI"),
    ("What CLI commands are installed?", "CLI"),
    ("How does the Python SDK talk to the API?", "CLI"),
    ("What JavaScript client ships with the repo?", "CLI"),
    ("How does the terminal UI subscribe to events?", "CLI"),
    ("Where is the Admin screen defined?", "CLI"),
    ("How do I install the TUI separately?", "CLI"),
)

# Canonical tag vocabulary: what a user or agent writes after the question.
KEYWORD_QUERIES: tuple[tuple[str, str], ...] = (
    ("How does the system verify an API key or JWT token? #auth #jwt", "Authentication"),
    ("How are duplicate Kafka events avoided on insert? #database #dedup", "Database"),
    ("How do I deploy this with a Helm chart? #deploy #helm", "Deployment"),
    ("What does the retention cleanup do with old partitions? #retention", "Retention"),
    ("How is OpenTelemetry tracing configured? #tracing #otel", "OpenTelemetry"),
    ("Where is the consumer batch processing loop? #ingestion #consumer", "Ingestion"),
    ("What env var sets the DB pool size? #configuration #env", "Configuration"),
    ("How do migrations acquire the advisory lock? #migration", "Migrations"),
    ("How are settings validated at startup? #configuration #validation", "Configuration"),
    ("How do I pass a secret via a file? #configuration #file", "Configuration"),
    ("How is the settings singleton built? #configuration #settings", "Configuration"),
    ("How do I launch the terminal dashboard? #cli #tui", "CLI"),
    ("Which screens exist in the terminal UI? #cli #tui", "CLI"),
    ("Does the SDK ship a JavaScript client? #cli #sdk", "CLI"),
)


@dataclass(frozen=True)
class Block:
    line: int
    header: str
    tags: tuple[str, ...]
    body: str
    domain: str


def parse_blocks(text: str) -> list[Block]:
    lines = text.splitlines()
    starts = [i for i, line in enumerate(lines) if TAG_LINE.match(line.strip())]
    blocks: list[Block] = []
    for n, start in enumerate(starts):
        end = starts[n + 1] if n + 1 < len(starts) else len(lines)
        body_lines = [
            line for line in lines[start + 1 : end] if not SEPARATOR.match(line.strip())
        ]
        body = "\n".join(body_lines)
        domain = "<MISSING>"
        for line in body_lines:
            if line.startswith("DOMAIN:"):
                domain = line.split(":", 1)[1].strip()
                break
        header = lines[start].strip()
        blocks.append(
            Block(
                line=start + 1,
                header=header,
                tags=tuple(tag.lstrip("#") for tag in header.split()),
                body=body,
                domain=domain,
            )
        )
    return blocks


def invalid_header_lines(text: str) -> list[int]:
    """Lines that look like tag headers but do not parse (e.g. #event_detail)."""
    bad: list[int] = []
    for i, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if not stripped.startswith("#"):
            continue
        tokens = stripped.split()
        if not tokens or not all(tok.startswith("#") for tok in tokens):
            continue
        if not TAG_LINE.match(stripped):
            bad.append(i)
    return bad


def check_structure(text: str, blocks: list[Block]) -> list[str]:
    failures: list[str] = []
    if not blocks:
        failures.append("no blocks parsed")

    for b in blocks:
        if b.domain == "<MISSING>":
            failures.append(f"line {b.line}: block {b.tags[0]} has no DOMAIN: line")

    for i, line in enumerate(text.splitlines(), start=1):
        if not TAG_LINE.match(line.strip()):
            continue
        tags = line.split()
        dupes = {t for t in tags if tags.count(t) > 1}
        if dupes:
            failures.append(f"line {i}: duplicate tags {sorted(dupes)}")

    for line_no in invalid_header_lines(text):
        raw = text.splitlines()[line_no - 1].strip()
        failures.append(
            f"line {line_no}: header does not parse (tags must be [a-z0-9]): {raw}"
        )
    return failures


def check_grounding(mem_path: Path, text: str) -> tuple[list[str], int]:
    refs = sorted({m.group(1).rstrip(".").rstrip(",") for m in PATH_RE.finditer(text)})
    missing = [p for p in refs if not (REPO_ROOT / p).exists()]
    return missing, len(refs)


def tokenize(query: str) -> list[str]:
    return [t for t in re.findall(r"[a-z0-9]+", query.lower()) if t not in STOPWORDS]


def rank(blocks: list[Block], query: str, top: int = 3) -> list[Block]:
    tokens = tokenize(query)
    scored: list[tuple[int, Block]] = []
    for b in blocks:
        body = b.body.lower()
        score = 0
        for tok in tokens:
            if tok in b.tags:
                score += TAG_WEIGHT
            elif re.search(r"\b" + re.escape(tok), body):
                score += BODY_WEIGHT
        if score:
            scored.append((score, b))
    # Same score: prefer the block with the tighter keyword surface.
    scored.sort(key=lambda pair: (-pair[0], len(pair[1].tags)))
    return [b for _, b in scored[:top]]


def run_retrieval(
    cases: tuple[tuple[str, str], ...],
    blocks: list[Block],
    label: str,
    top: int,
    verbose: bool,
) -> list[str]:
    failures: list[str] = []
    hits = 0
    print(f"[{label}]")
    for query, expected in cases:
        ranked = rank(blocks, query, top=max(top, 1))
        domains = [b.domain for b in ranked]
        hit = any(expected.lower() in d.lower() for d in domains)
        hits += int(hit)
        first = domains[0] if domains else "<no match>"
        print(f"  {'PASS' if hit else 'FAIL'}  {query}")
        if verbose or not hit:
            print(f"        rank1: {first}")
            if verbose:
                for n, d in enumerate(domains[1:], start=2):
                    print(f"        rank{n}: {d}")
        if not hit:
            failures.append(f"{label}: {query!r} -> {first} (wanted {expected!r})")
    print(f"  {hits}/{len(cases)} top-{top} hits\n")
    return failures


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--mem", default=".goosemem", help="path to memory file")
    parser.add_argument(
        "--only",
        choices=("structure", "grounding", "retrieval"),
        action="append",
        help="run only the named check (repeatable)",
    )
    parser.add_argument("--top", type=int, default=3, help="acceptable rank window")
    parser.add_argument("--list-blocks", action="store_true", help="print blocks and exit")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    mem_path = Path(args.mem)
    if not mem_path.is_absolute():
        mem_path = REPO_ROOT / mem_path
    if not mem_path.is_file():
        print(f"ERROR: {mem_path} not found", file=sys.stderr)
        return 1

    text = mem_path.read_text(encoding="utf-8")
    blocks = parse_blocks(text)
    wanted = set(args.only or ("structure", "grounding", "retrieval"))
    failures: list[str] = []

    if args.list_blocks:
        for b in blocks:
            print(f"{b.line:>5}  {' '.join(b.tags[:3]):<40}  {b.domain}")
        print(f"\n{len(blocks)} blocks, {len(text.splitlines())} lines, {len(text.encode())} bytes")
        return 0

    print(f"=== .goosemem verification ({mem_path.name}) ===")
    print(f"blocks={len(blocks)} lines={len(text.splitlines())} bytes={len(text.encode())}\n")

    if "structure" in wanted:
        structure = check_structure(text, blocks)
        print(f"[structure] {'PASS' if not structure else 'FAIL'}")
        for problem in structure:
            print(f"  - {problem}")
        print(f"  {len(blocks)} blocks, {sum(len(b.tags) for b in blocks)} tags\n")
        failures.extend(f"structure: {p}" for p in structure)

    if "grounding" in wanted:
        missing, total = check_grounding(mem_path, text)
        print(f"[grounding] {'PASS' if not missing else 'FAIL'}")
        print(f"  {total - len(missing)}/{total} referenced paths exist")
        for path in missing:
            print(f"  - missing: {path}")
        print()
        failures.extend(f"grounding: missing {p}" for p in missing)

    if "retrieval" in wanted:
        failures.extend(run_retrieval(NATURAL_QUERIES, blocks, "retrieval: natural", args.top, args.verbose))
        failures.extend(run_retrieval(KEYWORD_QUERIES, blocks, "retrieval: keyworded", args.top, args.verbose))

    if failures:
        print(f"FAIL: {len(failures)} problem(s)")
        return 1
    print("PASS: structure, grounding, and retrieval all clean")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
