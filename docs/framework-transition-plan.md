# Framework Transition Plan

## Status

- Date: 2026-05-23
- Type: planning/specification only
- Implementation status: not started

## Purpose

This document defines how to evolve DB Monitor from a repository that ships
with three opinionated source databases into a reusable audit-log
framework/library that can be adopted against arbitrary customer systems.

The current bundled `orderdb`, `catalogdb`, and `shippingdb` assets should
remain available, but only as examples, demos, and regression fixtures. They
should no longer define the mental model, configuration defaults, or
documentation narrative of the product.

This plan is intended to be concrete enough that another model or engineer can
pick up individual work packages and implement them incrementally.

## Problem Statement

Today, the repository still treats the bundled example databases as if they are
part of the product itself.

Current coupling points include:

- top-level docs describe the three demo databases as the default monitored
  system rather than one example deployment
- the connector manifest in `connectors/sources.json` is product-owned and
  checked into the repo with demo source names and tables
- local quick-start flows assume the demo stack is the primary way to run the
  product
- integration tests and operational language are tied closely to the bundled
  source names
- the architecture story is framed around one specific example stack rather
  than a reusable ingestion core with pluggable sources

That coupling makes the product feel like a vertical application for one demo
domain instead of an audit-log platform that users can install around their own
databases and brokers.

## Goals

- Make DB Monitor feel like a general-purpose audit-log framework/library first
- Keep the bundled Postgres/Debezium stack, but present it as an example
  package, not a core assumption
- Preserve the current runtime strengths: CDC ingestion, schema discovery,
  history, replay, metrics, auth, SDKs, TUI, and broker abstraction
- Create an implementation roadmap that can be executed in small, reviewable
  slices
- Keep migration risk low for existing local/demo users

## Non-Goals

- Rewriting the ingestion engine from scratch
- Removing the demo stack entirely
- Supporting every database and broker immediately
- Designing a public plugin ABI in one step
- Changing runtime behavior in this planning pass

## Desired End State

At the end of this transition, DB Monitor should have a clear split between the
core product and example assets.

### Core product

The core product should provide:

- the FastAPI app and ingestion engine
- broker adapters and recovery workflows
- schema discovery and change-history storage
- auth, RBAC, audit logging, metrics, tracing, and TUI surfaces
- migrations and deployable runtime artifacts
- a documented configuration contract for user-supplied sources

### Example assets

Example assets should provide:

- demo source databases and seed SQL
- example connector manifests and compose stacks
- example data generation scripts
- smoke and documentation flows for local evaluation

### Product narrative

The primary narrative should become:

1. install DB Monitor
2. point it at your broker and monitor database
3. supply one or more source manifests/connectors for your systems
4. optionally use the bundled example stack to learn or validate behavior

## Design Principles

- Core before examples: repo-owned demo assets must never be required for the
  runtime contract
- Configuration over hardcoding: source definitions should come from explicit
  manifests or settings, not product defaults tied to demo tables
- Example parity: examples should exercise the same public configuration path
  that real adopters use
- Incremental migration: changes should land in compatible slices where
  possible
- Test separation: example-stack tests and product-core tests should be
  distinguishable

## Current-State Coupling Inventory

The following areas currently encode demo assumptions and will need review:

| Area | Current coupling | Transition direction |
| --- | --- | --- |
| `README.md` | Presents demo databases as the default product shape | Rewrite around framework-first install and separate example quick start |
| `docs/architecture.md` | Lists demo databases as runtime components | Describe generic source systems and move demo topology to example docs |
| `connectors/sources.json` | Checked-in demo manifest is treated as canonical | Treat as example manifest and allow user-owned manifests |
| `docker-compose.yml` | Core and demo infrastructure are mixed together | Separate core services from example/demo overlays |
| `examples/sandbox/init/*.sql` | Demo schema seeds live under examples | Keep them labeled as demo fixtures and separate from core runtime docs |
| `scripts/*.py` | Some scripts assume the example topology | Split into product utilities versus example/demo tooling |
| integration tests | Live flows rely on demo sources | Keep demo-backed tests, but classify them as example-stack integration coverage |

## Proposed Repository Model

The repo should be reorganized conceptually into three layers.

### 1. Core runtime layer

Owns the application, SDKs, migrations, API contract, and deployment contract.

Likely contents:

- `app/`
- core docs
- SDKs
- migrations
- deployment/runbook docs

### 2. Configuration layer

Owns manifests and environment contracts that tell the core runtime what to
monitor.

Target characteristics:

- user-supplied source definitions
- documented schema for manifests
- optional starter templates
- no repo-specific service names required by default

### 3. Example layer

Owns demo databases, compose overrides, seed scripts, and sample manifests.

Target characteristics:

- clearly labeled as examples
- runnable out of the box for local evaluation
- uses the same public configuration mechanisms as customer deployments

## Proposed Configuration Model

The current manifest approach is a strong starting point and should be expanded
instead of replaced.

### Requirements

- source definitions must be externalizable from repo defaults
- one install should support zero, one, or many source systems
- the app should boot cleanly even when the demo manifest is absent
- example manifests should be optional starter files
- docs should distinguish required runtime settings from example-only values

### Proposed direction

- define a stable source-manifest contract as a public configuration surface
- allow the monitor to read source manifests from a user-specified path,
  mounted directory, or deployment package
- ship one or more example manifests alongside starter templates
- ensure connector registration and topic derivation continue to read from the
  same public manifest contract

### Open design questions

- whether manifests should support versioning from day one
- whether example manifests should live under `examples/` or `deploy/examples/`
- whether `docker-compose.yml` should stay monolithic or gain overlays/profiles
- whether the core app should start with no sources configured, or fail fast
  with a guided configuration error

## Documentation Strategy

Documentation should be rewritten around two entry points.

### Core product path

This path explains how adopters run DB Monitor against their own systems.

Must include:

- product overview
- configuration contract
- broker/database requirements
- install and deployment paths
- migration workflow
- security guidance
- operational readiness guidance

### Example path

This path explains how to run the bundled demo stack.

Must include:

- how to start the example compose stack
- how demo sources map to CDC tables
- how to generate test data
- how to reset and inspect the example environment

## Testing Strategy

The test plan should also split between core and example concerns.

### Core validation

- unit tests for parsing, discovery, routing, auth, replay, and configuration
- integration tests that validate product behavior against abstract fixtures
- migration validation independent of demo source names

### Example validation

- live integration tests against the demo compose stack
- example-manifest registration checks
- seeded smoke flows that prove the example assets still work

### Outcome

Failures in example-stack tests should be understandable as example breakages,
not confusion about whether the core product contract is broken.

## Delivery Plan

Implementation should be split into independently reviewable workstreams.

### Workstream 1: Product framing and docs

Objective:
Reframe DB Monitor docs so the product is described as a framework first and
the demo stack becomes one optional path.

Scope:

- rewrite `README.md` overview and quick start structure
- update architecture and configuration docs
- add explicit example-stack documentation

Acceptance criteria:

- a new reader can tell what is core versus example
- the default documentation no longer implies `orderdb` / `catalogdb` /
  `shippingdb` are required product entities

### Workstream 2: Source-manifest public contract

Objective:
Formalize the manifest and treat it as a public configuration API.

Scope:

- document manifest schema and ownership rules
- support template/example manifests distinctly from user manifests
- remove assumptions that the checked-in manifest is always the active runtime

Acceptance criteria:

- users can point the app and registrar at a manifest they own
- example manifests work through the same contract as real deployments

### Workstream 3: Runtime/bootstrap separation

Objective:
Separate the core runtime from demo infrastructure packaging.

Scope:

- decouple compose layout or introduce profiles/overlays
- make demo source databases optional
- ensure app startup messaging guides users when no sources are configured

Acceptance criteria:

- the product can be deployed without bundling demo databases
- the example stack remains easy to launch locally

### Workstream 4: Script and asset cleanup

Objective:
Move demo-specific scripts and seeds out of the product centerline.

Scope:

- classify scripts as core utilities versus example tooling
- relocate or relabel seed SQL and demo generators
- make script names and docs reflect their purpose clearly

Acceptance criteria:

- product utilities can be discovered without reading demo-specific scripts
- demo assets are clearly named and grouped

### Workstream 5: Test suite separation

Objective:
Separate core-product tests from example-stack tests.

Scope:

- classify test targets and CI jobs accordingly
- keep demo-backed integration coverage while clarifying its role
- stabilize the live integration workflow separately from product refactoring

Acceptance criteria:

- CI surfaces whether a failure belongs to core behavior or the example stack
- demo-stack readiness issues no longer block understanding of product quality

### Workstream 6: Packaging and adoption flow

Objective:
Make DB Monitor feel installable as a reusable platform component.

Scope:

- document deployable runtime artifacts
- define starter templates for manifests and secrets
- align SDK/examples with framework-first onboarding

Acceptance criteria:

- a new adopter has a clear path to onboard their own systems without editing
  demo-owned files first

## Suggested Implementation Order

1. Land documentation reframing and backlog/task structure
2. Formalize the public manifest contract
3. Separate demo assets from runtime defaults
4. Split CI and tests into core versus example tracks
5. Refine packaging and onboarding ergonomics

This order keeps user-facing clarity improving first while reducing the chance
of a large structural refactor without a documented target.

## Task Breakdown For Another Model

The following task slices are intended to be implementable independently.

### Slice A: Documentation reframing only

- rewrite top-level docs around framework-first positioning
- add an example-stack document
- update backlog references

Expected files:

- `README.md`
- `docs/architecture.md`
- `docs/configuration.md`
- new example-focused doc(s)

### Slice B: Manifest contract formalization

- document the manifest schema as a public interface
- add starter template examples
- ensure docs explain how to provide a custom manifest

Expected files:

- `docs/configuration.md`
- `connectors/` templates/examples
- registrar documentation

### Slice C: Compose and asset separation

- move or profile-gate demo source DBs and seed assets
- keep a runnable demo path
- update startup commands and docs

Expected files:

- `docker-compose.yml`
- example compose files or profiles
- `examples/sandbox/init/` and `scripts/` assets

### Slice D: Test and CI split

- split core checks from example-stack checks
- rename CI jobs to reflect their role
- keep the readiness timeout issue tracked as separate stabilization work

Expected files:

- `.github/workflows/ci.yml`
- test scripts
- docs/runbooks if needed

### Slice E: Packaging/onboarding polish

- add starter templates and clearer install flows
- align SDK and example client docs with framework-first usage

Expected files:

- `README.md`
- `sdk/README.md`
- example clients/scripts

## Review Checklist

When another model implements a slice from this plan, review against the
following questions:

- Does the change reduce demo-stack coupling, or only rename it?
- Can a user understand how to adopt DB Monitor for their own systems?
- Do examples still use the same public contract as real deployments?
- Did the change avoid breaking existing demo flows without explanation?
- Are docs, tests, and scripts clearly labeled as `core` or `example` where
  relevant?
- Did the implementation avoid embedding new repo-specific source names into
  runtime defaults?

## Deferred Related Issue

The current CI `Wait for app readiness` timeout should be tracked separately as
stabilization work. It matters because the live integration workflow is part of
the example stack story, but it should not block planning or implementation of
the broader framework transition.

## Exit Criteria For The Overall Initiative

This initiative can be considered complete when:

- DB Monitor can be explained and deployed without referencing the bundled demo
  databases as required product components
- the bundled source databases remain available as examples only
- configuration docs describe a public, user-owned source onboarding contract
- CI distinguishes core validation from example-stack validation
- a new contributor can pick up an isolated task slice from this plan and ship
  it without needing to rediscover the transition strategy