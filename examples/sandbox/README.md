# Evaluation Sandbox Assets

This directory contains the canonical example-stack assets for the bundled
DB Monitor sandbox.

Contents:

- `init/*.sql`: seed schemas and sample rows for the three example source
  databases
- `generate_test_data.py`: writes transactional changes into the example
  databases
- `load_test.py`: runs smoke-load checks against a live sandbox-backed API
- `run_integration_tests.py`: exercises recovery and ingestion paths against a
  running example stack

These assets are example-only. Core-platform utilities remain under `scripts/`.

For backwards compatibility, the older top-level demo entrypoints under
`scripts/` still exist as thin wrappers over these implementations. New docs,
automation, and operator workflows should target `examples/sandbox/` directly.