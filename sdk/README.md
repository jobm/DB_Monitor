# DB Monitor SDKs

This directory contains lightweight client SDKs for common DB Monitor API
workflows.

## Included SDKs

- `python/db_monitor_sdk`: synchronous Python package using the standard
  library
- `python/db_monitor_client.py`: compatibility module re-export for legacy
  imports
- `javascript/db_monitor_client.mjs`: JavaScript client using `fetch`

## Supported Workflows

- Exchange an API key for an access token
- List discovered tables
- Read recent events
- Query column changes
- Read admin checkpoints and DLQ rows

See `examples/python_api_client.py` and `examples/javascript_api_client.mjs`
for runnable usage examples.

## Python Packaging

Install the Python SDK in editable mode from the repository root:

```bash
pip install -e sdk/python
```

Then import it normally:

```python
from db_monitor_sdk import DBMonitorClient
```