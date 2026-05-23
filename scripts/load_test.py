from __future__ import annotations

from examples.sandbox import load_test as _impl


BASE_URL = _impl.BASE_URL
PRESEEDED_ADMIN_KEY = _impl.PRESEEDED_ADMIN_KEY
DEFAULT_TARGETS = _impl.DEFAULT_TARGETS
LOAD_TARGETS = _impl.LOAD_TARGETS
bootstrap_admin_key = _impl.bootstrap_admin_key
evaluate_thresholds = _impl.evaluate_thresholds
exchange_access_token = _impl.exchange_access_token
fetch_checkpoints = _impl.fetch_checkpoints
fetch_events = _impl.fetch_events
fetch_stats = _impl.fetch_stats
fetch_tables = _impl.fetch_tables
load_test = _impl.load_test
main = _impl.main
_build_task_cycle = _impl._build_task_cycle
_p95_milliseconds = _impl._p95_milliseconds
_parse_targets = _impl._parse_targets
_resolve_access_token = _impl._resolve_access_token


if __name__ == "__main__":
    raise SystemExit(main())
