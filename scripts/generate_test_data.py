from __future__ import annotations

from examples.sandbox import generate_test_data as _impl


generate_all = _impl.generate_all
main = _impl.main


if __name__ == "__main__":
    raise SystemExit(main())
