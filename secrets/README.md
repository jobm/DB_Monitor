Place local secret files in this directory for Docker Compose runs.

Expected files:

- `monitor_postgres_url`
- `monitor_jwt_secret`
- `monitor_jwt_secret_next` (optional, only during JWT rotation)

Copy the `*.example` files to the same names without the `.example` suffix and
replace their contents with real values before running the production-style
compose stack.