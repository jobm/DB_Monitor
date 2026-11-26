.PHONY: monitor-help monitor-up monitor-start monitor-check-context monitor-up-sandbox monitor-down monitor-down-sandbox monitor-logs monitor-logs-sandbox monitor-register monitor-dev monitor-migrate monitor-test monitor-test-core monitor-test-integration monitor-test-sandbox monitor-test-sandbox-pytest monitor-test-smoke monitor-test-scale monitor-test-scale-target monitor-test-container-readyz monitor-lint monitor-lint-fix monitor-typecheck monitor-package-smoke monitor-recovery monitor-tui monitor-tui-kind monitor-tui-build monitor-port-forward monitor-port-forward-stop monitor-seed-kind monitor-api-key monitor-api-key-kind monitor-status monitor-check monitor-demo-reset monitor-demo-reset-kind monitor-demo-kind monitor-test-quota monitor-test-lifecycle test lint typecheck

PODMAN_COMPOSE ?= $(shell command -v podman-compose 2>/dev/null || echo "docker compose")

.DEFAULT_GOAL := monitor-help

monitor-help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

monitor-up: ## Start only the core platform services via Docker Compose
	$(PODMAN_COMPOSE) up -d
	@echo ""
	@echo "Services started:"
	@echo "  - FastAPI app:     http://localhost:8000"
	@echo "  - Kafka UI:        http://localhost:8080"
	@echo "  - Grafana:         http://localhost:3000"
	@echo "  - Prometheus:      http://localhost:9090"
	@echo "  - Alertmanager:    http://localhost:9093"
	@echo ""
	@echo "For the bundled example databases and Debezium stack, run:"
	@echo "  make monitor-up-sandbox"

monitor-up-sandbox: ## Start the core platform plus the optional sandbox services
	$(PODMAN_COMPOSE) --profile sandbox up -d

monitor-demo-reset: ## Reset demo data in the local (Compose) monitor DB (requires DEMO_MODE=1)
	DEMO_MODE=1 uv run python scripts/reset_demo_db.py --yes

monitor-demo-reset-kind: ## Reset demo data in the Kind monitor DB
	@kubectl --context $${KIND_CONTEXT:-kind-dbm} -n $${NAMESPACE:-dbm-smoke} \
	 exec -i deploy/$${RELEASE:-smoke}-db-monitor-api -- \
	 env DEMO_MODE=1 python - --yes < scripts/reset_demo_db.py

monitor-demo-kind: ## Reset + seed demo data in Kind, then print next steps
	@$(MAKE) monitor-demo-reset-kind
	@SEED_BASE=0 $(MAKE) monitor-seed-kind
	@echo "Demo data ready. Run: make monitor-tui-kind"

monitor-start: ## One command: start the full local (Compose) stack and wait for health
	@$(PODMAN_COMPOSE) --profile sandbox up -d
	@echo "Waiting for the API to become healthy..."
	@until curl -fsS http://localhost:8000/health >/dev/null 2>&1; do sleep 2; done
	@echo "Stack is up. Next steps:"
	@echo "  make monitor-api-key          # generate an admin API key"
	@echo "  make monitor-tui             # launch the dashboard"

monitor-down: ## Stop the active core-platform services
	$(PODMAN_COMPOSE) down

monitor-down-sandbox: ## Stop the core platform plus sandbox services
	$(PODMAN_COMPOSE) --profile sandbox down

monitor-logs: ## Tail logs from the active core-platform services
	$(PODMAN_COMPOSE) logs -f

monitor-logs-sandbox: ## Tail logs from the core platform plus sandbox services
	$(PODMAN_COMPOSE) --profile sandbox logs -f

monitor-logs-app: ## Tail logs from the FastAPI app
	$(PODMAN_COMPOSE) logs -f monitor-server

monitor-register: ## Register Debezium connectors
	$(PODMAN_COMPOSE) --profile sandbox run --rm connector-registrar

monitor-recovery: ## One-command recovery: start sandbox + register connectors + restart monitor-server
	@echo "Starting sandbox services..."
	$(PODMAN_COMPOSE) --profile sandbox up -d
	@echo "Waiting for services to initialize..."
	sleep 8
	@echo "Registering Debezium connectors..."
	bash ./register-connectors.sh
	@echo "Waiting for connectors to settle..."
	sleep 3
	@echo "Restarting monitor-server to resume consumer..."
	$(PODMAN_COMPOSE) restart monitor-server
	@echo "Waiting for monitor-server health endpoint..."
	@until curl -fsS http://localhost:8000/health >/dev/null; do \
		sleep 3; \
	done
	@echo "Recovery complete. Monitor consumer should now be connected."
	@echo ""
	$(PODMAN_COMPOSE) ps monitor-server

monitor-dev: ## Run FastAPI app locally for debugging (Docker handles this - prefer 'make monitor-up')
	uv run python -m uvicorn db_monitor.asgi:app --reload --port 8001

monitor-migrate: ## Apply tracked database migrations locally
	uv run python -m db_monitor.cli migrate apply

monitor-test: ## Run all core-platform tests
	$(MAKE) monitor-test-core

monitor-test-core: ## Run reusable core-platform pytest coverage
	uv run --group dev python -m pytest -m core tests/

monitor-lint: ## Run Ruff lint checks for package and tests
	uv run --group dev python -m ruff check src/db_monitor tests

monitor-lint-fix: ## Auto-fix lint issues where possible
	uv run --group dev python -m ruff check --fix src/db_monitor tests

monitor-typecheck: ## Run mypy checks for the package namespace
	uv run --group dev python -m mypy

monitor-test-quota: ## Run ingestion quota tests (unit + property-based)
	uv run --group dev python -m pytest tests/test_ingestion_quota.py tests/test_ingestion_quota_property.py -v --tb=short

monitor-test-lifecycle: ## Run lifecycle manager tests
	uv run --group dev python -m pytest tests/test_lifecycle_manager.py -v --tb=short

# ── Short aliases ─────────────────────────────────────────────────────
# These mirror the canonical monitor-* targets for muscle-memory convenience.

test: ## Short alias for monitor-test-core
	$(MAKE) monitor-test-core

lint: ## Short alias for monitor-lint
	$(MAKE) monitor-lint

typecheck: ## Short alias for monitor-typecheck
	$(MAKE) monitor-typecheck

monitor-publish: ## Build wheel/sdist and publish to PyPI (use TWINE_USERNAME + TWINE_PASSWORD)
	uv run --group dev python -m build
	uv run --group dev python -m twine upload dist/*

monitor-publish-test: ## Build wheel/sdist and publish to Test PyPI
	uv run --group dev python -m build
	uv run --group dev python -m twine upload --repository-url https://test.pypi.org/legacy/ dist/*

monitor-publish-sdk: ## Build and publish the Python SDK package separately
	cd sdk/python && uv run --group dev python -m build
	cd sdk/python && uv run --group dev python -m twine upload dist/*

monitor-container-publish: ## Build and push the container image to GitHub Container Registry
	$(PODMAN_COMPOSE) build monitor-server
	docker tag db-monitor_monitor-server ghcr.io/jobm/db-monitor:latest
	docker push ghcr.io/jobm/db-monitor:latest

monitor-package-smoke: ## Build wheel/sdist and verify package import in clean venv
	rm -rf dist .pkg-smoke-wheel .pkg-smoke-sdist
	uv run --group dev python -m build
	uv venv --python 3.11 --seed .pkg-smoke-wheel
	.pkg-smoke-wheel/bin/pip install --upgrade pip
	.pkg-smoke-wheel/bin/pip install dist/*.whl
	.pkg-smoke-wheel/bin/python -c "import db_monitor; import db_monitor.cli; import db_monitor.public_api"
	uv venv --python 3.11 --seed .pkg-smoke-sdist
	.pkg-smoke-sdist/bin/pip install --upgrade pip
	.pkg-smoke-sdist/bin/pip install dist/*.tar.gz
	.pkg-smoke-sdist/bin/python -c "import db_monitor; import db_monitor.cli; import db_monitor.public_api"

monitor-test-container-readyz: ## Start monitor container and validate readiness endpoint semantics
	@test -f secrets/monitor_postgres_url || cp secrets/monitor_postgres_url.example secrets/monitor_postgres_url
	@test -f secrets/monitor_jwt_secret || cp secrets/monitor_jwt_secret.example secrets/monitor_jwt_secret
	@test -f secrets/monitor_jwt_secret_next || cp secrets/monitor_jwt_secret_next.example secrets/monitor_jwt_secret_next
	$(PODMAN_COMPOSE) stop monitor-server postgres-monitor kafka zookeeper 2>/dev/null || true
	$(PODMAN_COMPOSE) rm -fv monitor-server postgres-monitor kafka zookeeper 2>/dev/null || true
	@python3 scripts/check_port_free.py 8000
	DB_SCHEMA_MODE=apply APP_SHUTDOWN_TIMEOUT_SECONDS=30 $(PODMAN_COMPOSE) up -d --build zookeeper kafka postgres-monitor
	DB_SCHEMA_MODE=apply APP_SHUTDOWN_TIMEOUT_SECONDS=30 $(PODMAN_COMPOSE) up -d --build --no-deps monitor-server
	@echo "Waiting for monitor-server to start..."
	@sleep 30
	python3 scripts/check_container_readyz.py || ($(PODMAN_COMPOSE) logs --tail=200 monitor-server postgres-monitor kafka zookeeper || true; $(PODMAN_COMPOSE) stop monitor-server postgres-monitor kafka zookeeper 2>/dev/null || true; $(PODMAN_COMPOSE) rm -fv monitor-server postgres-monitor kafka zookeeper 2>/dev/null || true; exit 1)
	$(PODMAN_COMPOSE) stop monitor-server postgres-monitor kafka zookeeper 2>/dev/null || true
	$(PODMAN_COMPOSE) rm -fv monitor-server postgres-monitor kafka zookeeper 2>/dev/null || true

monitor-test-integration: ## Run sandbox-backed live integration checks
	uv run --group dev python examples/sandbox/run_integration_tests.py

monitor-test-integration-slow: ## Run slow-marked integration tests with real Kafka via testcontainers or KAFKA_BOOTSTRAP_SERVERS
	uv run --group dev python -m pytest -m slow tests/ -v --tb=short

monitor-test-sandbox-pytest: ## Run sandbox-only pytest checks against a live stack
	uv run --group dev python -m pytest -m sandbox tests/

monitor-test-sandbox: ## Run the full sandbox verification flow
	$(MAKE) monitor-test-sandbox-pytest
	$(MAKE) monitor-test-integration
	$(MAKE) monitor-test-smoke

monitor-test-smoke: ## Run the sandbox smoke-load gate against a running stack
	uv run --group dev python examples/sandbox/load_test.py --workers 8 --requests 10 --targets events,stats,tables,checkpoints --max-failures 0 --min-success-rate 1.0 --max-p95-ms 2000

monitor-test-scale: ## Validate cross-replica websocket delivery with two local app instances
	uv run --group dev python scripts/run_horizontal_scaling_validation.py

monitor-test-scale-target: ## Run scale validation at target throughput (100-DB profile)
	uv run python scripts/scale_test.py --sources 10 --rate 500 --duration 300

monitor-tui-build: ## Build/install TUI dependencies
	cd tui && uv sync

monitor-tui: ## Run the Textual UI dashboard against the local Compose stack (localhost:8000)
	cd tui && uv run python app.py

monitor-tui-kind: ## Run the Textual UI dashboard against the Kind stack (requires monitor-port-forward)
	@for p in 8000 8001; do \
	  code=$$(curl -s -o /dev/null -w "%{http_code}" -m 3 http://localhost:$$p/health 2>/dev/null || echo "000"); \
	  if [ "$$code" != "200" ]; then \
	    echo "localhost:$$p not reachable (got $$code). Run 'make monitor-port-forward' first."; \
	    exit 1; \
	  fi; \
	done
	cd tui && DB_MONITOR_BASE_URL=http://localhost:8000 uv run python app.py

monitor-status: ## Show which stack (Compose/Kind) is responding on 8000/8001
	@echo "localhost:8000/health -> $$(curl -s -o /dev/null -w '%{http_code}' -m 3 http://localhost:8000/health 2>/dev/null || echo 000)"
	@echo "localhost:8001/health -> $$(curl -s -o /dev/null -w '%{http_code}' -m 3 http://localhost:8001/health 2>/dev/null || echo 000)"
	@curl -s -m 3 http://localhost:8001/health 2>/dev/null | python3 -c "import json,sys; d=json.load(sys.stdin); c=d.get('checks',{}).get('consumer',{}); print('consumer running=',c.get('running'),'connected=',c.get('connected'),'lag=',c.get('lag_total'))" 2>/dev/null || true

monitor-port-forward: ## Keep Kind API/consumer port-forwards alive (8000/8001) for the TUI
	@KIND_CONTEXT=$${KIND_CONTEXT:-kind-dbm} \
	 NAMESPACE=$${NAMESPACE:-dbm-smoke} \
	 RELEASE=$${RELEASE:-smoke} \
	 API_PORT=$${API_PORT:-8000} \
	 CONSUMER_PORT=$${CONSUMER_PORT:-8001} \
	 bash scripts/kind_port_forward.sh

monitor-port-forward-stop: ## Stop the Kind port-forwards started by monitor-port-forward
	@bash scripts/kind_port_forward.sh --stop

monitor-seed-kind: ## Publish synthetic CDC events into the Kind cluster (TUI demo data)
	@kubectl --context $${KIND_CONTEXT:-kind-dbm} -n $${NAMESPACE:-dbm-smoke} \
	 exec -i deploy/$${RELEASE:-smoke}-db-monitor-consumer -- \
	 env SEED_BASE=$${SEED_BASE:-0} python - < scripts/seed_kind_events.py

monitor-api-key: ## Generate an admin API key against the local (Compose) database
	@uv run python scripts/seed_admin_api_key.py \
		--owner-name $${OWNER_NAME:-admin} \
		--ttl-days $${TTL_DAYS:-90}

monitor-api-key-kind: ## Generate an admin API key inside the Kind cluster (prints composite key)
	@kubectl --context $${KIND_CONTEXT:-kind-dbm} -n $${NAMESPACE:-dbm-smoke} \
	 exec -i deploy/$${RELEASE:-smoke}-db-monitor-api -- \
	 env OWNER_NAME=$${OWNER_NAME:-admin} TTL_DAYS=$${TTL_DAYS:-90} \
	 python - < scripts/seed_admin_api_key.py

monitor-check: ## Run the standard verification gate (lint + typecheck + core tests)
	@$(MAKE) monitor-lint
	@$(MAKE) monitor-typecheck
	@$(MAKE) monitor-test-core

monitor-docs-build: ## Build the MkDocs documentation site
	uv run --group dev mkdocs build --strict

monitor-docs-serve: ## Serve the MkDocs documentation site locally
	uv run --group dev mkdocs serve

monitor-docs-deploy: ## Deploy the MkDocs documentation site to GitHub Pages
	uv run --group dev mkdocs gh-deploy --force

monitor-check-context: ## Verify .goosemem structure, grounding, retrieval and module coverage
	python3 scripts/verify_goosemem_retrieval.py
	bash scripts/validate_goosemem_coverage.sh
