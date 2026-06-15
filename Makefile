.PHONY: monitor-help monitor-up monitor-validate-goosemem monitor-up-sandbox monitor-down monitor-down-sandbox monitor-logs monitor-logs-sandbox monitor-register monitor-dev monitor-migrate monitor-test monitor-test-core monitor-test-integration monitor-test-sandbox monitor-test-sandbox-pytest monitor-test-smoke monitor-test-scale monitor-test-scale-target monitor-test-container-readyz monitor-lint monitor-typecheck monitor-package-smoke monitor-recovery monitor-tui monitor-tui-build

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
	cd app && PYTHONPATH=../src:. uv run python -m uvicorn db_monitor.main:app --reload --port 8001

monitor-migrate: ## Apply tracked database migrations locally
	cd app && PYTHONPATH=../src:. uv run python -m db_monitor.cli migrate apply

monitor-test: ## Run all core-platform tests
	$(MAKE) monitor-test-core

monitor-test-core: ## Run reusable core-platform pytest coverage
	cd app && uv run --group dev python -m pytest -m core ../tests/

monitor-lint: ## Run Ruff lint checks for package and tests
	uv run --group dev python -m ruff check src/db_monitor tests

monitor-typecheck: ## Run mypy checks for the package namespace
	uv run --group dev python -m mypy

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
	@python scripts/check_port_free.py 8000
	DB_SCHEMA_MODE=apply $(PODMAN_COMPOSE) up -d --build monitor-server
	python scripts/check_container_readyz.py || ($(PODMAN_COMPOSE) logs --tail=200 monitor-server postgres-monitor kafka; $(PODMAN_COMPOSE) down -v; exit 1)
	$(PODMAN_COMPOSE) down -v

monitor-test-integration: ## Run sandbox-backed live integration checks
	cd app && uv run --group dev python ../examples/sandbox/run_integration_tests.py

monitor-test-sandbox-pytest: ## Run sandbox-only pytest checks against a live stack
	cd app && uv run --group dev python -m pytest -m sandbox ../tests/

monitor-test-sandbox: ## Run the full sandbox verification flow
	$(MAKE) monitor-test-sandbox-pytest
	$(MAKE) monitor-test-integration
	$(MAKE) monitor-test-smoke

monitor-test-smoke: ## Run the sandbox smoke-load gate against a running stack
	cd app && uv run --group dev python ../examples/sandbox/load_test.py --workers 8 --requests 10 --targets events,stats,tables,checkpoints --max-failures 0 --min-success-rate 1.0 --max-p95-ms 2000

monitor-test-scale: ## Validate cross-replica websocket delivery with two local app instances
	cd app && uv run --group dev python ../scripts/run_horizontal_scaling_validation.py

monitor-test-scale-target: ## Run scale validation at target throughput (100-DB profile)
	cd app && PYTHONPATH=../src:. uv run python ../scripts/scale_test.py \
		--sources 10 --rate 500 --duration 300

monitor-tui-build: ## Build/install TUI dependencies
	cd tui && uv sync

monitor-tui: ## Run the Textual UI dashboard
	cd tui && uv run python app.py

monitor-validate-goosemem: ## Validate .goosemem coverage against source files
	bash scripts/validate_goosemem_coverage.sh
