.PHONY: monitor-help monitor-up monitor-up-sandbox monitor-down monitor-down-sandbox monitor-logs monitor-logs-sandbox monitor-register monitor-dev monitor-migrate monitor-test monitor-test-core monitor-test-integration monitor-test-sandbox monitor-test-sandbox-pytest monitor-test-smoke monitor-test-scale monitor-recovery monitor-tui monitor-tui-build

.DEFAULT_GOAL := monitor-help

monitor-help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

monitor-up: ## Start only the core platform services via Docker Compose
	docker compose up -d
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
	docker compose --profile sandbox up -d

monitor-down: ## Stop the active core-platform services
	docker compose down

monitor-down-sandbox: ## Stop the core platform plus sandbox services
	docker compose --profile sandbox down

monitor-logs: ## Tail logs from the active core-platform services
	docker compose logs -f

monitor-logs-sandbox: ## Tail logs from the core platform plus sandbox services
	docker compose --profile sandbox logs -f

monitor-logs-app: ## Tail logs from the FastAPI app
	docker compose logs -f monitor-server

monitor-register: ## Register Debezium connectors
	docker compose --profile sandbox run --rm connector-registrar

monitor-recovery: ## One-command recovery: start sandbox + register connectors + restart monitor-server
	@echo "Starting sandbox services..."
	docker compose --profile sandbox up -d
	@echo "Waiting for services to initialize..."
	sleep 8
	@echo "Registering Debezium connectors..."
	bash ./register-connectors.sh
	@echo "Waiting for connectors to settle..."
	sleep 3
	@echo "Restarting monitor-server to resume consumer..."
	docker compose restart monitor-server
	@echo "Recovery complete. Monitor consumer should now be connected."
	@echo ""
	docker compose ps monitor-server

monitor-dev: ## Run FastAPI app locally for debugging (Docker handles this - prefer 'make monitor-up')
	cd app && uv run uvicorn main:app --reload --port 8001

monitor-migrate: ## Apply tracked database migrations locally
	cd app && uv run python migrate.py apply

monitor-test: ## Run all core-platform tests
	$(MAKE) monitor-test-core

monitor-test-core: ## Run reusable core-platform pytest coverage
	cd app && uv run --group dev python -m pytest -m core ../tests/

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

monitor-tui-build: ## Build/install TUI dependencies
	cd tui && uv sync

monitor-tui: ## Run the Textual UI dashboard
	cd tui && uv run python app.py
