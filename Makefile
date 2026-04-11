.PHONY: monitor-help monitor-up monitor-down monitor-logs monitor-register monitor-dev monitor-test monitor-tui monitor-tui-build

.DEFAULT_GOAL := monitor-help

monitor-help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

monitor-up: ## Start all services (PostgreSQL, Kafka, FastAPI app, monitoring) via Docker Compose
	docker compose up -d
	@echo ""
	@echo "Services started:"
	@echo "  - FastAPI app:     http://localhost:8000"
	@echo "  - Kafka UI:        http://localhost:8080"
	@echo "  - Grafana:         http://localhost:3000"
	@echo "  - Prometheus:      http://localhost:9090"
	@echo "  - Alertmanager:    http://localhost:9093"

monitor-down: ## Stop all services
	docker compose down

monitor-logs: ## Tail logs from all services
	docker compose logs -f

monitor-logs-app: ## Tail logs from the FastAPI app
	docker compose logs -f monitor-server

monitor-register: ## Register Debezium connectors
	docker compose run --rm connector-registrar

monitor-dev: ## Run FastAPI app locally for debugging (Docker handles this - prefer 'make monitor-up')
	cd app && uv run uvicorn main:app --reload --port 8001

monitor-test: ## Run integration and unit tests
	cd app && uv run pytest ../tests/

monitor-tui-build: ## Build/install TUI dependencies
	cd tui && uv sync

monitor-tui: ## Run the Textual UI dashboard
	cd tui && uv run python app.py
