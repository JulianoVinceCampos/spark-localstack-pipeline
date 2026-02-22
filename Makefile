# ──────────────────────────────────────────────────────────────────────────────
#  Makefile  –  spark-localstack-pipeline
#  Usage: make <target>
# ──────────────────────────────────────────────────────────────────────────────

.PHONY: help up down build logs shell \
        data pipeline pipeline-ingestion pipeline-transform pipeline-load \
        test test-cov lint format clean status

# ── Colors ────────────────────────────────────────────────────────────────────
GREEN  := \033[0;32m
YELLOW := \033[1;33m
CYAN   := \033[0;36m
RESET  := \033[0m

help: ## 📋 Show this help
	@echo ""
	@echo "  $(CYAN)spark-localstack-pipeline$(RESET)"
	@echo "  LocalStack + PySpark end-to-end data pipeline"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-22s$(RESET) %s\n", $$1, $$2}'
	@echo ""

# ── Infrastructure ────────────────────────────────────────────────────────────
up: ## 🚀 Start all services (LocalStack + Spark cluster)
	@echo "$(YELLOW)Starting services...$(RESET)"
	docker compose up -d
	@echo "$(YELLOW)Waiting for LocalStack health...$(RESET)"
	@timeout 120 bash -c 'until curl -sf http://localhost:4566/_localstack/health; do sleep 3; printf "."; done'
	@echo "\n$(GREEN)Stack is ready!$(RESET)"
	@echo "  Spark UI  → http://localhost:8080"
	@echo "  LocalStack → http://localhost:4566"

down: ## 🛑 Stop and remove all containers + volumes
	docker compose down -v
	@echo "$(GREEN)Stack stopped.$(RESET)"

build: ## 🏗️  Build Docker images from scratch
	docker compose build --no-cache

logs: ## 📜 Follow logs from all services
	docker compose logs -f

shell: ## 🔧 Open a shell inside the pipeline-runner container
	docker compose exec pipeline-runner bash

status: ## 📊 Show running containers and S3 buckets
	@echo "$(CYAN)=== Containers ===$(RESET)"
	docker compose ps
	@echo ""
	@echo "$(CYAN)=== S3 Buckets ===$(RESET)"
	@aws --endpoint-url=http://localhost:4566 s3 ls 2>/dev/null || echo "(LocalStack not running)"

# ── Data Generation ───────────────────────────────────────────────────────────
data: ## 🎲 Generate 50k synthetic orders and upload to S3 bronze
	@echo "$(YELLOW)Generating data...$(RESET)"
	python scripts/generate_data.py --rows 50000 --endpoint http://localhost:4566
	@echo "$(GREEN)Done! Data is in s3://pipeline-data/bronze/$(RESET)"

data-small: ## 🎲 Generate 5k rows (quick test)
	python scripts/generate_data.py --rows 5000 --endpoint http://localhost:4566

# ── Pipeline Execution ────────────────────────────────────────────────────────
pipeline: ## ⚡ Run full pipeline (ingestion → transformation → load)
	@echo "$(YELLOW)Running full pipeline...$(RESET)"
	docker compose exec -T pipeline-runner bash -c "\
		cd /opt/spark/work-dir && \
		SPARK_MASTER_URL=local[*] \
		AWS_ENDPOINT_URL=http://localstack:4566 \
		python -m src.pipeline --steps ingestion,transformation,load"

pipeline-ingestion: ## ⚡ Run ingestion step only (CSV → Bronze)
	docker compose exec -T pipeline-runner bash -c "\
		cd /opt/spark/work-dir && \
		SPARK_MASTER_URL=local[*] \
		AWS_ENDPOINT_URL=http://localstack:4566 \
		python -m src.pipeline --steps ingestion"

pipeline-transform: ## ⚡ Run transformation step only (Bronze → Silver)
	docker compose exec -T pipeline-runner bash -c "\
		cd /opt/spark/work-dir && \
		SPARK_MASTER_URL=local[*] \
		AWS_ENDPOINT_URL=http://localstack:4566 \
		python -m src.pipeline --steps transformation"

pipeline-load: ## ⚡ Run load step only (Silver → Gold)
	docker compose exec -T pipeline-runner bash -c "\
		cd /opt/spark/work-dir && \
		SPARK_MASTER_URL=local[*] \
		AWS_ENDPOINT_URL=http://localstack:4566 \
		python -m src.pipeline --steps load"

# ── Testing ───────────────────────────────────────────────────────────────────
install-dev: ## 📦 Install dev dependencies locally
	pip install -r requirements.txt -r requirements-dev.txt

test: ## 🧪 Run unit tests
	pytest tests/ -v --tb=short -p no:warnings

test-cov: ## 🧪 Run unit tests with HTML coverage report
	pytest tests/ -v \
		--cov=src \
		--cov-report=html:htmlcov \
		--cov-report=term-missing \
		--tb=short \
		-p no:warnings
	@echo "$(GREEN)Coverage report: open htmlcov/index.html$(RESET)"

# ── Code Quality ──────────────────────────────────────────────────────────────
lint: ## 🔍 Run ruff linter
	ruff check src/ tests/ scripts/

format: ## ✨ Auto-format with black and ruff
	black src/ tests/ scripts/
	ruff check --fix src/ tests/ scripts/

# ── Cleanup ───────────────────────────────────────────────────────────────────
clean: ## 🧹 Remove generated files and caches
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov"       -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -f coverage.xml
	rm -f data/sample/*.csv
	@echo "$(GREEN)Clean!$(RESET)"

# ── Quick-start (one command demo) ───────────────────────────────────────────
demo: up data-small pipeline ## 🎬 Full demo: start stack + generate 5k rows + run pipeline
	@echo ""
	@echo "$(GREEN)╔══════════════════════════════════════╗$(RESET)"
	@echo "$(GREEN)║  Pipeline demo complete! 🎉           ║$(RESET)"
	@echo "$(GREEN)║  Spark UI  → http://localhost:8080   ║$(RESET)"
	@echo "$(GREEN)╚══════════════════════════════════════╝$(RESET)"
