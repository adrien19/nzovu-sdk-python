.PHONY: help install install-dev lock update clean clean-all test lint format gen-proto check-proto setup-dirs update-proto all ci build publish

# Configuration
PROTO_PATH := ./proto
OUTPUT_PATH := ./chronoqueue/api
PYTHON := python3
CHRONOQUEUE_REPO ?= adrien19/chronoqueue
CHRONOQUEUE_BRANCH ?= develop
CHRONOQUEUE_PROTO_PATH ?= proto

# Default target
help:
	@echo "Chronoqueue Python SDK - Available Targets:"
	@echo ""
	@echo "  make install          - Install production dependencies"
	@echo "  make install-dev      - Install development dependencies"
	@echo "  make lock             - Lock dependencies (update poetry.lock)"
	@echo "  make update           - Update dependencies to latest versions"
	@echo "  make update-proto     - Download latest proto definitions from chronoqueue repo"
	@echo "  make gen-proto        - Generate Python gRPC classes from proto files"
	@echo "  make check-proto      - Verify proto file exists"
	@echo "  make clean            - Remove build artifacts and cache (keeps generated proto code)"
	@echo "  make clean-all        - Remove everything including generated proto code"
	@echo "  make test             - Run unit tests"
	@echo "  make test-coverage    - Run unit tests with coverage"
	@echo "  make lint             - Run linting checks"
	@echo "  make format           - Format code with black and isort"
	@echo "  make typecheck        - Run type checking with mypy"
	@echo "  make build            - Build package distribution"
	@echo "  make ci               - Run all CI checks (lint, test)"
	@echo "  make all              - Setup and generate everything"
	@echo ""

# Install production dependencies
install:
	@echo "Installing production dependencies..."
	$(PYTHON) -m pip install --user grpcio protobuf pydantic

# Install development dependencies
install-dev:
	@echo "Installing development dependencies..."
	$(PYTHON) -m pip install --user grpcio protobuf pydantic grpcio-tools mypy-protobuf pytest pytest-cov pytest-asyncio black isort flake8 mypy

# Lock dependencies
lock:
	@echo "Locking dependencies..."
	poetry lock
	@echo "Dependencies locked successfully!"

# Update dependencies to latest versions
update:
	@echo "Updating dependencies..."
	poetry update
	@echo "Dependencies updated successfully!"
	@echo "Remember to test with 'make ci' before committing!"

# Setup directory structure
setup-dirs:
	@echo "Setting up directory structure..."
	@mkdir -p $(PROTO_PATH)
	@mkdir -p $(OUTPUT_PATH)
	@touch $(OUTPUT_PATH)/__init__.py

# Update proto definitions from chronoqueue repository
update-proto: setup-dirs
	@echo "Downloading proto definitions from chronoqueue repository..."
	@echo "Fetching proto files from $(CHRONOQUEUE_REPO)/$(CHRONOQUEUE_BRANCH)..."
	@rm -rf /tmp/chronoqueue-proto-download
	@mkdir -p /tmp/chronoqueue-proto-download
	@echo "Downloading repository archive..."
	@curl -sL -H "Accept: application/vnd.github.v3+json" \
		"https://api.github.com/repos/$(CHRONOQUEUE_REPO)/tarball/$(CHRONOQUEUE_BRANCH)" \
		-o /tmp/chronoqueue-proto-download/repo.tar.gz
	@echo "Extracting proto files..."
	@tar -xzf /tmp/chronoqueue-proto-download/repo.tar.gz -C /tmp/chronoqueue-proto-download
	@rm -rf $(PROTO_PATH)/*
	@mkdir -p $(PROTO_PATH)
	@cp -r /tmp/chronoqueue-proto-download/*/$(CHRONOQUEUE_PROTO_PATH)/* $(PROTO_PATH)/
	@rm -rf /tmp/chronoqueue-proto-download
	@echo "Proto definitions updated successfully!"
	@find $(PROTO_PATH) -name "*.proto" | wc -l | xargs echo "Downloaded proto files:"
	@echo "Run 'make gen-proto' to regenerate Python classes."

# Check if proto file exists
check-proto:
	@echo "Checking for proto files..."
	@if [ -z "$$(find $(PROTO_PATH) -name '*.proto' -type f)" ]; then \
		echo "Error: No proto files found in $(PROTO_PATH)"; \
		echo "Run 'make update-proto' to download proto definitions."; \
		exit 1; \
	fi
	@echo "Found $$(find $(PROTO_PATH) -name '*.proto' -type f | wc -l) proto file(s)"

# Generate Python gRPC classes from proto files
gen-proto: setup-dirs check-proto
	@echo "Generating Python gRPC classes from proto files..."
	@echo "Cleaning old generated files..."
	@rm -rf $(OUTPUT_PATH)/*.py
	@rm -rf $(OUTPUT_PATH)/proto
	@rm -rf $(OUTPUT_PATH)/common $(OUTPUT_PATH)/google $(OUTPUT_PATH)/message $(OUTPUT_PATH)/queue $(OUTPUT_PATH)/queueservice $(OUTPUT_PATH)/schedule $(OUTPUT_PATH)/schema
	@find $(PROTO_PATH) -name "*.proto" -type f | while read proto_file; do \
		echo "Processing $$proto_file..."; \
	done
	@$(PYTHON) -m grpc_tools.protoc \
		-I=. \
		-I=$(PROTO_PATH) \
		--python_out=$(OUTPUT_PATH) \
		--grpc_python_out=$(OUTPUT_PATH) \
		$$(find $(PROTO_PATH) -name "*.proto" -type f)
	@echo "Reorganizing generated files..."
	@if [ -d "$(OUTPUT_PATH)/proto" ]; then \
		mv $(OUTPUT_PATH)/proto/* $(OUTPUT_PATH)/ 2>/dev/null || true; \
		rm -rf $(OUTPUT_PATH)/proto; \
	fi
	@echo "Fixing imports in generated files..."
	@find $(OUTPUT_PATH) -name "*_pb2.py" -o -name "*_pb2_grpc.py" | while read pb_file; do \
		if [ "$$(uname)" = "Darwin" ]; then \
			sed -i '' 's/from proto\./from chronoqueue.api./g' "$$pb_file"; \
			sed -i '' 's/^import \([a-zA-Z0-9_]*\)_pb2 as \([a-zA-Z0-9_]*\)/from . import \1_pb2 as \2/g' "$$pb_file"; \
		else \
			sed -i 's/from proto\./from chronoqueue.api./g' "$$pb_file"; \
			sed -i 's/^import \([a-zA-Z0-9_]*\)_pb2 as \([a-zA-Z0-9_]*\)/from . import \1_pb2 as \2/g' "$$pb_file"; \
		fi; \
	done
	@echo "Creating __init__.py files in all directories..."
	@find $(OUTPUT_PATH) -type d -exec touch {}/__init__.py \;
	@echo "Formatting generated code..."
	@$(PYTHON) -m black $(OUTPUT_PATH) --line-length=120 --quiet --exclude='__pycache__|\.pyc' --target-version=py310 || true
	@$(PYTHON) -m isort $(OUTPUT_PATH) --profile black --line-length 120 --quiet --skip-glob='*/__pycache__/*' || true
	@echo "Python gRPC classes generated successfully!"
	@echo "Generated $$(find $(OUTPUT_PATH) -name "*.py" -type f | wc -l) Python file(s)"

# Clean generated files and cache
clean:
	@echo "Cleaning build artifacts and cache..."
	@rm -rf dist
	@rm -rf build
	@rm -rf *.egg-info
	@rm -rf .pytest_cache
	@rm -rf .mypy_cache
	@rm -rf htmlcov
	@rm -rf .coverage
	@find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name '*.pyc' -delete
	@echo "Clean complete! (Generated proto code preserved)"

# Clean everything including generated proto code
clean-all: clean
	@echo "Cleaning generated proto code..."
	@rm -rf $(OUTPUT_PATH)/common
	@rm -rf $(OUTPUT_PATH)/google
	@rm -rf $(OUTPUT_PATH)/message
	@rm -rf $(OUTPUT_PATH)/queue
	@rm -rf $(OUTPUT_PATH)/queueservice
	@rm -rf $(OUTPUT_PATH)/schedule
	@rm -rf $(OUTPUT_PATH)/schema
	@echo "All generated files removed!"

# Run unit tests
test:
	@echo "Running unit tests..."
	$(PYTHON) -m pytest tests/ -v

# Run tests with coverage
test-coverage:
	@echo "Running unit tests with coverage..."
	$(PYTHON) -m pytest tests/ -v --cov=chronoqueue --cov-report=term-missing --cov-report=html

# Run linting checks
lint:
	@echo "Running linting checks..."
	@echo "Checking with flake8..."
	@$(PYTHON) -m flake8 chronoqueue/ tests/ --max-line-length=120 --count --statistics || true
	@echo "Checking with mypy..."
	@$(PYTHON) -m mypy chronoqueue/ || true

# Format code
format:
	@echo "Formatting code with black..."
	@$(PYTHON) -m black chronoqueue/ tests/ --line-length=120 \
		--exclude='/(common|google|message|queue|queueservice|schedule|schema)/'
	@echo "Sorting imports with isort..."
	@$(PYTHON) -m isort chronoqueue/ tests/ \
		--skip chronoqueue/api/common \
		--skip chronoqueue/api/google \
		--skip chronoqueue/api/message \
		--skip chronoqueue/api/queue \
		--skip chronoqueue/api/queueservice \
		--skip chronoqueue/api/schedule \
		--skip chronoqueue/api/schema \
		--profile black --line-length 120

# Type checking
typecheck:
	@echo "Running type checking with mypy..."
	@$(PYTHON) -m mypy chronoqueue/ || true

# Build package
build: clean
	@echo "Building package..."
	poetry build

# Publish to PyPI (use with caution)
publish: build
	@echo "Publishing to PyPI..."
	poetry publish

# Publish to Test PyPI
publish-test: build
	@echo "Publishing to Test PyPI..."
	poetry publish -r testpypi

# Run all CI checks
ci: lint test
	@echo "All CI checks passed!"

# Setup everything
all: install-dev
	@echo "Setup complete! Run 'make gen-proto' to generate proto classes."
