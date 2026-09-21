.PHONY: help install install-dev lock update update-proto gen-proto check-proto check-artifacts check-base-tests clean clean-all test test-coverage lint typecheck format build publish publish-test ci all

POETRY ?= poetry
PYTHON ?= $(POETRY) run python
FORMAT_FLAGS ?=
export POETRY_VIRTUALENVS_IN_PROJECT := true

help:
	@echo "Nzovu Python SDK: install-dev, test, lint, typecheck, format, build"
	@echo "Protocol: gen-proto, check-proto, update-proto NZOVU_SERVER_SOURCE=<checkout> NZOVU_SERVER_COMMIT=<full-sha>"

install:
	$(POETRY) check --lock
	$(POETRY) sync --only main

install-dev:
	$(POETRY) check --lock
	$(POETRY) sync --with dev --all-extras

lock:
	$(POETRY) lock

update:
	$(POETRY) update

update-proto:
	@test -n "$(NZOVU_SERVER_SOURCE)" && test -n "$(NZOVU_SERVER_COMMIT)" || { echo "Set NZOVU_SERVER_SOURCE and NZOVU_SERVER_COMMIT explicitly." >&2; exit 1; }
	$(PYTHON) scripts/update_proto.py --source "$(NZOVU_SERVER_SOURCE)" --ref "$(NZOVU_SERVER_COMMIT)"

gen-proto:
	$(PYTHON) scripts/generate_proto.py

check-proto:
	$(PYTHON) scripts/generate_proto.py --check

clean:
	rm -rf dist build .pytest_cache .mypy_cache htmlcov .coverage coverage.xml

clean-all: clean
	rm -rf nzovu/api

test:
	$(PYTHON) -m pytest tests/ -v

test-coverage:
	$(PYTHON) -m pytest tests/ -v --cov=nzovu --cov-report=term-missing --cov-report=html --cov-report=xml

lint:
	$(PYTHON) -m flake8 nzovu/ tests/ scripts/ --max-line-length=120 --count --statistics

typecheck:
	$(PYTHON) -m mypy nzovu/ scripts/

format:
	$(PYTHON) -m black $(FORMAT_FLAGS) nzovu/ tests/ scripts/
	$(PYTHON) -m isort $(FORMAT_FLAGS) nzovu/ tests/ scripts/ --skip nzovu/api

build: clean
	$(POETRY) build

check-artifacts:
	$(PYTHON) scripts/check_artifacts.py --version "$$($(POETRY) version -s)"

check-base-tests:
	$(PYTHON) scripts/check_base_tests.py --version "$$($(POETRY) version -s)"

publish publish-test:
	@echo "Publishing disabled during the SDK migration." >&2
	@exit 1

ci: check-proto lint typecheck test

all: install-dev gen-proto
