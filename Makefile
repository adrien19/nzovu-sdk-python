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
	$(PYTHON) -m flake8 nzovu/ tests/ scripts/ examples/ --max-line-length=120 --count --statistics

typecheck:
	$(PYTHON) -m mypy nzovu/ scripts/ examples/store-api/api/ examples/store-api/config/

format:
	$(PYTHON) -m black $(FORMAT_FLAGS) nzovu/ tests/ scripts/ examples/
	$(PYTHON) -m isort $(FORMAT_FLAGS) nzovu/ tests/ scripts/ examples/ --skip nzovu/api

build:
	rm -rf dist build
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

.PHONY: check-live-ownership
check-live-ownership:
	$(PYTHON) scripts/run_live_ownership.py --server-binary "$(NZOVU_SERVER_BINARY)" -- $(PYTHON) -m pytest tests/test_live_ownership.py -v

.PHONY: test-examples check-live

test-examples:
	cd examples/store-api && $(POETRY) check --lock && $(POETRY) run python -m pytest tests/ -v

check-live:
	@test -n "$$NZOVU_TEST_POSTGRES_DSN" || { echo "Set NZOVU_TEST_POSTGRES_DSN for an isolated test database." >&2; exit 1; }
	NZOVU_REQUIRE_BACKENDS=sqlite,postgres $(PYTHON) scripts/run_live_ownership.py --server-binary "$(NZOVU_SERVER_BINARY)" -- $(PYTHON) -m pytest tests/test_live_contracts.py tests/test_live_ownership.py -v

.PHONY: check-identity audit
PIP_AUDIT ?= pip-audit

check-identity:
	$(PYTHON) scripts/check_identity.py

audit:
	$(PIP_AUDIT) --path "$$($(PYTHON) -c 'import site; print(site.getsitepackages()[0])')" --skip-editable
	cd examples/store-api && $(PIP_AUDIT) --path "$$($(PYTHON) -c 'import site; print(site.getsitepackages()[0])')" --skip-editable
