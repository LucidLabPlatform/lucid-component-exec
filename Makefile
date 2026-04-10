PYTHON ?= python3
VENV ?= .venv
PACKAGE = lucid_component_exec

.PHONY: help setup-venv dev test test-unit test-coverage build clean

help:
	@echo "lucid-component-exec"
	@echo "  make setup-venv      - Create .venv, install project + deps (run this first)"
	@echo "  make test            - Run all tests"
	@echo "  make test-unit       - Unit tests only"
	@echo "  make test-coverage   - Tests with coverage report"
	@echo "  make build           - Build wheel and sdist"
	@echo "  make clean           - Remove build artifacts"

setup:
	@echo "Components run via agent-core; no .env needed here."

setup-venv:
	@test -d $(VENV) || ($(PYTHON) -m venv $(VENV) && echo "Created $(VENV).")
	@$(VENV)/bin/pip install -q -e . --no-deps
	@$(VENV)/bin/pip install -q \
		"lucid-component-base @ git+https://github.com/LucidLabPlatform/lucid-component-base@v2.0.0" \
		pytest pytest-cov build
	@echo "Ready. Run 'make test' or 'make build'."

dev:
	@echo "Component has no standalone runtime. Use 'make test' or 'make build'."

test: test-unit
	@echo "All tests passed."

test-unit:
	@$(VENV)/bin/python -m pytest tests/ -v -q

test-coverage:
	@$(VENV)/bin/python -m pytest tests/ --cov=src/$(PACKAGE) --cov-report=html --cov-report=term-missing -q

build:
	@test -d $(VENV) || (echo "Run 'make setup-venv' first." && exit 1)
	@$(VENV)/bin/python -m build

clean:
	@rm -rf build/ dist/ *.egg-info src/*.egg-info
	@rm -rf .pytest_cache .coverage htmlcov/
