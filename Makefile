# field_journal Makefile
# Public domain test journal for solstone pipeline validation

.PHONY: install test ci format clean

# Default target
all: install

# Virtual environment
VENV := .venv
VENV_BIN := $(VENV)/bin
PYTHON := $(VENV_BIN)/python

# Require uv
UV := $(shell command -v uv 2>/dev/null)
ifndef UV
$(error uv is not installed. Install it: curl -LsSf https://astral.sh/uv/install.sh | sh)
endif

# Venv tool shortcuts
PYTEST := $(VENV_BIN)/pytest
RUFF := $(VENV_BIN)/ruff
MYPY := $(VENV_BIN)/mypy

# Marker file to track installation
.installed: pyproject.toml
	@echo "Installing with uv..."
	$(UV) sync
	@touch .installed

# Install dependencies
install: .installed

# Run tests
test: .installed
	@echo "Running tests..."
	$(PYTEST) tests/ -q

# Auto-format and fix code
format: .installed
	@echo "Formatting with ruff..."
	@$(RUFF) format .
	@$(RUFF) check --fix .
	@echo ""
	@echo "Checking for remaining issues..."
	@RUFF_OK=true; MYPY_OK=true; \
	$(RUFF) check . || RUFF_OK=false; \
	$(MYPY) . || MYPY_OK=false; \
	if $$RUFF_OK && $$MYPY_OK; then \
		echo ""; \
		echo "All clean!"; \
	else \
		echo ""; \
		echo "Issues above need manual fixes."; \
	fi

# Full CI check
ci: .installed
	@echo "Running CI checks..."
	@echo "=== Checking formatting ==="
	@$(RUFF) format --check . || { echo "Run 'make format' to fix formatting"; exit 1; }
	@echo ""
	@echo "=== Running ruff ==="
	@$(RUFF) check . || { echo "Run 'make format' to auto-fix"; exit 1; }
	@echo ""
	@echo "=== Running mypy ==="
	@$(MYPY) . || true
	@echo ""
	@echo "=== Running tests ==="
	@$(MAKE) test
	@echo ""
	@echo "All CI checks passed!"

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -rf build/ dist/ *.egg-info/
	rm -rf .pytest_cache/ .coverage .mypy_cache/
	rm -rf .agents/ .claude/
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name ".DS_Store" -delete
	rm -f .installed
