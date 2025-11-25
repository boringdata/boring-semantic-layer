.PHONY: test examples docs-build check clean help

# Default target - show help
.DEFAULT_GOAL := help

# Ibis versions to test against (last 3 major versions)
ALL_IBIS_VERSIONS := 9.5.0 10.6.0 11.0.0

# Default to current version if not specified
IBIS_VERSION ?=

help:
	@echo "Available targets:"
	@echo "  make test                              - Run pytest tests"
	@echo "  make test IBIS_VERSION=all             - Run tests with all ibis versions (9.5.0, 10.6.0, 11.0.0)"
	@echo "  make test IBIS_VERSION=10.6.0          - Run tests with specific ibis version"
	@echo "  make examples                          - Run all example scripts"
	@echo "  make examples IBIS_VERSION=all         - Run examples with all ibis versions"
	@echo "  make examples IBIS_VERSION=10.6.0      - Run examples with specific ibis version"
	@echo "  make docs-build                        - Build documentation"
	@echo "  make check                             - Run all checks (tests + examples + docs)"
	@echo "  make check IBIS_VERSION=all            - Run all checks with all ibis versions"
	@echo "  make clean                             - Clean build artifacts"

# Run pytest with optional ibis version
test:
ifeq ($(IBIS_VERSION),all)
	@echo "========================================"
	@echo "Testing with multiple ibis versions"
	@echo "========================================"
	@for version in $(ALL_IBIS_VERSIONS); do \
		echo ""; \
		echo "========================================"; \
		echo "Testing with ibis-framework=$$version"; \
		echo "========================================"; \
		uv pip install "ibis-framework==$$version"; \
		uv run pytest -q || { echo "❌ Tests failed with ibis-framework=$$version"; exit 1; }; \
		echo "✓ Tests passed with ibis-framework=$$version"; \
	done; \
	echo ""; \
	echo "========================================"; \
	echo "✓ All ibis versions tested successfully!"; \
	echo "========================================"
else ifneq ($(IBIS_VERSION),)
	@echo "Installing ibis-framework==$(IBIS_VERSION)..."
	@uv pip install "ibis-framework==$(IBIS_VERSION)"
	@echo "Running tests with ibis-framework==$(IBIS_VERSION)..."
	@uv run pytest
else
	@echo "Running tests..."
	@uv run pytest
endif

# Run all examples (skip MCP examples as they require special setup)
examples:
ifeq ($(IBIS_VERSION),all)
	@echo "========================================"
	@echo "Testing examples with multiple ibis versions"
	@echo "========================================"
	@for version in $(ALL_IBIS_VERSIONS); do \
		echo ""; \
		echo "========================================"; \
		echo "Testing examples with ibis-framework=$$version"; \
		echo "========================================"; \
		uv pip install "ibis-framework==$$version"; \
		for file in examples/*.py; do \
			[ "$$(basename $$file)" = "__init__.py" ] && continue; \
			[ "$$(basename $$file)" = "run_all_examples.py" ] && continue; \
			echo "$$(basename $$file)" | grep -q "example_mcp" && continue; \
			echo "Running $$file..."; \
			uv run "$$file" || exit 1; \
		done || { echo "❌ Examples failed with ibis-framework=$$version"; exit 1; }; \
		echo "✓ Examples passed with ibis-framework=$$version"; \
	done; \
	echo ""; \
	echo "========================================"; \
	echo "✓ All ibis versions tested successfully with examples!"; \
	echo "========================================"
else ifneq ($(IBIS_VERSION),)
	@echo "Installing ibis-framework==$(IBIS_VERSION)..."
	@uv pip install "ibis-framework==$(IBIS_VERSION)"
	@echo "Running examples with ibis-framework==$(IBIS_VERSION)..."
	@for file in examples/*.py; do \
		[ "$$(basename $$file)" = "__init__.py" ] && continue; \
		[ "$$(basename $$file)" = "run_all_examples.py" ] && continue; \
		echo "$$(basename $$file)" | grep -q "example_mcp" && continue; \
		echo "Running $$file..."; \
		uv run "$$file" || exit 1; \
	done
	@echo "✓ All examples passed!"
else
	@echo "Running examples..."
	@for file in examples/*.py; do \
		[ "$$(basename $$file)" = "__init__.py" ] && continue; \
		[ "$$(basename $$file)" = "run_all_examples.py" ] && continue; \
		echo "$$(basename $$file)" | grep -q "example_mcp" && continue; \
		echo "Running $$file..."; \
		uv run "$$file" || exit 1; \
	done
	@echo "✓ All examples passed!"
endif

# Build docs
docs-build:
	@echo "Building documentation..."
	cd docs/web && npm run build

# Run all checks (CI target)
check: test examples docs-build
	@echo ""
	@echo "========================================"
	@echo "✓ All checks passed!"
	@echo "========================================"

# Clean build artifacts
clean:
	rm -rf dist/
	rm -rf build/
	rm -rf *.egg-info/
