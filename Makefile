.PHONY: test test-ibis-versions examples docs-build check clean help

# Default target - show help
.DEFAULT_GOAL := help

# Ibis versions to test against
IBIS_VERSIONS := 10.0.0 10.6.0 11.0.0 latest

help:
	@echo "Available targets:"
	@echo "  make test              - Run pytest tests"
	@echo "  make test-ibis-versions - Run tests with multiple ibis versions"
	@echo "  make examples          - Run all example scripts"
	@echo "  make docs-build        - Build documentation"
	@echo "  make check             - Run all checks (tests + examples + docs build)"
	@echo "  make clean             - Clean build artifacts"

# Run pytest
test:
	@echo "Running tests..."
	uv run pytest

# Run tests with multiple ibis versions
test-ibis-versions:
	@echo "========================================"
	@echo "Testing with multiple ibis versions"
	@echo "========================================"
	@for version in $(IBIS_VERSIONS); do \
		echo ""; \
		echo "========================================"; \
		echo "Testing with ibis-framework=$$version"; \
		echo "========================================"; \
		if [ "$$version" = "latest" ]; then \
			uv pip install --upgrade ibis-framework; \
		else \
			uv pip install "ibis-framework==$$version"; \
		fi; \
		uv run pytest -q || { echo "❌ Tests failed with ibis-framework=$$version"; exit 1; }; \
		echo "✓ Tests passed with ibis-framework=$$version"; \
	done
	@echo ""
	@echo "========================================"
	@echo "✓ All ibis versions tested successfully!"
	@echo "========================================"

# Run all examples (skip MCP examples as they require special setup)
examples:
	@echo "Running examples..."
	@for file in examples/*.py; do \
		[ "$$(basename $$file)" = "__init__.py" ] && continue; \
		[ "$$(basename $$file)" = "run_all_examples.py" ] && continue; \
		echo "$$(basename $$file)" | grep -q "example_mcp" && continue; \
		echo "Running $$file..."; \
		uv run "$$file" || exit 1; \
	done
	@echo "✓ All examples passed!"

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
