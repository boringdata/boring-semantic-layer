.PHONY: test examples docs-build check clean help

# Default target - show help
.DEFAULT_GOAL := help

help:
	@echo "Available targets:"
	@echo "  make test         - Run pytest tests"
	@echo "  make examples     - Run all example scripts"
	@echo "  make docs-build   - Build documentation"
	@echo "  make check        - Run all checks (tests + examples + docs build)"
	@echo "  make clean        - Clean build artifacts"

# Run pytest
test:
	@echo "Running tests..."
	uv run pytest

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
