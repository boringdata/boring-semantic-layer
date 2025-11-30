.PHONY: test examples docs-build skills-build skills-check check clean help

# Default target - show help
.DEFAULT_GOAL := help

# Ibis versions to test against (last 3 major versions)
ALL_IBIS_VERSIONS := 9.5.0 10.8.0 11.0.0

# Default to current version if not specified
IBIS_VERSION ?=

help:
	@echo "Available targets:"
	@echo "  make test                              - Run pytest tests"
	@echo "  make test IBIS_VERSION=all             - Run tests with all ibis versions (9.5.0, 10.8.0, 11.0.0)"
	@echo "  make test IBIS_VERSION=10.8.0          - Run tests with specific ibis version"
	@echo "  make examples                          - Run all example scripts"
	@echo "  make examples IBIS_VERSION=all         - Run examples with all ibis versions"
	@echo "  make examples IBIS_VERSION=10.8.0      - Run examples with specific ibis version"
	@echo "  make docs-build                        - Build documentation"
	@echo "  make skills-build                      - Build AI assistant skills from prompts"
	@echo "  make skills-check                      - Check if skills are up to date"
	@echo "  make check                             - Run all checks (tests + examples + docs + skills)"
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

# Build skills from prompts
skills-build:
	@echo "Building AI assistant skills..."
	uv run python docs/md/skills_builder.py

# Check if skills are up to date
skills-check:
	@echo "Checking if skills are up to date..."
	uv run python docs/md/skills_builder.py --check

# Run all checks (CI target)
check:
	@if [ "$(IBIS_VERSION)" = "all" ]; then \
		$(MAKE) test IBIS_VERSION=all && \
		$(MAKE) examples IBIS_VERSION=all && \
		$(MAKE) skills-check && \
		echo "" && \
		echo "========================================" && \
		echo "✓ All checks passed (tests + examples with all ibis versions)!" && \
		echo "Note: Run 'make docs-build' separately to build documentation" && \
		echo "========================================"; \
	elif [ -n "$(IBIS_VERSION)" ]; then \
		$(MAKE) test IBIS_VERSION=$(IBIS_VERSION) && \
		$(MAKE) examples IBIS_VERSION=$(IBIS_VERSION) && \
		$(MAKE) skills-check && \
		echo "" && \
		echo "========================================" && \
		echo "✓ All checks passed (tests + examples with ibis $(IBIS_VERSION))!" && \
		echo "Note: Run 'make docs-build' separately to build documentation" && \
		echo "========================================"; \
	else \
		$(MAKE) test && \
		$(MAKE) examples && \
		$(MAKE) docs-build && \
		$(MAKE) skills-check && \
		echo "" && \
		echo "========================================" && \
		echo "✓ All checks passed!" && \
		echo "========================================"; \
	fi

# Clean build artifacts
clean:
	rm -rf dist/
	rm -rf build/
	rm -rf *.egg-info/
