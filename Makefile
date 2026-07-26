.PHONY: check lint test all clean

check:
	@echo "=== py_compile ==="
	@for f in src/**/*.py; do python3 -m py_compile "$$f" && echo "  OK $$f" || exit 1; done

lint: check

test:
	@python3 -m pytest tests/ -v --timeout=60 -x

all: check test

clean:
	@find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@find . -name '*.pyc' -delete
	@echo "cleaned"
