.PHONY: all test lint check clean

all: test lint check

test:
	python3 -m pytest tests/ -q --tb=short --ignore=tests/test_integration.py --ignore=tests/test_role_interaction.py --ignore=tests/test_router.py --ignore=tests/test_drift_detector_e2e.py --ignore=tests/test_output_validator.py

test-full:
	python3 -m pytest tests/ -x -v --tb=short

lint:
	@failed=0; for f in $$(find . -name '*.py' -not -path './.git/*' -not -path './__pycache__/*' -not -path './venv/*'); do python3 -m py_compile "$$f" 2>&1 || failed=1; done; if [ "$$failed" = 1 ]; then exit 1; fi

check:
	python3 -c "from routing.auto import route_all; print('import OK')"

clean:
	@find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@find . -name '*.pyc' -delete
