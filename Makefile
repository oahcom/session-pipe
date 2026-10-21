.PHONY: all test lint check doccheck clean

all: test lint check doccheck

test:
	python3 -m pytest tests/ -q --tb=short --timeout=60

test-full:
	python3 -m pytest tests/ -x -v --tb=short

lint:
	@failed=0; for f in $$(find . -name '*.py' -not -path './.git/*' -not -path './__pycache__/*' -not -path './venv/*'); do python3 -m py_compile "$$f" 2>&1 || failed=1; done; if [ "$$failed" = 1 ]; then exit 1; fi

check: doccheck
	PYTHONPATH=src python3 -c "from routing.auto import route_all; print('import OK')"

doccheck:
	@echo "=== 文档/代码行数一致性 (doccheck，先刷新再校验) ==="
	@python3 scripts/doccheck.py --fix >/dev/null
	@python3 scripts/doccheck.py || (echo "doccheck 仍有漂移（MISSING_FILE？）"; exit 1)

clean:
	@find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@find . -name '*.pyc' -delete
