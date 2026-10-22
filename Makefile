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

# 只读校验：旧的 doccheck 自己先 --fix 再校验，等于每次 make 都写文档 ——
# 脏树掩盖真实漂移，CI 上改写结果还会被丢弃。刷新拆成单独目标。
doccheck:
	@echo "=== 文档↔代码一致性（行数 + xxx.py:NNN 行号引用）==="
	@python3 scripts/doccheck.py

docfix:
	@python3 scripts/doccheck.py --fix

clean:
	@find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@find . -name '*.pyc' -delete
