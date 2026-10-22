.PHONY: all test test-full verify-main lint check doccheck clean

all: test lint check doccheck

test:
	python3 -m pytest tests/ -q --tb=short --timeout=60

test-full:
	python3 -m pytest tests/ -x -v --tb=short

# 在**合并后的树**上跑全量。工作区的绿不等于 main 的绿：
# R134 一轮 1218 passed 是假绿（跑在分支上，合并后 main 的判据还是旧版）；
# R135 又踩到 "commit 在 main ≠ 内容在 main"（被后续提交覆盖）。
# 这条命令就是那两次教训的固化：导出 main → 全量 → 删临时树。
verify-main:
	@tmp=$$(mktemp -d) && git archive main | tar -x -C $$tmp && ( cd $$tmp && python3 -m pytest tests/ -q --tb=line -p no:cacheprovider ); rc=$$?; rm -rf $$tmp; exit $$rc

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
