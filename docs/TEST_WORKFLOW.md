# 测试工作流文档

> 更新: 2026-07-24 | 受众: 测试工程师 | 相关: [DOCS.md](DOCS.md)

## 如何运行全部测试

```bash
# session-pipeline（1241 例，2026-10-22 collect 实测）
cd /home/administrator/session-pipeline
python3 -m pytest tests/ -v

# hermes-session-roles（43 pass，2026-10-22 实测）
cd /home/administrator/hermes-session-roles
python3 -m pytest tests/ -v

# session-launcher（459 pass，2026-10-22 实测）
cd /home/administrator/session-launcher
python3 -m pytest tests/ -v
```
