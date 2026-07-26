#!/usr/bin/env python3
"""
配置加载器：读取 config.yaml，提供全局 config 对象。
"""
import json
import os
import sys
from pathlib import Path
from typing import Any, Optional

try:
    import yaml
    _HAS_YAML = True
except ImportError:
    _HAS_YAML = False

# 默认配置文件路径
_DEFAULT_CONFIG_PATH = Path.home() / "session-pipeline" / "config" / "config.yaml"

# 允许环境变量覆盖
_CONFIG_PATH = Path(os.environ.get("SESSION_PIPELINE_CONFIG", _DEFAULT_CONFIG_PATH))


class Config:
    """配置容器，支持属性式访问。"""

    def __init__(self, data: dict):
        self._data = data

    def __getattr__(self, name: str) -> Any:
        if name in self._data:
            return self._data[name]
        raise AttributeError(f"Config has no key: {name}")

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def nested_get(self, *keys: str, default: Any = None) -> Any:
        """嵌套键访问，如 config.nested_get('bus', 'db_path')。"""
        cur = self._data
        for k in keys:
            if not isinstance(cur, dict) or k not in cur:
                return default
            cur = cur[k]
        return cur

    def to_dict(self) -> dict:
        return self._data


def load_config(path: Optional[Path] = None) -> Config:
    """加载 YAML 配置文件。若文件不存在则写入默认配置。"""
    config_path = path or _CONFIG_PATH
    if not config_path.exists():
        cfg = Config(_default_config())
        # 写入默认配置到磁盘，方便用户自定义
        config_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            if _HAS_YAML:
                with open(config_path, "w", encoding="utf-8") as f:
                    yaml.safe_dump(_default_config(), f, default_flow_style=False,
                                   allow_unicode=True, sort_keys=False)
            else:
                with open(config_path, "w", encoding="utf-8") as f:
                    json.dump(_default_config(), f, ensure_ascii=False, indent=2)
        except (OSError, PermissionError):
            pass  # must-silent: writing default config, failure is non-fatal
        return cfg

    with open(config_path, "r", encoding="utf-8") as f:
        if _HAS_YAML:
            data = yaml.safe_load(f) or {}
        else:
            import json
            data = json.load(f) or {}
    if not isinstance(data, dict):
        data = {}
    return Config(data)


def _resolve_exceptions(names: list) -> tuple:
    """Convert string exception names to Exception classes."""
    import builtins
    classes = []
    for name in names:
        if isinstance(name, type) and issubclass(name, BaseException):
            classes.append(name)
            continue
        try:
            cls = getattr(builtins, name, None)
            if cls and isinstance(cls, type) and issubclass(cls, BaseException):
                classes.append(cls)
                continue
            parts = name.split('.')
            mod = __import__('.'.join(parts[:-1]), fromlist=[parts[-1]])
            cls = getattr(mod, parts[-1])
            classes.append(cls)
        except (ImportError, AttributeError, ValueError):
            pass  # must-silent: resolving exception class names, fallback to broad Exception
    return tuple(classes) if classes else (Exception,)


def _default_config() -> dict:
    """内置默认配置（YAML 文件缺失时回退）。"""
    return {
        "bus": {
            "db_path": "~/.hermes/sister_bus/blackboard.db",
            "poll_interval": 60,
            "max_messages_per_poll": 100,
        },
        "retry": {
            "max_retries": 3,
            "base_delay": 0.5,
            "max_delay": 10.0,
            "exponential_base": 2.0,
            "retry_exceptions": ["Exception"],
        },
        "circuit_breaker": {
            "failure_threshold": 5,
            "recovery_timeout": 30.0,
            "half_open_max_calls": 1,
        },
        "heartbeat": {
            "stale_threshold": 300,
            "cleanup_interval": 3600,
        },
        "ttl_pruner": {
            "max_age_days": 90,
            "max_facts": 10000,
            "interval": 3600,
            "auto_start": True,
        },
        "priority": {
            "security": 1,
            "code_fix": 2,
            "architecture": 3,
            "performance": 4,
            "evolution_report": 5,
            "reflexion_lesson": 6,
            "deception": 7,
            "default": 8,
        },
        "routing": {
            "default_consumer": "pipeline",
            "idempotent_consume": True,
            "optimistic_claim": True,
        },
        "logging": {
            "level": "INFO",
            "json_output": True,
            "include_trace_id": True,
        },
        "graceful_shutdown": {
            "timeout": 30.0,
        },
        "health": {
            "check_bus_connected": True,
            "check_consumer_stale": True,
            "backlog_warning_threshold": 100,
            "backlog_critical_threshold": 500,
        },
    }


# 全局配置实例（懒加载）
_CONFIG: Optional[Config] = None


def get_config() -> Config:
    """获取全局配置单例。"""
    global _CONFIG
    if _CONFIG is None:
        _CONFIG = load_config()
    return _CONFIG


def reload_config(path: Optional[Path] = None):
    """重新加载配置（热更新）。"""
    global _CONFIG
    _CONFIG = load_config(path)
    return _CONFIG


if __name__ == "__main__":
    import json
    cfg = get_config()
    print(json.dumps(cfg.to_dict(), ensure_ascii=False, indent=2))