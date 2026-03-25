"""Configuration loading: defaults → TOML file → CLI flags."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class DatabaseConfig:
    path: str = "gitreport.db"


@dataclass(frozen=True)
class AIConfig:
    provider: str = "auto"
    model: str = ""
    ollama_base_url: str = "http://localhost:11434"
    ollama_model: str = "llama3.1"
    ollama_max_context: int = 8_000


@dataclass(frozen=True)
class ReportConfig:
    stale_branch_days: int = 14
    max_diff_tokens: int = 8_000
    default_days: int = 30
    output: str = "report.html"


@dataclass(frozen=True)
class SyncConfig:
    pass


@dataclass(frozen=True)
class ServerConfig:
    host: str = "127.0.0.1"
    port: int = 8080


@dataclass(frozen=True)
class PromptConfig:
    period: str | None = None
    overall: str | None = None
    context: str | None = None


@dataclass(frozen=True)
class Config:
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    ai: AIConfig = field(default_factory=AIConfig)
    report: ReportConfig = field(default_factory=ReportConfig)
    sync: SyncConfig = field(default_factory=SyncConfig)
    server: ServerConfig = field(default_factory=ServerConfig)
    prompts: PromptConfig = field(default_factory=PromptConfig)


def _merge_section(dataclass_type, data: dict):
    """Create a dataclass instance from a dict, ignoring unknown keys."""
    valid = {f.name for f in dataclass_type.__dataclass_fields__.values()}
    return dataclass_type(**{k: v for k, v in data.items() if k in valid})


def load_config(cli_overrides: dict | None = None) -> Config:
    """Load config from TOML files, then apply CLI overrides.

    Load order (later wins):
    1. Built-in defaults (frozen dataclass)
    2. ./gitreport.toml (project-local)
    3. ~/.config/gitreport/config.toml (user-global)
    4. CLI flags (cli_overrides dict)
    """
    raw: dict = {}

    # Project-local config
    local_path = Path("gitreport.toml")
    if local_path.exists():
        with open(local_path, "rb") as f:
            raw = tomllib.load(f)

    # User-global config (merges on top)
    global_path = Path.home() / ".config" / "gitreport" / "config.toml"
    if global_path.exists():
        with open(global_path, "rb") as f:
            global_raw = tomllib.load(f)
        for section, values in global_raw.items():
            if isinstance(values, dict):
                raw.setdefault(section, {}).update(values)
            else:
                raw[section] = values

    # Build config from TOML sections
    db_data = raw.get("database", {})
    ai_data = raw.get("ai", {})
    # Flatten nested ai.ollama into top-level ai keys
    ollama_data = ai_data.pop("ollama", {})
    for k, v in ollama_data.items():
        ai_data[f"ollama_{k}"] = v

    report_data = raw.get("report", {})
    server_data = raw.get("server", {})
    prompt_data = raw.get("prompts", {})

    cfg = Config(
        database=_merge_section(DatabaseConfig, db_data),
        ai=_merge_section(AIConfig, ai_data),
        report=_merge_section(ReportConfig, report_data),
        server=_merge_section(ServerConfig, server_data),
        prompts=_merge_section(PromptConfig, prompt_data),
    )

    # Apply CLI overrides
    if cli_overrides:
        # Map flat CLI keys to nested config
        overrides = cli_overrides.copy()

        if "provider" in overrides and overrides["provider"] is not None:
            cfg = _replace_nested(cfg, "ai", provider=overrides["provider"])
        if "output" in overrides and overrides["output"] is not None:
            cfg = _replace_nested(cfg, "report", output=overrides["output"])
        if "max_diff_tokens" in overrides and overrides["max_diff_tokens"] is not None:
            cfg = _replace_nested(cfg, "report", max_diff_tokens=overrides["max_diff_tokens"])
        if "days" in overrides and overrides["days"] is not None:
            cfg = _replace_nested(cfg, "report", default_days=overrides["days"])
        if "db_path" in overrides and overrides["db_path"] is not None:
            cfg = _replace_nested(cfg, "database", path=overrides["db_path"])
        if "port" in overrides and overrides["port"] is not None:
            cfg = _replace_nested(cfg, "server", port=overrides["port"])
        if "host" in overrides and overrides["host"] is not None:
            cfg = _replace_nested(cfg, "server", host=overrides["host"])

    return cfg


def _replace_nested(cfg: Config, section: str, **kwargs) -> Config:
    """Return a new Config with updated fields in a nested section."""
    from dataclasses import replace
    old_section = getattr(cfg, section)
    new_section = replace(old_section, **kwargs)
    return replace(cfg, **{section: new_section})
