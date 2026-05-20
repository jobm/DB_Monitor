from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


CONFIG_PATH = Path(__file__).resolve().parents[1] / "app" / "config.py"


def _load_config_module(module_name: str):
    """Load the config module under an isolated module name."""
    spec = importlib.util.spec_from_file_location(module_name, CONFIG_PATH)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_config_loads_secret_files_in_production(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Production config should accept secrets injected through files."""
    postgres_url_file = tmp_path / "postgres_url"
    postgres_url_file.write_text(
        "postgresql://user:pass@db:5432/postgres\n",
        encoding="utf-8",
    )
    jwt_secret_file = tmp_path / "jwt_secret"
    jwt_secret_file.write_text(
        "current-secret-123456789012345678901234\n",
        encoding="utf-8",
    )
    jwt_secret_next_file = tmp_path / "jwt_secret_next"
    jwt_secret_next_file.write_text(
        "next-secret-1234567890123456789012345678\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.delenv("POSTGRES_URL", raising=False)
    monkeypatch.delenv("JWT_SECRET", raising=False)
    monkeypatch.delenv("JWT_SECRET_NEXT", raising=False)
    monkeypatch.setenv("POSTGRES_URL_FILE", str(postgres_url_file))
    monkeypatch.setenv("JWT_SECRET_FILE", str(jwt_secret_file))
    monkeypatch.setenv("JWT_SECRET_NEXT_FILE", str(jwt_secret_next_file))

    config = _load_config_module("config_from_secret_files")

    assert (
        config.POSTGRES_URL
        == "postgresql+asyncpg://user:pass@db:5432/postgres"
    )
    assert config.JWT_SECRET == "current-secret-123456789012345678901234"
    assert (
        config.JWT_SECRET_NEXT
        == "next-secret-1234567890123456789012345678"
    )


def test_config_rejects_missing_jwt_secret_in_production(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Production config should fail fast without an explicit JWT secret."""
    postgres_url_file = tmp_path / "postgres_url"
    postgres_url_file.write_text(
        "postgresql+asyncpg://user:pass@db:5432/postgres\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.delenv("POSTGRES_URL", raising=False)
    monkeypatch.delenv("JWT_SECRET", raising=False)
    monkeypatch.delenv("JWT_SECRET_FILE", raising=False)
    monkeypatch.delenv("JWT_SECRET_NEXT", raising=False)
    monkeypatch.delenv("JWT_SECRET_NEXT_FILE", raising=False)
    monkeypatch.setenv("POSTGRES_URL_FILE", str(postgres_url_file))

    with pytest.raises(
        ValueError,
        match="JWT_SECRET must be explicitly configured in production",
    ):
        _load_config_module("config_missing_secret")