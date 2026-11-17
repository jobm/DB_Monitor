"""Helm template checks for generated and existing secret workflows."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

CHART_DIR = (
    Path(__file__).resolve().parents[1] / "deploy" / "helm" / "db-monitor"
)
HELM = shutil.which("helm")


def _render(*args: str) -> list[dict]:
    if HELM is None:
        pytest.skip("helm is not installed")
    result = subprocess.run(
        [HELM, "template", "test-release", str(CHART_DIR), *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return [doc for doc in yaml.safe_load_all(result.stdout) if doc]


def _container(documents: list[dict]) -> tuple[dict, dict]:
    deployment = next(
        doc for doc in documents if doc.get("kind") == "Deployment"
    )
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    return deployment, container


def test_generated_secret_is_referenced_by_default():
    documents = _render(
        "--set",
        "auth.jwtSecret=test-jwt",
        "--set",
        "database.password=test-password",
    )
    _, container = _container(documents)

    secret_refs = [
        item["secretRef"]["name"]
        for item in container["envFrom"]
        if "secretRef" in item
    ]
    assert secret_refs == ["test-release-db-monitor-secrets"]
    assert "env" not in container

    secret = next(doc for doc in documents if doc.get("kind") == "Secret")
    assert secret["stringData"]["JWT_SECRET"] == "test-jwt"
    assert "POSTGRES_URL" in secret["stringData"]


def test_existing_secrets_are_wired_to_expected_env_names():
    documents = _render(
        "--set",
        "database.existingSecret=external-db",
        "--set",
        "auth.existingSecret=external-auth",
    )
    _, container = _container(documents)

    assert all("secretRef" not in item for item in container["envFrom"])
    env = {entry["name"]: entry for entry in container["env"]}

    assert env["POSTGRES_URL"]["valueFrom"]["secretKeyRef"] == {
        "name": "external-db",
        "key": "postgres-url",
    }
    assert env["JWT_SECRET"]["valueFrom"]["secretKeyRef"] == {
        "name": "external-auth",
        "key": "jwt-secret",
    }

    secret = next(doc for doc in documents if doc.get("kind") == "Secret")
    string_data = secret.get("stringData") or {}
    assert "JWT_SECRET" not in string_data
    assert "POSTGRES_URL" not in string_data


def test_partial_existing_secret_keeps_generated_secret_for_other_values():
    documents = _render(
        "--set",
        "database.existingSecret=external-db",
        "--set",
        "auth.jwtSecret=test-jwt",
    )
    _, container = _container(documents)

    secret_refs = [
        item["secretRef"]["name"]
        for item in container["envFrom"]
        if "secretRef" in item
    ]
    assert secret_refs == ["test-release-db-monitor-secrets"]

    env = {entry["name"]: entry for entry in container["env"]}
    assert env["POSTGRES_URL"]["valueFrom"]["secretKeyRef"]["name"] == (
        "external-db"
    )

    secret = next(doc for doc in documents if doc.get("kind") == "Secret")
    assert secret["stringData"]["JWT_SECRET"] == "test-jwt"
    assert "POSTGRES_URL" not in secret["stringData"]
