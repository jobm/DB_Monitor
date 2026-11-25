"""Helm template checks for the split API/consumer workloads."""

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


def _documents_of_kind(documents: list[dict], kind: str) -> list[dict]:
    return [doc for doc in documents if doc.get("kind") == kind]


def _component(doc: dict) -> str | None:
    return doc["metadata"].get("labels", {}).get(
        "app.kubernetes.io/component"
    )


def _deployment(documents: list[dict], role: str) -> dict:
    return next(
        doc
        for doc in _documents_of_kind(documents, "Deployment")
        if _component(doc) == role
    )


def _container(documents: list[dict], role: str = "api") -> dict:
    deployment = _deployment(documents, role)
    return deployment["spec"]["template"]["spec"]["containers"][0]


def _env(container: dict) -> dict[str, dict]:
    return {entry["name"]: entry for entry in container.get("env", [])}


def test_generated_secret_is_referenced_by_default():
    documents = _render(
        "--set",
        "auth.jwtSecret=test-jwt",
        "--set",
        "database.password=test-password",
    )

    for role in ("api", "consumer"):
        container = _container(documents, role)
        secret_refs = [
            item["secretRef"]["name"]
            for item in container["envFrom"]
            if "secretRef" in item
        ]
        assert secret_refs == ["test-release-db-monitor-secrets"]
        assert _env(container)["SERVICE_ROLE"]["value"] == role

    secret = _documents_of_kind(documents, "Secret")[0]
    assert secret["stringData"]["JWT_SECRET"] == "test-jwt"
    assert "POSTGRES_URL" in secret["stringData"]


def test_existing_secrets_are_wired_to_expected_env_names():
    documents = _render(
        "--set",
        "database.existingSecret=external-db",
        "--set",
        "auth.existingSecret=external-auth",
    )
    container = _container(documents)

    assert all("secretRef" not in item for item in container["envFrom"])
    env = _env(container)

    assert env["POSTGRES_URL"]["valueFrom"]["secretKeyRef"] == {
        "name": "external-db",
        "key": "postgres-url",
    }
    assert env["JWT_SECRET"]["valueFrom"]["secretKeyRef"] == {
        "name": "external-auth",
        "key": "jwt-secret",
    }
    assert env["SERVICE_ROLE"]["value"] == "api"

    secret = _documents_of_kind(documents, "Secret")[0]
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
    container = _container(documents)

    secret_refs = [
        item["secretRef"]["name"]
        for item in container["envFrom"]
        if "secretRef" in item
    ]
    assert secret_refs == ["test-release-db-monitor-secrets"]

    env = _env(container)
    assert env["POSTGRES_URL"]["valueFrom"]["secretKeyRef"]["name"] == (
        "external-db"
    )

    secret = _documents_of_kind(documents, "Secret")[0]
    assert secret["stringData"]["JWT_SECRET"] == "test-jwt"
    assert "POSTGRES_URL" not in secret["stringData"]


def test_chart_renders_one_deployment_per_role():
    documents = _render()

    deployments = _documents_of_kind(documents, "Deployment")
    assert {_component(doc) for doc in deployments} == {"api", "consumer"}
    assert {doc["metadata"]["name"] for doc in deployments} == {
        "test-release-db-monitor-api",
        "test-release-db-monitor-consumer",
    }

    for role in ("api", "consumer"):
        deployment = _deployment(documents, role)
        container = deployment["spec"]["template"]["spec"]["containers"][0]

        # The split is only real if the role is injected and resources render.
        assert _env(container)["SERVICE_ROLE"]["value"] == role
        assert container["resources"]["requests"]["cpu"]
        assert deployment["spec"]["replicas"] == 2
        assert (
            deployment["spec"]["selector"]["matchLabels"][
                "app.kubernetes.io/component"
            ]
            == role
        )


def test_services_select_only_their_own_workload():
    documents = _render()

    services = _documents_of_kind(documents, "Service")
    assert {_component(doc) for doc in services} == {"api", "consumer"}
    for service in services:
        role = _component(service)
        assert service["metadata"]["name"] == (
            f"test-release-db-monitor-{role}"
        )
        assert service["spec"]["selector"]["app.kubernetes.io/component"] == (
            role
        )


def test_service_account_is_created_and_referenced():
    documents = _render()

    accounts = _documents_of_kind(documents, "ServiceAccount")
    assert [doc["metadata"]["name"] for doc in accounts] == [
        "test-release-db-monitor"
    ]
    for role in ("api", "consumer"):
        pod = _deployment(documents, role)["spec"]["template"]["spec"]
        assert pod["serviceAccountName"] == "test-release-db-monitor"


def test_archive_volume_is_mounted_only_by_the_consumer():
    documents = _render(
        "--set",
        "retention.archiveBeforeDelete=true",
        "--set",
        "persistence.enabled=true",
    )

    consumer_pod = _deployment(documents, "consumer")["spec"]["template"]["spec"]
    api_pod = _deployment(documents, "api")["spec"]["template"]["spec"]

    consumer_mounts = consumer_pod["containers"][0]["volumeMounts"]
    api_mounts = api_pod["containers"][0]["volumeMounts"]
    assert {mount["name"] for mount in consumer_mounts} == {
        "tmp",
        "archive",
    }
    assert {mount["name"] for mount in api_mounts} == {"tmp"}
    assert [volume["name"] for volume in consumer_pod["volumes"]] == [
        "tmp",
        "archive",
    ]
    assert [volume["name"] for volume in api_pod["volumes"]] == ["tmp"]

    claim = _documents_of_kind(documents, "PersistentVolumeClaim")[0]
    assert claim["metadata"]["name"] == "test-release-db-monitor-archive"


def test_both_roles_get_a_writable_tmp_volume_for_the_read_only_rootfs():
    documents = _render()

    for role in ("api", "consumer"):
        pod = _deployment(documents, role)["spec"]["template"]["spec"]
        tmp = next(
            volume for volume in pod["volumes"] if volume["name"] == "tmp"
        )
        assert tmp["emptyDir"] == {}
        mounts = pod["containers"][0]["volumeMounts"]
        assert any(
            mount["name"] == "tmp" and mount["mountPath"] == "/tmp"
            for mount in mounts
        )


def test_hpa_and_pdb_cover_both_workloads():
    documents = _render()

    hpas = _documents_of_kind(documents, "HorizontalPodAutoscaler")
    pdbs = _documents_of_kind(documents, "PodDisruptionBudget")
    assert {_component(doc) for doc in hpas} == {"api", "consumer"}
    assert {_component(doc) for doc in pdbs} == {"api", "consumer"}
    for doc in hpas:
        assert doc["spec"]["scaleTargetRef"]["name"].endswith(
            _component(doc)
        )
    for doc in pdbs:
        assert doc["spec"]["selector"]["matchLabels"][
            "app.kubernetes.io/component"
        ] == _component(doc)


def test_service_monitor_and_network_policy_are_per_role():
    documents = _render(
        "--set",
        "networkPolicy.enabled=true",
        "--set",
        "monitoring.serviceMonitor.enabled=true",
    )

    monitors = _documents_of_kind(documents, "ServiceMonitor")
    assert {_component(doc) for doc in monitors} == {"api", "consumer"}
    for monitor in monitors:
        assert monitor["spec"]["selector"]["matchLabels"][
            "app.kubernetes.io/component"
        ] == _component(monitor)

    policies = _documents_of_kind(documents, "NetworkPolicy")
    assert {_component(doc) for doc in policies} == {"api", "consumer"}
    for policy in policies:
        egress = yaml.safe_dump(policy["spec"]["egress"])
        if _component(policy) == "consumer":
            assert "kafka" in egress
        else:
            # Only consumers talk to the broker.
            assert "kafka" not in egress
