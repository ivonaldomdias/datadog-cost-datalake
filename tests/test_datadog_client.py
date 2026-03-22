"""Testes unitários para extractor/datadog_client.py."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from extractor.datadog_client import DatadogClient, DatadogCostRecord, DatadogUsageRecord


@pytest.fixture
def client() -> DatadogClient:
    """Fixture: cliente Datadog com credenciais fake."""
    return DatadogClient(api_key="fake-api-key", app_key="fake-app-key")


def test_datadog_usage_record_defaults() -> None:
    record = DatadogUsageRecord(
        product="infrastructure",
        date="2024-01-15",
        org_name="acme",
        metric_name="aws_hosts",
        value=42.0,
    )
    assert record.unit == ""
    assert record.extracted_at != ""


def test_datadog_cost_record_defaults() -> None:
    record = DatadogCostRecord(
        product_name="infrastructure",
        charge_type="committed",
        cost_usd=1234.56,
        month="2024-01",
        org_name="acme",
    )
    assert record.extracted_at != ""
    assert record.cost_usd == 1234.56


@patch("extractor.datadog_client.requests.Session")
def test_get_infra_hosts_success(mock_session_cls: MagicMock, client: DatadogClient) -> None:
    mock_session = MagicMock()
    mock_session_cls.return_value = mock_session

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "usage": [
            {
                "hour": "2024-01-15T00",
                "org_name": "acme",
                "aws_host_count": 50,
                "total_host_count": 75,
                "agent_host_count": 25,
            }
        ]
    }
    mock_session.get.return_value = mock_response
    client._session = mock_session

    records = client.get_infra_hosts(date(2024, 1, 15), date(2024, 1, 15))

    assert len(records) > 0
    aws_record = next((r for r in records if r.metric_name == "aws_hosts"), None)
    assert aws_record is not None
    assert aws_record.value == 50.0
    assert aws_record.product == "infrastructure"


@patch("extractor.datadog_client.requests.Session")
def test_get_estimated_costs_success(mock_session_cls: MagicMock, client: DatadogClient) -> None:
    mock_session = MagicMock()
    mock_session_cls.return_value = mock_session

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "data": [
            {
                "attributes": {
                    "date": "2024-01-01",
                    "org_name": "acme",
                    "charges": [
                        {"product_name": "infrastructure", "charge_type": "committed", "cost": "4500.00"},
                        {"product_name": "apm",            "charge_type": "on_demand",  "cost": "890.50"},
                    ],
                }
            }
        ]
    }
    mock_response.raise_for_status = MagicMock()
    mock_session.get.return_value = mock_response
    client._session = mock_session

    records = client.get_estimated_costs("2024-01", "2024-01")

    assert len(records) == 2
    infra = next(r for r in records if r.product_name == "infrastructure")
    assert infra.cost_usd == 4500.0
    assert infra.month == "2024-01"


def test_client_raises_without_credentials() -> None:
    import os
    # Garante que as variáveis não estão definidas no ambiente
    env_backup = {k: os.environ.pop(k, None) for k in ["DD_API_KEY", "DD_APP_KEY"]}
    try:
        with pytest.raises(KeyError):
            DatadogClient()
    finally:
        for k, v in env_backup.items():
            if v is not None:
                os.environ[k] = v
