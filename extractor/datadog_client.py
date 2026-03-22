"""Cliente para a API do Datadog — Usage Metering e Cost Attribution.

Responsável por autenticar e buscar dados de uso e custo da API oficial
do Datadog, com retry, paginação e tratamento de erros.

Referências:
    https://docs.datadoghq.com/api/latest/usage-metering/
    https://docs.datadoghq.com/api/latest/cost-attribution/
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# ── Constantes ────────────────────────────────────────────────────────────────
DD_BASE_URL = "https://api.datadoghq.com/api/v2"
DD_V1_URL = "https://api.datadoghq.com/api/v1"
MAX_RETRIES = 3
BACKOFF_FACTOR = 2.0
REQUEST_TIMEOUT = 30


# ── Modelos de dados ──────────────────────────────────────────────────────────
@dataclass
class DatadogUsageRecord:
    """Representa um registro de uso do Datadog."""

    product: str
    date: str                          # YYYY-MM-DD
    org_name: str
    metric_name: str
    value: float
    unit: str = ""
    extracted_at: str = field(
        default_factory=lambda: datetime.utcnow().isoformat()
    )


@dataclass
class DatadogCostRecord:
    """Representa um registro de custo estimado do Datadog."""

    product_name: str
    charge_type: str
    cost_usd: float
    month: str                         # YYYY-MM
    org_name: str
    extracted_at: str = field(
        default_factory=lambda: datetime.utcnow().isoformat()
    )


# ── Cliente HTTP ──────────────────────────────────────────────────────────────
class DatadogClient:
    """Cliente autenticado para a API do Datadog.

    Args:
        api_key: Chave de API do Datadog (DD_API_KEY).
        app_key: Application Key do Datadog (DD_APP_KEY).
        site: Site do Datadog (ex: datadoghq.com, datadoghq.eu).
    """

    def __init__(
        self,
        api_key: str | None = None,
        app_key: str | None = None,
        site: str | None = None,
    ) -> None:
        self.api_key = api_key or os.environ["DD_API_KEY"]
        self.app_key = app_key or os.environ["DD_APP_KEY"]
        self.site = site or os.getenv("DD_SITE", "datadoghq.com")

        self._base_v2 = f"https://api.{self.site}/api/v2"
        self._base_v1 = f"https://api.{self.site}/api/v1"
        self._session = self._build_session()

    def _build_session(self) -> requests.Session:
        """Constrói sessão HTTP com retry automático e backoff."""
        session = requests.Session()
        retry = Retry(
            total=MAX_RETRIES,
            backoff_factor=BACKOFF_FACTOR,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.headers.update({
            "DD-API-KEY": self.api_key,
            "DD-APPLICATION-KEY": self.app_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        return session

    def _get(self, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Executa GET autenticado com tratamento de rate limit.

        Args:
            url: URL completa do endpoint.
            params: Parâmetros de query string.

        Returns:
            JSON de resposta parseado.

        Raises:
            requests.HTTPError: Em caso de erro HTTP não recuperável.
        """
        logger.debug("GET %s params=%s", url, params)
        response = self._session.get(url, params=params, timeout=REQUEST_TIMEOUT)

        # Respeitar rate limit do Datadog (429)
        if response.status_code == 429:
            retry_after = int(response.headers.get("X-RateLimit-Reset", 60))
            logger.warning("Rate limit atingido. Aguardando %ds...", retry_after)
            time.sleep(retry_after)
            response = self._session.get(url, params=params, timeout=REQUEST_TIMEOUT)

        response.raise_for_status()
        return response.json()  # type: ignore[no-any-return]

    # ── Usage Metering ─────────────────────────────────────────────────────────

    def get_infra_hosts(self, start_date: date, end_date: date) -> list[DatadogUsageRecord]:
        """Busca uso de hosts de infraestrutura faturáveis.

        Args:
            start_date: Data de início da consulta.
            end_date: Data de fim da consulta.

        Returns:
            Lista de DatadogUsageRecord com dados de hosts.
        """
        logger.info("Buscando uso de infra hosts: %s → %s", start_date, end_date)
        url = f"{self._base_v1}/usage/hosts"
        records: list[DatadogUsageRecord] = []

        current = start_date
        while current <= end_date:
            data = self._get(url, params={
                "start_hr": current.strftime("%Y-%m-%dT00"),
                "end_hr": current.strftime("%Y-%m-%dT23"),
            })

            for entry in data.get("usage", []):
                hour_str = entry.get("hour", "")
                day_str = hour_str[:10] if hour_str else current.isoformat()

                for metric, label in [
                    ("agent_host_count", "agent_hosts"),
                    ("alibaba_host_count", "alibaba_hosts"),
                    ("aws_host_count", "aws_hosts"),
                    ("azure_host_count", "azure_hosts"),
                    ("gcp_host_count", "gcp_hosts"),
                    ("infra_azure_app_service", "azure_app_service"),
                    ("total_host_count", "total_hosts"),
                ]:
                    value = entry.get(metric)
                    if value is not None and value > 0:
                        records.append(DatadogUsageRecord(
                            product="infrastructure",
                            date=day_str,
                            org_name=entry.get("org_name", "default"),
                            metric_name=label,
                            value=float(value),
                            unit="hosts",
                        ))

            current += timedelta(days=1)

        logger.info("Hosts coletados: %d registros", len(records))
        return records

    def get_apm_usage(self, start_date: date, end_date: date) -> list[DatadogUsageRecord]:
        """Busca uso de APM (serviços rastreados e spans indexados).

        Args:
            start_date: Data de início.
            end_date: Data de fim.

        Returns:
            Lista de DatadogUsageRecord com dados de APM.
        """
        logger.info("Buscando uso de APM: %s → %s", start_date, end_date)
        url = f"{self._base_v1}/usage/trace_summary"
        records: list[DatadogUsageRecord] = []

        current = start_date
        while current <= end_date:
            try:
                data = self._get(url, params={
                    "start_hr": current.strftime("%Y-%m-%dT00"),
                    "end_hr": current.strftime("%Y-%m-%dT23"),
                })
                for entry in data.get("usage", []):
                    day_str = entry.get("hour", current.isoformat())[:10]
                    for metric, label, unit in [
                        ("apm_host_count", "apm_hosts", "hosts"),
                        ("indexed_events_count", "indexed_spans", "spans"),
                    ]:
                        value = entry.get(metric)
                        if value is not None and value > 0:
                            records.append(DatadogUsageRecord(
                                product="apm",
                                date=day_str,
                                org_name=entry.get("org_name", "default"),
                                metric_name=label,
                                value=float(value),
                                unit=unit,
                            ))
            except requests.HTTPError as exc:
                logger.warning("Erro ao buscar APM para %s: %s", current, exc)

            current += timedelta(days=1)

        logger.info("APM coletado: %d registros", len(records))
        return records

    def get_logs_usage(self, start_date: date, end_date: date) -> list[DatadogUsageRecord]:
        """Busca volume de logs ingeridos e indexados.

        Args:
            start_date: Data de início.
            end_date: Data de fim.

        Returns:
            Lista de DatadogUsageRecord com dados de logs.
        """
        logger.info("Buscando uso de Logs: %s → %s", start_date, end_date)
        url = f"{self._base_v1}/usage/logs"
        records: list[DatadogUsageRecord] = []

        current = start_date
        while current <= end_date:
            try:
                data = self._get(url, params={
                    "start_hr": current.strftime("%Y-%m-%dT00"),
                    "end_hr": current.strftime("%Y-%m-%dT23"),
                })
                for entry in data.get("usage", []):
                    day_str = entry.get("hour", current.isoformat())[:10]
                    for metric, label, unit in [
                        ("ingested_events_bytes", "logs_ingested_bytes", "bytes"),
                        ("indexed_events_count", "logs_indexed_events", "events"),
                        ("ingested_events_count", "logs_ingested_events", "events"),
                    ]:
                        value = entry.get(metric)
                        if value is not None and value > 0:
                            records.append(DatadogUsageRecord(
                                product="logs",
                                date=day_str,
                                org_name=entry.get("org_name", "default"),
                                metric_name=label,
                                value=float(value),
                                unit=unit,
                            ))
            except requests.HTTPError as exc:
                logger.warning("Erro ao buscar Logs para %s: %s", current, exc)

            current += timedelta(days=1)

        logger.info("Logs coletados: %d registros", len(records))
        return records

    # ── Cost Attribution ───────────────────────────────────────────────────────

    def get_estimated_costs(
        self, start_month: str, end_month: str
    ) -> list[DatadogCostRecord]:
        """Busca custos estimados por produto via Cost Attribution API.

        Args:
            start_month: Mês inicial no formato YYYY-MM.
            end_month: Mês final no formato YYYY-MM.

        Returns:
            Lista de DatadogCostRecord com custos por produto.
        """
        logger.info("Buscando custos estimados: %s → %s", start_month, end_month)
        url = f"{self._base_v2}/usage/estimated_cost"
        records: list[DatadogCostRecord] = []

        try:
            data = self._get(url, params={
                "start_month": start_month,
                "end_month": end_month,
                "include_connected_accounts": "true",
            })

            for entry in data.get("data", []):
                attrs = entry.get("attributes", {})
                month = attrs.get("date", "")[:7]

                for charge in attrs.get("charges", []):
                    cost = float(charge.get("cost", 0.0))
                    if cost > 0:
                        records.append(DatadogCostRecord(
                            product_name=charge.get("product_name", "unknown"),
                            charge_type=charge.get("charge_type", "unknown"),
                            cost_usd=round(cost, 4),
                            month=month,
                            org_name=attrs.get("org_name", "default"),
                        ))

        except requests.HTTPError as exc:
            logger.error("Erro ao buscar custos estimados: %s", exc)

        logger.info("Custos coletados: %d registros", len(records))
        return records
