"""Orquestrador do pipeline Datadog → S3.

Coordena extração de uso e custos do Datadog e o carregamento
no S3 para posterior ingestão pelo Databricks.

Uso:
    poetry run python loader/pipeline.py --days 30
    poetry run python loader/pipeline.py --days 7 --product infrastructure
    poetry run python loader/pipeline.py --days 7 --dry-run
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, timedelta

from dotenv import load_dotenv

from extractor.datadog_client import DatadogClient
from extractor.s3_loader import S3Loader

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("pipeline")

SUPPORTED_PRODUCTS = ["infrastructure", "apm", "logs", "all"]


def run_pipeline(
    days: int = 30,
    product: str = "all",
    dry_run: bool = False,
) -> dict[str, list[str]]:
    """Executa o pipeline completo de extração e carga.

    Args:
        days: Número de dias retroativos para extrair.
        product: Produto específico ou 'all' para todos.
        dry_run: Se True, extrai mas não faz upload para o S3.

    Returns:
        Dicionário com listas de URIs S3 carregadas por produto.
    """
    end_date = date.today()
    start_date = end_date - timedelta(days=days)

    logger.info("=" * 60)
    logger.info("Pipeline Datadog → S3")
    logger.info("Período: %s → %s | Produto: %s | Dry-run: %s",
                start_date, end_date, product, dry_run)
    logger.info("=" * 60)

    client = DatadogClient()
    loader = S3Loader() if not dry_run else None
    results: dict[str, list[str]] = {}

    # ── Infrastructure ─────────────────────────────────────────────────────
    if product in ("infrastructure", "all"):
        logger.info("[1/4] Extraindo Infrastructure Hosts...")
        records = client.get_infra_hosts(start_date, end_date)

        if dry_run:
            logger.info("[DRY-RUN] %d registros de infra — nenhum upload realizado", len(records))
        else:
            uris: list[str] = []
            # Agrupa por data para particionamento correto
            dates = sorted({r.date for r in records})
            for ref_date in dates:
                day_records = [r for r in records if r.date == ref_date]
                uri = loader.upload_usage_records(day_records, "infrastructure", ref_date)  # type: ignore[union-attr]
                if uri:
                    uris.append(uri)
            results["infrastructure"] = uris
            logger.info("Infrastructure: %d arquivos carregados no S3", len(uris))

    # ── APM ────────────────────────────────────────────────────────────────
    if product in ("apm", "all"):
        logger.info("[2/4] Extraindo APM Usage...")
        records = client.get_apm_usage(start_date, end_date)

        if dry_run:
            logger.info("[DRY-RUN] %d registros de APM — nenhum upload realizado", len(records))
        else:
            uris = []
            dates = sorted({r.date for r in records})
            for ref_date in dates:
                day_records = [r for r in records if r.date == ref_date]
                uri = loader.upload_usage_records(day_records, "apm", ref_date)  # type: ignore[union-attr]
                if uri:
                    uris.append(uri)
            results["apm"] = uris
            logger.info("APM: %d arquivos carregados no S3", len(uris))

    # ── Logs ───────────────────────────────────────────────────────────────
    if product in ("logs", "all"):
        logger.info("[3/4] Extraindo Logs Usage...")
        records = client.get_logs_usage(start_date, end_date)

        if dry_run:
            logger.info("[DRY-RUN] %d registros de logs — nenhum upload realizado", len(records))
        else:
            uris = []
            dates = sorted({r.date for r in records})
            for ref_date in dates:
                day_records = [r for r in records if r.date == ref_date]
                uri = loader.upload_usage_records(day_records, "logs", ref_date)  # type: ignore[union-attr]
                if uri:
                    uris.append(uri)
            results["logs"] = uris
            logger.info("Logs: %d arquivos carregados no S3", len(uris))

    # ── Estimated Costs ────────────────────────────────────────────────────
    if product == "all":
        logger.info("[4/4] Extraindo Custos Estimados...")
        start_month = start_date.strftime("%Y-%m")
        end_month = end_date.strftime("%Y-%m")
        cost_records = client.get_estimated_costs(start_month, end_month)

        if dry_run:
            logger.info("[DRY-RUN] %d registros de custo — nenhum upload realizado", len(cost_records))
        else:
            uris = []
            months = sorted({r.month for r in cost_records})
            for month in months:
                month_records = [r for r in cost_records if r.month == month]
                uri = loader.upload_cost_records(month_records, month)  # type: ignore[union-attr]
                if uri:
                    uris.append(uri)
            results["estimated_cost"] = uris
            logger.info("Custos estimados: %d arquivos carregados no S3", len(uris))

    # ── Sumário ────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("Pipeline concluído!")
    total_files = sum(len(v) for v in results.values())
    logger.info("Total de arquivos carregados: %d", total_files)
    for prod, uris in results.items():
        logger.info("  %-20s → %d arquivo(s)", prod, len(uris))
    logger.info("=" * 60)

    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pipeline Datadog Cost → S3 → Databricks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--days", type=int, default=30,
                        help="Dias retroativos para extração (padrão: 30)")
    parser.add_argument("--product", choices=SUPPORTED_PRODUCTS, default="all",
                        help="Produto Datadog a extrair (padrão: all)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Extrai dados mas não faz upload para o S3")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_pipeline(days=args.days, product=args.product, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
