"""Carregador de dados para AWS S3 em formato Parquet particionado.

Converte registros do Datadog em DataFrames e os persiste no S3 seguindo
a estrutura de particionamento por data compatível com Delta Lake / Auto Loader.

Estrutura no S3:
    s3://{bucket}/datadog-costs/
    ├── raw/product=infrastructure/year=2024/month=01/day=15/
    │   └── usage_20240115_143022.parquet
    ├── raw/product=apm/year=2024/month=01/day=15/
    │   └── usage_20240115_143022.parquet
    └── raw/product=estimated_cost/year=2024/month=01/
        └── cost_202401_143022.parquet
"""

from __future__ import annotations

import io
import logging
import os
from dataclasses import asdict
from datetime import datetime
from typing import Any

import boto3
import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError

from extractor.datadog_client import DatadogCostRecord, DatadogUsageRecord

logger = logging.getLogger(__name__)


class S3Loader:
    """Carrega dados do Datadog no S3 em formato Parquet particionado.

    Args:
        bucket: Nome do bucket S3 de destino.
        prefix: Prefixo base no S3 (padrão: 'datadog-costs/raw').
        region: Região AWS do bucket.
    """

    def __init__(
        self,
        bucket: str | None = None,
        prefix: str = "datadog-costs/raw",
        region: str | None = None,
    ) -> None:
        self.bucket = bucket or os.environ["S3_BUCKET"]
        self.prefix = prefix.rstrip("/")
        self.region = region or os.getenv("AWS_REGION", "us-east-1")
        self._s3 = boto3.client("s3", region_name=self.region)

    def _upload_parquet(self, df: pd.DataFrame, s3_key: str) -> str:
        """Converte DataFrame para Parquet e faz upload para o S3.

        Args:
            df: DataFrame a ser persistido.
            s3_key: Chave de destino no S3 (caminho completo).

        Returns:
            URI S3 do arquivo criado (s3://bucket/key).

        Raises:
            ClientError: Em caso de falha no upload.
        """
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow", compression="snappy")
        buffer.seek(0)

        try:
            self._s3.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=buffer.getvalue(),
                ContentType="application/octet-stream",
                Metadata={
                    "pipeline": "datadog-cost-datalake",
                    "created_at": datetime.utcnow().isoformat(),
                    "rows": str(len(df)),
                },
            )
        except (BotoCoreError, ClientError) as exc:
            logger.error("Falha no upload para s3://%s/%s: %s", self.bucket, s3_key, exc)
            raise

        uri = f"s3://{self.bucket}/{s3_key}"
        logger.info("Upload concluído: %s (%d linhas)", uri, len(df))
        return uri

    def upload_usage_records(
        self,
        records: list[DatadogUsageRecord],
        product: str,
        reference_date: str,
    ) -> str | None:
        """Converte e faz upload de registros de uso para o S3.

        Estrutura de partição: raw/product={product}/year={Y}/month={M}/day={D}/

        Args:
            records: Lista de DatadogUsageRecord.
            product: Nome do produto Datadog (infrastructure, apm, logs...).
            reference_date: Data de referência no formato YYYY-MM-DD.

        Returns:
            URI S3 do arquivo criado, ou None se não houver registros.
        """
        if not records:
            logger.warning("Nenhum registro de uso para %s em %s", product, reference_date)
            return None

        df = pd.DataFrame([asdict(r) for r in records])
        df["extracted_at"] = pd.to_datetime(df["extracted_at"])

        year, month, day = reference_date.split("-")
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        s3_key = (
            f"{self.prefix}/product={product}/"
            f"year={year}/month={month}/day={day}/"
            f"usage_{timestamp}.parquet"
        )

        return self._upload_parquet(df, s3_key)

    def upload_cost_records(
        self,
        records: list[DatadogCostRecord],
        month: str,
    ) -> str | None:
        """Converte e faz upload de registros de custo estimado para o S3.

        Estrutura de partição: raw/product=estimated_cost/year={Y}/month={M}/

        Args:
            records: Lista de DatadogCostRecord.
            month: Mês de referência no formato YYYY-MM.

        Returns:
            URI S3 do arquivo criado, ou None se não houver registros.
        """
        if not records:
            logger.warning("Nenhum registro de custo para %s", month)
            return None

        df = pd.DataFrame([asdict(r) for r in records])
        df["extracted_at"] = pd.to_datetime(df["extracted_at"])

        year, mon = month.split("-")
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        s3_key = (
            f"{self.prefix}/product=estimated_cost/"
            f"year={year}/month={mon}/"
            f"cost_{month.replace('-', '')}_{timestamp}.parquet"
        )

        return self._upload_parquet(df, s3_key)

    def list_uploaded_files(self, product: str | None = None) -> list[dict[str, Any]]:
        """Lista arquivos já carregados no S3 para auditoria.

        Args:
            product: Filtrar por produto específico (opcional).

        Returns:
            Lista de dicts com key, size e last_modified.
        """
        prefix = f"{self.prefix}/product={product}" if product else self.prefix
        paginator = self._s3.get_paginator("list_objects_v2")
        files: list[dict[str, Any]] = []

        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                files.append({
                    "key": obj["Key"],
                    "size_bytes": obj["Size"],
                    "last_modified": obj["LastModified"].isoformat(),
                })

        return files
