# 📊 datadog-cost-datalake

> Pipeline de observabilidade de custos do Datadog → S3 → Databricks Delta Lake com visualizações estruturadas.

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![AWS S3](https://img.shields.io/badge/AWS_S3-FF9900?style=flat-square&logo=amazonaws&logoColor=white)](https://aws.amazon.com/s3)
[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white)](https://databricks.com)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=flat-square&logo=delta&logoColor=white)](https://delta.io)
[![Datadog](https://img.shields.io/badge/Datadog-632CA6?style=flat-square&logo=datadog&logoColor=white)](https://datadoghq.com)
[![Terraform](https://img.shields.io/badge/Terraform-7B42BC?style=flat-square&logo=terraform&logoColor=white)](https://terraform.io)
[![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)](LICENSE)

---

## 🏗️ Arquitetura

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────────────────────────┐
│                 │     │                  │     │           AWS S3 (Data Lake)        │
│   Datadog API   │────▶│   Extractor      │────▶│                                     │
│  (Usage Metrics │     │   (Python)       │     │  s3://bucket/datadog-costs/          │
│   Cost API)     │     │                  │     │  ├── raw/YYYY/MM/DD/*.parquet        │
│                 │     │  - Custos por    │     │  ├── processed/                     │
└─────────────────┘     │    produto       │     │  └── _delta_log/                    │
                        │  - Infra hosts   │     │                                     │
                        │  - APM services  │     └──────────────┬──────────────────────┘
                        │  - Logs volume   │                    │
                        └──────────────────┘                    │ Mount / Auto Loader
                                                                ▼
                                                ┌──────────────────────────────────────┐
                                                │          Databricks                  │
                                                │                                      │
                                                │  Bronze → Silver → Gold (Medallion) │
                                                │                                      │
                                                │  ┌────────────────────────────────┐ │
                                                │  │  Dashboards & Visualizações    │ │
                                                │  │  • Custo por produto/serviço   │ │
                                                │  │  • Tendência mensal            │ │
                                                │  │  • Top hosts por custo         │ │
                                                │  │  • Anomalias de consumo        │ │
                                                │  └────────────────────────────────┘ │
                                                └──────────────────────────────────────┘
```

---

## 🗂️ Estrutura do Repositório

```
datadog-cost-datalake/
├── extractor/
│   ├── datadog_client.py      # Cliente Datadog API (Usage + Cost)
│   ├── cost_extractor.py      # Extração de custos por produto/serviço
│   └── s3_loader.py           # Upload para S3 em formato Parquet
├── loader/
│   └── pipeline.py            # Orquestrador do pipeline (extract → transform → load)
├── databricks/
│   ├── notebooks/
│   │   ├── 01_bronze_ingest.py    # Ingestão raw → Bronze (Auto Loader)
│   │   ├── 02_silver_transform.py # Transformações → Silver (Delta Lake)
│   │   └── 03_gold_dashboard.py   # Agregações → Gold + Visualizações
│   └── schemas/
│       └── cost_schema.py         # Schemas Delta Lake
├── terraform/
│   ├── main.tf                # Bucket S3 + IAM + Databricks mount
│   ├── variables.tf
│   └── outputs.tf
├── tests/
│   ├── test_datadog_client.py
│   ├── test_cost_extractor.py
│   └── test_s3_loader.py
├── docs/
│   └── architecture.md
├── .env.example
├── pyproject.toml
└── README.md
```

---

## ⚡ Quick Start

### Pré-requisitos

- Python 3.11+
- AWS CLI configurado (`aws configure`)
- Datadog API Key + Application Key
- Databricks workspace com acesso ao S3
- [Poetry](https://python-poetry.org/)

### Instalação

```bash
git clone https://github.com/ivonaldomdias/datadog-cost-datalake.git
cd datadog-cost-datalake

poetry install
cp .env.example .env
# Edite .env com suas credenciais
```

### Executar o Pipeline

```bash
# Extrair custos do Datadog e carregar no S3 (últimos 30 dias)
poetry run python loader/pipeline.py --days 30

# Extrair apenas produto específico
poetry run python loader/pipeline.py --days 7 --product infrastructure

# Executar com dry-run (sem upload para S3)
poetry run python loader/pipeline.py --days 7 --dry-run
```

### Provisionar Infraestrutura

```bash
cd terraform/
terraform init
terraform plan
terraform apply
```

### Executar Notebooks Databricks

Importe os notebooks de `databricks/notebooks/` no seu workspace e execute na ordem:
1. `01_bronze_ingest.py` — ingestão via Auto Loader
2. `02_silver_transform.py` — limpeza e enriquecimento
3. `03_gold_dashboard.py` — agregações e visualizações

---

## 📊 Dados Coletados do Datadog

| Produto | Métrica | Granularidade |
|---|---|---|
| Infrastructure | Hosts faturáveis por tipo | Diária |
| APM | Serviços rastreados, spans indexados | Diária |
| Logs | Volume ingerido e indexado (GB) | Diária |
| Synthetics | Execuções de testes | Diária |
| Custom Metrics | Contagem de métricas customizadas | Diária |
| Estimated Cost | Custo estimado por produto (USD) | Mensal |

---

## 🥉🥈🥇 Arquitetura Medallion (Delta Lake)

| Camada | Tabela | Descrição |
|---|---|---|
| Bronze | `datadog_costs.bronze_raw` | Dados brutos do Datadog, sem transformação |
| Silver | `datadog_costs.silver_costs` | Dados limpos, tipados e enriquecidos |
| Gold | `datadog_costs.gold_daily_summary` | Agregações diárias prontas para dashboard |
| Gold | `datadog_costs.gold_product_trend` | Tendência mensal por produto |
| Gold | `datadog_costs.gold_top_hosts` | Top hosts por custo de infraestrutura |

---

## 🧪 Testes

```bash
poetry run pytest tests/ -v --cov=extractor --cov=loader --cov-report=term-missing
```

---

## 📄 Licença

MIT License — veja [LICENSE](LICENSE) para detalhes.

---

<p align="center">
  Desenvolvido por <a href="https://www.linkedin.com/in/ivonaldo-micheluti-dias-61580470/">Ivonaldo Micheluti Dias</a> · Cloud & FinOps Engineer
</p>
