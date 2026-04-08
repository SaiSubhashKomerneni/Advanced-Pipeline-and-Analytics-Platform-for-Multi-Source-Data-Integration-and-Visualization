# APAP — Advanced Pipeline and Analytics Platform

> End-to-end data engineering platform that extracts, transforms, and visualises 352,000+ records from three heterogeneous public APIs using a production-grade three-tier architecture.

![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![Dagster](https://img.shields.io/badge/Dagster-654FF0?style=flat&logo=dagster&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-47A248?style=flat&logo=mongodb&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=flat&logo=postgresql&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=flat&logo=streamlit&logoColor=white)
![Pydantic](https://img.shields.io/badge/Pydantic-E92063?style=flat&logo=pydantic&logoColor=white)

---

## 📊 At a Glance

| Metric | Value |
|---|---|
| Records Processed | 352,465 |
| Data Quality Rate | 97.8% |
| Full Pipeline Runtime | 12 min 18 sec |
| Data Sources Integrated | 3 |
| Asset Materialisation Success | 100% |
| PostgreSQL Upsert Throughput | 11,500 records/sec |

---

## 🗂 Overview

APAP demonstrates modern data engineering best practices: automated ETL orchestration with Dagster, schema validation with Pydantic, raw data immutability in MongoDB, structured warehousing in PostgreSQL, and interactive BI dashboards in Streamlit — all wired together in a fully reproducible pipeline.

---

## 🏗 Architecture

```
External APIs (REST / GeoJSON / CSV)
        │
        ▼
┌─────────────────────────────────────┐
│  Staging Layer  →  MongoDB          │  Raw, immutable API responses
└─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────┐
│  Mart Layer     →  PostgreSQL       │  Validated, dimensional model
└─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────┐
│  BI Reporting   →  Streamlit        │  Interactive dashboards + static charts
└─────────────────────────────────────┘
        ▲
        │
   Dagster Orchestration
   (DAGs · asset lineage · retry logic · parallel execution)
```

---

## 📁 Datasets

| Dataset | Source | Records | Format |
|---|---|---|---|
| World Bank Development Indicators | World Bank API v2 | 48,327 | REST API |
| Ireland Groundwater Wells & Springs | Geological Survey Ireland (ESRI) | 36,247 | GeoJSON |
| Electric Vehicle Population | Washington State Open Data (Socrata) | 267,891 | CSV |

---

## ✅ Key Results

- **97.8% data quality** — Pydantic validation rejected malformed records at Mart ingestion boundary
- **100% asset materialisation success** — Dagster orchestrated all pipeline stages with automatic retry on transient failures
- **11,500 records/sec** PostgreSQL upsert throughput via `ON CONFLICT DO UPDATE`
- **EV adoption grew 340%** between 2018–2023; average electric range improved from 84 to 272 miles
- **GDP–population correlation r = 0.73** across 166 countries (2015–2023)
- **Dashboard time-to-insight: 3.2 minutes** in simulated analyst testing

---

## 🛠 Tech Stack

| Layer | Tools |
|---|---|
| Orchestration | Dagster |
| Staging Store | MongoDB |
| Data Warehouse | PostgreSQL |
| Validation | Pydantic |
| Visualisation | Streamlit, Plotly, Matplotlib, Seaborn |
| Data Processing | Python, Pandas, NumPy |

---

## ⚙️ Engineering Highlights

- **Incremental loading** — EV dataset updates reduced from O(n) to O(Δn) by querying only modified records
- **Exponential backoff** — custom retry decorator handles transient API failures gracefully
- **Paginated batch ingestion** — 1,000-record batches with 500ms rate limiting for GeoJSON APIs
- **Parallel staging** — 3 Dagster assets execute concurrently, achieving 100% speedup vs. sequential
- **Idempotent upserts** — `ON CONFLICT DO UPDATE` enables safe automated retries without duplicates
- **Accessible dashboards** — colorblind-safe Husl palettes, keyboard navigation, text alternatives for all charts

---

## 👥 Team

| Name | Contribution |
|---|---|
| Sandhyarani Gorantla | Architecture design, MongoDB staging, Dagster orchestration |
| Sai Subhash Komerneni | PostgreSQL mart layer, Pydantic validation, ETL pipelines |
| Nichelle Patric Thomas | Streamlit dashboard, visualisation modules, BI reporting |

*National College of Ireland — School of Computing*
