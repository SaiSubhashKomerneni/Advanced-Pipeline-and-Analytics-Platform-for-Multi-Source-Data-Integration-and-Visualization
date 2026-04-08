# Advanced-Pipeline-and-Analytics-Platform-for-Multi-Source-Data-Integration-and-Visualization
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>APAP GitHub README</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      font-family: 'Courier New', Courier, monospace;
      font-size: 14px;
      line-height: 1.7;
      color: #1a1a1a;
      background: #f5f5f5;
      padding: 2rem;
    }

    .block {
      background: #eeeeee;
      border: 0.5px solid #cccccc;
      border-radius: 12px;
      padding: 1.25rem 1.5rem;
      margin-bottom: 1rem;
    }

    .badges {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      margin-bottom: 1rem;
    }

    .badge {
      font-family: Arial, sans-serif;
      font-size: 11px;
      font-weight: 500;
      padding: 3px 10px;
      border-radius: 20px;
    }

    .b-blue   { background: #E6F1FB; color: #0C447C; }
    .b-green  { background: #EAF3DE; color: #3B6D11; }
    .b-purple { background: #EEEDFE; color: #3C3489; }
    .b-teal   { background: #E1F5EE; color: #0F6E56; }
    .b-amber  { background: #FAEEDA; color: #854F0B; }
    .b-coral  { background: #FAECE7; color: #712B13; }

    .section-label {
      font-family: Arial, sans-serif;
      font-size: 11px;
      font-weight: 500;
      color: #666666;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      margin-bottom: 0.6rem;
    }

    .metrics {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 10px;
      margin-top: 1rem;
    }

    .metric {
      background: #ffffff;
      border: 0.5px solid #cccccc;
      border-radius: 8px;
      padding: 10px 14px;
      text-align: center;
    }

    .metric .val {
      font-size: 18px;
      font-weight: 500;
      color: #1a1a1a;
    }

    .metric .lbl {
      font-family: Arial, sans-serif;
      font-size: 11px;
      color: #666666;
      margin-top: 2px;
    }

    .copy-btn {
      font-family: Arial, sans-serif;
      font-size: 12px;
      padding: 5px 12px;
      border: 0.5px solid #aaaaaa;
      border-radius: 8px;
      background: transparent;
      color: #666666;
      cursor: pointer;
      float: right;
    }

    .copy-btn:hover {
      background: #e0e0e0;
    }

    .md {
      font-family: 'Courier New', Courier, monospace;
      font-size: 13px;
      line-height: 1.8;
      color: #1a1a1a;
      white-space: pre-wrap;
      word-break: break-word;
    }
  </style>
</head>
<body>

  <!-- Badges + Metrics Block -->
  <div class="block">
    <div class="section-label">Badges</div>
    <div class="badges">
      <span class="badge b-blue">Python</span>
      <span class="badge b-blue">Dagster</span>
      <span class="badge b-blue">MongoDB</span>
      <span class="badge b-blue">PostgreSQL</span>
      <span class="badge b-green">Streamlit</span>
      <span class="badge b-purple">ETL · Data Engineering</span>
      <span class="badge b-teal">Pydantic</span>
      <span class="badge b-amber">REST API · GeoJSON · CSV</span>
      <span class="badge b-coral">Three-Tier Architecture</span>
    </div>
    <div class="metrics">
      <div class="metric"><div class="val">352K+</div><div class="lbl">Records processed</div></div>
      <div class="metric"><div class="val">97.8%</div><div class="lbl">Data quality rate</div></div>
      <div class="metric"><div class="val">12.3 min</div><div class="lbl">Full pipeline run</div></div>
      <div class="metric"><div class="val">3</div><div class="lbl">Data sources integrated</div></div>
    </div>
  </div>

  <!-- README Preview Block -->
  <div class="block">
    <button class="copy-btn" onclick="copyMd()">Copy markdown</button>
    <div class="section-label">README preview</div>
    <div class="md" id="md-content"># APAP — Advanced Pipeline and Analytics Platform

> End-to-end data engineering platform that extracts, transforms, and visualises 352,000+ records from three heterogeneous public APIs using a production-grade three-tier architecture.

## Overview
APAP demonstrates modern data engineering best practices: automated ETL orchestration with Dagster, schema validation with Pydantic, raw data immutability in MongoDB, structured warehousing in PostgreSQL, and interactive BI dashboards in Streamlit — all wired together in a fully reproducible pipeline.

## Architecture
```
External APIs (REST / GeoJSON / CSV)
        ↓
Staging Layer    →  MongoDB       (raw, immutable API responses)
        ↓
Mart Layer       →  PostgreSQL    (validated, dimensional model)
        ↓
BI Reporting     →  Streamlit     (interactive dashboards + static charts)
        ↑
   Dagster Orchestration (DAGs, asset lineage, retry logic)
```

## Datasets
| Dataset | Source | Records | Format |
|---|---|---|---|
| World Bank Development Indicators | World Bank API v2 | 48,327 | REST API |
| Ireland Groundwater Wells & Springs | Geological Survey Ireland | 36,247 | GeoJSON |
| Electric Vehicle Population | Washington State Open Data (Socrata) | 267,891 | CSV |

## Key Results
- **97.8% data quality** — Pydantic validation rejected malformed records at Mart ingestion boundary
- **100% asset materialisation success** — Dagster orchestrated all pipeline stages with automatic retry on transient failures
- **11,500 records/sec** PostgreSQL upsert throughput via `ON CONFLICT DO UPDATE`
- **EV adoption grew 340%** between 2018–2023; average range improved from 84 to 272 miles
- **GDP–population correlation r = 0.73** across 166 countries (2015–2023)
- **Dashboard time-to-insight: 3.2 minutes** in simulated analyst testing

## Tech Stack
Python · Dagster · MongoDB · PostgreSQL · Pydantic · Streamlit · Plotly · Pandas · NumPy · Matplotlib · Seaborn

## Engineering Highlights
- Incremental update strategy for EV data: O(n) → O(Δn) by querying only changed records
- Exponential backoff retry decorator for resilient API ingestion
- Paginated batch processing with rate limiting (500ms delay) for GeoJSON APIs
- Parallel staging execution — 3 assets concurrent, 100% speedup vs. sequential
- Colorblind-safe Husl palettes and accessibility-first dashboard design

## Team
Sandhyarani Gorantla · Sai Subhash Komerneni · Nichelle Patric Thomas — National College of Ireland</div>
  </div>

  <script>
    function copyMd() {
      const text = document.getElementById('md-content').innerText;
      navigator.clipboard.writeText(text).then(() => {
        const btn = document.querySelector('.copy-btn');
        btn.textContent = 'Copied!';
        setTimeout(() => btn.textContent = 'Copy markdown', 2000);
      });
    }
  </script>

</body>
</html>
