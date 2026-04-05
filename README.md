# World Bank Data360 Indicator Service

A small Python data service that fetches World Development Indicators from the
[World Bank Data360 API](https://data360.worldbank.org/en/api), performs
meaningful transformations, and exports the results as structured JSON and CSV files.

## Quick Start

```bash
# Clone and set up
git clone <repo-url> && cd world-bank-data-service
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Copy and (optionally) edit config
cp .env.example .env

# Run with live API
python main.py

# Run with sample data (no network required)
python main.py --sample

# Run tests
python -m pytest tests/ -v
```

## What It Does

The service runs a three-stage pipeline:

### 1. Ingestion
Fetches data from the Data360 API for three indicators across five G7 economies
(2018–2023):

| Indicator | API Code | Description |
|-----------|----------|-------------|
| Population | `WB_WDI_SP_POP_TOTL` | Total population |
| GDP per capita | `WB_WDI_NY_GDP_PCAP_CD` | GDP per capita in current US$ |
| Unemployment | `WB_WDI_SL_UEM_TOTL_ZS` | % of total labour force |

The API client includes retry logic with exponential back-off and can fall back
to a bundled sample-data file for offline or CI use.

### 2. Transformations

**Year-over-year growth rates** — For each country and indicator, computes the
percentage change between consecutive years. Useful for spotting trends and
anomalies (e.g. the 2020 GDP drop).

**COVID-19 impact analysis** — Compares each country's 2020 value against its
2018–2019 average baseline, then checks whether 2023 values have recovered past
the baseline. For unemployment (where lower = better), the recovery logic is
inverted.

**Country ranking & normalisation** — Within each indicator and year, ranks
countries and applies min-max normalisation (0–100 scale) so different indicators
become comparable. Unemployment ranking is inverted so lower unemployment = rank 1.

### 3. Output

Writes six files to the `output/` directory (JSON + CSV for each transformation):
- `growth_rates.json / .csv`
- `covid_impact.json / .csv`
- `rankings.json / .csv`

## Project Structure

```
world-bank-data-service/
├── main.py                  # CLI entry point
├── config/
│   └── settings.py          # Env-based configuration
├── src/
│   ├── ingestion/
│   │   └── client.py        # Data360 API client
│   ├── processing/
│   │   └── transformations.py  # Three transformation functions
│   └── output/
│       └── writer.py        # JSON/CSV writer
├── tests/
│   └── test_transformations.py
├── data/
│   └── samples/
│       └── sample_response.json
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

## Assumptions

1. **Annual data only** — The API returns annual frequency (`FREQ: "A"`) data.
   The transformations assume one observation per country per indicator per year.

2. **Indicator semantics** — Unemployment is treated as a "lower is better"
   metric for ranking and recovery checks. GDP and population are "higher is
   better". If new indicators are added, this logic may need extending.

3. **API response format** — The Data360 API returns data in a
   `{"count": N, "value": [...]}` wrapper with uppercase field names. The
   ingestion layer normalises these to lowercase for a cleaner processing
   interface.

4. **No authentication** — The Data360 API is public and requires no API key.
   If this changes, the `Settings` class can be extended to accept one via
   environment variable.

5. **Sample data accuracy** — The bundled sample data uses realistic values
   sourced from World Bank publications, but should not be treated as
   authoritative. It exists for development and testing convenience.

## Trade-offs & Design Decisions

- **Client-side time filtering** — The API returns HTTP 417 when multiple
  `TIME_PERIOD` values are requested at once. As a pragmatic workaround,
  we fetch all years and filter to the configured range in Python. This
  fetches more data than strictly needed but avoids unreliable API behaviour.

- **Uppercase-to-lowercase normalisation** — The API uses uppercase field names
  (`OBS_VALUE`, `REF_AREA`, etc.). The ingestion layer normalises these to
  lowercase (`obs_value`, `ref_area`) so the processing layer stays clean and
  readable regardless of API quirks.

- **Pandas over raw dicts** — Pandas adds a dependency but makes grouping,
  ranking, and normalisation dramatically simpler and more readable than manual
  loops. For 90 records this is overkill; for production data volumes it is
  the right call.

- **Sample data fallback** — Rather than mocking `requests` in tests, the
  ingestion layer supports loading from a local file. This keeps tests fast,
  deterministic, and runnable offline without a mock library dependency.

- **Flat output files** — JSON and CSV were chosen over a database or API
  endpoint because they are the lowest-friction option for downstream consumers
  (another service, a notebook, a BI tool). Adding a FastAPI layer would be
  straightforward but felt like unnecessary scope for this exercise.

- **Frozen dataclass for config** — Immutability prevents accidental mutation
  mid-pipeline and makes it clear that config is set once at startup.

- **Pure transformation functions** — Each transformation takes a DataFrame in
  and returns a new DataFrame. No side effects, no I/O. This makes them easy
  to test with small hand-crafted datasets.

## What I Would Improve in Production

- **Incremental ingestion** — Only fetch new data since the last run rather
  than re-fetching everything. Store a watermark timestamp.

- **Raw data persistence** — Save the raw API response before processing so
  data can be reprocessed or audited without re-fetching.

- **Schema validation** — Use Pydantic models to validate API responses at
  ingestion time so malformed data fails fast with clear error messages.

- **Persistent storage** — Write to a database (e.g. DuckDB or PostgreSQL)
  instead of flat files, enabling incremental updates and SQL queries.

- **Orchestration** — Run on a schedule via Airflow, Dagster, or a cron job
  with proper alerting on failure.

- **Observability** — Structured JSON logging, request tracing, and metrics
  (records fetched, transform durations, error rates).

- **Data quality checks** — Assert expected row counts, check for NULL values,
  validate value ranges (e.g. unemployment should be 0–100%).

- **Containerisation** — Dockerfile + docker-compose for reproducible
  environments.

- **CI pipeline** — Run tests and linting on every push (GitHub Actions).