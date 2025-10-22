# IESO Power Analysis

Automated data pipeline for monitoring Ontario's electricity grid using Apache Airflow.

## Overview

This project collects, processes, and stores electricity demand, generation capacity, and fuel-type output data from the Independent Electricity System Operator (IESO). It includes time-series forecasting capabilities using ARIMA models.

## Features

- **Automated Data Collection**: Hourly ingestion from IESO public API
- **Multi-Source Integration**: Handles CSV and XML data formats
- **Data Transformation**: Hierarchical ETL pipeline (00_RAW → 01_PRI schemas)
- **Audit Trail**: Table register tracking all data modifications
- **Time-Series Forecasting**: ARIMA model for demand prediction
- **MLflow Integration**: Model versioning and experiment tracking

## Project Structure

```
IESO-Power-Analysis/
├── dags/
│   ├── hourly_runs/          # Hourly DAGs (@hourly schedule)
│   ├── one_time_runs/        # Initial data load DAGs
│   └── utils/                # Shared utility modules
│       ├── database.py       # Database connection utilities
│       ├── updates.py        # Table register updates
│       └── validators.py     # Data validation functions
├── config/
│   ├── settings.yaml         # Centralized configuration
│   └── config_loader.py      # Configuration loading utilities
├── Azure-ML/
│   └── forecasts.ipynb       # ARIMA forecasting notebook
├── docker-compose.yaml       # Airflow infrastructure
└── requirements.txt          # Python dependencies
```

## Quick Start

### Prerequisites

- Docker Desktop 25+
- Docker Compose 2.0+
- PostgreSQL database (local or cloud-hosted like NeonDB)
- Git

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/aleemkirk/IESO-Power-Analysis.git
   cd IESO-Power-Analysis
   ```

2. **Configure environment**
   ```bash
   echo "AIRFLOW_UID=$(id -u)" > .env
   ```

3. **Start Airflow**
   ```bash
   docker-compose up -d
   ```

4. **Access Airflow UI**
   - Navigate to http://localhost:8080
   - Username: `airflow`
   - Password: `airflow`

5. **Configure Airflow Variables**

   Go to Admin → Variables and add:
   - `DB_USER` - PostgreSQL username
   - `DB_PASSWORD` - PostgreSQL password
   - `HOST` - PostgreSQL host (e.g., your-db.neon.tech)
   - `DB_PORT` - PostgreSQL port (default: 5432)
   - `DB` - Database name

6. **Initialize Database Schemas**

   Connect to your PostgreSQL database and run:
   ```sql
   CREATE SCHEMA IF NOT EXISTS "00_RAW";
   CREATE SCHEMA IF NOT EXISTS "01_PRI";
   CREATE SCHEMA IF NOT EXISTS "00_REF";

   CREATE TABLE IF NOT EXISTS "00_REF"."00_TABLE_REGISTER" (
       id SERIAL PRIMARY KEY,
       "TABLE_NAME" VARCHAR(100) NOT NULL,
       "TABLE_SCHEMA" VARCHAR(50) NOT NULL,
       "MODIFIED_DT" DATE NOT NULL,
       "MODIFIED_TIME" TIMESTAMP NOT NULL,
       "ROW_COUNT" INTEGER,
       created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
   );
   ```

7. **Enable DAGs**
   - In Airflow UI, toggle DAGs to "On"
   - One-time DAGs will run once to load historical data
   - Hourly DAGs will run every hour

## Configuration

All configuration is centralized in `config/settings.yaml`:

```yaml
# IESO API endpoints
ieso:
  base_url: "https://reports-public.ieso.ca/public/"
  endpoints:
    demand: "Demand/PUB_Demand.csv"
    zonal_demand: "DemandZonal/PUB_DemandZonal.csv"

# Database schemas and tables
database:
  schemas:
    raw: "00_RAW"
    primary: "01_PRI"
  tables:
    demand: "00_IESO_DEMAND"
```

To modify configuration:
1. Edit `config/settings.yaml`
2. Restart Airflow: `docker-compose restart`

## Data Pipeline Architecture

### Schema Design

- **00_RAW**: Raw data as ingested from IESO API
- **01_PRI**: Transformed/normalized data ready for analysis
- **00_REF**: Reference tables (table register, metadata)

### Pipeline Flow

```
IESO API → Download → Parse/Clean → 00_RAW → Transform → 01_PRI
                                       ↓
                                Table Register
```

### Available DAGs

#### Hourly Runs
- `hourly_ieso_demand_pipeline` - Ontario electricity demand
- `hourly_ieso_zonal_demand_pipeline` - Demand by geographic zone
- `01_hourly_ieso_zonal_demand_pipeline` - Normalized zonal demand
- `hourly_ieso_output_by_fuel_hourly` - Generation by fuel type
- `hourly_ieso_gen_output_capability_report` - Generator capacity

#### One-Time Runs (Historical Data)
- `create_ieso_demand_pipeline_one_time`
- `create_ieso_zonal_demand_pipeline_one_time`
- `create_ieso_output_by_fuel_hourly_one_time`
- `create_ieso_gen_output_capability_report`

## Development

### Local Development Setup

```bash
# Install Python dependencies
pip install -r requirements.txt

# Run tests (when implemented)
pytest tests/

# Code formatting
black dags/ config/

# Type checking
mypy dags/
```

### Using Refactored Code

New refactored DAG files are available with improved:
- **Centralized configuration** via `config/settings.yaml`
- **Shared utilities** in `dags/utils/`
- **Better error handling** with specific exception types
- **Comprehensive docstrings** for all functions
- **Data validation** before database writes

Example refactored DAG: `dags/hourly_runs/IESO_PUBLIC_DEMAND_HOURLY_REFACTORED.py`

### Adding New DAGs

1. Import shared utilities:
   ```python
   from dags.utils.database import get_database_url, get_engine
   from config.config_loader import get_config, get_ieso_url
   ```

2. Use configuration instead of hardcoding:
   ```python
   # Bad
   url = "https://reports-public.ieso.ca/public/Demand/PUB_Demand.csv"

   # Good
   url = get_ieso_url('demand')
   ```

3. Use shared database utilities:
   ```python
   # Bad
   db_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"

   # Good
   db_url = get_database_url()
   ```

## Machine Learning

### ARIMA Forecasting

Located in `Azure-ML/forecasts.ipynb`:

1. Loads historical demand data
2. Trains ARIMA model (configurable in `config/settings.yaml`)
3. Generates forecasts
4. Logs model to MLflow

**Configuration**:
```yaml
forecasting:
  arima:
    order: [5, 1, 0]  # (p, d, q) parameters
    train_test_split: 0.8
```

## Monitoring

### Table Register

All data writes are logged to `00_REF.00_TABLE_REGISTER`:

```sql
SELECT
    "TABLE_NAME",
    "TABLE_SCHEMA",
    "MODIFIED_DT",
    "ROW_COUNT"
FROM "00_REF"."00_TABLE_REGISTER"
ORDER BY "MODIFIED_TIME" DESC
LIMIT 10;
```

### Airflow Logs

- Access logs via Airflow UI → DAGs → [DAG Name] → [Task] → Logs
- Or check Docker logs: `docker-compose logs -f airflow-scheduler`

## Troubleshooting

### DAGs Not Appearing

1. Check git-sync logs: `docker-compose logs git-sync`
2. Verify DAG file has no syntax errors
3. Restart dag-processor: `docker-compose restart airflow-dag-processor`

### Database Connection Errors

1. Verify Airflow Variables are set correctly
2. Test connection from Airflow UI: Admin → Connections
3. Check database allows connections from Docker containers

### Import Errors (PyYAML)

1. Update docker-compose.yaml:
   ```yaml
   environment:
     _PIP_ADDITIONAL_REQUIREMENTS: pyyaml>=6.0
   ```
2. Restart: `docker-compose down && docker-compose up -d`

### XML Parsing Errors

- Check IESO API is accessible
- Verify XML namespace in `config/settings.yaml`

## Data Sources

All data is sourced from IESO's public reporting system:
- https://reports-public.ieso.ca/public/

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests (when test framework is set up)
5. Submit a pull request

## Documentation

- **CLAUDE.md** - Comprehensive code review and refactoring guide
- **config/settings.yaml** - All configuration options
- **Inline docstrings** - Function-level documentation

## License

MIT License - See LICENSE file for details

## Support

- **Issues**: https://github.com/aleemkirk/IESO-Power-Analysis/issues
- **Pull Requests**: Welcome!

## Roadmap

### Short-Term
- [ ] Complete DAG refactoring for all pipelines
- [ ] Implement unit tests (pytest)
- [ ] Add data quality monitoring (Great Expectations)
- [ ] Set up CI/CD with GitHub Actions

### Medium-Term
- [ ] Create FastAPI service for data queries
- [ ] Build Streamlit dashboard for visualization
- [ ] Implement multi-step forecasting
- [ ] Add anomaly detection

### Long-Term
- [ ] Experiment with deep learning models (LSTM)
- [ ] Migrate to Kubernetes
- [ ] Integrate weather data
- [ ] Add carbon emissions tracking

---

**Built with:** Apache Airflow, PostgreSQL, Pandas, MLflow, Docker
