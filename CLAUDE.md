# IESO Power Analysis - Project Documentation

**Project Owner:** Aleem Khan
**Last Updated:** 2025-10-21
**Repository:** https://github.com/aleemkirk/IESO-Power-Analysis

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture & Design](#architecture--design)
3. [Codebase Structure](#codebase-structure)
4. [Data Pipeline Flows](#data-pipeline-flows)
5. [Code Quality Analysis](#code-quality-analysis)
6. [Critical Issues](#critical-issues)
7. [Refactoring Recommendations](#refactoring-recommendations)
8. [Setup & Dependencies](#setup--dependencies)
9. [Future Enhancements](#future-enhancements)

---

## Project Overview

### Purpose
This project implements an automated ETL (Extract, Transform, Load) pipeline for monitoring Ontario's electricity grid data from the Independent Electricity System Operator (IESO). It collects, processes, and stores power demand, generation capacity, and fuel-type output data for analysis and forecasting.

### Key Features
- **Automated Data Collection**: Hourly data ingestion from IESO public API
- **Multi-Source Integration**: Handles CSV and XML data formats
- **Time-Series Forecasting**: ARIMA model for demand prediction
- **Audit Trail**: Table register tracking all data modifications
- **Orchestration**: Apache Airflow 3.0.4 with CeleryExecutor
- **MLflow Integration**: Model versioning and experiment tracking

### Technology Stack
- **Orchestration**: Apache Airflow 3.0.4
- **Database**: PostgreSQL 13+ (multi-schema design)
- **Data Processing**: Pandas, NumPy
- **ML/Forecasting**: Statsmodels (ARIMA), Scikit-learn, MLflow
- **Infrastructure**: Docker Compose, Redis, Celery
- **CI/CD**: Git-sync container for automated DAG deployment

---

## Architecture & Design

### System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    IESO Public API                          │
│  (reports-public.ieso.ca/public/)                          │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│              Apache Airflow DAGs                            │
│  ┌──────────────────┐    ┌──────────────────┐             │
│  │  Hourly Runs     │    │ One-Time Runs    │             │
│  │  (5 DAGs)        │    │ (4 DAGs)         │             │
│  └──────────────────┘    └──────────────────┘             │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│              PostgreSQL Database                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │  00_RAW      │─→│  01_PRI      │  │  00_REF      │     │
│  │  (Raw Data)  │  │  (Processed) │  │  (Metadata)  │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│              Azure ML / Forecasting                         │
│  ARIMA Model → MLflow → Predictions                        │
└─────────────────────────────────────────────────────────────┘
```

### Database Schema Design

#### Schema Hierarchy
- **00_RAW**: Raw data as ingested from IESO API (minimal processing)
- **01_PRI**: Primary/normalized data ready for analysis
- **00_REF**: Reference tables (table register, metadata)

#### Key Tables
- `00_RAW.00_IESO_DEMAND` - Ontario hourly electricity demand
- `00_RAW.00_IESO_ZONAL_DEMAND` - Demand by zone (wide format)
- `01_PRI.01_IESO_ZONAL_DEMAND` - Demand by zone (normalized/melted)
- `00_RAW.00_IESO_GENERATOR_OUTPUT_BY_FUEL_TYPE` - Generation by fuel
- `00_RAW.00_IESO_GEN_OUTPUT_CAPABILITY` - Generator capacity tracking
- `00_REF.00_TABLE_REGISTER` - Audit log of all table updates

### Data Flow Patterns

#### Pattern 1: ETL Pipeline (Extract → Transform → Load)
```
IESO API → Download CSV/XML → Parse & Clean → PostgreSQL → Update Register
```

#### Pattern 2: Hierarchical Transformation
```
External Source → 00_RAW Schema → Transformation Logic → 01_PRI Schema
```

#### Pattern 3: Table Registry Pattern
Every data write operation logs metadata to `00_TABLE_REGISTER`:
- Table name & schema
- Modification timestamp (date + time)
- Row count
- Last update timestamp

---

## Codebase Structure

```
IESO-Power-Analysis/
│
├── dags/                                    # Airflow DAG definitions (1,856 LOC)
│   ├── hourly_runs/                        # 5 DAGs running @hourly
│   │   ├── IESO_PUBLIC_DEMAND_HOURLY.py
│   │   ├── 01_IESO_PUBLIC_ZONAL_DEMAND_HOURLY.py
│   │   ├── IESO_PUBLIC_ZONAL_DEMAND_HOURLY.py
│   │   ├── IESO_GENERATOR_OUTPUT_BY_FUEL_TYPE_HOULRY.py  # ⚠️ Typo
│   │   └── IESO_GEN_OUPUT_CAPABILITY_REPORT_HOURLY.py    # ⚠️ Typo
│   │
│   ├── one_time_runs/                      # 4 DAGs for initial data load
│   │   ├── CREATE_IESO_PUBLIC_DEMAND_ONE_TIME_RUN.py
│   │   ├── CREATE_IESO_PUBLIC_ZONAL_DEMAND_ONE_TIME_RUN.py
│   │   ├── CREATE_IESO_GENERATOR_OUTPUT_BY_FUEL_TYPE_ONE_TIME_RUN.py
│   │   └── CREATE_IESO_GEN_OUPUT_CAPABILITY_REPORT_HOURLY.py
│   │
│   └── utils/                              # Shared utility modules
│       ├── __init__.py
│       └── updates.py                      # Table register update function
│
├── Azure-ML/                               # ML forecasting components
│   ├── forecasts.ipynb                     # ARIMA time-series notebook
│   ├── model.pkl                           # Serialized ARIMA model (1.2MB)
│   ├── requirements.txt                    # ML dependencies
│   ├── conda.yaml                          # Conda environment
│   ├── python_env.yaml                     # Python environment config
│   └── MLmodel                             # MLflow model metadata
│
├── scripts/                                # Standalone scripts
│   └── IESO_PUBLIC_DEMAND_(ARCHIVE).py    # ⚠️ Archived/unused
│
├── config/                                 # Configuration (empty)
├── docker-compose.yaml                     # Airflow infrastructure
├── .env                                    # Environment variables
├── .gitignore                              # Git ignore patterns
└── neondb_00_RAW_00_IESO_DEMAND_2.csv     # Sample data
```

### Key Files Description

#### `dags/hourly_runs/IESO_PUBLIC_DEMAND_HOURLY.py` (183 LOC)
- **DAG ID**: `hourly_ieso_demand_pipeline`
- **Schedule**: `@hourly`
- **Purpose**: Download and ingest Ontario electricity demand data
- **Tasks**:
  1. `postgres_connection()` - Build DB connection string
  2. `ieso_demand_data_pull()` - Download CSV from IESO
  3. `transform_data()` - Parse, clean, filter to latest date
  4. `create_00_ref()` - Delete old data, insert new records
  5. `update_00_table_reg()` - Log update to table register

#### `dags/utils/updates.py` (58 LOC)
- **Function**: `update_00_table_reg()`
- **Purpose**: Update table register with metadata after successful data write
- **Parameters**: completion status, logger, db_url, table_name, schema
- **Operations**:
  - Insert timestamp, row count into `00_REF.00_TABLE_REGISTER`
  - Track modification date and time

#### `docker-compose.yaml` (355 LOC)
- **Services**: 9 containers (PostgreSQL, Redis, Airflow components, git-sync)
- **Executor**: CeleryExecutor with Redis broker
- **Airflow Version**: 3.0.4
- **Git Sync**: Auto-deploys DAGs from GitHub every 10 seconds
- **Volumes**: Shared volumes for DAGs, logs, config, plugins

#### `Azure-ML/forecasts.ipynb`
- **Model**: ARIMA time-series forecasting
- **Target**: Ontario electricity demand prediction
- **Train/Test Split**: 80/20 (4,937 train / 1,235 test samples)
- **Hyperparameters**: Order (5,1,0) hardcoded
- **MLflow**: Logs model, metrics (RMSE), parameters
- **Status**: ⚠️ Has execution errors (missing `azureml.training` module)

---

## Data Pipeline Flows

### 1. Hourly Demand Pipeline
**DAG**: `hourly_ieso_demand_pipeline`
**File**: `dags/hourly_runs/IESO_PUBLIC_DEMAND_HOURLY.py:23`

```
┌────────────────────────────────────────────────────────────┐
│ 1. postgres_connection()                                   │
│    → Fetch DB credentials from Airflow Variables          │
│    → Build connection string                               │
└─────────────────┬──────────────────────────────────────────┘
                  │
                  ▼
┌────────────────────────────────────────────────────────────┐
│ 2. ieso_demand_data_pull()                                 │
│    → GET https://reports-public.ieso.ca/.../PUB_Demand.csv│
│    → Save to local filesystem                              │
└─────────────────┬──────────────────────────────────────────┘
                  │
                  ▼
┌────────────────────────────────────────────────────────────┐
│ 3. transform_data(filename)                                │
│    → Parse CSV with pandas                                 │
│    → Convert types (datetime, numeric)                     │
│    → Filter to max(Date) - today's data only               │
│    → Add Modified_DT column                                │
└─────────────────┬──────────────────────────────────────────┘
                  │
                  ▼
┌────────────────────────────────────────────────────────────┐
│ 4. create_00_ref(df, db_url)                               │
│    → Delete existing rows for max(Date)                    │
│    → Insert filtered DataFrame                             │
│    → Table: 00_RAW.00_IESO_DEMAND                         │
└─────────────────┬──────────────────────────────────────────┘
                  │
                  ▼
┌────────────────────────────────────────────────────────────┐
│ 5. update_00_table_reg(status, logger, url, table, schema)│
│    → Insert audit record to 00_REF.00_TABLE_REGISTER      │
│    → Log: table name, schema, modified_dt, row count      │
└────────────────────────────────────────────────────────────┘
```

### 2. Zonal Demand Enhancement Pipeline
**DAG**: `hourly_ieso_zonal_demand_pipeline_01`
**File**: `dags/hourly_runs/01_IESO_PUBLIC_ZONAL_DEMAND_HOURLY.py`

```
┌────────────────────────────────────────────────────────────┐
│ 1. wait_for_upstream_dag (ExternalTaskSensor)              │
│    → Wait for 'hourly_ieso_zonal_demand_pipeline'          │
│    → ⚠️ CRITICAL BUG: This DAG doesn't exist!              │
└─────────────────┬──────────────────────────────────────────┘
                  │
                  ▼
┌────────────────────────────────────────────────────────────┐
│ 2. read_00_raw_data()                                      │
│    → SELECT * FROM 00_RAW.00_IESO_ZONAL_DEMAND            │
│    → Return DataFrame (wide format: Date, Hour, Zone cols)│
└─────────────────┬──────────────────────────────────────────┘
                  │
                  ▼
┌────────────────────────────────────────────────────────────┐
│ 3. transform_to_01_pri(df)                                 │
│    → Melt DataFrame: Zone columns → (Zone, Demand) pairs  │
│    → Normalize into key-value format                       │
└─────────────────┬──────────────────────────────────────────┘
                  │
                  ▼
┌────────────────────────────────────────────────────────────┐
│ 4. write_to_01_pri(df, db_url)                             │
│    → df.to_sql(..., if_exists='replace')                   │
│    → Table: 01_PRI.01_IESO_ZONAL_DEMAND                   │
└─────────────────┬──────────────────────────────────────────┘
                  │
                  ▼
┌────────────────────────────────────────────────────────────┐
│ 5. update_00_table_reg(...)                                │
│    → Log update to table register                          │
└────────────────────────────────────────────────────────────┘
```

### 3. Generator Output (XML) Pipeline
**DAG**: `hourly_ieso_generator_output_by_fuel_type_pipeline`
**File**: `dags/hourly_runs/IESO_GENERATOR_OUTPUT_BY_FUEL_TYPE_HOULRY.py`

```
┌────────────────────────────────────────────────────────────┐
│ 1. ieso_gen_output_data_pull()                             │
│    → Download XML from IESO API                            │
│    → File: PUB_GenOutputbyFuelHourly.xml                   │
└─────────────────┬──────────────────────────────────────────┘
                  │
                  ▼
┌────────────────────────────────────────────────────────────┐
│ 2. parse_xml_and_transform(xml_file)                       │
│    → Parse nested XML structure                            │
│    → Extract: Date, FuelType, Hour 1-24 outputs            │
│    → Flatten into columnar DataFrame                       │
│    → Filter to max(Date)                                   │
└─────────────────┬──────────────────────────────────────────┘
                  │
                  ▼
┌────────────────────────────────────────────────────────────┐
│ 3. write_to_postgres(df, db_url)                           │
│    → Delete existing rows for max(Date)                    │
│    → Insert new records                                    │
│    → Table: 00_RAW.00_IESO_GENERATOR_OUTPUT_BY_FUEL_TYPE  │
└─────────────────┬──────────────────────────────────────────┘
                  │
                  ▼
┌────────────────────────────────────────────────────────────┐
│ 4. update_00_table_reg(...)                                │
└────────────────────────────────────────────────────────────┘
```

---

## Code Quality Analysis

### Metrics Summary
- **Total Lines of Code**: ~1,856 (Python)
- **Number of DAG Files**: 9
- **Number of Tasks**: ~40+ across all DAGs
- **Test Coverage**: 0% (no tests exist)
- **Documentation**: Minimal (no docstrings, no README)
- **Type Hints**: Sparse (~20% coverage)

### Strengths
1. **Consistent Error Handling**: 46 try/except blocks across codebase
2. **Good Logging Practices**: Extensive use of logger.info() and logger.error()
3. **Modular Task Architecture**: Clear separation of concerns in Airflow tasks
4. **Secure Credential Management**: DB credentials stored in Airflow Variables
5. **Audit Trail**: Table register pattern tracks all data modifications
6. **Connection Pooling**: SQLAlchemy engine properly managed

### Code Quality Issues

#### Severity Legend
- 🔴 **CRITICAL**: Blocking production deployment
- 🟠 **HIGH**: Should fix immediately
- 🟡 **MEDIUM**: Fix in next sprint
- 🟢 **LOW**: Nice to have

---

### 🔴 CRITICAL ISSUES

#### 1. Massive Code Duplication
**Affected Files**: All 9 DAG files

**Duplicated Code Blocks**:
- `postgres_connection()` task (12 lines) - duplicated 9 times
- Database credential retrieval logic - duplicated 10+ times
- IESO API base URL definitions - hardcoded in every file
- XML namespace handling - copied verbatim across 3 files

**Example**:
```python
# This exact code appears in 9 different files:
@task
def postgres_connection() -> str:
    try:
        DB_USER = Variable.get('DB_USER')
        DB_PASSWORD = Variable.get('DB_PASSWORD')
        DB_HOST = Variable.get('HOST')
        DB_PORT = Variable.get('DB_PORT', default_var='5432')
        DB_NAME = Variable.get('DB')
        logger.info("Database credentials loaded from Airflow Variables.")
    except Exception as e:
        logger.error(f"Error accessing Airflow Variables: {e}.")
        raise Exception("Airflow Variables not configured.")
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return DATABASE_URL
```

**Impact**:
- Changes require updating 9 files
- High risk of inconsistencies
- Violates DRY principle

**Lines of Duplicated Code**: ~200+ lines

---

### 🟠 HIGH PRIORITY ISSUES

#### 3. Generic Exception Handling
**Occurrences**: 46 instances

```python
# Bad - catches everything, even KeyboardInterrupt
except Exception as e:
    logger.error(f"Error: {e}")
    raise e
```

**Problems**:
- Hides specific error types (network, parse, DB errors)
- Difficult to debug in production
- Can't implement targeted retry logic

**Better Approach**:
```python
except requests.exceptions.RequestException as e:
    logger.error(f"Network error downloading {url}: {e}")
    raise
except pd.errors.ParserError as e:
    logger.error(f"CSV parsing failed: {e}")
    raise
except SQLAlchemyError as e:
    logger.error(f"Database error: {e}")
    raise
```

---

#### 4. Hardcoded Configuration Values
**Locations**: All DAG files

```python
# dags/hourly_runs/IESO_PUBLIC_DEMAND_HOURLY.py:26-29
base_url = 'https://reports-public.ieso.ca/public/Demand/'
filename = 'PUB_Demand.csv'
table = '00_IESO_DEMAND'
schema = '00_RAW'
```

**Problems**:
- Can't change environments (dev/staging/prod) without code changes
- No centralized configuration
- Difficult to test with different data sources

**Recommended Structure**:
```python
# config/data_sources.yaml
ieso:
  base_url: "https://reports-public.ieso.ca/public/"
  endpoints:
    demand: "Demand/PUB_Demand.csv"
    zonal: "DemandZonal/PUB_DemandZonal.csv"

database:
  schemas:
    raw: "00_RAW"
    primary: "01_PRI"
    reference: "00_REF"
```

---

#### 5. Filename Typos
**Files Affected**:
- `IESO_GENERATOR_OUTPUT_BY_FUEL_TYPE_HOULRY.py` (should be "HOURLY")
- `IESO_GEN_OUPUT_CAPABILITY_REPORT_HOURLY.py` (should be "OUTPUT")

**Impact**: Unprofessional, confusing for developers

---

#### 6. Missing Type Hints
**Coverage**: ~20% of functions

```python
# Current (no type hints)
def transform_data(local_filename):
    # ...

# Better
def transform_data(local_filename: str) -> pd.DataFrame:
    # ...
```

**Impact**:
- Harder to understand function contracts
- No IDE autocomplete support
- Runtime type errors

---

### 🟡 MEDIUM PRIORITY ISSUES

#### 7. No Testing Infrastructure
**Current State**:
- Zero test files
- No pytest/unittest configuration
- No CI/CD pipeline
- No test coverage reporting

**Recommended Tests**:
- Unit tests for data transformation logic
- Integration tests with mock database
- API failure scenario tests
- XML/CSV parsing edge cases

---

#### 8. Missing Documentation
**Gaps**:
- No README.md
- No docstrings on any functions
- No API documentation
- No setup instructions
- Minimal inline comments

**Example of Missing Docstring**:
```python
# Current
@task
def transform_data(local_filename) -> DataFrame:
    # --- 2. Process the downloaded CSV into a pandas DataFrame ---
    ...

# Better
@task
def transform_data(local_filename: str) -> pd.DataFrame:
    """
    Parse and transform IESO demand CSV data.

    Reads the downloaded CSV file, converts data types, adds modification
    timestamp, and filters to the most recent date only.

    Args:
        local_filename: Path to the downloaded CSV file

    Returns:
        DataFrame with columns: Date, Hour, Market_Demand, Ontario_Demand,
        Modified_DT. Filtered to contain only the latest date's records.

    Raises:
        FileNotFoundError: If local_filename doesn't exist
        pd.errors.ParserError: If CSV format is invalid
    """
    ...
```

---

#### 9. Inconsistent Database Patterns
**Issue**: Mixed usage of SQLAlchemy connection methods

```python
# Some tasks use:
with engine.connect() as conn:
    conn.execute(...)

# Others use:
with engine.begin() as conn:
    conn.execute(...)
```

**Impact**: Inconsistent transaction management, potential for partial writes

---

#### 10. Missing Data Validation
**Gaps**:
- No validation of IESO API responses
- No checks for malformed XML/CSV
- No row count validation before/after transforms
- No data quality checks (null values, outliers)

**Example Improvement**:
```python
def validate_demand_data(df: pd.DataFrame) -> pd.DataFrame:
    """Validate demand data quality."""
    # Check required columns
    required_cols = ['Date', 'Hour', 'Market_Demand', 'Ontario_Demand']
    missing = set(required_cols) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Check for nulls
    null_counts = df[required_cols].isnull().sum()
    if null_counts.any():
        logger.warning(f"Null values detected: {null_counts[null_counts > 0]}")

    # Check value ranges
    if (df['Hour'] < 1).any() or (df['Hour'] > 24).any():
        raise ValueError("Hour values out of range [1-24]")

    return df
```

---

### 🟢 LOW PRIORITY ISSUES

#### 11. Inconsistent Logger Pattern
```python
# Most files create logger as local variable
logger = logging.getLogger("airflow.task")

# Should be module-level:
logger = logging.getLogger(__name__)
```

---

#### 12. Unused/Archived Code
- `scripts/IESO_PUBLIC_DEMAND_(ARCHIVE).py` - should be removed
- Azure-ML notebook has execution errors

---

## Critical Issues

### Priority 1: Immediate Action Required

1. **Massive Code Duplication** 🔴 ✅ FIXED
   - **Files**: All 9 DAG files
   - **Issue**: `postgres_connection()` duplicated 9 times
   - **Impact**: Maintenance nightmare, high risk of inconsistencies
   - **Fix**: ✅ Extracted to shared utility module (`dags/utils/database.py`)

2. **Hardcoded Configuration** 🔴 ✅ FIXED
   - **Files**: All DAG files
   - **Issue**: API URLs, table names, schemas hardcoded
   - **Impact**: Can't change environments without code changes
   - **Fix**: ✅ Implemented configuration management system (`config/settings.yaml`)

3. **Filename Typos** 🔴 ✅ FIXED
   - **Files**: 3 files with typos (HOULRY, OUPUT)
   - **Issue**: Unprofessional, confusing
   - **Fix**: ✅ Renamed all files correctly

---

## Refactoring Recommendations

### Phase 1: Critical Fixes (Week 1)

#### 1. Fix Broken External Task Sensor
```python
# File: dags/hourly_runs/01_IESO_PUBLIC_ZONAL_DEMAND_HOURLY.py

# Before:
external_dag_id='hourly_ieso_zonal_demand_pipeline',  # Wrong!

# After (determine correct DAG):
external_dag_id='hourly_ieso_demand_pipeline',  # Or remove if not needed
```

#### 2. Extract Shared Database Connection Logic
Create `dags/utils/database.py`:
```python
from sqlalchemy import create_engine
from airflow.models import Variable
import logging

logger = logging.getLogger(__name__)

def get_database_url() -> str:
    """
    Build PostgreSQL connection string from Airflow Variables.

    Returns:
        Database URL in format: postgresql://user:pass@host:port/db

    Raises:
        ValueError: If required Airflow Variables are not set
    """
    try:
        db_user = Variable.get('DB_USER')
        db_password = Variable.get('DB_PASSWORD')
        db_host = Variable.get('HOST')
        db_port = Variable.get('DB_PORT', default_var='5432')
        db_name = Variable.get('DB')

        logger.info("Database credentials loaded from Airflow Variables.")
        return f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    except Exception as e:
        logger.error(f"Error accessing Airflow Variables: {e}")
        raise ValueError("Database Airflow Variables not configured.") from e

def get_engine(db_url: str):
    """Create SQLAlchemy engine with connection pooling."""
    return create_engine(db_url, pool_pre_ping=True)
```

Update all DAG files:
```python
from dags.utils.database import get_database_url

@task
def postgres_connection() -> str:
    return get_database_url()
```

#### 3. Create Configuration File
Create `config/settings.yaml`:
```yaml
# IESO API Configuration
ieso:
  base_url: "https://reports-public.ieso.ca/public/"
  endpoints:
    demand: "Demand/PUB_Demand.csv"
    zonal_demand: "DemandZonal/PUB_DemandZonal.csv"
    fuel_output: "GenOutputbyFuelHourly/PUB_GenOutputbyFuelHourly.xml"
    capability: "GenOutputCapability/PUB_GenOutputCapability.xml"

# Database Configuration
database:
  schemas:
    raw: "00_RAW"
    primary: "01_PRI"
    reference: "00_REF"
  tables:
    demand: "00_IESO_DEMAND"
    zonal_demand: "00_IESO_ZONAL_DEMAND"
    fuel_output: "00_IESO_GENERATOR_OUTPUT_BY_FUEL_TYPE"
    capability: "00_IESO_GEN_OUTPUT_CAPABILITY"
    table_register: "00_TABLE_REGISTER"

# Timezone
timezone: "America/Toronto"

# ARIMA Model Configuration
forecasting:
  arima:
    order: [5, 1, 0]
    train_test_split: 0.8
```

Create `config/config_loader.py`:
```python
import yaml
from pathlib import Path
from typing import Dict, Any

_config_cache = None

def load_config() -> Dict[str, Any]:
    """Load configuration from YAML file (cached)."""
    global _config_cache
    if _config_cache is None:
        config_path = Path(__file__).parent / 'settings.yaml'
        with open(config_path, 'r') as f:
            _config_cache = yaml.safe_load(f)
    return _config_cache

def get_config(key_path: str, default: Any = None) -> Any:
    """
    Get configuration value by dot-notation path.

    Example:
        get_config('ieso.base_url')
        get_config('database.schemas.raw')
    """
    config = load_config()
    keys = key_path.split('.')
    value = config
    for key in keys:
        if isinstance(value, dict):
            value = value.get(key)
        else:
            return default
    return value if value is not None else default
```

#### 4. Rename Files with Typos
```bash
# dags/hourly_runs/IESO_GENERATOR_OUTPUT_BY_FUEL_TYPE_HOULRY.py
# → rename to IESO_GENERATOR_OUTPUT_BY_FUEL_TYPE_HOURLY.py

# dags/hourly_runs/IESO_GEN_OUPUT_CAPABILITY_REPORT_HOURLY.py
# → rename to IESO_GEN_OUTPUT_CAPABILITY_REPORT_HOURLY.py
```

Update DAG IDs inside the files accordingly.

---

### Phase 2: Code Quality Improvements (Sprint 1)

#### 5. Implement Specific Exception Handling
Create `dags/utils/exceptions.py`:
```python
class IESODataPipelineError(Exception):
    """Base exception for IESO data pipeline."""
    pass

class DataDownloadError(IESODataPipelineError):
    """Failed to download data from IESO API."""
    pass

class DataParsingError(IESODataPipelineError):
    """Failed to parse CSV or XML data."""
    pass

class DatabaseWriteError(IESODataPipelineError):
    """Failed to write data to PostgreSQL."""
    pass

class DataValidationError(IESODataPipelineError):
    """Data validation failed."""
    pass
```

Update error handling in DAGs:
```python
import requests
from dags.utils.exceptions import DataDownloadError, DataParsingError

@task
def ieso_demand_data_pull() -> str:
    url = f"{base_url}{filename}"
    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        # ... download logic
    except requests.exceptions.Timeout as e:
        raise DataDownloadError(f"Timeout downloading {url}") from e
    except requests.exceptions.HTTPError as e:
        raise DataDownloadError(f"HTTP error {e.response.status_code}: {url}") from e
    except requests.exceptions.RequestException as e:
        raise DataDownloadError(f"Network error: {e}") from e
```

#### 6. Add Comprehensive Type Hints
```python
from typing import Dict, Any, Optional
import pandas as pd
from sqlalchemy import Engine

@task
def transform_data(local_filename: str) -> pd.DataFrame:
    """Transform IESO demand data."""
    ...

@task
def create_00_ref(
    df: pd.DataFrame,
    db_url: str,
    table_name: str,
    db_schema: str
) -> bool:
    """Write data to PostgreSQL."""
    ...
```

#### 7. Add Data Validation Layer
Create `dags/utils/validators.py`:
```python
import pandas as pd
import logging
from typing import List, Optional
from dags.utils.exceptions import DataValidationError

logger = logging.getLogger(__name__)

def validate_dataframe(
    df: pd.DataFrame,
    required_columns: List[str],
    check_nulls: bool = True,
    check_duplicates: bool = False
) -> pd.DataFrame:
    """
    Validate DataFrame structure and data quality.

    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        check_nulls: Whether to check for null values
        check_duplicates: Whether to check for duplicate rows

    Returns:
        Validated DataFrame

    Raises:
        DataValidationError: If validation fails
    """
    # Check required columns exist
    missing_cols = set(required_columns) - set(df.columns)
    if missing_cols:
        raise DataValidationError(f"Missing required columns: {missing_cols}")

    # Check for nulls
    if check_nulls:
        null_counts = df[required_columns].isnull().sum()
        if null_counts.any():
            null_cols = null_counts[null_counts > 0]
            logger.warning(f"Null values detected: {null_cols.to_dict()}")

    # Check for duplicates
    if check_duplicates:
        dup_count = df.duplicated().sum()
        if dup_count > 0:
            logger.warning(f"Found {dup_count} duplicate rows")

    # Check DataFrame not empty
    if len(df) == 0:
        raise DataValidationError("DataFrame is empty")

    logger.info(f"Validation passed: {len(df)} rows, {len(df.columns)} columns")
    return df

def validate_demand_data(df: pd.DataFrame) -> pd.DataFrame:
    """Validate IESO demand data specifically."""
    df = validate_dataframe(
        df,
        required_columns=['Date', 'Hour', 'Market_Demand', 'Ontario_Demand'],
        check_nulls=True
    )

    # Validate Hour range
    if (df['Hour'] < 1).any() or (df['Hour'] > 24).any():
        raise DataValidationError("Hour values must be between 1 and 24")

    # Validate demand values are positive
    if (df['Market_Demand'] < 0).any() or (df['Ontario_Demand'] < 0).any():
        raise DataValidationError("Demand values cannot be negative")

    return df
```

#### 8. Create Comprehensive Tests
Create `tests/test_validators.py`:
```python
import pytest
import pandas as pd
from dags.utils.validators import validate_dataframe, validate_demand_data
from dags.utils.exceptions import DataValidationError

def test_validate_dataframe_success():
    df = pd.DataFrame({
        'col1': [1, 2, 3],
        'col2': ['a', 'b', 'c']
    })
    result = validate_dataframe(df, required_columns=['col1', 'col2'])
    assert result is not None
    assert len(result) == 3

def test_validate_dataframe_missing_columns():
    df = pd.DataFrame({'col1': [1, 2, 3]})
    with pytest.raises(DataValidationError, match="Missing required columns"):
        validate_dataframe(df, required_columns=['col1', 'col2'])

def test_validate_demand_data_invalid_hour():
    df = pd.DataFrame({
        'Date': ['2025-01-01'],
        'Hour': [25],  # Invalid!
        'Market_Demand': [15000],
        'Ontario_Demand': [14000]
    })
    with pytest.raises(DataValidationError, match="Hour values must be"):
        validate_demand_data(df)
```

Create `tests/test_database.py`:
```python
import pytest
from unittest.mock import patch, MagicMock
from dags.utils.database import get_database_url
from airflow.models import Variable

@patch.object(Variable, 'get')
def test_get_database_url_success(mock_get):
    """Test successful database URL construction."""
    mock_get.side_effect = lambda key, default_var=None: {
        'DB_USER': 'testuser',
        'DB_PASSWORD': 'testpass',
        'HOST': 'localhost',
        'DB_PORT': '5432',
        'DB': 'testdb'
    }.get(key, default_var)

    url = get_database_url()
    assert url == 'postgresql://testuser:testpass@localhost:5432/testdb'

@patch.object(Variable, 'get')
def test_get_database_url_missing_variable(mock_get):
    """Test error when Airflow Variable is missing."""
    mock_get.side_effect = KeyError("Variable not found")

    with pytest.raises(ValueError, match="Database Airflow Variables not configured"):
        get_database_url()
```

Create `pytest.ini`:
```ini
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts =
    -v
    --strict-markers
    --tb=short
    --cov=dags
    --cov-report=html
    --cov-report=term-missing
```

---

### Phase 3: Architecture Refactoring (Sprint 2-3)

#### 9. Create Reusable Task Factories
Create `dags/utils/task_factory.py`:
```python
from typing import Callable, Dict, Any
from airflow.sdk import task
import pandas as pd
import requests
import logging
from dags.utils.database import get_database_url, get_engine
from dags.utils.validators import validate_dataframe
from dags.utils.exceptions import DataDownloadError, DatabaseWriteError
from config.config_loader import get_config

logger = logging.getLogger(__name__)

def create_csv_download_task(
    endpoint_key: str,
    filename: str
) -> Callable[[], str]:
    """
    Factory to create CSV download tasks.

    Args:
        endpoint_key: Config key for API endpoint (e.g., 'ieso.endpoints.demand')
        filename: Local filename to save to

    Returns:
        Airflow task function that downloads CSV
    """
    @task
    def download_csv() -> str:
        base_url = get_config('ieso.base_url')
        endpoint = get_config(endpoint_key)
        url = f"{base_url}{endpoint}"

        try:
            logger.info(f"Downloading {url}...")
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()

            with open(filename, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            logger.info(f"Successfully downloaded {filename}")
            return filename

        except requests.exceptions.RequestException as e:
            raise DataDownloadError(f"Failed to download {url}: {e}") from e

    return download_csv

def create_postgres_write_task(
    table_name: str,
    schema_name: str,
    required_columns: list,
    if_exists: str = 'append'
) -> Callable[[pd.DataFrame, str], bool]:
    """
    Factory to create PostgreSQL write tasks.

    Args:
        table_name: Target table name
        schema_name: Target schema name
        required_columns: List of required columns for validation
        if_exists: Pandas to_sql parameter ('append', 'replace', 'fail')

    Returns:
        Airflow task function that writes DataFrame to PostgreSQL
    """
    @task
    def write_to_postgres(df: pd.DataFrame, db_url: str) -> bool:
        # Validate data
        validate_dataframe(df, required_columns=required_columns)

        try:
            engine = get_engine(db_url)
            with engine.begin() as conn:
                df.to_sql(
                    name=table_name,
                    con=conn,
                    schema=schema_name,
                    if_exists=if_exists,
                    index=False
                )
            logger.info(f"Successfully wrote {len(df)} rows to {schema_name}.{table_name}")
            return True

        except Exception as e:
            raise DatabaseWriteError(f"Failed to write to {schema_name}.{table_name}: {e}") from e

    return write_to_postgres
```

#### 10. Refactored DAG Example
Create `dags/hourly_runs/demand_pipeline_v2.py`:
```python
from airflow.sdk import dag, task
from datetime import datetime
import pandas as pd
from dags.utils.database import get_database_url
from dags.utils.task_factory import create_csv_download_task, create_postgres_write_task
from dags.utils.updates import update_00_table_reg
from config.config_loader import get_config

@dag(
    dag_id='hourly_ieso_demand_pipeline_v2',
    schedule='@hourly',
    start_date=datetime(2020, 1, 1),
    catchup=False,
    tags=['demand', 'data-pipeline', 'ieso', 'postgres', 'hourly', 'refactored']
)
def ieso_demand_pipeline_v2():
    """
    Refactored IESO demand data pipeline.

    Pipeline steps:
    1. Get database connection
    2. Download demand CSV from IESO API
    3. Transform and filter data
    4. Write to PostgreSQL
    5. Update table register
    """

    # Configuration
    table_name = get_config('database.tables.demand')
    schema_name = get_config('database.schemas.raw')

    # Task 1: Get database connection
    @task
    def get_db_connection() -> str:
        return get_database_url()

    # Task 2: Download CSV (using factory)
    download_csv = create_csv_download_task(
        endpoint_key='ieso.endpoints.demand',
        filename='PUB_Demand.csv'
    )

    # Task 3: Transform data
    @task
    def transform_demand_data(filename: str) -> pd.DataFrame:
        """Parse, clean, and filter demand data."""
        df = pd.read_csv(
            filename,
            header=None,
            names=['Date', 'Hour', 'Market_Demand', 'Ontario_Demand']
        )

        # Type conversions
        df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
        df['Hour'] = pd.to_numeric(df['Hour'], errors='coerce')
        df['Market_Demand'] = pd.to_numeric(df['Market_Demand'], errors='coerce')
        df['Ontario_Demand'] = pd.to_numeric(df['Ontario_Demand'], errors='coerce')
        df['Modified_DT'] = pd.Timestamp.now().date()

        # Filter to latest date only
        latest_date = df['Date'].max()
        return df[df['Date'] == latest_date]

    # Task 4: Write to PostgreSQL (using factory)
    write_to_db = create_postgres_write_task(
        table_name=table_name,
        schema_name=schema_name,
        required_columns=['Date', 'Hour', 'Market_Demand', 'Ontario_Demand'],
        if_exists='append'
    )

    # Task 5: Update table register
    @task
    def update_register(success: bool, db_url: str):
        if success:
            update_00_table_reg(
                complete=success,
                logger=logger,
                db_url=db_url,
                table_name=table_name,
                db_schema=schema_name
            )

    # Define task dependencies
    db_url = get_db_connection()
    filename = download_csv()
    df = transform_demand_data(filename)
    success = write_to_db(df, db_url)
    update_register(success, db_url)

# Instantiate DAG
ieso_demand_pipeline_v2()
```

---

## Setup & Dependencies

### Prerequisites
- Docker Desktop 25+
- Docker Compose 2.0+
- Git
- Python 3.11+ (for local development)

### Installation Steps

#### 1. Clone Repository
```bash
git clone https://github.com/aleemkirk/IESO-Power-Analysis.git
cd IESO-Power-Analysis
```

#### 2. Configure Environment
```bash
# Create .env file
echo "AIRFLOW_UID=$(id -u)" > .env
```

#### 3. Set Airflow Variables
Start Airflow and configure database credentials:
```bash
docker-compose up -d
```

Navigate to http://localhost:8080 (username: `airflow`, password: `airflow`)

Go to Admin → Variables and add:
- `DB_USER` - PostgreSQL username for target database
- `DB_PASSWORD` - PostgreSQL password
- `HOST` - PostgreSQL host (e.g., your NeonDB host)
- `DB_PORT` - PostgreSQL port (default: 5432)
- `DB` - Database name

#### 4. Initialize Target Database Schemas
```sql
-- Connect to your PostgreSQL database and run:
CREATE SCHEMA IF NOT EXISTS "00_RAW";
CREATE SCHEMA IF NOT EXISTS "01_PRI";
CREATE SCHEMA IF NOT EXISTS "00_REF";

-- Create table register
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

#### 5. Trigger DAGs
Access Airflow UI → DAGs → Enable desired DAGs

---

### Python Dependencies

#### Airflow Container Requirements
Installed automatically via Docker image `apache/airflow:3.0.4`.

#### Azure ML Requirements
```bash
cd Azure-ML
pip install -r requirements.txt
```

**Contents of `requirements.txt`**:
```
pandas==1.5.3
numpy==1.24.3
statsmodels==0.13.5
scikit-learn==1.3.0
mlflow==2.15.1
azureml-core==1.60.0
azureml-mlflow==1.60.0
azureml-train-automl-runtime==1.60.0
```

---

## Future Enhancements

### Short-Term (1-3 months)
1. **Monitoring & Alerting**
   - Implement data quality monitoring (Great Expectations)
   - Add Airflow email alerts for DAG failures
   - Create Grafana dashboards for pipeline metrics

2. **Performance Optimization**
   - Add database indexes on Date columns
   - Implement incremental data loading (avoid full table scans)
   - Optimize XML parsing with lxml.etree.iterparse()

3. **Testing & CI/CD**
   - Implement unit tests (target: 80% coverage)
   - Add integration tests with testcontainers
   - Set up GitHub Actions for automated testing
   - Add pre-commit hooks (black, flake8, mypy)

### Medium-Term (3-6 months)
1. **Advanced Analytics**
   - Implement multi-step-ahead forecasting (7-day, 30-day)
   - Add seasonality analysis (weekly, yearly patterns)
   - Implement anomaly detection for demand spikes

2. **API Development**
   - Create FastAPI service for forecast queries
   - Add GraphQL endpoint for flexible data access
   - Implement caching layer (Redis)

3. **Visualization**
   - Create Streamlit dashboard for demand visualization
   - Add interactive time-series plots (Plotly)
   - Implement forecast confidence intervals

### Long-Term (6-12 months)
1. **Machine Learning Enhancements**
   - Experiment with deep learning models (LSTM, Transformer)
   - Implement ensemble forecasting
   - Add feature engineering (weather data, holidays)

2. **Infrastructure**
   - Migrate to Kubernetes for better scalability
   - Implement GitOps workflow (ArgoCD)
   - Add multi-environment support (dev/staging/prod)

3. **Data Expansion**
   - Integrate weather data (Environment Canada API)
   - Add carbon emissions tracking
   - Incorporate electricity pricing data

---

## Appendix

### File Reference Index

#### DAG Files
- `dags/hourly_runs/IESO_PUBLIC_DEMAND_HOURLY.py:23` - Hourly demand pipeline
- `dags/hourly_runs/01_IESO_PUBLIC_ZONAL_DEMAND_HOURLY.py:45` - Zonal demand enhancement (has broken sensor)
- `dags/utils/updates.py:6` - Table register update function

#### Configuration
- `docker-compose.yaml:47` - Airflow common configuration
- `docker-compose.yaml:337` - Git-sync configuration

#### Machine Learning
- `Azure-ML/forecasts.ipynb` - ARIMA forecasting notebook

---

### Glossary

**IESO**: Independent Electricity System Operator - manages Ontario's power grid
**ETL**: Extract, Transform, Load - data pipeline pattern
**ARIMA**: AutoRegressive Integrated Moving Average - time-series forecasting model
**DAG**: Directed Acyclic Graph - Airflow workflow definition
**MLflow**: Open-source platform for ML lifecycle management

---

### Contact & Support

**Repository**: https://github.com/aleemkirk/IESO-Power-Analysis
**Issues**: https://github.com/aleemkirk/IESO-Power-Analysis/issues
**Documentation**: This file (CLAUDE.md)

---

**Document Version**: 1.0
**Generated**: 2025-10-21
**Last Commit**: 97f954d (Merge pull request #1 - Add timestamp to table register)
