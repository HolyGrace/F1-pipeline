# Formula 1 Data Pipeline

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Airflow 3.1.2](https://img.shields.io/badge/airflow-3.1.2-red.svg)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/docker-required-blue.svg)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A production-ready ETL pipeline for Formula 1 racing data, implementing the Medallion Architecture (Bronze/Silver/Gold) with incremental processing and full orchestration using Apache Airflow.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Pipeline Stages](#pipeline-stages)
- [Performance Metrics](#performance-metrics)
- [Contributing](#contributing)
- [License](#license)

## Overview

This project processes historical Formula 1 racing data (1950-2024) through a modern data engineering pipeline, transforming raw CSV files into analytics-ready datasets optimized for machine learning and business intelligence.

**Key Highlights:**

- Processes **700,000+ records** across 14 tables
- Achieves **76% storage reduction** using Parquet format
- Implements **incremental processing** to handle only new data
- Fully **orchestrated** with Apache Airflow
- **Dockerized** for easy deployment and reproducibility

## Architecture

The pipeline follows the **Medallion Architecture** pattern:

```txt
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Raw CSV   │────▶│   Bronze    │────▶│   Silver    │────▶│    Gold     │
│  (14 files) │     │  (Parquet)  │     │  (Cleaned)  │     │ (Analytics) │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                           │                    │                    │
                           │                    │                    │
                    ┌──────▼────────────────────▼────────────────────▼──────┐
                    │          Apache Airflow Orchestration                 │
                    │    (Automated scheduling and dependency management)   │
                    └───────────────────────────────────────────────────────┘
```

### Data Flow

1. **Bronze Layer**: Raw data ingestion with minimal transformation
2. **Silver Layer**: Data cleaning, type conversion, and validation
3. **Gold Layer**: Business-ready aggregations and analytics tables
4. **Orchestration**: Airflow manages dependencies and scheduling

## Features

### Data Engineering

- **Medallion Architecture** - Industry-standard 3-layer approach
- **Incremental Processing** - Only processes new data, avoiding full reloads
- **State Management** - Tracks processed years to prevent duplicates
- **Schema Evolution** - Handles mixed data types and format changes
- **Data Quality Validation** - Automated null percentage checks

### Performance

- **76% Storage Reduction** - Parquet compression vs CSV
- **Parallel Processing** - Dimensions and fact tables processed concurrently
- **Optimized Queries** - Columnar format for analytics workloads
- **Efficient Memory Usage** - Polars for high-performance data manipulation

### Orchestration

- **Automated Workflows** - Airflow DAGs with task dependencies
- **Error Handling** - Retry logic and failure notifications
- **Monitoring** - Real-time task status and logs
- **Dockerized** - Consistent environment across systems

## Tech Stack

| Category | Technology | Purpose |
|----------|-----------|---------|
| **Data Processing** | Polars 1.35.1 | High-performance DataFrame library |
| **Storage Format** | Apache Parquet | Columnar storage with compression |
| **Orchestration** | Apache Airflow 3.1.2 | Workflow management |
| **Containerization** | Docker Compose | Service orchestration |
| **Database** | PostgreSQL 16 | Airflow metadata store |
| **Message Queue** | Redis 7.2 | Celery task queue |
| **Language** | Python 3.11 | Core development |

## Prerequisites

Before you begin, ensure you have the following installed:

- **Docker Desktop** (version 20.10+)
  - [Download for Windows](https://docs.docker.com/desktop/install/windows-install/)
  - [Download for Mac](https://docs.docker.com/desktop/install/mac-install/)
  - [Download for Linux](https://docs.docker.com/desktop/install/linux-install/)
- **Git** for version control
- **4GB+ RAM** available for Docker
- **10GB+ disk space** for data and Docker images

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/f1-data-pipeline.git
cd f1-data-pipeline
```

### 2. Download F1 Dataset

Place your F1 CSV files in the `data/raw/` directory:

```txt
data/raw/
├── circuits.csv
├── drivers.csv
├── constructors.csv
├── races.csv
├── results.csv
├── qualifying.csv
├── lap_times.csv
├── pit_stops.csv
├── driver_standings.csv
├── constructor_standings.csv
├── constructor_results.csv
├── sprint_results.csv
├── seasons.csv
└── status.csv
```

**Dataset Source:** [Kaggle - Formula 1 World Championship](https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020)

### 3. Configure Environment

The `.env` file is already configured with defaults:

```bash
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin
_PIP_ADDITIONAL_REQUIREMENTS=polars==1.35.1 pyarrow==22.0.0
```

### 4. Start Airflow with Docker

**Windows (PowerShell):**

```powershell
.\start_airflow.ps1
```

**Linux/Mac:**

```bash
chmod +x start_airflow.sh
./start_airflow.sh
```

**Or manually:**

```bash
docker-compose up -d
```

**Wait 2-3 minutes** for all services to start and dependencies to install.

### 5. Access Airflow UI

Open your browser and navigate to:

```txt
http://localhost:8080
```

**Login credentials:**

- Username: `admin`
- Password: `admin`

## Usage

### Running the Pipeline

#### Option 1: Through Airflow UI (Recommended)

1. Navigate to [](http://localhost:8080)
2. Find the DAG named `f1_etl_pipeline`
3. Toggle the switch to **activate** the DAG
4. Click the **"Play"** button and select **"Trigger DAG"**
5. Monitor progress in the **Graph View**

#### Option 2: Manual Execution

Run individual components locally:

```bash
# Activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run components individually
python src/ingestion.py           # Bronze layer
python src/process_dimensions.py  # Dimension tables
python src/incremental_processing.py initial  # Initial load
python src/incremental_processing.py incremental  # Incremental
python src/analytics.py           # Gold layer
```

### Pipeline Stages Explained

The Airflow DAG executes in this order:

```txt
1. bronze_ingestion
   │
   ├─▶ 2a. dimension_processing (parallel)
   │
   └─▶ 2b. incremental_processing (parallel)
       │
       └─▶ 3. gold_analytics
           │
           └─▶ 4. validate_completion
```

**Execution time:** ~5-10 minutes for initial load, <1 minute for incremental updates

### Stopping the Pipeline

**Windows:**

```powershell
.\stop_airflow.ps1
```

**Linux/Mac/Manual:**

```bash
docker-compose down
```

**Clean everything (including data):**

```bash
docker-compose down -v
```

## Project Structure

```txt
f1-data-pipeline/
│
├── dags/                          # Airflow DAG definitions
│   └── f1_etl_pipeline.py        # Main orchestration DAG
│
├── src/                          # Pipeline source code
│   ├── __init__.py
│   ├── config.py                 # Centralized configuration
│   ├── ingestion.py              # Bronze layer ingestion
│   ├── transformation.py         # Silver layer transformations
│   ├── process_dimensions.py    # Dimension table processor
│   ├── incremental_processing.py # Incremental logic
│   └── analytics.py              # Gold layer analytics
│
├── data/                         # Data storage (gitignored)
│   ├── raw/                      # Original CSV files
│   ├── bronze/                   # Parquet (raw schema)
│   ├── silver/                   # Parquet (cleaned)
│   └── gold/                     # Parquet (aggregated)
│
├── logs/                         # Execution logs
│   └── processing_state.json     # Incremental state tracking
│
├── notebooks/                    # Exploratory analysis
│   └── exploration.ipynb
│
├── docker-compose.yaml           # Docker services definition
├── .env                          # Environment variables
├── requirements.txt              # Python dependencies
├── .gitignore                   # Git ignore patterns
└── README.md                    # This file
```

## Pipeline Stages

### 1. Bronze Layer (Raw Ingestion)

**Purpose:** Convert CSV files to optimized Parquet format with minimal transformation

**Key Operations:**

- Read 14 CSV files (700K+ total rows)
- Apply schema overrides for problematic columns
- Write to Parquet with Snappy compression
- Preserve original data structure

**Output:**

- 14 Parquet files in `data/bronze/`
- 76% average size reduction
- All original data preserved

**Code:** `src/ingestion.py`

### 2. Silver Layer (Data Cleaning)

#### 2a. Dimension Processing

**Purpose:** Process slowly-changing dimension tables

**Tables Processed:**

- circuits (77 rows)
- drivers (861 rows)
- constructors (212 rows)
- seasons (75 rows)
- status (139 rows)

**Transformations:**

- Type casting (altitude, coordinates to float)
- Column renaming for consistency
- No incremental processing needed

**Code:** `src/process_dimensions.py`

#### 2b. Incremental Processing

**Purpose:** Process fact tables incrementally by year

**Tables Processed:**

- races
- results
- qualifying
- lap_times
- pit_stops
- sprint_results
- driver_standings
- constructor_standings
- constructor_results

**Key Features:**

- Tracks processing state in `logs/processing_state.json`
- Initial load: Years 1950-2010 (configurable)
- Incremental: Only new years after initial load
- Automatic append to existing data

**Transformations:**

- Parse time strings to seconds (lap times, pit stops)
- Convert mixed-type columns (points, positions)
- Create calculated fields (DNF flags, positions gained)
- Combine date/time into datetime objects

**Code:** `src/incremental_processing.py`, `src/transformation.py`

### 3. Gold Layer (Analytics)

**Purpose:** Create business-ready aggregated datasets

**Tables Created:**

1. **driver_performance** (3,211 rows)
   - Performance metrics by driver/year
   - Wins, podiums, points, DNF rates
   - Average positions, pole positions

2. **constructor_performance** (1,111 rows)
   - Team performance by year
   - Championship positions
   - Win rates, podium percentages

3. **circuit_analysis** (77 rows)
   - Circuit characteristics
   - Historical statistics
   - DNF rates, fastest lap averages

4. **race_results_enriched** (26,759 rows)
   - Complete fact table with all context
   - Joined driver, constructor, circuit info
   - Ready for ML feature engineering

**Code:** `src/analytics.py`

### 4. Validation

**Purpose:** Ensure pipeline completed successfully

**Checks:**

- Bronze ingestion success rate
- Dimension processing completion
- Incremental processing status
- Gold analytics creation

**Code:** `dags/f1_etl_pipeline.py` (validate_pipeline_completion)

## Performance Metrics

### Storage Efficiency

| Metric | Value |
|--------|-------|
| Original CSV size | 17.55 MB |
| Parquet size | 4.21 MB |
| **Reduction** | **76.0%** |

### Processing Performance

| Stage | Records | Time |
|-------|---------|------|
| Bronze Ingestion | 602,424 | ~10s |
| Silver Transformation | 701,433 | ~15s |
| Gold Analytics | 31,158 | ~5s |
| **Total Pipeline** | **~700K** | **~40s** |

### Data Quality

| Table | Rows | Null % | Status |
|-------|------|--------|--------|
| driver_performance | 3,211 | 0% | ✅ Clean |
| constructor_performance | 1,111 | 0% | ✅ Clean |
| circuit_analysis | 77 | 5% | ⚠️ Historical gaps |
| race_results_enriched | 26,759 | 71% | ⚠️ DNF records |

*Note: High null percentages in race_time are expected (drivers who didn't finish)*

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

### Development Setup

```bash
# Clone and setup
git clone https://github.com/yourusername/f1-data-pipeline.git
cd f1-data-pipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run tests (when implemented)
pytest tests/
```

### Commit Convention

This project follows [Conventional Commits](https://www.conventionalcommits.org/):

```txt
feat: add new feature
fix: bug fix
docs: documentation changes
refactor: code refactoring
test: add tests
chore: maintenance tasks
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- **Dataset:** [Ergast Developer API](http://ergast.com/mrd/) via Kaggle
- **Inspiration:** Modern data engineering best practices
- **Tools:** Apache Airflow, Polars, Docker communities

---

**⭐ If you found this project helpful, please consider giving it a star!**
