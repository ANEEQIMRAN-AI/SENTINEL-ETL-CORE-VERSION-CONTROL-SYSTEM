# SENTINEL-ETL-CORE

SENTINEL-ETL-CORE is a **production-grade Data Engineering framework** designed for real-world reliability. It features dynamic ingestion, expert-level cleaning, strict quality gates with drift detection, and a FastAPI serving layer with Git-like data versioning for traceability and reproducibility.

---

## 🚀 Project Overview

### Production-Grade Universal Data Pipeline
A robust, modular, and extensible data pipeline capable of handling messy, real-world datasets. Implements industry best practices including dynamic schema handling, expert cleaning, and comprehensive logging.

### Features

- **Dynamic Ingestion:** Supports CSV, Excel, and PDF formats.
- **Expert Cleaning:**
  - Automatic column standardization (lowercase, trimmed, snake_case)
  - Whitespace trimming for all values
  - Context-aware deduplication (ignoring surrogate keys like `id`)
  - Missing value imputation (median for numeric, `Unknown` for categorical)
  - Automatic data type inference (datetime, numeric)
- **Schema Validation:** Ensures required columns exist and critical data is present.
- **Robust Storage:** Saves intermediate cleaned data, final output, and stores results in a database.
- **Production-Ready:**
  - YAML-based configuration management
  - Comprehensive logging (file + console)
  - Graceful exception handling
  - Modular architecture for extensibility

---

## 📁 Project Structure

```
project/
  data/
    raw/         # Input messy files
    cleaned/     # Intermediate cleaned files
    processed/   # Final output files
  logs/          # Pipeline execution logs
  config/        # Configuration files (YAML)
  src/           # Source code modules
    ingest.py    # Data ingestion logic
    clean.py     # Expert cleaning rules
    validate.py  # Schema validation
    store.py     # Database and file storage
  run_pipeline.py # Main entry point
```

---

## 🛠️ Setup & Usage

### 1. Install Dependencies

```bash
pip install pandas pyyaml sqlalchemy openpyxl pypdf2
```

### 2. Run the Pipeline

```bash
python run_pipeline.py --input data/raw/sample_messy_data.csv
```

### ⚙️ Configuration

Configuration is managed via `config/config.yaml`:
- **Paths:** Directories for data and logs
- **Cleaning Rules:** Imputation strategies, deduplication toggles, etc.
- **Validation Rules:** Required columns and expected types
- **Database Settings:** Connection details for storage

---

## 🛡️ Data Cleaning Philosophy

- **Maintain Lineage:** Raw data is never modified; transformations are logged
- **Work on Copies:** All operations are performed on dataframe copies
- **Standardize Semantic Meaning:** Map various null representations to a standard format
- **Strict Typing:** Ensure data types are correct for downstream analysis

---

## 🔁 Data Versioning & Rollback System (Task 12)

Provides robust management of multiple dataset versions to ensure reproducibility, traceability, and safe data lifecycle management.

### Features

- **Automated Versioning:** Creates isolated version directories (`v1`, `v2`, ...) for each dataset update
- **Metadata Tracking:** Captures timestamp, source file, row counts, column lists, and quality scores
- **Master Indexing:** Maintains `versions_index.json` to track all versions and the currently active one
- **Version Comparison:** Compares any two versions for row count differences, schema changes, and data type shifts
- **Safe Rollback:** Switch active version to any previous state without deleting data
- **Production Logging:** Comprehensive logging of all versioning operations

### Structure

```
src/create_version.py       # CLI to create new version
src/compare_versions.py    # CLI to compare two versions
src/rollback.py            # CLI to switch active versions
config/versioning_config.yaml # Centralized config
data/versions/             # Storage for versioned datasets and metadata
```

### Usage

1. **Create a New Version**

```bash
python src/create_version.py --input data/raw/sample_messy_data.csv
```

2. **Compare Two Versions**

```bash
python src/compare_versions.py --version1 v1 --version2 v2
```
Results are saved to `version_comparison.json`

3. **Rollback to Previous Version**

```bash
python src/rollback.py --version v1
```

### ⚙️ Configuration

Modify `config/versioning_config.yaml` to adjust:
- Storage paths
- Version naming conventions
- Logging levels and formats
- Max versions to retain

### 🛡️ Safety & Reproducibility

- **Immutability:** Existing versions are never overwritten
- **Traceability:** Every version linked to metadata and quality status
- **Reversibility:** Rollbacks update pointers without data loss

---

## 👨‍💻 Author

**Aneeq Imran**  


