astro-loader
============

Overview
--------

This project implements a Python-based **Data Ingestion Subsystem** designed to reliably ingest structured data from CSV and JSON files into PostgreSQL. The system validates, cleans, deduplicates, and loads data into staging tables while capturing invalid records for review.

The design prioritizes **clarity, correctness, and extensibility** making it suitable as a foundational ingestion layer for analytics or downstream processing.

Architecture & Data Flow
------------------------

### High-Level Data Flow
```
Source File (CSV / JSON)
 │
 ▼
Source Reader
 │
 ▼
Schema Validation
 │
 ├──► Rejects (invalid records + reason)
 │
 ▼
Data Cleaning & Standardization
 │
 ▼
Deduplication
 │
 ▼
PostgreSQL Staging Table (stg_<dataset>)
```
### Flow Explanation

1. **Source Reading**: Input files (CSV or JSON) are read using source-specific readers that stream records to avoid loading entire files into memory.
2. Records that fail validation are **not dropped silently**; they are written to a dedicated reject table with detailed failure reasons.
    * A structural schema (required fields, types)  
    * Business rules (domain-specific correctness)
3. **Cleaning & Transformation**: Valid records are standardized (e.g., date formats, casing, default values) to ensure consistency for downstream analytics.
4. **Deduplication**: Duplicate records are removed using a deterministic strategy (natural key or record hash) to support idempotent ingestion.
5. **Loading**: Clean, deduplicated records are loaded into PostgreSQL staging tables. Invalid records are loaded into a reject table for auditing and debugging.

Why This Structure
------------------

This project is intentionally modular:

* **Separation of concerns** ensures each layer (reading, validation, transformation, loading) can evolve independently.  
* **Explicit reject handling** mirrors real-world data engineering systems, where bad data is expected and must be explainable. 
* **Abstracted readers and validators** make it easy to add new data sources or datasets without refactoring core logic.

The result is a system that is easy to reason about, test, and extend.

Project Structure
-----------------
```
data_ingestion/

├── ingest.py

├── config/

│   ├── base.yaml

│   ├── local.yaml

│   ├── dev.yaml

├── src/

│   ├── readers/

│   ├── validation/

│   ├── transform/

│   ├── dedup/

│   ├── db/

│   ├── logging/

│   └── models/

├── tests/

│   ├── unit/

│   └── integration/

├── .env.example

├── requirements.txt

└── README.md
```

Setup
-----

### Prerequisites

* Python 3.9+
* PostgreSQL (AWS)
* pip / virtualenv

### Environment Variables

Create a .env file in root folder:

```
DB_HOST=localhost  
DB_PORT=5432  
DB_NAME=ingestion  
DB_USER=postgres  
DB_PASSWORD=your_password  
ENV=local
```

An example is provided in .env.example.

Configuration
-------------

Configuration is environment-aware and layered:

* config/base.yaml – shared defaults
* config/local.yaml – local overrides
* config/dev.yaml – development overrides

**Precedence order:**

` Environment Variables > Environment YAML > Base YAML `


How to Run
----------

### Install Dependencies
` pip install -r requirements.txt `

### Run Ingestion
```
python ingest.py 
  --source meteorites
  --file data/meteorite_landings.csv
  --env local
```
The command will:

* Ingest the file
* Load valid records into stg\_meteorites
* Load invalid records into stg\_rejects
* Emit logs and a run summary

Error Handling Strategy
-----------------------

* **Data errors** (missing fields, invalid values)→ Captured and written to stg\_rejects, ingestion continues. 
* **Source or configuration errors** (missing file, invalid config)→ Fail fast, no partial loads.
* **Database errors**→ Transaction rollback, ingestion fails explicitly.

This approach ensures data quality issues do not block ingestion while system failures remain visible and actionable.

Design Decisions & Trade-offs
-----------------------------

### Decisions

* Use **PostgreSQL** as a realistic analytical staging layer. 
* Use **row-level rejects** instead of failing entire batches. 
* Favor **readability and correctness** over maximum throughput. 

### Trade-offs

* Row-by-row inserts are slower than bulk loading but simpler and clearer.
* Local file ingestion is supported before cloud storage to reduce complexity. 
* Schema validation is strict, which may reject borderline-but-usable data. 

These trade-offs are intentional given the project scope.

Scaling Considerations
------------------------------------------------

This system is designed for small to medium batch ingestion. If ingestion volume increased to **~100GB/day**, the following changes would be made:

### What Would Change at 100GB/day?

* **Chunked Reads**: Process data in fixed-size chunks to control memory usage.
* **Bulk Loads (COPY)**: Replace row-level INSERT statements with PostgreSQL COPY for higher throughput.
* **S3 as a Source**: Read files directly from S3 instead of local disk.
* **Workflow Orchestration**: Schedule and monitor ingestion using Airflow or a similar orchestrator.

Importantly, the current architecture already supports these changes without major refactoring.

Testing
-------

* **Unit tests** validate:
  * Schema rules
  * Business rules
  * Deduplication logic
* **Integration tests** validate:
  * PostgreSQL loading
  * End-to-end ingestion behavior

Test coverage focuses on logic rather than boilerplate.

Future Improvements
-------------------

* Add support for Parquet and API-based sources
* Introduce batch metadata tables
* Implement data quality metrics and profiling
* Add Docker-based local development environment
* Integrate with orchestration (Airflow) and monitoring

Final Notes
-----------

This project represents a **baseline ingestion layer** designed with production realities in mind. It intentionally avoids unnecessary complexity while establishing patterns that scale with data volume, team size, and system maturity.
