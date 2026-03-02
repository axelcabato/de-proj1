# News ETL Pipeline with Apache Airflow

A production-style data engineering pipeline that extracts news articles from an external API, transforms the data to handle inconsistencies, and loads it into a PostgreSQL database. The pipeline is orchestrated by Apache Airflow running in Docker containers, demonstrating industry-standard workflow automation practices.

**Note:** This is my first data engineering project, built to develop hands-on experience with the tools and patterns used in professional data platform environments.

---

## Project Overview

This project implements a complete ETL (Extract, Transform, Load) pipeline with the following capabilities:

- **Automated data ingestion** from NewsData.io API on a configurable schedule
- **Data transformation** including type normalization and schema enforcement
- **Idempotent loading** using PostgreSQL upsert patterns to prevent duplicates
- **Workflow orchestration** through Apache Airflow with full observability
- **Containerized infrastructure** enabling single-command deployment

---

## Architecture

```
┌───────────────────────────────────────────────────────────────────┐
│                       Docker Compose Network                      │
├───────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐         │
│  │   Airflow    │    │   Airflow    │    │  PostgreSQL  │         │
│  │  Scheduler   │───▶│  Webserver   │    │   Database   │         │
│  │              │    │  (Port 8080) │    │ (Port 5432)  │         │
│  └──────┬───────┘    └──────────────┘    └──────▲───────┘         │
│         │                                       │                 │
│         │ Triggers via DockerOperator           │                 │
│         ▼                                       │                 │
│  ┌──────────────┐                               │                 │
│  │   ETL App    │───────────────────────────────┘                 │
│  │  Container   │       Writes transformed data                   │
│  └──────────────┘                                                 │
│         │                                                         │
│         │ Fetches articles                                        │
│         ▼                                                         │
│  ┌──────────────┐                                                 │
│  │ NewsData.io  │                                                 │
│  │     API      │                                                 │
│  └──────────────┘                                                 │
│                                                                   │
└───────────────────────────────────────────────────────────────────┘
```

---

## Technical Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Orchestration | Apache Airflow 2.8.1 | DAG scheduling, task management, monitoring |
| Database | PostgreSQL 14 | Article storage, Airflow metadata |
| Containerization | Docker & Docker Compose | Service isolation, reproducible deployments |
| ETL Application | Python 3.13 | Data extraction, transformation, loading |
| API Client | newsdataapi | News article retrieval |

---

## Key Features

### Idempotent Data Loading
The pipeline uses PostgreSQL's `ON CONFLICT DO UPDATE` clause to ensure that re-running the pipeline with overlapping data updates existing records rather than creating duplicates. This is critical for production pipelines that may need to retry failed runs.

```sql
INSERT INTO articles (id, title, author, body, source, published_at)
VALUES (...)
ON CONFLICT (id) DO UPDATE SET
    title = EXCLUDED.title,
    author = EXCLUDED.author,
    ...
```

### Containerized Task Execution
Airflow triggers the ETL job using `DockerOperator`, which spins up an isolated container for each pipeline run. This pattern ensures consistent execution environments and clean resource management.

### Data Transformation
The pipeline handles real-world data quality issues, including type inconsistencies where the `creator` field may arrive as either a string or a list from the source API.

---

## Project Structure

```
news-etl/
├── dags/
│   └── news_etl_dag.py          # Airflow DAG definition
├── logs/                         # Airflow execution logs
├── plugins/                      # Airflow plugins (extensibility)
├── docker-compose.yml            # Multi-container orchestration
├── Dockerfile                    # ETL application image
├── Dockerfile.airflow            # Custom Airflow image with Docker CLI
├── etl.py                        # Core ETL logic
├── init-db.sql                   # Database initialization
└── requirements.txt              # Python dependencies
```

---

## Getting Started

### Prerequisites
- Docker Desktop installed and running
- A NewsData.io API key (free tier available)

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/news-etl.git
   cd news-etl
   ```

2. Create a `.env` file with your API key:
   ```
   NEWS_API_KEY=your_api_key_here
   AIRFLOW_UID=50000
   ```

3. Build and start all services:
   ```bash
   docker compose up --build
   ```

4. Access the Airflow UI at `http://localhost:8080` (credentials: admin/admin)

5. Build the ETL application image:
   ```bash
   docker compose build etl_app
   ```

6. Enable and trigger the `news_etl_pipeline` DAG from the Airflow interface

---

## Usage

### Manual Pipeline Trigger
Navigate to the Airflow UI, select the `news_etl_pipeline` DAG, and click the play button to trigger an immediate run.

### Scheduled Execution
The pipeline is configured to run daily at midnight UTC. Enable the DAG toggle to activate scheduled execution.

### Verify Data
Connect to PostgreSQL to query ingested articles:
```bash
docker exec -it de_postgres_db psql -U user -d news_db -c "SELECT title, source FROM articles LIMIT 5;"
```

---

## Skills Demonstrated

This project was designed to build proficiency in core data engineering competencies:

- **ETL Development:** Designing extraction, transformation, and loading logic with error handling
- **Workflow Orchestration:** Building and scheduling DAGs in Apache Airflow
- **Containerization:** Multi-service Docker Compose configurations with health checks and dependencies
- **Database Design:** Schema creation, upsert patterns, and idempotent operations
- **Infrastructure as Code:** Reproducible environments through declarative configuration
- **Python Development:** Type hints, context managers, exception handling

---

## Future Enhancements

Planned improvements to further develop this pipeline:

- [ ] Add NLP processing (sentiment analysis, word counts)
- [ ] Implement data validation and quality checks
- [ ] Add structured logging and alerting for failures
- [ ] Build a Streamlit dashboard for data visualization
- [ ] Implement incremental loading based on publication date

---

## License

This project is licensed under the GNU General Public License v2.0. See the [LICENSE](LICENSE) file for details.

---

## Acknowledgments

Built as a learning project to gain practical experience with production data engineering tools and patterns. Feedback and suggestions are welcome.