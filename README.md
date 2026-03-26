## Youtube Data Extract Pipeline

Pipeline and tooling for extracting YouTube trending data.  
Current functionality focuses on capturing the YouTube Trending search results as PNG screenshots using headless Chrome via Selenium, storing metadata in DuckDB, and providing a Streamlit UI for querying. Plans to evolve into a richer data pipeline with OCR extraction and Content Idea App.

---

### Problem Statement

**Goal**: Build a Content Idea App that quickly tells you what topics and formats are currently hot in the market, starting from YouTube trending data.

High‑level objectives:

- **Ingest**: Regularly collect YouTube trending (and later search) data.
- **Store**: Keep raw and processed data in a form that is easy to query.
- **Transform**: Create analytics‑ready views (metrics, aggregations, topics).
- **Surface insights**: Provide a UI where creators can:
  - Discover trending topics and channels.
  - Compare formats (shorts vs long‑form, etc.).
  - Explore engagement patterns (views, likes, comments).

---

### Current Components

- `tasks.py` – Python script that:
  - Opens the YouTube Trending search results page in a headless Chrome browser via Selenium.
  - Scrolls through the page to capture multiple viewports.
  - Uses Chrome DevTools to capture the page as a PNG screenshot.
  - Saves the PNG locally with a timestamp in the filename.

- `dags/dag-yt_trending_pipeline.py` – Airflow DAG that orchestrates:
  - Capturing trending snapshots.
  - Storing metadata in DuckDB.
  - Purging old snapshots after retention period.

- `fetch_and_query_duckdb.py` – Script to fetch new snapshots and run queries on the DuckDB database.

- `ui.py` – Streamlit app for querying the DuckDB database with custom SQL.

- `Dockerfile` – Docker image for running Apache Superset for data visualization.

- `start_airflow.sh` – Script to start Airflow standalone.

- `templates/deployment.yaml` – Example Kubernetes manifest for a simple Nginx‑based “Hello World” web app.  
  This is currently a template / example and not yet wired to the project.

- `requirements.txt` – Python dependencies used by the project:
  - `selenium` – Browser automation for headless Chrome.
  - `duckdb` – Embedded database for storing metadata.
  - `streamlit` – UI for querying data.
  - `apache-airflow` – Orchestration for data pipelines.
  - `pytesseract` – (Planned) OCR for extracting text from screenshots.
  - `pypdf` – (Planned) PDF processing.

---

### Getting Started

#### Prerequisites

- Python 3.10+ installed.
- Google Chrome installed (required for Selenium to capture screenshots).
- A Linux environment (this repo is currently developed on Ubuntu).

#### Install Python dependencies

From the project root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

If you prefer `uv`, you can instead run:

```bash
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

#### Install Airflow

To install Airflow with version constraints for compatibility:

```bash
./install_airflow.sh
```

If you encounter an error like "Error while accessing remote requirements file" due to an unsupported Python version (e.g., Python 3.14), edit `install_airflow.sh` and set `PYTHON_VERSION` manually to a supported version such as "3.13".

Supported Python versions for Airflow 3.1.3 are 3.10, 3.11, 3.12, and 3.13.

#### Running the Pipeline

To run a single snapshot capture:

```bash
python tasks.py
```

This will generate a PNG screenshot in the `data/` directory.

To run the Airflow DAG:

```bash
./start_airflow.sh
```

Then access Airflow UI at http://localhost:8080 to trigger the DAG.

#### Running the Streamlit UI

```bash
streamlit run ui.py
```

Access at http://localhost:8501 to query the DuckDB database.

#### Running Superset

Using Docker:

```bash
docker build -t yt-superset .
docker run -p 8088:8088 yt-superset
```

Access Superset at http://localhost:8088 (admin/admin).

---

### Configuring ChromeDriver

Selenium Manager automatically handles ChromeDriver download and matching versions. No manual configuration needed.

---

### Data Storage

Metadata is stored in `data/yt_trending.duckdb` with the following table:

- `trending_snapshots`:
  - `snapshot_timestamp` (TIMESTAMPTZ): When the record was stored.
  - `file_mtime` (TIMESTAMPTZ): File modification time.
  - `file_path` (TEXT): Path to the PNG file.
  - `file_size_bytes` (BIGINT): File size in bytes.

---

### Kubernetes template (optional)

The `templates/deployment.yaml` file is a self‑contained example Kubernetes manifest:

- Creates a `ConfigMap` with an `index.html` “Hello World” page.
- Deploys an `nginx` container that mounts that HTML file.
- Exposes the app via a `NodePort` `Service`.

Usage:

```bash
kubectl apply -f templates/deployment.yaml
```

You can use this as a starting point for deploying the Streamlit UI or Superset.

---

### Roadmap

Planned enhancements for `yt_extract`:

- Integrate OCR (Tesseract) to extract video metadata from PNG screenshots.
- Store structured video data in DuckDB/Postgres.
- Build transformations and metrics for content insights.
- Expand UI with visualizations and content idea suggestions.
- Add more data sources (YouTube API, etc.).

---

### License

Add your preferred license here (e.g., MIT, Apache 2.0).
