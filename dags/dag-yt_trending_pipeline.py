"""
yt_trending_pdf_dag

DAG to capture YouTube Trending as an image snapshot, store metadata in DuckDB,
and purge old snapshots.
"""

from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
import sys
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# Add the yt_extract project to sys.path so we can import project.tasks
YT_EXTRACT_DIR = Path("/Users/atinmaiti/Documents/Github/yt_extract")
if str(YT_EXTRACT_DIR) not in sys.path:
    sys.path.insert(0, str(YT_EXTRACT_DIR))

# Import business logic from tasks module
from project.tasks import (
    create_trending_snapshot,
    store_snapshot_metadata,
    load_extracted_data_from_datalake,
    purge_old_files,
)


def create_snapshot_wrapper(**kwargs: dict) -> str:
    """Wrapper to call create_trending_snapshot from tasks"""
    return create_trending_snapshot()


def store_metadata_wrapper(ti=None, **kwargs) -> None:
    """Wrapper to call store_snapshot_metadata with XCom data"""
    # Get task instance from kwargs for Airflow 2.7+
    if not ti:
        ti = kwargs.get('task_instance')
    
    if not ti:
        raise ValueError("Task instance context not available")
    
    snapshot_path = ti.xcom_pull(task_ids="create_snapshot")
    if not snapshot_path:
        raise ValueError("No snapshot path returned from create_snapshot")
    store_snapshot_metadata(snapshot_path)


def load_data_wrapper(**kwargs: dict) -> None:
    """Wrapper to call load_extracted_data_from_datalake from tasks"""
    load_extracted_data_from_datalake()


def purge_files_wrapper(**kwargs: dict) -> None:
    """Wrapper to call purge_old_files from tasks"""
    purge_old_files(retention_days=30)



with DAG(
    dag_id="yt_extract_trending_dag",
    description=(
        "Capture YouTube Trending as an image snapshot, store metadata in DuckDB, "
        "extract data from datalake, and purge old snapshots."
    ),
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(hours=2),
    catchup=False,
    tags={"youtube", "trending"},
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    create_snapshot = PythonOperator(
        task_id="create_snapshot",
        python_callable=create_snapshot_wrapper,
        do_xcom_push=True,
    )

    save_metadata = PythonOperator(
        task_id="save_metadata",
        python_callable=store_metadata_wrapper,
    )

    load_extracted_data = PythonOperator(
        task_id="load_extracted_data",
        python_callable=load_data_wrapper,
    )

    purge_old_snapshots = PythonOperator(
        task_id="purge_old_snapshots",
        python_callable=purge_files_wrapper,
    )

    stop = EmptyOperator(
        task_id="stop",
    )

    # Main workflow: capture snapshot -> save metadata -> load data from datalake
    start >> create_snapshot >> save_metadata >> load_extracted_data >> stop
    
    # Parallel task: purge old files
    start >> purge_old_snapshots >> stop


