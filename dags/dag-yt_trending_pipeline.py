"""
yt_trending_pdf_dag

DAG to capture YouTube Trending as an image snapshot, store metadata in DuckDB,
and purge old snapshots.
"""

from __future__ import annotations
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import os
import sys
import uuid
import dotenv
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

dotenv.load_dotenv()
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DUCKDB_PATH = DATA_DIR / "yt_trending.duckdb"
RETENTION_DAYS = 30
PROMPTS_DIR = BASE_DIR / "prompts"
YT_EXTRACT_PROMPT_PATH = PROMPTS_DIR / "yt_extract_prompt.md"


def create_trending_snapshot(**_: dict) -> str:
    """
    Capture the YouTube Trending page as a PNG screenshot using headless Chrome.

    Returns:
        The path to the generated image file.
    """
    from project.tasks import create_trending_snapshot

    return create_trending_snapshot()


def store_snapshot_metadata(ti, **_: dict) -> None:
    """
    Persist metadata about the generated snapshot file into DuckDB.

    Data model (table: trending_snapshots):
      - snapshot_timestamp (TIMESTAMPTZ): when this pipeline run stored the record
      - file_mtime        (TIMESTAMPTZ): filesystem modified time of the snapshot
      - file_path         (TEXT): absolute path to the snapshot file
      - file_size_bytes   (BIGINT): size of the snapshot in bytes
    """
    import duckdb

    # Pull the snapshot path from the current task id used in the DAG.
    snapshot_path = ti.xcom_pull(task_ids="create_snapshot")
    if not snapshot_path:
        raise ValueError("No snapshot path returned from create_snapshot")

    snapshot_path_obj = Path(snapshot_path)
    if not snapshot_path_obj.is_absolute():
        snapshot_path_obj = (BASE_DIR / snapshot_path_obj).resolve()

    if not snapshot_path_obj.exists():
        raise FileNotFoundError(f"Generated snapshot not found at {snapshot_path_obj}")

    file_stats = snapshot_path_obj.stat()
    snapshot_timestamp = datetime.now(timezone.utc)
    file_mtime = datetime.fromtimestamp(file_stats.st_mtime, tz=timezone.utc)
    file_size_bytes = file_stats.st_size

    conn = duckdb.connect(str(DUCKDB_PATH))
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trending_snapshots (
                snapshot_timestamp TIMESTAMPTZ,
                file_mtime TIMESTAMPTZ,
                file_path TEXT,
                file_size_bytes BIGINT
            )
            """)
        conn.execute(
            """
            INSERT INTO trending_snapshots (
                snapshot_timestamp,
                file_mtime,
                file_path,
                file_size_bytes
            )
            VALUES (?, ?, ?, ?)
            """,
            [snapshot_timestamp, file_mtime, str(snapshot_path_obj), file_size_bytes],
        )
    finally:
        conn.close()


def extract_trending_data_with_llm(ti, **_: dict) -> None:
    """
    Use OCR + an LLM (via LangChain + OpenAI) to extract structured
    video data from the generated PNG snapshot and store a flattened
    view into DuckDB.

    Table: trending_videos
      - uuid              (TEXT)          unique row identifier
      - rec_insert_ts     (TIMESTAMPTZ)   ingestion timestamp
      - extraction_date   (DATE)          extraction_date from the JSON payload
      - raw_variant       (JSON)          full JSON object for the video
      - video_title       (TEXT)
      - video_url         (TEXT)
      - video_id          (TEXT)
      - channel_name      (TEXT)
      - channel_url       (TEXT)
      - views_text        (TEXT)
      - views_count       (BIGINT)
      - upload_time_text  (TEXT)
      - upload_type       (TEXT)
      - duration          (TEXT)
      - category_section  (TEXT)
      - description_full  (TEXT)
      - hashtags          (JSON)
      - external_links    (JSON)
      - mentioned_handles (JSON)
      - sponsor_mentions  (JSON)
      - language          (TEXT)
      - repeated_listing  (BOOLEAN)
      - additional_metadata (TEXT)
    """
    from PIL import Image
    import pytesseract
    from langchain_openai import ChatOpenAI
    from langchain_core.prompts import ChatPromptTemplate

    openai_api_key = os.getenv("OPENAI_API_KEY")
    if not openai_api_key:
        raise RuntimeError(
            "OPENAI_API_KEY environment variable must be set for LLM extraction task."
        )

    # Prefer the current task name, but fall back to legacy ones if needed.
    snapshot_path = (
        ti.xcom_pull(task_ids="create_snapshot")
        or ti.xcom_pull(task_ids="task_create_trending_snapshot")
        or ti.xcom_pull(task_ids="create_trending_pdf")
    )
    if not snapshot_path:
        raise ValueError(
            "No snapshot path returned from upstream task. "
            "Expected XCom from 'task_create_trending_snapshot' or 'create_trending_pdf'."
        )

    snapshot_path_obj = Path(snapshot_path)
    if not snapshot_path_obj.is_absolute():
        snapshot_path_obj = (BASE_DIR / snapshot_path_obj).resolve()

    if not snapshot_path_obj.exists():
        raise FileNotFoundError(f"Generated snapshot not found at {snapshot_path_obj}")

    if not YT_EXTRACT_PROMPT_PATH.exists():
        raise FileNotFoundError(f"Prompt file not found at {YT_EXTRACT_PROMPT_PATH}")

    with YT_EXTRACT_PROMPT_PATH.open("r", encoding="utf-8") as f:
        prompt_instructions = f.read()

    # Allow large images produced by the snapshot step. Pillow has a built‑in
    # safety limit (to prevent decompression bomb attacks) that can be too
    # strict for our legitimate large screenshots, so we disable that check
    # in this controlled pipeline context.
    Image.MAX_IMAGE_PIXELS = None

    # OCR the PNG snapshot to extract text content for the LLM.
    try:
        image = Image.open(snapshot_path_obj)

        # Tesseract struggles with extremely tall images (like long scrolling
        # screenshots). If the height is excessive, downscale proportionally
        # to keep the image within a safe size while preserving readability.
        max_height = 8000
        if image.height > max_height:
            scale = max_height / float(image.height)
            new_width = int(image.width * scale)
            image = image.resize((new_width, max_height), Image.LANCZOS)

        # Use a standard mode for OCR.
        image = image.convert("L")
    except Exception as exc:
        raise RuntimeError(
            f"Failed to open snapshot image at {snapshot_path_obj}"
        ) from exc

    ocr_text = pytesseract.image_to_string(image) or ""

    if not ocr_text.strip():
        raise ValueError(f"No text extracted from image at {snapshot_path_obj}")

    llm = ChatOpenAI(
        model="gpt-4.1-mini",
        temperature=0,
        openai_api_key=openai_api_key,
    )

    # Treat the markdown prompt as a literal string so any `{` / `}` in the
    # JSON example are not interpreted as template variables.
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", "{instructions}"),
            (
                "user",
                "Here is the full text content of the PDF:\n\n{pdf_text}",
            ),
        ]
    )

    chain = prompt | llm
    response = chain.invoke(
        {
            "pdf_text": ocr_text,
            "instructions": prompt_instructions,
        }
    )
    content = response.content if hasattr(response, "content") else str(response)

    try:
        payload = json.loads(content)
    except json.JSONDecodeError as exc:
        raise ValueError(
            "LLM did not return valid JSON as required by the prompt."
        ) from exc

    extraction_date_str = payload.get("extraction_date")
    try:
        extraction_date = (
            datetime.strptime(extraction_date_str, "%Y-%m-%d").date()
            if extraction_date_str
            else None
        )
    except ValueError:
        extraction_date = None

    videos = payload.get("videos") or []
    rec_insert_ts = datetime.now(timezone.utc)

    import duckdb

    conn = duckdb.connect(str(DUCKDB_PATH))
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trending_videos (
                uuid TEXT,
                rec_insert_ts TIMESTAMPTZ,
                extraction_date DATE,
                raw_variant JSON,
                video_title TEXT,
                video_url TEXT,
                video_id TEXT,
                channel_name TEXT,
                channel_url TEXT,
                views_text TEXT,
                views_count BIGINT,
                upload_time_text TEXT,
                upload_type TEXT,
                duration TEXT,
                category_section TEXT,
                description_full TEXT,
                hashtags JSON,
                external_links JSON,
                mentioned_handles JSON,
                sponsor_mentions JSON,
                language TEXT,
                repeated_listing BOOLEAN,
                additional_metadata TEXT
            )
            """)

        insert_sql = """
            INSERT INTO trending_videos (
                uuid,
                rec_insert_ts,
                extraction_date,
                raw_variant,
                video_title,
                video_url,
                video_id,
                channel_name,
                channel_url,
                views_text,
                views_count,
                upload_time_text,
                upload_type,
                duration,
                category_section,
                description_full,
                hashtags,
                external_links,
                mentioned_handles,
                sponsor_mentions,
                language,
                repeated_listing,
                additional_metadata
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        for video in videos:
            row_uuid = str(uuid.uuid4())

            conn.execute(
                insert_sql,
                [
                    row_uuid,
                    rec_insert_ts,
                    extraction_date,
                    json.dumps(video),
                    video.get("video_title"),
                    video.get("video_url"),
                    video.get("video_id"),
                    video.get("channel_name"),
                    video.get("channel_url"),
                    video.get("views_text"),
                    video.get("views_count"),
                    video.get("upload_time_text"),
                    video.get("upload_type"),
                    video.get("duration"),
                    video.get("category_section"),
                    video.get("description_full"),
                    json.dumps(video.get("hashtags")),
                    json.dumps(video.get("external_links")),
                    json.dumps(video.get("mentioned_handles")),
                    json.dumps(video.get("sponsor_mentions")),
                    video.get("language"),
                    bool(video.get("repeated_listing", False)),
                    video.get("additional_metadata"),
                ],
            )
    finally:
        conn.close()


def purge_old_files(**_: dict) -> None:
    """
    Remove snapshot files and DuckDB records older than RETENTION_DAYS.
    """
    import duckdb

    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)

    # Delete old snapshot files from the project root.
    for path in BASE_DIR.glob("output_*.png"):
        if not path.is_file():
            continue
        file_mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        if file_mtime < cutoff:
            path.unlink()

    # Delete old snapshot files from the data directory.
    for path in DATA_DIR.glob("output_*.png"):
        if not path.is_file():
            continue
        file_mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        if file_mtime < cutoff:
            path.unlink()

    # Delete old metadata rows from DuckDB.
    if DUCKDB_PATH.exists():
        conn = duckdb.connect(str(DUCKDB_PATH))
        try:
            conn.execute(
                """
                DELETE FROM trending_snapshots
                WHERE snapshot_timestamp < ?
                """,
                [cutoff],
            )
        finally:
            conn.close()


with DAG(
    dag_id="yt_extract_trending_dag",
    description=(
        "Capture YouTube Trending as an image snapshot, store metadata in DuckDB, "
        "and purge old snapshots."
    ),
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(hours=2),
    catchup=False,
    tags=["youtube", "trending"],
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    create_snapshot = PythonOperator(
        task_id="create_snapshot",
        python_callable=create_trending_snapshot,
        do_xcom_push=True,
    )

    save_metadata = PythonOperator(
        task_id="save_metadata",
        python_callable=store_snapshot_metadata,
    )

    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=extract_trending_data_with_llm,
    )

    purge_old_snapshots = PythonOperator(
        task_id="purge_old_snapshots",
        python_callable=purge_old_files,
    )

    stop = EmptyOperator(
        task_id="stop",
    )

    start >> create_snapshot >> save_metadata >> extract_data >> stop
    start >> purge_old_snapshots >> stop
