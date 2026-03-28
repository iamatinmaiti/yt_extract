"""
yt_extract

Headless Chromium + Playwright script to capture the YouTube Trending page
data. This is the first building block
for a broader YouTube data extraction and analysis pipeline.
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Final
import uuid
import numpy as np
from playwright.sync_api import sync_playwright, ViewportSize

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# YouTube Trending search results endpoint to snapshots.
TARGET_URL: Final[str] = "https://www.youtube.com/results?search_query=trending"
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "datalake"
WAREHOUSE_DIR = BASE_DIR / "warehouse"


def create_trending_snapshot(target_url: str = TARGET_URL) -> str:
    """
    Create snapshot of the YouTube Trending page
    Args:
        target_url: str
    Returns: str

    """


    logger.info("Starting trending snapshot extraction")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    output_file = DATA_DIR / f"output_{datetime.now().isoformat()}.json"
    logger.info(f"Output file will be: {output_file}")

    with sync_playwright() as p:
        logger.info("Launching browser")
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport=ViewportSize(width=1920, height=1080))

        try:
            # Navigate to the target page.
            logger.info(f"Navigating to {target_url}")
            page.goto(target_url, wait_until="networkidle")

            # Wait for at least one video tile to appear.
            logger.info("Waiting for video elements to load")
            page.wait_for_selector("ytd-video-renderer", timeout=20000)
            # Give thumbnails/styles a brief extra moment to settle.
            page.wait_for_timeout(np.random.randint(1000, 3000))

            # Perform a limited number of viewport-height scrolls.
            viewport_height = page.viewport_size['height']
            logger.info(f"Starting scrolling with viewport height: {viewport_height}")

            max_viewport_scrolls = 3
            for i in range(max_viewport_scrolls):
                page.evaluate("(height) => window.scrollBy(0, height)", viewport_height)
                page.wait_for_timeout(3000)
                logger.info(f"Completed scroll {i+1}/{max_viewport_scrolls}")

            # Extract video data using Playwright locators
            logger.info("Extracting video data")
            video_locators = page.locator("ytd-video-renderer").all()
            logger.info(f"Found {len(video_locators)} video elements")
            data = []
            for idx, video in enumerate(video_locators):
                title_elem = video.locator("#video-title")
                title = title_elem.text_content().strip() if title_elem.count() > 0 else ''
                video_url = title_elem.get_attribute("href") if title_elem.count() > 0 else ''
                channel_elem = video.locator("a.yt-simple-endpoint.style-scope.yt-formatted-string").first
                channel = channel_elem.text_content().strip() if channel_elem.count() > 0 else ''
                channel_url = channel_elem.get_attribute("href") if channel_elem.count() > 0 else ''
                thumbnail_elem = video.locator("img").first
                thumbnail_url = thumbnail_elem.get_attribute("src") if thumbnail_elem.count() > 0 else ''
                duration_elem = video.locator("ytd-thumbnail-overlay-time-status-renderer span").first
                duration = duration_elem.text_content().strip() if duration_elem.count() > 0 else ''
                meta_spans = video.locator("span.style-scope.ytd-video-meta-block").all()
                views = meta_spans[0].text_content().strip() if len(meta_spans) > 0 else ''
                upload_time = meta_spans[1].text_content().strip() if len(meta_spans) > 1 else ''
                data.append({
                    'title': title,
                    'channel': channel,
                    'views': views,
                    'upload_time': upload_time,
                    'video_url': video_url,
                    'channel_url': channel_url,
                    'thumbnail_url': thumbnail_url,
                    'duration': duration
                })
                if (idx + 1) % 10 == 0:
                    logger.info(f"Extracted data for {idx + 1} videos")

            logger.info(f"Extracted data for {len(data)} videos total")

            # Save the data to JSON
            logger.info("Saving data to JSON file")
            with open(output_file, 'w') as f:
                json.dump(data, f, indent=4)
            logger.info("Data saved successfully")
        except Exception as e:
            logger.error(f"An error occurred during extraction: {e}")
            raise
        finally:
            logger.info("Closing browser")
            browser.close()

    logger.info("Trending snapshot extraction completed")
    return str(output_file)


def store_snapshot_metadata(snapshot_path: str) -> None:
    """
    Persist metadata about the generated snapshot file into DuckDB.

    Args:
        snapshot_path: Path to the snapshot file
        
    Data model (table: trending_snapshots):
      - snapshot_timestamp (TIMESTAMP): when this pipeline run stored the record
      - file_mtime        (TIMESTAMP): filesystem modified time of the snapshot
      - file_path         (TEXT): absolute path to the snapshot file
      - file_size_bytes   (BIGINT): size of the snapshot in bytes
    """
    import duckdb

    snapshot_path_obj = Path(snapshot_path)
    if not snapshot_path_obj.is_absolute():
        snapshot_path_obj = (BASE_DIR / snapshot_path_obj).resolve()

    if not snapshot_path_obj.exists():
        raise FileNotFoundError(f"Generated snapshot not found at {snapshot_path_obj}")

    file_stats = snapshot_path_obj.stat()
    snapshot_timestamp = datetime.now(timezone.utc)
    file_mtime = datetime.fromtimestamp(file_stats.st_mtime, tz=timezone.utc)
    file_size_bytes = file_stats.st_size

    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
    duckdb_path = WAREHOUSE_DIR / "yt_trending.duckdb"

    conn = duckdb.connect(str(duckdb_path))
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
        logger.info(f"Stored metadata for snapshot: {snapshot_path_obj}")
    finally:
        conn.close()


def load_extracted_data_from_datalake() -> None:
    """
    Load previously extracted YouTube trending data from JSON files in the datalake
    and store into DuckDB.
    
    This function looks for the most recent output_*.json file in the datalake
    directory and loads its contents into the trending_videos table.
    
    Expected JSON structure (array of objects):
    [
        {
            "title": str,
            "channel": str,
            "views": str (e.g., "12M views"),
            "upload_time": str,
            "video_url": str,
            "channel_url": str,
            "thumbnail_url": str,
            "duration": str,
            ... (optional additional fields)
        }
    ]
    """
    import duckdb
    
    # Find the most recent output_*.json file
    json_files = sorted(DATA_DIR.glob("output_*.json"), reverse=True)
    
    if not json_files:
        logger.info("No extracted data files found in datalake. Skipping data load.")
        return
    
    latest_json_file = json_files[0]
    logger.info(f"Loading extracted data from: {latest_json_file}")
    
    # Read the JSON file
    with open(latest_json_file, "r", encoding="utf-8") as f:
        videos = json.load(f)
    
    if not isinstance(videos, list):
        raise ValueError(f"Expected JSON array in {latest_json_file}, got {type(videos)}")
    
    rec_insert_ts = datetime.now(timezone.utc)
    extraction_date = datetime.now(timezone.utc).date()
    
    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
    duckdb_path = WAREHOUSE_DIR / "yt_trending.duckdb"

    conn = duckdb.connect(str(duckdb_path))
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trending_videos (
                uuid TEXT PRIMARY KEY,
                rec_insert_ts TIMESTAMPTZ,
                extraction_date DATE,
                video_title TEXT,
                video_url TEXT,
                video_id TEXT,
                channel_name TEXT,
                channel_url TEXT,
                views_text TEXT,
                upload_time_text TEXT,
                duration TEXT,
                thumbnail_url TEXT,
                raw_variant JSON
            )
            """)

        insert_sql = """
            INSERT INTO trending_videos (
                uuid,
                rec_insert_ts,
                extraction_date,
                video_title,
                video_url,
                video_id,
                channel_name,
                channel_url,
                views_text,
                upload_time_text,
                duration,
                thumbnail_url,
                raw_variant
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        for video in videos:
            row_uuid = str(uuid.uuid4())
            
            # Extract video_id from video_url (e.g., "/shorts/2sokLXhqRFQ" -> "2sokLXhqRFQ")
            video_id = None
            video_url = video.get("video_url", "")
            if video_url:
                video_id = video_url.split("/")[-1].split("?")[0]
            
            conn.execute(
                insert_sql,
                [
                    row_uuid,
                    rec_insert_ts,
                    extraction_date,
                    video.get("title"),
                    video.get("video_url"),
                    video_id,
                    video.get("channel"),
                    video.get("channel_url"),
                    video.get("views"),
                    video.get("upload_time"),
                    video.get("duration"),
                    video.get("thumbnail_url"),
                    json.dumps(video),
                ],
            )
        
        logger.info(f"Successfully loaded {len(videos)} videos into DuckDB")
    finally:
        conn.close()


def purge_old_files(retention_days: int = 30) -> None:
    """
    Remove snapshot files and DuckDB records older than retention_days.
    
    Args:
        retention_days: Number of days to retain files (default: 30)
    """
    import duckdb

    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)

    # Delete old snapshot JSON files from datalake.
    for path in DATA_DIR.glob("output_*.json"):
        if not path.is_file():
            continue
        file_mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        if file_mtime < cutoff:
            path.unlink()
            logger.info(f"Deleted old snapshot: {path}")

    # Delete old metadata rows from DuckDB.
    duckdb_path = WAREHOUSE_DIR / "yt_trending.duckdb"
    
    if duckdb_path.exists():
        conn = duckdb.connect(str(duckdb_path))
        try:
            conn.execute(
                """
                DELETE FROM trending_snapshots
                WHERE snapshot_timestamp < ?
                """,
                [cutoff],
            )
            logger.info("Purged old snapshot metadata from DuckDB")
        finally:
            conn.close()
