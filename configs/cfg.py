"""
Central configuration file for yt_extract project.
All project variables, paths, and settings are defined here.
"""

import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Final

# ============================================================================
# PROJECT PATHS
# ============================================================================

BASE_DIR: Final[Path] = Path(__file__).resolve().parents[1]
CONFIG_DIR: Final[Path] = BASE_DIR / "configs"
DATA_DIR: Final[Path] = BASE_DIR / "data"
DAGS_DIR: Final[Path] = BASE_DIR / "dags"
PROJECT_DIR: Final[Path] = BASE_DIR / "project"
PROMPTS_DIR: Final[Path] = BASE_DIR / "prompts"
TEMPLATES_DIR: Final[Path] = BASE_DIR / "templates"
AIRFLOW_DIR: Final[Path] = BASE_DIR / "airflow"

# Ensure data directory exists
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================

DUCKDB_PATH: Final[Path] = DATA_DIR / "yt_trending.duckdb"
AIRFLOW_DB_PATH: Final[Path] = AIRFLOW_DIR / "airflow.db"

# ============================================================================
# SNAPSHOT AND FILE CONFIGURATION
# ============================================================================

# Snapshot-related settings
SNAPSHOT_PREFIX: Final[str] = "output_"
SNAPSHOT_FORMAT: Final[str] = "png"

# Chrome/Selenium settings for screenshots
CHROME_WINDOW_WIDTH: Final[int] = 1920
CHROME_WINDOW_HEIGHT: Final[int] = 1080
MAX_VIEWPORT_SCROLLS: Final[int] = 3
VIEWPORT_SCROLL_DELAY: Final[float] = 3.0

# Screenshot capture settings
MAX_CAPTURE_HEIGHT_MULTIPLIER: Final[int] = 3  # Multiplier for viewport height
SELENIUM_WAIT_TIMEOUT: Final[int] = 20  # seconds

# Image processing settings
IMAGE_MAX_PIXELS: Final[int] = None  # None means unlimited
OCR_MAX_HEIGHT: Final[int] = 8000  # Max height for OCR image processing
OCR_MIN_UPSCALE_THRESHOLD: Final[float] = 0.8  # Scale factor for downscaling

# ============================================================================
# YOUTUBE/DATA EXTRACTION CONFIGURATION
# ============================================================================

# YouTube Trending endpoint
TARGET_URL: Final[str] = "https://www.youtube.com/results?search_query=trending"

# ============================================================================
# PROMPT AND LLM CONFIGURATION
# ============================================================================

YT_EXTRACT_PROMPT_PATH: Final[Path] = PROMPTS_DIR / "yt_extract_prompt.md"

# LLM settings
LLM_MODEL: Final[str] = "gpt-4.1-mini"
LLM_TEMPERATURE: Final[float] = 0.0

# ============================================================================
# AIRFLOW CONFIGURATION
# ============================================================================

# DAG settings
DAG_ID: Final[str] = "yt_extract_trending_dag"
DAG_START_DATE: Final[datetime] = datetime(2025, 1, 1)
DAG_SCHEDULE_INTERVAL: Final[timedelta] = timedelta(hours=2)
DAG_CATCHUP: Final[bool] = False
DAG_TAGS: Final[list[str]] = ["youtube", "trending"]

# Data retention policy
RETENTION_DAYS: Final[int] = 30

# Airflow admin credentials
AIRFLOW_ADMIN_USER: Final[str] = "admin"
AIRFLOW_ADMIN_PASSWORD: Final[str] = "admin"

# ============================================================================
# API AND ENVIRONMENT CONFIGURATION
# ============================================================================

# OpenAI API Key (loaded from environment)
OPENAI_API_KEY: Final[str | None] = os.getenv("OPENAI_API_KEY")

# ============================================================================
# DATABASE TABLE SCHEMAS
# ============================================================================

# Trending snapshots table columns
TRENDING_SNAPSHOTS_COLUMNS: Final[dict[str, str]] = {
    "snapshot_timestamp": "TIMESTAMPTZ",
    "file_mtime": "TIMESTAMPTZ",
    "file_path": "TEXT",
    "file_size_bytes": "BIGINT",
}

# Trending videos table columns
TRENDING_VIDEOS_COLUMNS: Final[dict[str, str]] = {
    "uuid": "TEXT",
    "rec_insert_ts": "TIMESTAMPTZ",
    "extraction_date": "DATE",
    "raw_variant": "JSON",
    "video_title": "TEXT",
    "video_url": "TEXT",
    "video_id": "TEXT",
    "channel_name": "TEXT",
    "channel_url": "TEXT",
    "views_text": "TEXT",
    "views_count": "BIGINT",
    "upload_time_text": "TEXT",
    "upload_type": "TEXT",
    "duration": "TEXT",
    "category_section": "TEXT",
    "description_full": "TEXT",
    "hashtags": "JSON",
    "external_links": "JSON",
    "mentioned_handles": "JSON",
    "sponsor_mentions": "JSON",
    "language": "TEXT",
    "repeated_listing": "BOOLEAN",
    "additional_metadata": "TEXT",
}

# ============================================================================
# QUERY LIMITS AND DEFAULTS
# ============================================================================

DEFAULT_QUERY_LIMIT: Final[int] = 20
DEFAULT_TOP_CHANNELS_LIMIT: Final[int] = 10
COLUMN_WIDTH_MAX: Final[int] = 50

# ============================================================================
# STREAMLIT UI CONFIGURATION
# ============================================================================

STREAMLIT_PAGE_TITLE: Final[str] = "YouTube Trending Snapshots"
STREAMLIT_LAYOUT: Final[str] = "wide"

# Default SQL query for UI
DEFAULT_SQL_QUERY: Final[str] = """SELECT table_catalog, table_schema, table_name
FROM information_schema.tables
ORDER BY table_catalog, table_schema, table_name;"""

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_snapshot_output_path(timestamp: datetime | None = None) -> Path:
    """
    Generate a snapshot output file path.
    
    Args:
        timestamp: Optional timestamp for naming. If None, uses current time.
    
    Returns:
        Path object for the snapshot file.
    """
    if timestamp is None:
        timestamp = datetime.now()
    filename = f"{SNAPSHOT_PREFIX}{timestamp.isoformat()}.{SNAPSHOT_FORMAT}"
    return DATA_DIR / filename


def get_project_data_path(filename: str) -> Path:
    """
    Get a path within the project data directory.
    
    Args:
        filename: Name of the file.
    
    Returns:
        Full path to the file in the data directory.
    """
    return DATA_DIR / filename


def validate_environment() -> dict[str, bool]:
    """
    Validate that all required environment variables and paths exist.
    
    Returns:
        Dictionary with validation results.
    """
    validations = {
        "base_dir_exists": BASE_DIR.exists(),
        "data_dir_exists": DATA_DIR.exists(),
        "prompts_dir_exists": PROMPTS_DIR.exists(),
        "prompt_file_exists": YT_EXTRACT_PROMPT_PATH.exists(),
        "openai_key_set": OPENAI_API_KEY is not None,
    }
    return validations

