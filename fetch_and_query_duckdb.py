#!/usr/bin/env python3
"""
Fetch data (optional) and run queries on the yt_trending DuckDB database.

Usage:
  # Query only (uses existing DB):
  python fetch_and_query_duckdb.py
  python fetch_and_query_duckdb.py --query "SELECT * FROM trending_snapshots LIMIT 5"

  # Fetch a new snapshot (PDF + metadata) then query:
  python fetch_and_query_duckdb.py --fetch
"""

from __future__ import annotations
import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DUCKDB_PATH = DATA_DIR / "yt_trending.duckdb"


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def fetch_snapshot() -> str | None:
    """
    Create a new trending snapshot image and store its metadata in DuckDB.
    Returns the image path, or None on failure.
    """
    import duckdb

    try:
        from app import create_trending_pdf
    except ImportError as e:
        print(
            "Fetch requires app.create_trending_pdf (install selenium, run from project root).",
            file=sys.stderr,
        )
        raise SystemExit(1) from e

    ensure_data_dir()
    snapshot_path = create_trending_pdf()
    snapshot_path_obj = Path(snapshot_path)
    if not snapshot_path_obj.is_absolute():
        snapshot_path_obj = (BASE_DIR / snapshot_path_obj).resolve()

    if not snapshot_path_obj.exists():
        print(
            f"Snapshot file not found after create_trending_pdf: {snapshot_path_obj}",
            file=sys.stderr,
        )
        return None

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
                snapshot_timestamp, file_mtime, file_path, file_size_bytes
            )
            VALUES (?, ?, ?, ?)
            """,
            [snapshot_timestamp, file_mtime, str(snapshot_path_obj), file_size_bytes],
        )
    finally:
        conn.close()

    print(f"Snapshot stored: {snapshot_path_obj}")
    return str(snapshot_path_obj)


def run_query(conn, sql: str, params: list | None = None):
    """Execute SQL and return result as list of rows (DuckDB result)."""
    if params:
        return conn.execute(sql, params)
    return conn.execute(sql)


def query_snapshots(conn, limit: int = 20):
    """List recent snapshot metadata."""
    return run_query(
        conn,
        """
        SELECT snapshot_timestamp, file_path, file_size_bytes
        FROM trending_snapshots
        ORDER BY snapshot_timestamp DESC
        LIMIT ?
        """,
        [limit],
    )


def query_videos_summary(conn, limit: int = 20):
    """List recent trending video rows (summary columns)."""
    return run_query(
        conn,
        """
        SELECT extraction_date, video_title, channel_name, views_count, video_url
        FROM trending_videos
        ORDER BY rec_insert_ts DESC
        LIMIT ?
        """,
        [limit],
    )


def query_top_channels(conn, limit: int = 10):
    """Channels with most videos in trending_videos."""
    return run_query(
        conn,
        """
        SELECT channel_name, COUNT(*) AS video_count
        FROM trending_videos
        WHERE channel_name IS NOT NULL AND channel_name != ''
        GROUP BY channel_name
        ORDER BY video_count DESC
        LIMIT ?
        """,
        [limit],
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch data (optional) and run queries on yt_trending DuckDB."
    )
    parser.add_argument(
        "--fetch",
        action="store_true",
        help="Create a new trending snapshot image and store metadata in DuckDB.",
    )
    parser.add_argument(
        "--query",
        type=str,
        metavar="SQL",
        help='Run a custom SQL query (e.g. "SELECT * FROM trending_snapshots LIMIT 5").',
    )
    parser.add_argument(
        "--list-snapshots",
        action="store_true",
        help="List recent snapshots (default when no other query is given).",
    )
    parser.add_argument(
        "--list-videos",
        action="store_true",
        help="List recent trending videos summary.",
    )
    parser.add_argument(
        "--top-channels",
        action="store_true",
        help="Show channels with most videos in trending_videos.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        metavar="N",
        help="Row limit for list-snapshots / list-videos / top-channels (default: 20).",
    )
    args = parser.parse_args()

    if args.fetch:
        fetch_snapshot()

    if not DUCKDB_PATH.exists():
        print(
            f"DuckDB not found at {DUCKDB_PATH}. Run with --fetch first or run the Airflow DAG.",
            file=sys.stderr,
        )
        sys.exit(1)

    import duckdb

    conn = duckdb.connect(str(DUCKDB_PATH), read_only=not args.fetch)

    try:
        if args.query:
            result = run_query(conn, args.query)
            rows = result.fetchall()
            names = [d[0] for d in result.description]
            col_widths = [
                max(len(str(n)), *(len(str(r[i])) for r in rows))
                for i, n in enumerate(names)
            ]
            fmt = "  ".join(f"{{:{w}}}" for w in col_widths)
            print(fmt.format(*names))
            print("-" * (sum(col_widths) + 2 * (len(col_widths) - 1)))
            for row in rows:
                print(fmt.format(*[str(x) for x in row]))
            return

        # Default: list snapshots
        if args.list_videos:
            result = query_videos_summary(conn, limit=args.limit)
        elif args.top_channels:
            result = query_top_channels(conn, limit=args.limit)
        else:
            result = query_snapshots(conn, limit=args.limit)

        rows = result.fetchall()
        names = [d[0] for d in result.description]
        if not rows:
            print("No rows returned.")
            return
        col_widths = [
            max(len(str(names[i])), *(min(50, len(str(r[i]))) for r in rows))
            for i in range(len(names))
        ]
        fmt = "  ".join(f"{{:{w}}}" for w in col_widths)
        print(fmt.format(*names))
        print("-" * (sum(col_widths) + 2 * (len(names) - 1)))
        for row in rows:
            print(fmt.format(*[str(x)[:50] for x in row]))
    finally:
        conn.close()
