"""
yt_extract

Headless Chromium + Playwright script to capture the YouTube Trending page
data. This is the first building block
for a broader YouTube data extraction and analysis pipeline.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Final
import numpy as np
from playwright.sync_api import sync_playwright, ViewportSize

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# YouTube Trending search results endpoint to snapshots.
TARGET_URL: Final[str] = "https://www.youtube.com/results?search_query=trending"
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "datalake"
DUCKDB_PATH = DATA_DIR / "yt_trending.duckdb"


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
