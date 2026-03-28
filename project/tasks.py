"""
yt_extract

Headless Chromium + Playwright script to capture the YouTube Trending page
data. This is the first building block
for a broader YouTube data extraction and analysis pipeline.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Final
import numpy as np
from playwright.sync_api import sync_playwright, ViewportSize

# YouTube Trending search results endpoint to snapshots.
TARGET_URL: Final[str] = "https://www.youtube.com/results?search_query=trending"


def create_trending_snapshot(
    target_url: str = TARGET_URL) -> str:
    """
    Extract trending video data from the YouTube Trending page using Playwright.

    Args:
        target_url: The URL to open and extract data from.

    Returns:
        The path to the generated JSON file containing the extracted data.
    """

    data_dir = Path(__file__).parent.parent / "datalake"
    output_file = data_dir / f"yt_extract_{datetime.now().isoformat()}.json"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport=ViewportSize(width=1920, height=1080))

        try:
            # Navigate to the target page.
            page.goto(target_url, wait_until="networkidle")

            # Wait for at least one video tile to appear.
            page.wait_for_selector("ytd-video-renderer", timeout=20000)
            # Give thumbnails/styles a brief extra moment to settle.
            page.wait_for_timeout(np.random.randint(1000, 3000))

            # Perform a limited number of viewport-height scrolls.
            viewport_height = page.viewport_size['height']

            max_viewport_scrolls = 3
            for _ in range(max_viewport_scrolls):
                page.evaluate("(height) => window.scrollBy(0, height)", viewport_height)
                page.wait_for_timeout(3000)

            # Extract video data using Playwright locators
            video_locators = page.locator("ytd-video-renderer").all()
            data = []
            for video in video_locators:
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

            # Save the data to JSON
            with open(output_file, 'w') as f:
                json.dump(data, f, indent=4)
        finally:
            browser.close()

    return str(output_file)
