"""
yt_extract

Headless Chrome + Selenium script to capture the YouTube Trending page
as a local image file (screenshot). This is the first building block
for a broader YouTube data extraction and analysis pipeline.
"""

import base64
import time
from datetime import datetime
from pathlib import Path
from typing import Final
import numpy as np
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# YouTube Trending search results endpoint to snapshots.
TARGET_URL: Final[str] = "https://www.youtube.com/results?search_query=trending"


def create_trending_snapshot(
    target_url: str = TARGET_URL,
) -> str:
    """
    Capture the YouTube Trending page as a PNG screenshot using headless Chrome.

    Args:
        target_url: The URL to open and capture as an image.

    Returns:
        The path to the generated image file.
    """

    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    output_image = data_dir / f"output_{datetime.now().isoformat()}.png"

    options = Options()
    options.add_argument(
        "--headless=new"
    )  # Required for PDF printing in recent versions
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")

    # Let Selenium Manager resolve and download the correct ChromeDriver
    # version for the locally installed Chrome.
    driver = webdriver.Chrome(options=options)

    try:
        # Navigate to the target page.
        driver.get(target_url)

        # Wait for at least one video tile to appear so we don't
        # capture an empty shell layout.
        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "ytd-video-renderer"))
            )
            # Give thumbnails/styles a brief extra moment to settle.
            time.sleep(np.random.randint(1, 3))
        except TimeoutException:
            # If we never see a video tile, fall back to a short
            # static wait so we still get *something* in the screenshot.
            time.sleep(np.random.randint(1, 3))

        # Perform a limited number of viewport-height scrolls so we only
        # capture roughly the first "three pages" of results. This keeps
        # the resulting screenshot from becoming extremely tall, which
        # causes issues for downstream OCR.
        try:
            viewport_height = driver.execute_script(
                "return window.innerHeight || document.documentElement.clientHeight || 1080;"
            )
        except Exception:
            viewport_height = 1080

        max_viewport_scrolls = 3
        for _ in range(max_viewport_scrolls):
            driver.execute_script("window.scrollBy(0, arguments[0]);", viewport_height)
            time.sleep(3.0)

        # Use Chrome DevTools Protocol to capture a PNG screenshot of the
        # loaded content. We clamp the height to a small multiple of the
        # viewport to avoid generating excessively tall images.
        driver.execute_cdp_cmd("Page.enable", {})
        metrics = driver.execute_cdp_cmd("Page.getLayoutMetrics", {})
        content_size = metrics.get("contentSize") or metrics.get("cssContentSize") or {}
        width = content_size.get("width", 1920)
        height = content_size.get("height", 1080)

        # Limit capture height to ~three viewports worth of content.
        max_capture_height = viewport_height * 3
        if isinstance(height, (int, float)) and height > max_capture_height:
            height = max_capture_height

        screenshot = driver.execute_cdp_cmd(
            "Page.captureScreenshot",
            {
                "format": "png",
                "fromSurface": True,
                "captureBeyondViewport": True,
                "clip": {
                    "x": 0,
                    "y": 0,
                    "width": width,
                    "height": height,
                    "scale": 1,
                },
            },
        )

        with open(output_image, "wb") as f:
            f.write(base64.b64decode(screenshot["data"]))
    finally:
        driver.quit()

    return str(output_image)