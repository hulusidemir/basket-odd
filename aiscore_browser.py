"""Shared Scrapling settings; each workload keeps its own persistent profile."""

import os


def session_options(purpose: str, page_timeout_ms: int, *, max_pages: int = 1) -> dict:
    profiles = {
        "live": ("AISCORE_BROWSER_PROFILE_DIR", "scrapling-profile"),
        "finished": ("AISCORE_FINISHED_BROWSER_PROFILE_DIR", "scrapling-finished-profile"),
        "refresh": ("AISCORE_REFRESH_BROWSER_PROFILE_DIR", "scrapling-refresh-profile"),
    }
    env_key, directory = profiles[purpose]
    options = {
        "headless": True, "solve_cloudflare": True, "block_webrtc": True,
        "retries": 1, "timeout": max(90_000, page_timeout_ms),
        "max_pages": max(1, int(max_pages)), "locale": "en-US",
        "user_data_dir": os.getenv(env_key, os.path.join(
            os.path.expanduser("~"), ".cache", "basket-odd", directory,
        )),
    }
    proxy = os.getenv("PLAYWRIGHT_PROXY")
    if proxy:
        options["proxy"] = proxy
    return options
