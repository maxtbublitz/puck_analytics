"""HTTP utility helpers (requests with retry and backoff).

Extracted from `crud.py` so it can be reused and unit tested separately.
"""
import requests
import time


def get_with_retry(url, session=None, max_retries=5, backoff_factor=1, timeout=10):
    """Simple GET with exponential backoff and Retry-After handling.

    Returns the final `requests.Response` (may be non-200 if all retries exhausted).
    """
    sess = session or requests
    for attempt in range(1, max_retries + 1):
        print(f"get_with_retry: attempt {attempt}/{max_retries} GET {url}")
        try:
            resp = sess.get(url, timeout=timeout)
        except requests.RequestException as e:
            print(f"get_with_retry: request exception on attempt {attempt}: {e}")
            if attempt == max_retries:
                print("get_with_retry: max retries reached, raising")
                raise
            sleep = backoff_factor * (2 ** (attempt - 1))
            print(f"get_with_retry: sleeping {sleep}s before retry")
            time.sleep(sleep)
            continue

        # Handle rate limit explicitly
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            try:
                wait = int(retry_after) if retry_after is not None else backoff_factor * (2 ** (attempt - 1))
            except Exception:
                wait = backoff_factor * (2 ** (attempt - 1))
            print(f"get_with_retry: 429 received, Retry-After={retry_after}, waiting {wait}s")
            if attempt == max_retries:
                print("get_with_retry: max retries reached after 429, returning response")
                return resp
            time.sleep(wait)
            continue

        # Retry on server errors
        if 500 <= resp.status_code < 600 and attempt < max_retries:
            sleep = backoff_factor * (2 ** (attempt - 1))
            print(f"get_with_retry: server error {resp.status_code}, sleeping {sleep}s and retrying")
            time.sleep(sleep)
            continue

        print(f"get_with_retry: success/terminal response status={resp.status_code}")
        return resp
