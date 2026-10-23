"""
TransactionFlow Extraction Module
Provides streaming batch extractors for CSV files and paginated REST API sources.
Designed for bounded memory footprint and resilient network communication.
"""

import csv
import logging
import time
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

import httpx

logger = logging.getLogger("transactionflow.extract")


def extract_csv_batches(
    file_path: Union[str, Path],
    batch_size: int = 10000,
    encoding: str = "utf-8",
) -> Generator[List[Dict[str, Any]], None, int]:
    """
    Extract transaction records from a CSV file in bounded streaming batches.

    Args:
        file_path: Path to the CSV file.
        batch_size: Number of records per yielded batch.
        encoding: File character encoding.

    Yields:
        Batches of raw record dictionaries.

    Returns:
        Total number of records extracted from the file.
    """
    path = Path(file_path).resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Source CSV file not found: {path}")

    logger.info("Starting CSV extraction from '%s' (batch_size=%d)...", path, batch_size)
    total_extracted = 0
    current_batch: List[Dict[str, Any]] = []

    with open(path, mode="r", newline="", encoding=encoding) as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            logger.warning("CSV file '%s' is empty or has no header.", path)
            return 0

        # Clean header keys
        [h.strip() if h else f"col_{idx}" for idx, h in enumerate(reader.fieldnames)]

        for row_idx, raw_row in enumerate(reader, start=1):
            # Clean keys and strip whitespace from string values
            cleaned_row = {}
            for k, v in raw_row.items():
                if k is not None:
                    cleaned_key = k.strip()
                    cleaned_row[cleaned_key] = v.strip() if isinstance(v, str) else v

            cleaned_row["_source_line_num"] = row_idx + 1  # 1-indexed, accounting for header
            current_batch.append(cleaned_row)
            total_extracted += 1

            if len(current_batch) >= batch_size:
                yield current_batch
                current_batch = []

    if current_batch:
        yield current_batch

    logger.info("CSV extraction completed. Total records extracted: %d", total_extracted)
    return total_extracted


def extract_api_pages(
    base_url: str = "http://localhost:8000",
    endpoint: str = "/api/v1/transactions",
    page_size: int = 500,
    max_pages: Optional[int] = None,
    checkpoint_timestamp: Optional[str] = None,
    max_retries: int = 4,
    backoff_factor: float = 1.5,
    timeout: float = 10.0,
    client: Optional[httpx.Client] = None,
) -> Generator[Tuple[List[Dict[str, Any]], Optional[str]], None, int]:
    """
    Extract transaction records from a paginated REST API endpoint.
    Implements HTTP timeouts, exponential backoff for 429/5xx, and checkpoint tracking.

    Args:
        base_url: Base URL of the API server.
        endpoint: Endpoint path.
        page_size: Records requested per page.
        max_pages: Optional limit on pages to fetch.
        checkpoint_timestamp: Timestamp for incremental extraction.
        max_retries: Max retry attempts for transient errors.
        backoff_factor: Multiplier for exponential backoff delay.
        timeout: HTTP request timeout in seconds.
        client: Optional pre-configured httpx.Client (e.g., for test mocks).

    Yields:
        Tuple of (batch_of_items, latest_checkpoint_timestamp).

    Returns:
        Total records extracted across all pages.
    """
    total_extracted = 0
    current_page = 1
    latest_checkpoint = checkpoint_timestamp

    url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"
    logger.info("Starting API extraction from '%s' (page_size=%d)...", url, page_size)

    close_client = False
    if client is None:
        client = httpx.Client(timeout=timeout)
        close_client = True

    try:
        while True:
            if max_pages is not None and current_page > max_pages:
                logger.info("Reached configured max_pages limit (%d).", max_pages)
                break

            params: Dict[str, Any] = {
                "page": current_page,
                "page_size": page_size,
            }
            if latest_checkpoint:
                params["since_timestamp"] = latest_checkpoint

            response_data = None
            for attempt in range(1, max_retries + 1):
                try:
                    resp = client.get(url, params=params)
                    if resp.status_code == 200:
                        response_data = resp.json()
                        break
                    elif resp.status_code == 429:
                        retry_after = float(resp.headers.get("Retry-After", backoff_factor**attempt))
                        logger.warning(
                            "API rate limited (429) on page %d (attempt %d/%d). Sleeping %.1fs...",
                            current_page,
                            attempt,
                            max_retries,
                            retry_after,
                        )
                        time.sleep(retry_after)
                    elif resp.status_code in (500, 502, 503, 504):
                        sleep_time = backoff_factor**attempt
                        logger.warning(
                            "Transient server error (%d) on page %d (attempt %d/%d). Sleeping %.1fs...",
                            resp.status_code,
                            current_page,
                            attempt,
                            max_retries,
                            sleep_time,
                        )
                        time.sleep(sleep_time)
                    else:
                        resp.raise_for_status()
                except (httpx.ConnectError, httpx.TimeoutException) as exc:
                    sleep_time = backoff_factor**attempt
                    logger.warning(
                        "Network error (%s) on page %d (attempt %d/%d). Retrying in %.1fs...",
                        type(exc).__name__,
                        current_page,
                        attempt,
                        max_retries,
                        sleep_time,
                    )
                    time.sleep(sleep_time)
                    if attempt == max_retries:
                        raise

            if response_data is None:
                raise RuntimeError(f"Failed to fetch page {current_page} after {max_retries} retries.")

            items = response_data.get("items", [])
            has_next = response_data.get("has_next", False)
            next_page = response_data.get("next_page")
            chk = response_data.get("checkpoint_timestamp")
            if chk:
                latest_checkpoint = chk

            if not items:
                logger.info("API returned 0 items on page %d. Ending extraction.", current_page)
                break

            total_extracted += len(items)
            yield items, latest_checkpoint

            if not has_next or next_page is None:
                logger.info("Reached last API page (%d).", current_page)
                break

            current_page = next_page

    finally:
        if close_client:
            client.close()

    logger.info("API extraction completed. Total records extracted: %d", total_extracted)
    return total_extracted
