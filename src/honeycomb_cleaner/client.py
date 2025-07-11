import sys
import time
from datetime import datetime, timezone

import requests

# Example usage of rate limiting:
# The client will automatically handle 429 responses by:
# 1. Checking the Retry-After header
# 2. Parsing it as seconds (integer) or HTTP date string
# 3. Sleeping until the retry time
# 4. Retrying the request up to 3 times
# 5. Falling back to 60 seconds if no Retry-After header is present


class HoneycombClient:
    """Client for interacting with Honeycomb API"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update(
            {"X-Honeycomb-Team": api_key, "Content-Type": "application/json"}
        )

    def _handle_rate_limit(self, response):
        """Handle 429 rate limit responses by sleeping until retry time"""
        if response.status_code != 429:
            return

        retry_after = response.headers.get('Retry-After')
        if not retry_after:
            # Fallback to a default wait time if no header is present
            print("Rate limited but no Retry-After header found, waiting 60 seconds...")
            time.sleep(60)
            return

        try:
            # Retry-After can be either seconds or an HTTP date
            if retry_after.isdigit():
                # It's seconds
                wait_seconds = int(retry_after)
            else:
                # It's an HTTP date, parse it
                retry_time = datetime.strptime(retry_after, '%a, %d %b %Y %H:%M:%S %Z')
                retry_time = retry_time.replace(tzinfo=timezone.utc)
                now = datetime.now(timezone.utc)
                wait_seconds = max(0, (retry_time - now).total_seconds())

            if wait_seconds > 0:
                print(f"Rate limited, waiting {wait_seconds:.0f} seconds until {retry_after}...")
                time.sleep(wait_seconds)

        except (ValueError, TypeError) as e:
            print(f"Error parsing Retry-After header '{retry_after}': {e}")
            print("Waiting 60 seconds as fallback...")
            time.sleep(60)

    def _make_request_with_retry(self, method, url, **kwargs):
        """Make a request with automatic retry on rate limiting"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self.session.request(method, url, **kwargs)

                if response.status_code == 429:
                    if attempt < max_retries - 1:  # Don't sleep on the last attempt
                        self._handle_rate_limit(response)
                        continue

                return response

            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    print(f"Request failed, retrying... ({e})")
                    time.sleep(1)
                    continue
                raise

        return response

    def get_environment_info(self) -> dict:
        """Fetch environment information"""
        url = "https://api.honeycomb.io/1/auth"

        try:
            response = self._make_request_with_retry('GET', url)
            response.raise_for_status()
            auth_info = response.json()
            return {
                "environment": auth_info.get(
                    "environment", {"name": "Unknown", "slug": "unknown"}
                ),
                "team": auth_info.get("team", {"name": "Unknown", "slug": "unknown"}),
            }
        except requests.exceptions.RequestException as e:
            print(f"Error fetching environment info: {e}")
            return {
                "environment": {"name": "Unknown", "slug": "unknown"},
                "team": {"name": "Unknown", "slug": "unknown"},
            }

    def get_columns(self, dataset_slug: str) -> list[dict]:
        """Fetch all columns for a dataset"""
        url = f"https://api.honeycomb.io/1/columns/{dataset_slug}"

        try:
            response = self._make_request_with_retry('GET', url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if hasattr(e, "response") and e.response.status_code == 401:
                print(f"Error fetching columns for {dataset_slug}: Unauthorized (401)")
                print("  → API key may lack 'Manage Queries and Columns' permission")
            else:
                print(f"Error fetching columns for {dataset_slug}: {e}")
            return []

    def delete_column(self, dataset_slug: str, column_id: str) -> bool:
        """Delete a column from a dataset"""
        url = f"https://api.honeycomb.io/1/columns/{dataset_slug}/{column_id}"

        try:
            response = self._make_request_with_retry('DELETE', url)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            if hasattr(e, "response") and e.response is not None:
                status_code = e.response.status_code
                print(f"FAILED - Error {status_code} deleting column {column_id}")
                try:
                    error_details = e.response.json()
                    if "error" in error_details:
                        print(f"  → {error_details['error']}")
                except (ValueError, KeyError):
                    pass
            else:
                print(f"FAILED - Error deleting column {column_id}: {e}")
            return False

    def get_datasets(self) -> list[dict]:
        """Fetch all datasets from Honeycomb"""
        url = "https://api.honeycomb.io/1/datasets"

        try:
            response = self._make_request_with_retry('GET', url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching datasets: {e}")
            sys.exit(1)

    def disable_deletion_protection(self, dataset_slug: str) -> bool:
        """Disable deletion protection for a dataset"""
        url = f"https://api.honeycomb.io/1/datasets/{dataset_slug}"

        payload = {"settings": {"delete_protected": False}}

        try:
            response = self._make_request_with_retry('PUT', url, json=payload)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            if hasattr(e, "response") and e.response is not None:
                status_code = e.response.status_code
                print(
                    f"FAILED - Error {status_code} disabling protection for {dataset_slug}"
                )
            else:
                print(f"FAILED - Error disabling protection for {dataset_slug}: {e}")
            return False

    def delete_dataset(
        self, dataset_slug: str, disable_protection: bool = False
    ) -> bool:
        """Delete a dataset, optionally disabling deletion protection first"""
        url = f"https://api.honeycomb.io/1/datasets/{dataset_slug}"

        try:
            response = self._make_request_with_retry('DELETE', url)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            return self._handle_delete_error(e, dataset_slug, url, disable_protection)

    def _handle_delete_error(
        self, e, dataset_slug: str, url: str, disable_protection: bool
    ) -> bool:
        """Handle deletion errors with protection retry logic"""
        if not hasattr(e, "response") or e.response is None:
            print(f"FAILED - Error deleting {dataset_slug}: {e}")
            return False

        status_code = e.response.status_code

        # Try to handle deletion protection
        if (
            status_code == 409
            and disable_protection
            and self._is_deletion_protected(e.response)
        ):
            return self._retry_delete_after_unprotect(dataset_slug, url)

        # Handle other errors
        self._print_delete_error(e.response, dataset_slug)
        return False

    def _is_deletion_protected(self, response) -> bool:
        """Check if error is due to deletion protection"""
        if response.text and "delete protected" in response.text.lower():
            return True

        try:
            error_details = response.json()
            return (
                "error" in error_details
                and "delete protected" in error_details["error"].lower()
            )
        except (ValueError, KeyError):
            return False

    def _retry_delete_after_unprotect(self, dataset_slug: str, url: str) -> bool:
        """Disable protection and retry deletion"""
        print("deletion protection detected, disabling... ", end="", flush=True)

        if not self.disable_deletion_protection(dataset_slug):
            return False

        print("retrying delete... ", end="", flush=True)
        try:
            response = self._make_request_with_retry('DELETE', url)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as retry_e:
            if hasattr(retry_e, "response") and retry_e.response is not None:
                print(f"FAILED - Error {retry_e.response.status_code} on retry")
            else:
                print(f"FAILED - Error on retry: {retry_e}")
            return False

    def _print_delete_error(self, response, dataset_slug: str):
        """Print formatted deletion error"""
        print(f"FAILED - Error {response.status_code} deleting {dataset_slug}")
        try:
            error_details = response.json()
            if "error" in error_details:
                print(f"  → {error_details['error']}")
        except (ValueError, KeyError):
            pass
