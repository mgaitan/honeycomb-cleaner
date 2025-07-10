import sys
import requests


class HoneycombClient:
    """Client for interacting with Honeycomb API"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update(
            {"X-Honeycomb-Team": api_key, "Content-Type": "application/json"}
        )

    def get_environment_info(self) -> dict:
        """Fetch environment information"""
        url = "https://api.honeycomb.io/1/auth"

        try:
            response = self.session.get(url)
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
            response = self.session.get(url)
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
            response = self.session.delete(url)
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
            response = self.session.get(url)
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
            response = self.session.put(url, json=payload)
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
            response = self.session.delete(url)
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
            response = self.session.delete(url)
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
