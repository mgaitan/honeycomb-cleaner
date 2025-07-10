import argparse
import os
import sys
from datetime import datetime, timedelta
from typing import List, Dict

import requests
from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt

console = Console()


class HoneycombClient:
    """Client for interacting with Honeycomb API"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "X-Honeycomb-Team": api_key,
            "Content-Type": "application/json"
        })

    def get_environment_info(self) -> Dict:
        """Fetch environment information"""
        url = "https://api.honeycomb.io/1/auth"

        try:
            response = self.session.get(url)
            response.raise_for_status()
            auth_info = response.json()
            return {
                "environment": auth_info.get("environment", {"name": "Unknown", "slug": "unknown"}),
                "team": auth_info.get("team", {"name": "Unknown", "slug": "unknown"})
            }
        except requests.exceptions.RequestException as e:
            print(f"Error fetching environment info: {e}")
            return {
                "environment": {"name": "Unknown", "slug": "unknown"},
                "team": {"name": "Unknown", "slug": "unknown"}
            }

    def get_columns(self, dataset_slug: str) -> List[Dict]:
        """Fetch all columns for a dataset"""
        url = f"https://api.honeycomb.io/1/columns/{dataset_slug}"

        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and e.response.status_code == 401:
                print(f"Error fetching columns for {dataset_slug}: Unauthorized (401)")
                print(f"  → API key may lack 'Manage Queries and Columns' permission")
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
            if hasattr(e, 'response'):
                status_code = e.response.status_code
                print(f"FAILED - Error {status_code} deleting column {column_id}")
                try:
                    error_details = e.response.json()
                    if 'error' in error_details:
                        print(f"  → {error_details['error']}")
                except:
                    pass
            else:
                print(f"FAILED - Error deleting column {column_id}: {e}")
            return False

    def get_datasets(self) -> List[Dict]:
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

        payload = {
            "settings": {
                "delete_protected": False
            }
        }

        try:
            response = self.session.put(url, json=payload)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response'):
                status_code = e.response.status_code
                print(f"FAILED - Error {status_code} disabling protection for {dataset_slug}")
            else:
                print(f"FAILED - Error disabling protection for {dataset_slug}: {e}")
            return False


def is_column_inactive(column: Dict, days: int) -> bool:
    """Check if a column is inactive based on last_written timestamp"""
    last_written = column.get("last_written")

    if not last_written:
        # No last_written means the column was never used
        return True

    try:
        # Parse the timestamp (assuming ISO format)
        last_written_dt = datetime.fromisoformat(last_written.replace('Z', '+00:00'))
        cutoff_date = datetime.now(last_written_dt.tzinfo) - timedelta(days=days)

        return last_written_dt < cutoff_date
    except (ValueError, TypeError):
        # If we can't parse the date, consider it inactive
        print(f"Warning: Could not parse last_written for column {column.get('key_name', 'unknown')}")
        return True

    def delete_dataset(self, dataset_slug: str, disable_protection: bool = False) -> bool:
        """Delete a dataset, optionally disabling deletion protection first"""
        url = f"https://api.honeycomb.io/1/datasets/{dataset_slug}"

        try:
            response = self.session.delete(url)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response'):
                status_code = e.response.status_code

                # If deletion protection error and flag is enabled, try to disable it
                is_protected = False
                if status_code == 409 and disable_protection and hasattr(e, 'response'):
                    # Check both text and JSON for deletion protection indicators
                    if e.response.text and 'delete protected' in e.response.text.lower():
                        is_protected = True
                    else:
                        try:
                            error_details = e.response.json()
                            if 'error' in error_details and 'delete protected' in error_details['error'].lower():
                                is_protected = True
                        except:
                            pass

                if is_protected:
                    print(f"deletion protection detected, disabling... ", end="", flush=True)

                    if self.disable_deletion_protection(dataset_slug):
                        print(f"retrying delete... ", end="", flush=True)
                        try:
                            response = self.session.delete(url)
                            response.raise_for_status()
                            return True
                        except requests.exceptions.RequestException as retry_e:
                            if hasattr(retry_e, 'response'):
                                print(f"FAILED - Error {retry_e.response.status_code} on retry")
                            else:
                                print(f"FAILED - Error on retry: {retry_e}")
                            return False
                    else:
                        return False

                print(f"FAILED - Error {status_code} deleting {dataset_slug}")
                try:
                    error_details = e.response.json()
                    if 'error' in error_details:
                        print(f"  → {error_details['error']}")
                except:
                    pass
            else:
                print(f"FAILED - Error deleting {dataset_slug}: {e}")
            return False


def is_dataset_inactive(dataset: Dict, days: int) -> bool:
    """Check if a dataset is inactive based on last_written_at timestamp"""
    last_written = dataset.get("last_written_at")

    if not last_written:
        # No last_written_at means no data was ever written
        return True

    try:
        # Parse the timestamp (assuming ISO format)
        last_written_dt = datetime.fromisoformat(last_written.replace('Z', '+00:00'))
        cutoff_date = datetime.now(last_written_dt.tzinfo) - timedelta(days=days)

        return last_written_dt < cutoff_date
    except (ValueError, TypeError):
        # If we can't parse the date, consider it inactive
        print(f"Warning: Could not parse last_written_at for dataset {dataset.get('name', 'unknown')}")
        return True


def format_date(date_str: str) -> str:
    """Format date string for display"""
    if not date_str or date_str == "null":
        return "Never"
    try:
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        return "Unknown"


def get_dataset_url(dataset: Dict, team_slug: str, env_slug: str) -> str:
    """Get the canonical URL for a dataset"""
    slug = dataset.get("slug", "")
    if not slug:
        return "N/A"
    # Honeycomb dataset URL format
    return f"https://ui.honeycomb.io/{team_slug}/environments/{env_slug}/datasets/{slug}/home"


def display_datasets_table(datasets: List[Dict], title: str, team_slug: str, env_slug: str):
    """Display datasets in a formatted table"""
    table = Table(title=title)
    table.add_column("Name", style="cyan", no_wrap=True)
    table.add_column("Created", style="blue")
    table.add_column("Last Activity", style="yellow")
    table.add_column("URL", style="green")

    for dataset in datasets:
        name = dataset.get("name", "Unknown")
        created = format_date(dataset.get("created_at", ""))
        last_activity = format_date(dataset.get("last_written_at", ""))
        url = get_dataset_url(dataset, team_slug, env_slug)

        table.add_row(
            name,
            created,
            last_activity,
            url
        )

    console.print(table)


def display_columns_table(columns: List[Dict], title: str, dataset_name: str):
    """Display columns in a formatted table"""
    # Limit to first 100 columns for performance
    LIMIT = 150
    display_columns = columns[:LIMIT]
    total_columns = len(columns)

    table_title = f"{title} - {dataset_name}"
    if total_columns > LIMIT:
        table_title += f" (showing first {LIMIT} of {total_columns})"

    table = Table(title=table_title)
    table.add_column("Column Name", style="cyan", no_wrap=True)
    table.add_column("Type", style="magenta")
    table.add_column("Created", style="blue")
    table.add_column("Last Written", style="yellow")
    table.add_column("Hidden", style="red")

    for column in display_columns:
        key_name = column.get("key_name") or "Unknown"
        col_type = column.get("type") or "unknown"
        created = format_date(column.get("created_at"))
        last_used = format_date(column.get("last_written"))
        hidden = "Yes" if column.get("hidden", False) else "No"

        table.add_row(
            key_name,
            col_type,
            created,
            last_used,
            hidden
        )

    console.print(table)

    if total_columns > 100:
        print(f"... and {total_columns - 100} more columns (use --delete-columns to see deletion progress)")


def check_columns_for_dataset(client: HoneycombClient, dataset: Dict, days: int) -> Dict:
    """Check columns for a single dataset and return active/inactive counts"""
    dataset_name = dataset.get("name", "Unknown")
    dataset_slug = dataset.get("slug", "")

    if not dataset_slug:
        print(f"Skipping {dataset_name}: no slug found")
        return {"active": 0, "inactive": 0, "inactive_columns": []}

    columns = client.get_columns(dataset_slug)
    if not columns:
        return {"active": 0, "inactive": 0, "inactive_columns": []}

    active_columns = []
    inactive_columns = []

    for column in columns:
        if is_column_inactive(column, days):
            inactive_columns.append(column)
        else:
            active_columns.append(column)

    return {
        "active": len(active_columns),
        "inactive": len(inactive_columns),
        "inactive_columns": inactive_columns,
        "dataset_name": dataset_name,
        "dataset_slug": dataset_slug
    }


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Clean up inactive Honeycomb datasets and columns")
    parser.add_argument("--days", type=int, default=60, help="Days to look back for activity (default: 60)")
    parser.add_argument("--delete", action="store_true", help="Enable deletion mode")
    parser.add_argument("--delete-protected", action="store_true", help="Also delete datasets with deletion protection enabled")
    parser.add_argument("--name", "-n", action="append", help="Only consider datasets with these names for deletion (can be used multiple times)")
    parser.add_argument("--check-columns", action="store_true", help="Check for unused columns in active datasets")
    parser.add_argument("--delete-columns", action="store_true", help="Enable deletion of unused columns (requires --check-columns)")
    parser.add_argument("--api-key", type=str, help="Honeycomb API key (overrides env var)")
    return parser.parse_args()


def setup_client(args):
    """Setup Honeycomb client with API key validation"""
    api_key = args.api_key or os.getenv("HONEYCOMB_API_KEY")
    if not api_key:
        print("Error: HONEYCOMB_API_KEY environment variable not set and --api-key not provided")
        print("Set it with: export HONEYCOMB_API_KEY=your_api_key_here")
        sys.exit(1)

    return HoneycombClient(api_key)


def categorize_datasets(datasets, args):
    """Categorize datasets into active, inactive, and filtered out"""
    inactive_datasets = []
    active_datasets = []
    filtered_out_datasets = []

    for dataset in datasets:
        dataset_name = dataset.get("name", "")

        # If specific datasets are specified, only consider those
        if args.name and dataset_name not in args.name:
            filtered_out_datasets.append(dataset)
            continue

        if is_dataset_inactive(dataset, args.days):
            inactive_datasets.append(dataset)
        else:
            active_datasets.append(dataset)

    return active_datasets, inactive_datasets, filtered_out_datasets


def process_column_cleanup(client, active_datasets, args):
    """Process column cleanup for active datasets"""
    print(f"\nChecking columns in active datasets for inactivity over {args.days} days...")
    print(f"Processing {len(active_datasets)} active datasets...")

    total_inactive_columns = 0
    datasets_with_inactive_columns = []

    for i, dataset in enumerate(active_datasets):
        dataset_name = dataset.get("name", "Unknown")
        dataset_slug = dataset.get("slug", "")
        print(f"  [{i+1}/{len(active_datasets)}] Checking {dataset_name} ({dataset_slug})...")

        result = check_columns_for_dataset(client, dataset, args.days)
        print(f"    Found {result['active']} active, {result['inactive']} inactive columns")

        if result["inactive"] > 0:
            datasets_with_inactive_columns.append(result)
            total_inactive_columns += result["inactive"]

            # Display inactive columns for this dataset
            display_columns_table(
                result["inactive_columns"],
                f"Inactive columns (last {args.days} days)",
                result["dataset_name"]
            )

    print(f"\nFound {total_inactive_columns} inactive columns across {len(datasets_with_inactive_columns)} datasets")

    if total_inactive_columns > 0 and args.delete_columns:
        delete_columns(client, datasets_with_inactive_columns, total_inactive_columns)
    elif total_inactive_columns > 0:
        print(f"\nTo delete these columns, run: honeycomb-cleaner --check-columns --delete-columns --days {args.days}")


def delete_columns(client, datasets_with_inactive_columns, total_inactive_columns):
    """Delete inactive columns after confirmation"""
    console.print("[bold red]⚠️ WARNING: COLUMN DELETION MODE ⚠️[/bold red]")
    console.print("[bold red]This action cannot be undone![/bold red]")

    if Prompt.ask(f"\nDo you want to delete {total_inactive_columns} inactive columns?", default="no", choices=["yes I do", "no"]) == "no":
        print("Column deletion aborted.")
        return

    print(f"\nDeleting {total_inactive_columns} columns...")
    deleted_columns = 0

    for dataset_info in datasets_with_inactive_columns:
        dataset_name = dataset_info["dataset_name"]
        dataset_slug = dataset_info["dataset_slug"]

        print(f"\nDataset: {dataset_name}")
        for column in dataset_info["inactive_columns"]:
            column_name = column.get("key_name", "Unknown")
            column_id = column.get("id", "")

            if not column_id:
                print(f"  Skipping {column_name}: no ID found")
                continue

            print(f"  Deleting column {column_name}... ", end="", flush=True)

            if client.delete_column(dataset_slug, column_id):
                print("OK")
                deleted_columns += 1

    print(f"\nDeleted {deleted_columns} out of {total_inactive_columns} columns.")


def delete_datasets(client, inactive_datasets, args):
    """Delete inactive datasets after confirmation"""
    console.print("[bold red]⚠️ WARNING: DATASET DELETION MODE ⚠️[/bold red]")
    console.print("[bold red]This action cannot be undone![/bold red]")

    if Prompt.ask(f"\nDo you want to delete {len(inactive_datasets)} inactive datasets?", default="no", choices=["yes I do", "no"]) == "no":
        print("Dataset deletion aborted.")
        return

    print(f"\nDeleting {len(inactive_datasets)} datasets...")
    deleted_count = 0

    for dataset in inactive_datasets:
        name = dataset.get("name", "Unknown")
        slug = dataset.get("slug", "")

        if not slug:
            print(f"Skipping {name}: no slug found")
            continue

        print(f"Deleting {name}... ", end="", flush=True)

        if client.delete_dataset(slug, args.delete_protected):
            print("OK")
            deleted_count += 1

    print(f"\nDeleted {deleted_count} out of {len(inactive_datasets)} datasets.")


def main():
    args = parse_arguments()
    client = setup_client(args)

    # Get environment info
    auth_info = client.get_environment_info()
    env_info = auth_info.get("environment", {})
    team_info = auth_info.get("team", {})

    env_name = env_info.get("name", "Unknown")
    env_slug = env_info.get("slug", "unknown")
    team_slug = team_info.get("slug", "unknown")

    console.print(f"[bold blue]Honeycomb Environment:[/bold blue] [green]{env_name}[/green]")
    console.print(f"[bold blue]Fetching datasets and checking for inactivity over {args.days} days...[/bold blue]")

    # Get and categorize datasets
    datasets = client.get_datasets()
    active_datasets, inactive_datasets, filtered_out_datasets = categorize_datasets(datasets, args)

    # Display results
    print(f"\nFound {len(active_datasets)} active datasets")
    if active_datasets:
        display_datasets_table(active_datasets, f"Active datasets (last {args.days} days)", team_slug, env_slug)

    print(f"\nFound {len(inactive_datasets)} inactive datasets")
    if args.name:
        print(f"Filtered out {len(filtered_out_datasets)} datasets (not in specified list)")

    if not inactive_datasets:
        print("No inactive datasets found. Nothing to clean up!")

    if inactive_datasets:
        display_datasets_table(inactive_datasets, f"Datasets with no activity in the last {args.days} days", team_slug, env_slug)

    # Process column cleanup if requested
    if args.check_columns:
        process_column_cleanup(client, active_datasets, args)

    # Process dataset deletion if requested
    if args.delete and inactive_datasets:
        delete_datasets(client, inactive_datasets, args)
    elif args.delete and not inactive_datasets:
        print(f"\nTo delete datasets, run: honeycomb-cleaner --days {args.days} --delete")


if __name__ == "__main__":
    main()
