import argparse
import os
import sys
from datetime import datetime, timedelta
from functools import wraps

from rich.console import Console, Group
from rich.live import Live
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
)
from rich.prompt import Prompt
from rich.table import Table
from rich.text import Text

from .client import HoneycombClient

console = Console()


def handle_keyboard_interrupt(func):
    """Decorator to catch KeyboardInterrupt and print 'Aborted'"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            console.print("\n[bold red]Aborted[/bold red]")
            sys.exit(130)

    return wrapper


def is_column_inactive(column: dict, days: int) -> bool:
    """Check if a column is inactive based on last_written timestamp"""
    last_written = column.get("last_written")
    if not last_written:
        return True

    try:
        last_written_dt = datetime.fromisoformat(last_written.replace("Z", "+00:00"))
        cutoff_date = datetime.now(last_written_dt.tzinfo) - timedelta(days=days)
        return last_written_dt < cutoff_date
    except (ValueError, TypeError):
        print(
            f"Warning: Could not parse last_written for column {column.get('key_name', 'unknown')}"
        )
        return True


def is_dataset_inactive(dataset: dict, days: int) -> bool:
    """Check if a dataset is inactive based on last_written_at timestamp"""
    last_written = dataset.get("last_written_at")

    if not last_written:
        # No last_written_at means no data was ever written
        return True

    try:
        # Parse the timestamp (assuming ISO format)
        last_written_dt = datetime.fromisoformat(last_written.replace("Z", "+00:00"))
        cutoff_date = datetime.now(last_written_dt.tzinfo) - timedelta(days=days)

        return last_written_dt < cutoff_date
    except (ValueError, TypeError):
        # If we can't parse the date, consider it inactive
        print(
            f"Warning: Could not parse last_written_at for dataset {dataset.get('name', 'unknown')}"
        )
        return True


def format_date(date_str: str | None) -> str:
    """Format date string for display"""
    if date_str == "null":
        return "Never"
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return "Unknown"


def get_dataset_url(dataset: dict, team_slug: str, env_slug: str) -> str:
    """Get the canonical URL for a dataset"""
    slug = dataset.get("slug", "")
    if not slug:
        return "N/A"
    # Honeycomb dataset URL format
    return f"https://ui.honeycomb.io/{team_slug}/environments/{env_slug}/datasets/{slug}/home"


def display_datasets_table(
    datasets: list[dict], title: str, team_slug: str, env_slug: str
):
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

        table.add_row(name, created, last_activity, url)

    console.print(table)


def display_columns_table(columns: list[dict], title: str, dataset_name: str):
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

        table.add_row(key_name, col_type, created, last_used, hidden)

    console.print(table)

    if total_columns > 100:
        print(
            f"... and {total_columns - 100} more columns (use --delete-columns to see deletion progress)"
        )


def check_columns_for_dataset(
    client: HoneycombClient, dataset: dict, days: int
) -> dict:
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
        "dataset_slug": dataset_slug,
    }


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Clean up inactive Honeycomb datasets and columns"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=60,
        help="Days to look back for activity (default: 60)",
    )
    parser.add_argument("--delete", action="store_true", help="Enable deletion mode")
    parser.add_argument(
        "--delete-protected",
        action="store_true",
        help="Also delete datasets with deletion protection enabled",
    )
    parser.add_argument(
        "--name",
        "-n",
        action="append",
        help="Only consider datasets with these names for deletion (can be used multiple times)",
    )
    parser.add_argument(
        "--check-columns",
        action="store_true",
        help="Check for unused columns in active datasets",
    )
    parser.add_argument(
        "--delete-columns",
        action="store_true",
        help="Enable deletion of unused columns (requires --check-columns)",
    )
    parser.add_argument(
        "--api-key", type=str, help="Honeycomb API key (overrides env var)"
    )
    return parser.parse_args()


def setup_client(args):
    """Setup Honeycomb client with API key validation"""
    api_key = args.api_key or os.getenv("HONEYCOMB_API_KEY")
    if not api_key:
        print(
            "Error: HONEYCOMB_API_KEY environment variable not set and --api-key not provided"
        )
        print("Set it with: export HONEYCOMB_API_KEY=your_api_key_here")
        sys.exit(1)

    return HoneycombClient(api_key, console)


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
    print(
        f"\nChecking columns in active datasets for inactivity over {args.days} days..."
    )
    print(f"Processing {len(active_datasets)} active datasets...")

    total_inactive_columns = 0
    datasets_with_inactive_columns = []

    try:
        for i, dataset in enumerate(active_datasets):
            dataset_name = dataset.get("name", "Unknown")
            dataset_slug = dataset.get("slug", "")
            print(
                f"  [{i + 1}/{len(active_datasets)}] Checking {dataset_name} ({dataset_slug})..."
            )

            result = check_columns_for_dataset(client, dataset, args.days)
            print(
                f"    Found {result['active']} active, {result['inactive']} inactive columns"
            )

            if result["inactive"] > 0:
                datasets_with_inactive_columns.append(result)
                total_inactive_columns += result["inactive"]

                # Display inactive columns for this dataset
                display_columns_table(
                    result["inactive_columns"],
                    f"Inactive columns (last {args.days} days)",
                    result["dataset_name"],
                )

        print(
            f"\nFound {total_inactive_columns} inactive columns across {len(datasets_with_inactive_columns)} datasets"
        )

        if total_inactive_columns > 0 and args.delete_columns:
            delete_columns(
                client, datasets_with_inactive_columns, total_inactive_columns
            )
        elif total_inactive_columns > 0:
            print(
                f"\nTo delete these columns, run: honeycomb-cleaner --check-columns --delete-columns --days {args.days}"
            )
    except KeyboardInterrupt:
        print("\nAborted")
        sys.exit(0)


def delete_columns(client, datasets_with_inactive_columns, total_inactive_columns):
    """Delete inactive columns after confirmation"""
    console.print("[bold red]⚠️ WARNING: COLUMN DELETION MODE ⚠️[/bold red]")
    console.print("[bold red]This action cannot be undone![/bold red]")

    if (
        Prompt.ask(
            f"\nDo you want to delete {total_inactive_columns} inactive columns?",
            default="no",
            choices=["yes I do", "no"],
        )
        == "no"
    ):
        print("Column deletion aborted.")
        return

    deleted_columns = 0
    failed_columns = {}  # Dictionary to group by error reason

    # Create a quiet client for deletion operations
    quiet_client = HoneycombClient(client.api_key, console, quiet=True)

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TextColumn("[blue]({task.completed}/{task.total})[/blue]"),
    )
    main_task = progress.add_task("Deleting columns...", total=total_inactive_columns)

    with Live(progress, refresh_per_second=10) as live:
        for dataset_info in datasets_with_inactive_columns:
            dataset_name = dataset_info["dataset_name"]
            dataset_slug = dataset_info["dataset_slug"]

            for column in dataset_info["inactive_columns"]:
                column_name = column.get("key_name", "Unknown")
                column_id = column.get("id", "")

                if not column_id:
                    progress.advance(main_task)
                    continue

                current_item = f"{column_name} from {dataset_name}"
                progress.update(main_task, description="Deleting column...")

                # Update live display with current item
                current_text = Text(f"Deleting: {current_item}", style="dim")
                live.update(Group(progress, current_text))

                success = quiet_client.delete_column(dataset_slug, column_id)
                if success:
                    deleted_columns += 1
                else:
                    # Group failures by error reason
                    error_reason = quiet_client.last_error or "Unknown error"
                    if error_reason not in failed_columns:
                        failed_columns[error_reason] = []
                    failed_columns[error_reason].append(current_item)

                progress.advance(main_task)

    # Print summary after progress bar is complete
    console.print(
        f"\n[bold green]✓ Deleted {deleted_columns} columns successfully[/bold green]"
    )

    if failed_columns:
        total_failed = sum(len(items) for items in failed_columns.values())
        console.print(
            f"[bold red]✗ Failed to delete {total_failed} columns:[/bold red]"
        )
        for error_reason, items in failed_columns.items():
            console.print(f"\n[red]  {error_reason}:[/red]")
            for item in items:
                console.print(f"    - {item}")

    total_failed = sum(len(items) for items in failed_columns.values())
    console.print(
        f"\n[bold blue]Summary: {deleted_columns} deleted, {total_failed} failed out of {total_inactive_columns} total[/bold blue]"
    )


def delete_datasets(client, inactive_datasets, args):
    """Delete inactive datasets after confirmation"""
    console.print("[bold red]⚠️ WARNING: DATASET DELETION MODE ⚠️[/bold red]")
    console.print("[bold red]This action cannot be undone![/bold red]")

    if (
        Prompt.ask(
            f"\nDo you want to delete {len(inactive_datasets)} inactive datasets?",
            default="no",
            choices=["yes I do", "no"],
        )
        == "no"
    ):
        print("Dataset deletion aborted.")
        return

    deleted_count = 0
    failed_datasets = {}  # Dictionary to group by error reason

    # Create a quiet client for deletion operations
    quiet_client = HoneycombClient(client.api_key, console, quiet=True)

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TextColumn("[blue]({task.completed}/{task.total})[/blue]"),
    )
    main_task = progress.add_task("Deleting datasets...", total=len(inactive_datasets))

    with Live(progress, refresh_per_second=10) as live:
        for dataset in inactive_datasets:
            name = dataset.get("name", "Unknown")
            slug = dataset.get("slug", "")

            if not slug:
                progress.advance(main_task)
                continue

            progress.update(main_task, description="Deleting dataset...")

            # Update live display with current item
            current_text = Text(f"Deleting: {name}", style="dim")
            live.update(Group(progress, current_text))

            success = quiet_client.delete_dataset(slug, args.delete_protected)
            if success:
                deleted_count += 1
            else:
                # Group failures by error reason
                error_reason = quiet_client.last_error or "Unknown error"
                if error_reason not in failed_datasets:
                    failed_datasets[error_reason] = []
                failed_datasets[error_reason].append(name)

            progress.advance(main_task)

    # Print summary after progress bar is complete
    console.print(
        f"\n[bold green]✓ Deleted {deleted_count} datasets successfully[/bold green]"
    )

    if failed_datasets:
        total_failed = sum(len(items) for items in failed_datasets.values())
        console.print(
            f"[bold red]✗ Failed to delete {total_failed} datasets:[/bold red]"
        )
        for error_reason, items in failed_datasets.items():
            console.print(f"\n[red]  {error_reason}:[/red]")
            for item in items:
                console.print(f"    - {item}")

    total_failed = sum(len(items) for items in failed_datasets.values())
    console.print(
        f"\n[bold blue]Summary: {deleted_count} deleted, {total_failed} failed out of {len(inactive_datasets)} total[/bold blue]"
    )


@handle_keyboard_interrupt
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

    console.print(
        f"[bold blue]Honeycomb Environment:[/bold blue] [green]{env_name}[/green]"
    )
    console.print(
        f"[bold blue]Fetching datasets and checking for inactivity over {args.days} days...[/bold blue]"
    )

    # Get and categorize datasets
    datasets = client.get_datasets()
    active_datasets, inactive_datasets, filtered_out_datasets = categorize_datasets(
        datasets, args
    )

    # Display results
    print(f"\nFound {len(active_datasets)} active datasets")
    if active_datasets:
        display_datasets_table(
            active_datasets,
            f"Active datasets (last {args.days} days)",
            team_slug,
            env_slug,
        )

    print(f"\nFound {len(inactive_datasets)} inactive datasets")
    if args.name:
        print(
            f"Filtered out {len(filtered_out_datasets)} datasets (not in specified list)"
        )

    if not inactive_datasets:
        print("No inactive datasets found. Nothing to clean up!")

    if inactive_datasets:
        display_datasets_table(
            inactive_datasets,
            f"Datasets with no activity in the last {args.days} days",
            team_slug,
            env_slug,
        )

    # Process column cleanup if requested
    if args.check_columns:
        process_column_cleanup(client, active_datasets, args)

    # Process dataset deletion if requested
    if args.delete and inactive_datasets:
        delete_datasets(client, inactive_datasets, args)
    elif args.delete and not inactive_datasets:
        print(
            f"\nTo delete datasets, run: honeycomb-cleaner --days {args.days} --delete"
        )
