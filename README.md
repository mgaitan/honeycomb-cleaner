# Honeycomb Cleaner

A command-line tool to clean up inactive datasets and columns in Honeycomb to reduce clutter and improve performance.

## Features

- 🗂️ **Dataset Cleanup**: Find and delete datasets with no activity in the last N days
- 📊 **Column Cleanup**: Find and delete unused columns in active datasets
- 🛡️ **Protection Handling**: Automatically disable deletion protection when needed
- 🎯 **Selective Targeting**: Filter by specific dataset names
- 📋 **Rich Tables**: Beautiful output with clickable dataset URLs
- ⚠️ **Safety First**: Multiple confirmations before deletion

## Run / Installation

You can run the tool directly using `uvx`

```bash
uvx honeycomb-cleaner
```

To install it permanently:

```bash
uv tool install honeycomb-cleaner
```


## Configuration

Set your Honeycomb configuration API key as an environment variable:

```bash
export HONEYCOMB_API_KEY=your_api_key_here
```

Your API key needs the following permissions:
- **Send Events** (to read dataset metadata)
- **Manage Queries and Columns** (to read and delete columns)
- **Create Datasets** (to delete datasets)

## Usage

### Basic Dataset Cleanup

```bash
# List datasets inactive for 60 days (default)
honeycomb-cleaner

# List datasets inactive for 30 days
honeycomb-cleaner --days 30

# Delete inactive datasets (interactive)
honeycomb-cleaner --delete

# Also delete datasets with deletion protection
honeycomb-cleaner --delete --delete-protected
```

### Column Cleanup

```bash
# Check for unused columns in active datasets
honeycomb-cleaner --check-columns

# Check for columns unused in last 30 days
honeycomb-cleaner --check-columns --days 30

# Delete unused columns (interactive)
honeycomb-cleaner --check-columns --delete-columns
```

### Selective Cleanup

```bash
# Only consider specific datasets
honeycomb-cleaner --name app-endpoints --name shipping-api

# Check columns in specific datasets only
honeycomb-cleaner --check-columns -n app-endpoints -n logs

# Delete datasets and columns for specific services
honeycomb-cleaner --delete --delete-protected --check-columns --delete-columns -n old-service
```

### Combined Operations

```bash
# Complete cleanup: datasets + columns
honeycomb-cleaner --delete --delete-protected --check-columns --delete-columns

# Dry run: see what would be deleted
honeycomb-cleaner --check-columns --days 90
```

## Command Line Options

| Option | Description |
|--------|-------------|
| `--days N` | Look back N days for activity (default: 60) |
| `--delete` | Enable dataset deletion mode |
| `--delete-protected` | Also delete datasets with deletion protection |
| `--check-columns` | Check for unused columns in active datasets |
| `--delete-columns` | Enable column deletion (requires --check-columns) |
| `--name NAME` / `-n NAME` | Only consider specific datasets (can be used multiple times) |
| `--api-key KEY` | Honeycomb API key (overrides environment variable) |

## Example Output

```
Honeycomb Environment: production

Found 85 active datasets
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Name                        ┃ Created    ┃ Last Activity ┃ URL                                                                           ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ current-service             │ 2024-01-01 │ 2024-01-08    │ https://ui.honeycomb.io/team/environments/prod/datasets/current-service/home │
│ active-logs                 │ 2024-01-15 │ 2024-01-07    │ https://ui.honeycomb.io/team/environments/prod/datasets/active-logs/home     │
└─────────────────────────────┴────────────┴───────────────┴───────────────────────────────────────────────────────────────────────────────┘

Found 3 inactive datasets
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Name                        ┃ Created    ┃ Last Activity ┃ URL                                                                           ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ old-service                 │ 2023-05-01 │ 2023-05-15    │ https://ui.honeycomb.io/team/environments/prod/datasets/old-service/home     │
│ test-dataset                │ 2023-06-01 │ Never         │ https://ui.honeycomb.io/team/environments/prod/datasets/test-dataset/home    │
│ legacy-logs                 │ 2023-04-01 │ 2023-04-20    │ https://ui.honeycomb.io/team/environments/prod/datasets/legacy-logs/home     │
└─────────────────────────────┴────────────┴───────────────┴───────────────────────────────────────────────────────────────────────────────┘

Checking columns in active datasets...
Found 245 inactive columns across 12 datasets

Inactive columns (last 60 days) - current-service (showing first 100 of 245)
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━┓
┃ Column Name                     ┃ Type   ┃ Created    ┃ Last Used  ┃ Hidden ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━┩
│ debug_column                    │ string │ 2023-01-01 │ 2023-05-15 │ No     │
│ old_field                       │ int    │ 2023-02-01 │ Never      │ Yes    │
│ legacy_attribute                │ string │ 2023-03-01 │ 2023-06-01 │ No     │
└─────────────────────────────────┴────────┴────────────┴────────────┴────────┘
... and 145 more columns
```

## Safety Features

- **Environment Display**: Shows which Honeycomb environment you're working with
- **Multiple Confirmations**: Requires explicit confirmation before deletion
- **Exact Text Matching**: Must type "yes I do" exactly for final confirmation
- **Error Handling**: Clear error messages for API failures
- **Dry Run Mode**: Preview what would be deleted without `--delete` flags

## Common Use Cases

### 1. Regular Cleanup
```bash
# Monthly cleanup of old datasets and columns
honeycomb-cleaner --days 90 --delete --delete-protected --check-columns --delete-columns
```

### 2. Service Decommissioning
```bash
# Remove all traces of an old service
honeycomb-cleaner --delete --delete-protected --check-columns --delete-columns -n old-service-name
```

### 3. Development Environment Cleanup
```bash
# Clean up test datasets in dev environment
honeycomb-cleaner --days 30 --delete -n test-dataset -n debug-logs -n temp-data
```

### 4. Column-Only Cleanup
```bash
# Just clean up unused columns, keep all datasets
honeycomb-cleaner --check-columns --delete-columns --days 60
```

## Troubleshooting

### Permission Errors
If you get 401/403 errors, ensure your API key has the required permissions:
- Go to Honeycomb UI → Environment Settings → API Keys
- Edit your API key and add missing permissions

### Deletion Protection
Some datasets may have deletion protection enabled. Use `--delete-protected` to automatically disable protection before deletion.

### Large Datasets
For datasets with many columns (>100), only the first 100 are displayed in tables for performance reasons. All columns are still processed for deletion.

## Development

To run from source:
```bash
cd honeycomb-cleaner
uv pip install -e .
honeycomb-cleaner --help
```

## License

This project is part of the Shiphero monorepo and follows the same license terms.
