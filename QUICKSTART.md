# Quick Start Guide

This guide will help you get started with the ETL system in minutes.

## Prerequisites

- Node.js 18+ installed
- pnpm installed (`npm install -g pnpm`)

## Step 1: Install Dependencies

```bash
pnpm install
```

## Step 2: Build the Project

```bash
pnpm build
```

## Step 3: Run Your First ETL Pipeline

The project comes with sample CSV data in the `data/` directory. Run the ETL pipeline:

```bash
pnpm etl:run --cube sales
```

You should see output like:
```
[CLI] Starting ETL run...
[Orchestrator] Processing cube: sales
[Source: sales_csv] Extracted 10 rows
[Source: sales_csv] Validation passed
[Source: sales_csv] Loaded 10 new, 0 updated rows
...
[CLI] All sources processed successfully
```

## Step 4: Check the Results

### View Metadata
```bash
cat etl-metadata/summary.json
```

### View Watermarks
```bash
cat etl-metadata/watermarks.json
```

## Step 5: Try Incremental Refresh

Run the ETL again with a date filter to only load recent data:

```bash
pnpm etl:run --cube sales --since 2024-01-20T00:00:00Z
```

This will only process records with dates after January 20, 2024.

## Step 6: Run Tests

```bash
pnpm test
```

All 17 tests should pass.

## What's Next?

### Custom Data Sources

1. **CSV Files**: Place your CSV files in `data/` and update `etl-config.json`
2. **PostgreSQL**: Set up a database and run `data/postgres-seed.sql`, then use `etl-config.postgres.json`
3. **REST APIs**: Configure your API endpoint in `etl-config.rest.json`

### Configuration

Edit `etl-config.json` to:
- Add new data sources
- Define cubes and their dimensions
- Configure watermark columns for incremental refresh

### Validation

The system validates data automatically:
- Schema validation (data types, formats)
- Required field checks
- Uniqueness constraints
- Email validation for customer emails

If validation fails, you'll see detailed error messages with row numbers and field names.

### SCD Type 2

Dimension tables (products, customers) automatically track historical changes:
- Price changes are versioned
- Old versions remain queryable
- Current version is marked with `is_current = true`

### Monitoring

Check `etl-metadata/` directory for:
- Individual run logs
- Summary statistics
- Watermark tracking

## Example Workflows

### Daily Production Run
```bash
pnpm etl:run --cube sales
```

### Backfill Historical Data
```bash
pnpm etl:run --cube sales --since 2024-01-01T00:00:00Z
```

### Run Specific Configuration
```bash
pnpm etl:run --cube sales --config ./etl-config.postgres.json
```

## Troubleshooting

### No data loaded?
- Check watermarks: `cat etl-metadata/watermarks.json`
- Use `--since` to override the watermark

### Validation errors?
- Check the error details in the console output
- Review source data quality
- Verify schema definitions

### Connection errors?
- Verify database credentials
- Check network connectivity
- Ensure CSV files exist

## Learn More

- Full documentation: [README.md](README.md)
- Test examples: `test/` directory
- Sample configurations: `etl-config*.json` files
