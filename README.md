# ETL System with Pluggable Connectors

A comprehensive ETL (Extract, Transform, Load) system with pluggable connectors, incremental refresh capabilities, and data validation.

## Features

- **Pluggable Connectors**: Support for CSV files, PostgreSQL, and REST APIs
- **Data Validation**: Schema validation, null checks, and uniqueness constraints using Zod
- **Incremental Refresh**: Watermark-based incremental data loading
- **SCD Type 2**: Slowly Changing Dimension support for dimension tables
- **Metadata Tracking**: Comprehensive refresh metadata with row counts and timing
- **Cube Invalidation**: Automatic cache invalidation and rebuild triggers
- **CLI Interface**: Command-line interface for running ETL pipelines

## Installation

```bash
pnpm install
```

## Quick Start

1. Build the project:
```bash
pnpm build
```

2. Run the ETL pipeline:
```bash
pnpm etl:run --cube sales
```

3. Run with incremental refresh:
```bash
pnpm etl:run --cube sales --since 2024-01-20T00:00:00Z
```

## Configuration

Create an `etl-config.json` file to define your data sources and cubes:

```json
{
  "sources": [
    {
      "name": "sales_csv",
      "type": "csv",
      "connection": {
        "filePath": "./data/sales.csv",
        "hasHeader": true
      },
      "schema": "sales",
      "watermarkColumn": "sale_date"
    },
    {
      "name": "products_db",
      "type": "postgres",
      "connection": {
        "host": "localhost",
        "port": 5432,
        "database": "warehouse",
        "user": "etl_user",
        "password": "password",
        "table": "products"
      },
      "schema": "product",
      "watermarkColumn": "updated_at"
    },
    {
      "name": "orders_api",
      "type": "rest",
      "connection": {
        "url": "https://api.example.com/orders",
        "method": "GET",
        "headers": {
          "X-API-Key": "your-api-key"
        },
        "dataPath": "data.orders"
      },
      "schema": "sales"
    }
  ],
  "target": {
    "type": "memory"
  },
  "cubes": [
    {
      "name": "sales",
      "factTable": "fact_sales",
      "dimensions": ["product", "customer"],
      "measures": ["quantity", "amount"],
      "sources": ["sales_csv", "products_db", "orders_api"]
    }
  ]
}
```

### Source Configuration

#### CSV Connector
```json
{
  "type": "csv",
  "connection": {
    "filePath": "./data/file.csv",
    "hasHeader": true,
    "delimiter": ","
  }
}
```

#### PostgreSQL Connector
```json
{
  "type": "postgres",
  "connection": {
    "host": "localhost",
    "port": 5432,
    "database": "mydb",
    "user": "user",
    "password": "pass",
    "table": "my_table",
    "query": "SELECT * FROM my_table WHERE active = true"
  }
}
```

#### REST API Connector
```json
{
  "type": "rest",
  "connection": {
    "url": "https://api.example.com/data",
    "method": "GET",
    "headers": {
      "Authorization": "Bearer token"
    },
    "auth": {
      "type": "bearer",
      "token": "your-token"
    },
    "dataPath": "results.data"
  }
}
```

## Data Schemas

The system supports the following predefined schemas:

### Sales (Fact Table)
```typescript
{
  id: string | number,
  product_id: string | number,
  customer_id: string | number,
  quantity: number (positive),
  amount: number (positive),
  sale_date: Date,
  region: string,
  updated_at?: Date
}
```

### Product (Dimension Table - SCD Type 2)
```typescript
{
  product_id: string | number,
  product_name: string,
  category: string,
  price: number (positive),
  valid_from: Date,
  valid_to?: Date | null,
  is_current: boolean
}
```

### Customer (Dimension Table - SCD Type 2)
```typescript
{
  customer_id: string | number,
  customer_name: string,
  email?: string (valid email),
  segment: string,
  valid_from: Date,
  valid_to?: Date | null,
  is_current: boolean
}
```

## Architecture

### Components

1. **Connectors** (`src/etl/connectors/`)
   - Base connector interface
   - CSV, PostgreSQL, and REST implementations
   - Watermark-based incremental extraction

2. **Validation** (`src/etl/validation/`)
   - Schema validation using Zod
   - Null checks and uniqueness constraints
   - Detailed error reporting

3. **SCD Handler** (`src/etl/scd/`)
   - Type 2 Slowly Changing Dimension merges
   - Automatic versioning and validity tracking

4. **Storage** (`src/etl/storage/`)
   - In-memory data store (default)
   - Support for fact and dimension tables

5. **Orchestrator** (`src/etl/orchestrator/`)
   - Coordinates the ETL pipeline
   - Manages connectors, validation, and loading
   - Triggers cube invalidation

6. **Watermark Tracker** (`src/etl/watermark/`)
   - Tracks last processed timestamp per source
   - Enables incremental refresh

7. **Metadata Emitter** (`src/etl/metadata/`)
   - Emits refresh metadata to disk
   - Tracks row counts, timing, and status

8. **Cube Manager** (`src/cube/`)
   - Handles cache invalidation
   - Triggers cube rebuilds

## CLI Commands

### Run ETL Pipeline
```bash
pnpm etl:run [options]
```

Options:
- `--cube <name>`: Name of the cube to process (optional, processes all if not specified)
- `--since <date>`: Process data since this date (ISO format, e.g., 2024-01-20T00:00:00Z)
- `--config <path>`: Path to configuration file (default: ./etl-config.json)

### Examples

Process all cubes:
```bash
pnpm etl:run
```

Process specific cube:
```bash
pnpm etl:run --cube sales
```

Incremental refresh since specific date:
```bash
pnpm etl:run --cube sales --since 2024-01-20T00:00:00Z
```

Use custom config:
```bash
pnpm etl:run --cube sales --config ./config/production.json
```

## Incremental Refresh

The system supports incremental refresh by tracking watermarks for each data source:

1. Configure a `watermarkColumn` in your source definition (e.g., `updated_at`, `sale_date`)
2. On first run, all data is extracted
3. The maximum watermark value is stored in `etl-metadata/watermarks.json`
4. On subsequent runs, only data with watermark > last watermark is processed
5. Use `--since` flag to override the stored watermark

## Validation

The ETL system validates data before loading:

- **Schema Validation**: Ensures data types match the schema
- **Null Checks**: Validates required fields are not null
- **Uniqueness**: Checks for duplicate values in unique fields
- **Type Coercion**: Automatically converts compatible types (e.g., string to number)

When validation fails:
- Failed rows are reported with detailed error messages
- No data is loaded to fact/dimension tables
- The pipeline returns a failed status

## SCD Type 2 (Slowly Changing Dimensions)

Dimension tables support SCD Type 2 to track historical changes:

- **valid_from**: Start date of the version
- **valid_to**: End date of the version (null for current)
- **is_current**: Boolean flag indicating current version

When a dimension record changes:
1. The existing current record is updated with `valid_to` = now, `is_current` = false
2. A new record is inserted with `valid_from` = now, `is_current` = true

## Metadata and Observability

ETL runs emit metadata to `etl-metadata/`:

- **Individual run files**: `{cube}_{source}_{timestamp}.json`
- **Latest run files**: `{cube}_{source}_latest.json`
- **Summary file**: `summary.json` (aggregated statistics)

Metadata includes:
- Start and end timestamps
- Rows processed, inserted, updated
- Status (success, failed, partial)
- Error messages (if any)
- Watermark values

## Testing

Run unit tests:
```bash
pnpm test
```

Run tests in watch mode:
```bash
pnpm test:watch
```

The test suite includes:
- Connector tests with mocked data sources
- Validation tests for various error scenarios
- SCD Type 2 merge logic tests
- End-to-end orchestrator tests
- Cache invalidation tests

## Operational Runbook

### Initial Setup

1. Install dependencies: `pnpm install`
2. Create `etl-config.json` with your data sources
3. Prepare sample data files or database connections
4. Run initial load: `pnpm etl:run`

### Daily Operations

1. Schedule ETL runs (e.g., via cron):
   ```bash
   0 2 * * * cd /path/to/etl && pnpm etl:run --cube sales
   ```

2. Monitor metadata files in `etl-metadata/`:
   - Check `summary.json` for overall health
   - Review individual run files for failures

3. Handle validation errors:
   - Review error messages in metadata files
   - Fix source data quality issues
   - Re-run ETL after corrections

### Incremental Refresh

1. Initial full load:
   ```bash
   pnpm etl:run --cube sales
   ```

2. Subsequent incremental loads:
   ```bash
   pnpm etl:run --cube sales
   ```
   (automatically uses stored watermarks)

3. Backfill specific date range:
   ```bash
   pnpm etl:run --cube sales --since 2024-01-01T00:00:00Z
   ```

### Troubleshooting

**No data extracted:**
- Check watermark values in `etl-metadata/watermarks.json`
- Verify watermarkColumn configuration
- Use `--since` to override watermark

**Validation failures:**
- Review error messages in metadata
- Check source data quality
- Verify schema definitions match source data

**Connection errors:**
- Verify database credentials
- Check network connectivity
- Ensure CSV files exist and are readable

**Performance issues:**
- Consider partitioning large data sources
- Optimize database queries
- Use incremental refresh instead of full loads

## Development

### Project Structure

```
.
├── src/
│   ├── domain/
│   │   └── models.ts          # Domain models and schemas
│   ├── etl/
│   │   ├── connectors/        # Data source connectors
│   │   ├── orchestrator/      # ETL orchestration
│   │   ├── validation/        # Data validation
│   │   ├── scd/              # SCD handling
│   │   ├── storage/          # Data storage
│   │   ├── watermark/        # Watermark tracking
│   │   ├── metadata/         # Metadata emission
│   │   ├── cli/              # CLI interface
│   │   └── types.ts          # Type definitions
│   └── cube/
│       └── manager.ts         # Cube management
├── test/                      # Unit tests
├── data/                      # Sample data files
├── etl-config.json           # ETL configuration
└── README.md
```

### Adding New Connectors

1. Extend `BaseConnector` class
2. Implement `connect()`, `extract()`, `disconnect()` methods
3. Add connector type to `types.ts`
4. Update orchestrator to instantiate new connector
5. Add tests

### Adding New Schemas

1. Define schema in `src/domain/models.ts` using Zod
2. Add to `SchemaRegistry`
3. Update orchestrator field mappings if needed
4. Add validation tests

## License

MIT
