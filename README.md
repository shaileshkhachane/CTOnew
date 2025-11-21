# OLAP Backend Service

A TypeScript-based OLAP (Online Analytical Processing) backend service that provides a RESTful API for querying cube metadata, dimensions, hierarchies, and measures. The service supports star and snowflake schemas with Slowly Changing Dimension (SCD) Type 1 and Type 2 configurations.

## Features

- 🎯 **Star & Snowflake Schema Support**: Model multi-dimensional data warehouses with fact tables and dimensions
- 📊 **Measures**: Define aggregations (SUM, COUNT, AVG, MIN, MAX, DISTINCT_COUNT) with calculated measures
- 🏗️ **Hierarchies**: Support for drill-down hierarchies with ordered levels
- 🔄 **SCD Support**: Type 1 (overwrite) and Type 2 (versioned with temporal tracking) Slowly Changing Dimensions
- ✅ **Schema Validation**: Zod-based validation for all domain models
- 📖 **OpenAPI Documentation**: Auto-generated Swagger UI at `/docs`
- 🧪 **Comprehensive Testing**: Unit tests with Vitest for domain logic and SCD resolution

## Architecture

### Domain Models

The service models OLAP concepts using Zod for runtime validation:

- **Cube**: Top-level container representing a star/snowflake schema
- **FactTable**: Central table containing measures and foreign keys to dimensions
- **Dimension**: Descriptive attributes with optional SCD configuration
- **Measure**: Aggregatable metrics with aggregation types
- **Hierarchy**: Ordered drill-down paths through dimension levels
- **SCD Config**: Type 1 or Type 2 configuration for tracking dimension changes

### Sample Model

The service includes a pre-configured sales analytics cube:

#### Fact Table: `fact_sales`
- **Measures**: Quantity, Revenue, Cost, Profit, Transaction Count, Average Order Value
- **Grain**: Individual sale transaction

#### Dimensions:
1. **dim_date** - Time dimension with calendar hierarchies
2. **dim_customer** - Customer dimension with SCD Type 2 for address history
3. **dim_product** - Product dimension with SCD Type 1 for price updates
4. **dim_category** - Product category dimension

#### Hierarchies:
1. **Calendar**: Year → Quarter → Month → Day
2. **Geography**: Country → State → City → Customer
3. **Product**: Department → Category → Product

## Getting Started

### Prerequisites

- Node.js 18+ 
- pnpm 8+

### Installation

```bash
# Install dependencies
pnpm install
```

### Running the Service

```bash
# Development mode with hot reload
pnpm dev

# Build for production
pnpm build

# Run production build
pnpm start
```

The service will start on `http://localhost:3000` by default.

### Configuration

Environment variables:
- `PORT` - Server port (default: 3000)
- `HOST` - Server host (default: 0.0.0.0)
- `LOG_LEVEL` - Log level (default: info)

## API Endpoints

### Base URLs
- Health check: `GET /health`
- API documentation: `GET /docs`

### Schema Endpoints

#### Get All Cubes
```http
GET /schema/cubes
```
Returns a summary of all registered cubes with counts of dimensions, measures, and hierarchies.

**Response:**
```json
[
  {
    "id": "sales-cube",
    "name": "Sales Analytics Cube",
    "description": "Star schema for sales data analysis",
    "schemaType": "STAR",
    "dimensionCount": 4,
    "measureCount": 6,
    "hierarchyCount": 3
  }
]
```

#### Get Cube Details
```http
GET /schema/cubes/:cubeId
```
Returns complete cube definition including fact table, dimensions, measures, and hierarchies.

**Response:** Full cube object with nested structures.

#### Get Dimension
```http
GET /schema/cubes/:cubeId/dimensions/:dimensionId
```
Returns dimension definition with attributes, SCD configuration, and associated hierarchies.

**Example Response:**
```json
{
  "id": "dim-customer",
  "name": "Customer",
  "tableName": "dim_customer",
  "primaryKey": "customer_id",
  "attributes": [
    {
      "id": "attr-customer-name",
      "name": "Customer Name",
      "column": "customer_name",
      "dataType": "string"
    }
  ],
  "scdConfig": {
    "type": "TYPE_2",
    "versionColumn": "version",
    "startDateColumn": "effective_date",
    "endDateColumn": "expiration_date",
    "currentFlagColumn": "is_current",
    "trackedAttributes": ["city", "state", "country", "segment"]
  },
  "hierarchies": [
    {
      "id": "hier-geography",
      "name": "Geography Hierarchy",
      "description": "Geographic drill-down hierarchy"
    }
  ]
}
```

#### Get Hierarchy
```http
GET /schema/cubes/:cubeId/hierarchies/:hierarchyId
```
Returns hierarchy definition with levels sorted by order.

**Example Response:**
```json
{
  "id": "hier-calendar",
  "name": "Calendar Hierarchy",
  "dimensionId": "dim-date",
  "description": "Standard calendar drill-down hierarchy",
  "levels": [
    {
      "id": "level-year",
      "name": "Year",
      "column": "year",
      "order": 1
    },
    {
      "id": "level-quarter",
      "name": "Quarter",
      "column": "quarter",
      "order": 2
    },
    {
      "id": "level-month",
      "name": "Month",
      "column": "month_name",
      "order": 3
    },
    {
      "id": "level-day",
      "name": "Day",
      "column": "full_date",
      "order": 4
    }
  ]
}
```

## Testing

```bash
# Run all tests
pnpm test

# Run tests in watch mode
pnpm test:watch

# Generate coverage report
pnpm test:coverage
```

### Test Coverage

The test suite covers:

1. **Schema Registration**
   - Valid cube registration
   - Duplicate cube detection
   - Invalid dimension reference detection
   - Zod schema validation

2. **Hierarchy Validation**
   - Valid hierarchy registration
   - Unknown dimension detection
   - Duplicate level order detection
   - Non-consecutive order detection

3. **SCD Type 1**
   - Record passthrough
   - History merging by primary key

4. **SCD Type 2**
   - Current version resolution (boolean, numeric, string flags)
   - Point-in-time version resolution
   - Version history merging and sorting
   - Natural key extraction

5. **Domain Model Validation**
   - Measure schema validation
   - Hierarchy schema validation
   - SCD configuration validation
   - Dimension and fact table validation
   - Cube schema validation

## Development

### Project Structure

```
.
├── data/
│   └── schema.json           # Sample cube definition
├── src/
│   ├── domain/               # Domain models with Zod schemas
│   │   ├── measure.ts
│   │   ├── hierarchy.ts
│   │   ├── scd.ts
│   │   ├── dimension.ts
│   │   ├── fact-table.ts
│   │   ├── schema.ts
│   │   ├── domain.test.ts
│   │   └── index.ts
│   ├── services/             # Business logic
│   │   ├── metadata-store.ts
│   │   ├── metadata-store.test.ts
│   │   ├── scd-resolver.ts
│   │   └── scd-resolver.test.ts
│   ├── routes/               # API route handlers
│   │   └── schema-routes.ts
│   └── server.ts             # Fastify server entry point
├── package.json
├── tsconfig.json
├── vitest.config.ts
└── README.md
```

### Adding New Cubes

1. Create a JSON file in the `data/` directory following the schema structure
2. The metadata store will automatically load it on boot
3. Validate your cube definition against the Zod schemas

### Code Quality

```bash
# Type checking
pnpm type-check

# Linting
pnpm lint
```

## SCD (Slowly Changing Dimension) Support

### Type 1 - Overwrite

Overwrites historical values with new values. No history is maintained.

**Configuration:**
```json
{
  "type": "TYPE_1",
  "overwriteAttributes": ["price", "description"]
}
```

**Use Case:** Product price updates where history is not needed.

### Type 2 - Historical Tracking

Maintains full history with versioning and temporal validity.

**Configuration:**
```json
{
  "type": "TYPE_2",
  "versionColumn": "version",
  "startDateColumn": "effective_date",
  "endDateColumn": "expiration_date",
  "currentFlagColumn": "is_current",
  "trackedAttributes": ["city", "state", "country"]
}
```

**Use Case:** Customer address changes where historical accuracy is required for point-in-time analysis.

**Features:**
- Version tracking with natural key extraction
- Current version resolution
- Point-in-time queries
- Temporal validity windows

## Tech Stack

- **Runtime**: Node.js 20+
- **Language**: TypeScript 5.3+
- **Web Framework**: Fastify 4.x
- **Validation**: Zod 3.x
- **Testing**: Vitest 1.x
- **API Documentation**: @fastify/swagger + @fastify/swagger-ui
- **Package Manager**: pnpm

## License

MIT
