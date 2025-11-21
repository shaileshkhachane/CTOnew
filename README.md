# Cube Engine OLAP Service

This project implements a lightweight OLAP cube engine with an Express façade. It materializes a sample sales cube in-memory, exposes a `POST /olap/query` endpoint, and provides a compact query DSL (JSON + optional lightweight MDX) capable of slice/dice/drill/roll-up operations. Query results include pivot metadata, flattened row sets, breadcrumb trails, cache status, and visualization suggestions.

## Getting started

```bash
npm install
npm test
npm start # launches the Express server on port 3000 by default
```

The default server hosts a sample `sales` cube. You can import `buildSampleCubeManager` to reuse the cube manager in other contexts or register your own cube definitions.

## Query contract

All OLAP requests are JSON payloads validated with Zod using the following top-level structure:

| Field | Type | Description |
| --- | --- | --- |
| `cube` | `string` | Registered cube identifier (e.g. `"sales"`). |
| `measures` | `string[]` | Measures to aggregate. Measures are validated against the cube definition. |
| `rows` / `columns` | `AxisSpec[]` | Defines pivot axes. Each axis includes `dimension`, optional `level`, optional `alias`, and optional `sort`. If omitted, the engine defaults to the top-most time hierarchy. |
| `pivot` | `{ rows?: AxisSpec[]; columns?: AxisSpec[] }` | Convenience wrapper to override axes without clobbering the base `rows/columns` definition. |
| `slices`, `dices`, `filters` | `FilterSpec[]` | Apply slice (`eq`), dice (`in`), or generic comparison filters (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `between`). Each filter targets a dimension + optional level. |
| `drill` | `{ dimension, fromLevel, toLevel, path?: (string \| number)[] }` | Describes a drill-up/down. Breadcrumbs are generated from the provided path. |
| `rollup` | `{ dimension, level }` | Raises the specified dimension to a higher level (e.g. month → quarter). |
| `mdx` | `string` | Optional lightweight MDX helper string described below. When provided, it is parsed and merged into the JSON payload (JSON values win on conflicts). |
| `includeFlattened` | `boolean` | Whether to include flattened row sets (defaults to `true`). |

### Lightweight MDX helper

The optional `mdx` string is a terse helper that supports the following clauses separated by semicolons:

```
MEASURES revenue, units;
ROWS time.year;
COLUMNS geography.region;
SLICE geography.country = USA;
DICE product.category IN (Electronics,Accessories);
FILTER time.year >= 2023;
DRILL time year -> month PATH 2023;
ROLLUP time quarter;
```

Each clause mirrors the JSON attributes:

- `MEASURES` → measure list
- `ROWS` / `COLUMNS` → comma-separated `dimension.level` pairs
- `SLICE` → equality slice (single value)
- `DICE` → multi-value inclusion list
- `FILTER` → comparison operators (`=`, `!=`, `>`, `>=`, `<`, `<=`)
- `DRILL` → `DRILL <dimension> <fromLevel> -> <toLevel> [PATH value1,value2,...]`
- `ROLLUP` → `ROLLUP <dimension> <level>`

### Response structure

Successful queries return:

```json
{
  "data": {
    "pivot": {
      "rows": [{ "key": "time.year:2023", "label": "2023", "coordinates": [...] }],
      "columns": [...],
      "measures": [
        { "name": "revenue", "values": [[3500], [1300]] }
      ]
    },
    "flat": [
      {
        "rowKey": "time.year:2023",
        "columnKey": "__all__",
        "coordinates": { "time.year": 2023 },
        "measures": { "revenue": 3500 }
      }
    ]
  },
  "metadata": {
    "cube": "sales",
    "measures": ["revenue"],
    "availableMeasures": [...],
    "breadcrumbs": [{ "dimension": "time", "level": "year", "value": 2023 }],
    "cache": {
      "hit": false,
      "key": "...",
      "ttlRemainingMs": 29800,
      "stats": { "hits": 0, "misses": 1, "size": 1 }
    },
    "planner": { "strategy": "raw-scan", "reason": "..." },
    "suggestions": ["column", "line"]
  }
}
```

Errors are surfaced with appropriate HTTP status codes (e.g. 400 for validation errors, 404 for unknown cubes).

## Testing

`npm test` runs the Vitest integration suite located in `tests/`. Coverage focuses on:

- Slice/dice queries returning deterministic pivots
- Drill-down paths with breadcrumb metadata
- Roll-up aggregation correctness
- LRU cache hit/miss behavior and TTL exposure
- Input validation errors

## Extending the cube engine

- Register additional cubes by instantiating `CubeManager` and calling `registerCube` with your `CubeDefinition`.
- Extend the MDX helper parser in `cube-manager.ts` if you need extra keywords.
- Replace the sample cube by wiring your own manager instance into `createApp`.

The engine keeps all data in-memory for simplicity, but the planner + aggregation layers are structured so they can be swapped with real fact stores or materialized views in future iterations.
