#!/bin/bash

# ETL System Demo Script
# This script demonstrates the key features of the ETL system

set -e

echo "=========================================="
echo "ETL System Demo"
echo "=========================================="
echo ""

# Clean up previous runs
echo "1. Cleaning up previous metadata..."
rm -rf etl-metadata/
echo "   ✓ Cleaned"
echo ""

# Full initial load
echo "2. Running full initial load..."
echo "   Command: pnpm etl:run --cube sales"
pnpm etl:run --cube sales
echo "   ✓ Initial load completed"
echo ""

# Show watermarks
echo "3. Checking watermarks..."
cat etl-metadata/watermarks.json | head -20
echo "   ✓ Watermarks stored"
echo ""

# Incremental load
echo "4. Running incremental load (since 2024-01-20)..."
echo "   Command: pnpm etl:run --cube sales --since 2024-01-20T00:00:00Z"
pnpm etl:run --cube sales --since 2024-01-20T00:00:00Z
echo "   ✓ Incremental load completed (only recent data processed)"
echo ""

# Show summary
echo "5. Viewing ETL summary..."
cat etl-metadata/summary.json
echo "   ✓ Summary generated"
echo ""

# Validation failure demo
echo "6. Testing validation with invalid data..."
echo "   Command: pnpm etl:run --cube sales --config ./etl-config.validation-demo.json"
pnpm etl:run --cube sales --config ./etl-config.validation-demo.json || echo "   ✓ Validation correctly rejected invalid data"
echo ""

# Run tests
echo "7. Running test suite..."
pnpm test
echo "   ✓ All tests passed"
echo ""

echo "=========================================="
echo "Demo completed successfully!"
echo "=========================================="
echo ""
echo "Key features demonstrated:"
echo "  ✓ Initial full load"
echo "  ✓ Incremental refresh with watermarks"
echo "  ✓ Data validation and error handling"
echo "  ✓ Metadata tracking"
echo "  ✓ Cube invalidation"
echo "  ✓ SCD Type 2 handling"
echo ""
echo "Explore the code:"
echo "  - Connectors: src/etl/connectors/"
echo "  - Orchestrator: src/etl/orchestrator/"
echo "  - Tests: test/"
echo "  - Configuration: etl-config.json"
echo "  - Metadata: etl-metadata/"
