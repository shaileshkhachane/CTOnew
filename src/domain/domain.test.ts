import { describe, it, expect } from 'vitest';
import {
  MeasureSchema,
  HierarchySchema,
  DimensionSchema,
  FactTableSchema,
  CubeSchema,
  SCDConfigSchema,
} from './index.js';

describe('Domain Model Validation', () => {
  describe('MeasureSchema', () => {
    it('should validate a valid measure', () => {
      const measure = {
        id: 'measure-1',
        name: 'Total Sales',
        aggregationType: 'SUM',
        expression: 'sales_amount',
      };

      expect(() => MeasureSchema.parse(measure)).not.toThrow();
    });

    it('should reject invalid aggregation type', () => {
      const measure = {
        id: 'measure-1',
        name: 'Total Sales',
        aggregationType: 'INVALID',
        expression: 'sales_amount',
      };

      expect(() => MeasureSchema.parse(measure)).toThrow();
    });

    it('should apply default for isCalculated', () => {
      const measure = {
        id: 'measure-1',
        name: 'Total Sales',
        aggregationType: 'SUM',
        expression: 'sales_amount',
      };

      const parsed = MeasureSchema.parse(measure);
      expect(parsed.isCalculated).toBe(false);
    });
  });

  describe('HierarchySchema', () => {
    it('should validate a valid hierarchy', () => {
      const hierarchy = {
        id: 'hier-1',
        name: 'Calendar',
        dimensionId: 'dim-date',
        levels: [
          { id: 'level-1', name: 'Year', column: 'year', order: 1 },
          { id: 'level-2', name: 'Month', column: 'month', order: 2 },
        ],
      };

      expect(() => HierarchySchema.parse(hierarchy)).not.toThrow();
    });

    it('should require at least one level', () => {
      const hierarchy = {
        id: 'hier-1',
        name: 'Calendar',
        dimensionId: 'dim-date',
        levels: [],
      };

      expect(() => HierarchySchema.parse(hierarchy)).toThrow();
    });
  });

  describe('SCDConfigSchema', () => {
    it('should validate Type 1 SCD config', () => {
      const config = {
        type: 'TYPE_1',
        overwriteAttributes: ['price', 'description'],
      };

      expect(() => SCDConfigSchema.parse(config)).not.toThrow();
    });

    it('should validate Type 2 SCD config', () => {
      const config = {
        type: 'TYPE_2',
        versionColumn: 'version',
        startDateColumn: 'start_date',
        endDateColumn: 'end_date',
        currentFlagColumn: 'is_current',
        trackedAttributes: ['address', 'city'],
      };

      expect(() => SCDConfigSchema.parse(config)).not.toThrow();
    });

    it('should require all Type 2 fields', () => {
      const config = {
        type: 'TYPE_2',
        versionColumn: 'version',
        startDateColumn: 'start_date',
      };

      expect(() => SCDConfigSchema.parse(config)).toThrow();
    });
  });

  describe('DimensionSchema', () => {
    it('should validate dimension with SCD config', () => {
      const dimension = {
        id: 'dim-1',
        name: 'Customer',
        tableName: 'dim_customer',
        primaryKey: 'customer_id',
        attributes: [
          {
            id: 'attr-1',
            name: 'Customer ID',
            column: 'customer_id',
            dataType: 'string',
            isKey: true,
          },
        ],
        scdConfig: {
          type: 'TYPE_2',
          versionColumn: 'version',
          startDateColumn: 'start_date',
          endDateColumn: 'end_date',
          trackedAttributes: ['address'],
        },
      };

      expect(() => DimensionSchema.parse(dimension)).not.toThrow();
    });

    it('should require at least one attribute', () => {
      const dimension = {
        id: 'dim-1',
        name: 'Customer',
        tableName: 'dim_customer',
        primaryKey: 'customer_id',
        attributes: [],
      };

      expect(() => DimensionSchema.parse(dimension)).toThrow();
    });
  });

  describe('FactTableSchema', () => {
    it('should validate fact table with measures and dimensions', () => {
      const factTable = {
        id: 'fact-1',
        name: 'Sales',
        tableName: 'fact_sales',
        measures: [
          {
            id: 'measure-1',
            name: 'Revenue',
            aggregationType: 'SUM',
            expression: 'revenue',
          },
        ],
        dimensionReferences: [
          {
            dimensionId: 'dim-date',
            foreignKey: 'date_id',
          },
        ],
      };

      expect(() => FactTableSchema.parse(factTable)).not.toThrow();
    });

    it('should require at least one measure', () => {
      const factTable = {
        id: 'fact-1',
        name: 'Sales',
        tableName: 'fact_sales',
        measures: [],
        dimensionReferences: [
          {
            dimensionId: 'dim-date',
            foreignKey: 'date_id',
          },
        ],
      };

      expect(() => FactTableSchema.parse(factTable)).toThrow();
    });

    it('should require at least one dimension reference', () => {
      const factTable = {
        id: 'fact-1',
        name: 'Sales',
        tableName: 'fact_sales',
        measures: [
          {
            id: 'measure-1',
            name: 'Revenue',
            aggregationType: 'SUM',
            expression: 'revenue',
          },
        ],
        dimensionReferences: [],
      };

      expect(() => FactTableSchema.parse(factTable)).toThrow();
    });
  });

  describe('CubeSchema', () => {
    it('should validate complete cube definition', () => {
      const cube = {
        id: 'cube-1',
        name: 'Sales Cube',
        schemaType: 'STAR',
        factTable: {
          id: 'fact-1',
          name: 'Sales',
          tableName: 'fact_sales',
          measures: [
            {
              id: 'measure-1',
              name: 'Revenue',
              aggregationType: 'SUM',
              expression: 'revenue',
            },
          ],
          dimensionReferences: [
            {
              dimensionId: 'dim-1',
              foreignKey: 'date_id',
            },
          ],
        },
        dimensions: [
          {
            id: 'dim-1',
            name: 'Date',
            tableName: 'dim_date',
            primaryKey: 'date_id',
            attributes: [
              {
                id: 'attr-1',
                name: 'Date',
                column: 'date',
                dataType: 'date',
              },
            ],
          },
        ],
      };

      expect(() => CubeSchema.parse(cube)).not.toThrow();
      const parsed = CubeSchema.parse(cube);
      expect(parsed.hierarchies).toEqual([]);
    });

    it('should validate SNOWFLAKE schema type', () => {
      const cube = {
        id: 'cube-1',
        name: 'Sales Cube',
        schemaType: 'SNOWFLAKE',
        factTable: {
          id: 'fact-1',
          name: 'Sales',
          tableName: 'fact_sales',
          measures: [
            {
              id: 'measure-1',
              name: 'Revenue',
              aggregationType: 'SUM',
              expression: 'revenue',
            },
          ],
          dimensionReferences: [
            {
              dimensionId: 'dim-1',
              foreignKey: 'date_id',
            },
          ],
        },
        dimensions: [
          {
            id: 'dim-1',
            name: 'Date',
            tableName: 'dim_date',
            primaryKey: 'date_id',
            attributes: [
              {
                id: 'attr-1',
                name: 'Date',
                column: 'date',
                dataType: 'date',
              },
            ],
          },
        ],
      };

      expect(() => CubeSchema.parse(cube)).not.toThrow();
    });

    it('should reject invalid schema type', () => {
      const cube = {
        id: 'cube-1',
        name: 'Sales Cube',
        schemaType: 'INVALID',
        factTable: {
          id: 'fact-1',
          name: 'Sales',
          tableName: 'fact_sales',
          measures: [
            {
              id: 'measure-1',
              name: 'Revenue',
              aggregationType: 'SUM',
              expression: 'revenue',
            },
          ],
          dimensionReferences: [
            {
              dimensionId: 'dim-1',
              foreignKey: 'date_id',
            },
          ],
        },
        dimensions: [
          {
            id: 'dim-1',
            name: 'Date',
            tableName: 'dim_date',
            primaryKey: 'date_id',
            attributes: [
              {
                id: 'attr-1',
                name: 'Date',
                column: 'date',
                dataType: 'date',
              },
            ],
          },
        ],
      };

      expect(() => CubeSchema.parse(cube)).toThrow();
    });
  });
});
