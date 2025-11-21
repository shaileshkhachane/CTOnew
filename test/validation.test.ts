import { describe, it, expect } from 'vitest';
import { DataValidator } from '../src/etl/validation/validator.js';
import { StagingData } from '../src/etl/types.js';

describe('DataValidator', () => {
  const validator = new DataValidator();

  it('should pass validation for valid sales data', () => {
    const staging: StagingData = {
      source: 'test',
      schema: 'sales',
      data: [
        {
          id: '1',
          product_id: 'P001',
          customer_id: 'C001',
          quantity: 5,
          amount: 150,
          sale_date: '2024-01-15',
          region: 'North',
        },
      ],
    };

    const result = validator.validate(staging, {
      uniqueFields: ['id'],
      requiredFields: ['id', 'product_id', 'customer_id'],
    });

    expect(result.valid).toBe(true);
    expect(result.rowsPassed).toBe(1);
    expect(result.rowsFailed).toBe(0);
    expect(result.errors).toHaveLength(0);
  });

  it('should fail validation for missing required fields', () => {
    const staging: StagingData = {
      source: 'test',
      schema: 'sales',
      data: [
        {
          id: '1',
          product_id: 'P001',
          quantity: 5,
          amount: 150,
          sale_date: '2024-01-15',
          region: 'North',
        },
      ],
    };

    const result = validator.validate(staging, {
      requiredFields: ['id', 'product_id', 'customer_id'],
    });

    expect(result.valid).toBe(false);
    expect(result.rowsPassed).toBe(0);
    expect(result.rowsFailed).toBe(1);
    expect(result.errors.length).toBeGreaterThan(0);
    expect(result.errors[0].field).toBe('customer_id');
  });

  it('should fail validation for duplicate unique fields', () => {
    const staging: StagingData = {
      source: 'test',
      schema: 'sales',
      data: [
        {
          id: '1',
          product_id: 'P001',
          customer_id: 'C001',
          quantity: 5,
          amount: 150,
          sale_date: '2024-01-15',
          region: 'North',
        },
        {
          id: '1',
          product_id: 'P002',
          customer_id: 'C002',
          quantity: 3,
          amount: 100,
          sale_date: '2024-01-16',
          region: 'South',
        },
      ],
    };

    const result = validator.validate(staging, {
      uniqueFields: ['id'],
    });

    expect(result.valid).toBe(false);
    expect(result.rowsFailed).toBeGreaterThan(0);
    const duplicateError = result.errors.find(e => e.message.includes('Duplicate'));
    expect(duplicateError).toBeDefined();
  });

  it('should fail validation for invalid data types', () => {
    const staging: StagingData = {
      source: 'test',
      schema: 'sales',
      data: [
        {
          id: '1',
          product_id: 'P001',
          customer_id: 'C001',
          quantity: -5,
          amount: 150,
          sale_date: '2024-01-15',
          region: 'North',
        },
      ],
    };

    const result = validator.validate(staging);

    expect(result.valid).toBe(false);
    expect(result.errors.length).toBeGreaterThan(0);
  });
});
