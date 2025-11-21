import { describe, it, expect } from 'vitest';
import { SCDHandler } from '../src/etl/scd/handler.js';

describe('SCDHandler', () => {
  const handler = new SCDHandler();

  it('should insert new records for Type 2 SCD', () => {
    const existing: any[] = [];
    const incoming = [
      {
        product_id: 'P001',
        product_name: 'Widget A',
        category: 'Electronics',
        price: 30.0,
      },
    ];

    const result = handler.mergeType2(
      existing,
      incoming,
      ['product_id'],
      ['product_name', 'category', 'price']
    );

    expect(result.toInsert).toHaveLength(1);
    expect(result.toUpdate).toHaveLength(0);
    expect(result.toInsert[0].is_current).toBe(true);
    expect(result.toInsert[0].valid_from).toBeDefined();
  });

  it('should create new version when dimension changes', () => {
    const existing = [
      {
        product_id: 'P001',
        product_name: 'Widget A',
        category: 'Electronics',
        price: 30.0,
        valid_from: new Date('2024-01-01'),
        valid_to: null,
        is_current: true,
      },
    ];

    const incoming = [
      {
        product_id: 'P001',
        product_name: 'Widget A',
        category: 'Electronics',
        price: 35.0,
      },
    ];

    const result = handler.mergeType2(
      existing,
      incoming,
      ['product_id'],
      ['product_name', 'category', 'price']
    );

    expect(result.toUpdate).toHaveLength(1);
    expect(result.toInsert).toHaveLength(1);
    expect(result.toUpdate[0].is_current).toBe(false);
    expect(result.toUpdate[0].valid_to).toBeDefined();
    expect(result.toInsert[0].is_current).toBe(true);
    expect(result.toInsert[0].price).toBe(35.0);
  });

  it('should not create new version when no changes detected', () => {
    const existing = [
      {
        product_id: 'P001',
        product_name: 'Widget A',
        category: 'Electronics',
        price: 30.0,
        valid_from: new Date('2024-01-01'),
        valid_to: null,
        is_current: true,
      },
    ];

    const incoming = [
      {
        product_id: 'P001',
        product_name: 'Widget A',
        category: 'Electronics',
        price: 30.0,
      },
    ];

    const result = handler.mergeType2(
      existing,
      incoming,
      ['product_id'],
      ['product_name', 'category', 'price']
    );

    expect(result.toUpdate).toHaveLength(0);
    expect(result.toInsert).toHaveLength(0);
  });

  it('should handle multiple records with mixed changes', () => {
    const existing = [
      {
        product_id: 'P001',
        product_name: 'Widget A',
        category: 'Electronics',
        price: 30.0,
        valid_from: new Date('2024-01-01'),
        valid_to: null,
        is_current: true,
      },
      {
        product_id: 'P002',
        product_name: 'Gadget B',
        category: 'Electronics',
        price: 75.0,
        valid_from: new Date('2024-01-01'),
        valid_to: null,
        is_current: true,
      },
    ];

    const incoming = [
      {
        product_id: 'P001',
        product_name: 'Widget A',
        category: 'Electronics',
        price: 30.0,
      },
      {
        product_id: 'P002',
        product_name: 'Gadget B Pro',
        category: 'Electronics',
        price: 85.0,
      },
      {
        product_id: 'P003',
        product_name: 'Device C',
        category: 'Electronics',
        price: 499.99,
      },
    ];

    const result = handler.mergeType2(
      existing,
      incoming,
      ['product_id'],
      ['product_name', 'category', 'price']
    );

    expect(result.toInsert).toHaveLength(2);
    expect(result.toUpdate).toHaveLength(1);
  });
});
