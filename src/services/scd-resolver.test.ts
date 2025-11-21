import { describe, it, expect, beforeEach } from 'vitest';
import { SCDResolver, SCDVersionRecord } from './scd-resolver.js';
import { Dimension } from '../domain/index.js';

describe('SCDResolver', () => {
  let resolver: SCDResolver;

  beforeEach(() => {
    resolver = new SCDResolver();
  });

  describe('Type 1 SCD', () => {
    const type1Dimension: Dimension = {
      id: 'dim-product',
      name: 'Product',
      tableName: 'dim_product',
      primaryKey: 'product_id',
      attributes: [
        { id: 'attr-id', name: 'Product ID', column: 'product_id', dataType: 'string' },
        { id: 'attr-price', name: 'Price', column: 'price', dataType: 'number' },
      ],
      scdConfig: {
        type: 'TYPE_1',
        overwriteAttributes: ['price'],
      },
    };

    it('should return records as-is for Type 1', () => {
      const records: SCDVersionRecord[] = [
        { product_id: 'P1', name: 'Product 1', price: 100 },
        { product_id: 'P2', name: 'Product 2', price: 200 },
      ];

      const result = resolver.resolveCurrentVersion(type1Dimension, records);
      expect(result).toEqual(records);
    });

    it('should merge Type 1 history by primary key', () => {
      const records: SCDVersionRecord[] = [
        { product_id: 'P1', name: 'Product 1', price: 100 },
        { product_id: 'P2', name: 'Product 2', price: 200 },
      ];

      const merged = resolver.mergeVersionHistory(type1Dimension, records);
      expect(merged.size).toBe(2);
      expect(merged.get('P1')).toEqual([records[0]]);
      expect(merged.get('P2')).toEqual([records[1]]);
    });
  });

  describe('Type 2 SCD', () => {
    const type2Dimension: Dimension = {
      id: 'dim-customer',
      name: 'Customer',
      tableName: 'dim_customer',
      primaryKey: 'customer_id',
      attributes: [
        { id: 'attr-id', name: 'Customer ID', column: 'customer_id', dataType: 'string' },
        { id: 'attr-city', name: 'City', column: 'city', dataType: 'string' },
      ],
      scdConfig: {
        type: 'TYPE_2',
        versionColumn: 'version',
        startDateColumn: 'effective_date',
        endDateColumn: 'expiration_date',
        currentFlagColumn: 'is_current',
        trackedAttributes: ['city', 'state'],
      },
    };

    describe('resolveCurrentVersion', () => {
      it('should return only current records', () => {
        const records: SCDVersionRecord[] = [
          { customer_id: 'C1-v1', name: 'John', city: 'NYC', is_current: false },
          { customer_id: 'C1-v2', name: 'John', city: 'LA', is_current: true },
          { customer_id: 'C2-v1', name: 'Jane', city: 'SF', is_current: true },
        ];

        const result = resolver.resolveCurrentVersion(type2Dimension, records);
        expect(result).toHaveLength(2);
        expect(result.map(r => r.customer_id)).toEqual(['C1-v2', 'C2-v1']);
      });

      it('should handle numeric current flag', () => {
        const records: SCDVersionRecord[] = [
          { customer_id: 'C1-v1', name: 'John', city: 'NYC', is_current: 0 },
          { customer_id: 'C1-v2', name: 'John', city: 'LA', is_current: 1 },
        ];

        const result = resolver.resolveCurrentVersion(type2Dimension, records);
        expect(result).toHaveLength(1);
        expect(result[0].customer_id).toBe('C1-v2');
      });

      it('should handle string current flag', () => {
        const records: SCDVersionRecord[] = [
          { customer_id: 'C1-v1', name: 'John', city: 'NYC', is_current: '0' },
          { customer_id: 'C1-v2', name: 'John', city: 'LA', is_current: '1' },
        ];

        const result = resolver.resolveCurrentVersion(type2Dimension, records);
        expect(result).toHaveLength(1);
        expect(result[0].customer_id).toBe('C1-v2');
      });
    });

    describe('resolveVersionAtDate', () => {
      it('should return version valid at specific date', () => {
        const records: SCDVersionRecord[] = [
          {
            customer_id: 'C1-v1',
            name: 'John',
            city: 'NYC',
            effective_date: '2020-01-01',
            expiration_date: '2022-06-01',
            is_current: false,
          },
          {
            customer_id: 'C1-v2',
            name: 'John',
            city: 'LA',
            effective_date: '2022-06-01',
            expiration_date: '9999-12-31',
            is_current: true,
          },
        ];

        const result = resolver.resolveVersionAtDate(
          type2Dimension,
          records,
          new Date('2021-01-01')
        );
        expect(result).toHaveLength(1);
        expect(result[0].customer_id).toBe('C1-v1');
        expect(result[0].city).toBe('NYC');
      });

      it('should handle null expiration date as open-ended', () => {
        const records: SCDVersionRecord[] = [
          {
            customer_id: 'C1-v1',
            name: 'John',
            city: 'NYC',
            effective_date: '2020-01-01',
            expiration_date: null,
            is_current: true,
          },
        ];

        const result = resolver.resolveVersionAtDate(
          type2Dimension,
          records,
          new Date()
        );
        expect(result).toHaveLength(1);
        expect(result[0].customer_id).toBe('C1-v1');
      });

      it('should return multiple records for different natural keys', () => {
        const records: SCDVersionRecord[] = [
          {
            customer_id: 'C1-v1',
            name: 'John',
            effective_date: '2020-01-01',
            expiration_date: '9999-12-31',
            is_current: true,
          },
          {
            customer_id: 'C2-v1',
            name: 'Jane',
            effective_date: '2020-01-01',
            expiration_date: '9999-12-31',
            is_current: true,
          },
        ];

        const result = resolver.resolveVersionAtDate(
          type2Dimension,
          records,
          new Date('2021-01-01')
        );
        expect(result).toHaveLength(2);
      });
    });

    describe('mergeVersionHistory', () => {
      it('should group versions by natural key', () => {
        const records: SCDVersionRecord[] = [
          {
            customer_id: 'C1-v1',
            name: 'John',
            city: 'NYC',
            effective_date: '2020-01-01',
            is_current: false,
          },
          {
            customer_id: 'C1-v2',
            name: 'John',
            city: 'LA',
            effective_date: '2022-06-01',
            is_current: true,
          },
          {
            customer_id: 'C2-v1',
            name: 'Jane',
            city: 'SF',
            effective_date: '2021-01-01',
            is_current: true,
          },
        ];

        const merged = resolver.mergeVersionHistory(type2Dimension, records);
        expect(merged.size).toBe(2);
        expect(merged.get('C1')?.length).toBe(2);
        expect(merged.get('C2')?.length).toBe(1);
      });

      it('should sort versions by effective date', () => {
        const records: SCDVersionRecord[] = [
          {
            customer_id: 'C1-v2',
            city: 'LA',
            effective_date: '2022-06-01',
            is_current: true,
          },
          {
            customer_id: 'C1-v1',
            city: 'NYC',
            effective_date: '2020-01-01',
            is_current: false,
          },
          {
            customer_id: 'C1-v3',
            city: 'SF',
            effective_date: '2023-01-01',
            is_current: false,
          },
        ];

        const merged = resolver.mergeVersionHistory(type2Dimension, records);
        const c1Versions = merged.get('C1')!;
        
        expect(c1Versions[0].customer_id).toBe('C1-v1');
        expect(c1Versions[1].customer_id).toBe('C1-v2');
        expect(c1Versions[2].customer_id).toBe('C1-v3');
      });
    });
  });

  describe('No SCD configuration', () => {
    const noDimension: Dimension = {
      id: 'dim-simple',
      name: 'Simple',
      tableName: 'dim_simple',
      primaryKey: 'id',
      attributes: [
        { id: 'attr-id', name: 'ID', column: 'id', dataType: 'string' },
      ],
    };

    it('should return all records when no SCD config', () => {
      const records: SCDVersionRecord[] = [
        { id: '1', name: 'Item 1' },
        { id: '2', name: 'Item 2' },
      ];

      const result = resolver.resolveCurrentVersion(noDimension, records);
      expect(result).toEqual(records);
    });

    it('should merge by primary key when no SCD config', () => {
      const records: SCDVersionRecord[] = [
        { id: '1', name: 'Item 1' },
        { id: '2', name: 'Item 2' },
      ];

      const merged = resolver.mergeVersionHistory(noDimension, records);
      expect(merged.size).toBe(2);
    });
  });
});
