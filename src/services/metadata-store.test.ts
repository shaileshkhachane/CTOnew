import { describe, it, expect, beforeEach } from 'vitest';
import { MetadataStore } from './metadata-store.js';
import { Cube } from '../domain/index.js';

describe('MetadataStore', () => {
  let store: MetadataStore;

  const validCube: Cube = {
    id: 'test-cube',
    name: 'Test Cube',
    schemaType: 'STAR',
    factTable: {
      id: 'fact-test',
      name: 'Test Facts',
      tableName: 'fact_test',
      measures: [
        {
          id: 'measure-1',
          name: 'Count',
          aggregationType: 'COUNT',
          expression: '*',
        },
      ],
      dimensionReferences: [
        {
          dimensionId: 'dim-1',
          foreignKey: 'dim1_id',
        },
      ],
    },
    dimensions: [
      {
        id: 'dim-1',
        name: 'Dimension 1',
        tableName: 'dim_1',
        primaryKey: 'dim1_id',
        attributes: [
          {
            id: 'attr-1',
            name: 'Attribute 1',
            column: 'attr1',
            dataType: 'string',
          },
        ],
      },
    ],
    hierarchies: [],
  };

  beforeEach(() => {
    store = new MetadataStore();
  });

  describe('registerCube', () => {
    it('should register a valid cube', () => {
      expect(() => store.registerCube(validCube)).not.toThrow();
      const registered = store.getCube('test-cube');
      expect(registered).toBeDefined();
      expect(registered?.id).toBe('test-cube');
      expect(registered?.name).toBe('Test Cube');
    });

    it('should throw error for duplicate cube registration', () => {
      store.registerCube(validCube);
      expect(() => store.registerCube(validCube)).toThrow(
        "Cube with id 'test-cube' is already registered"
      );
    });

    it('should throw error for invalid dimension reference', () => {
      const invalidCube: Cube = {
        ...validCube,
        id: 'invalid-cube',
        factTable: {
          ...validCube.factTable,
          dimensionReferences: [
            {
              dimensionId: 'non-existent-dim',
              foreignKey: 'foreign_key',
            },
          ],
        },
      };

      expect(() => store.registerCube(invalidCube)).toThrow(
        "references unknown dimension 'non-existent-dim'"
      );
    });

    it('should validate schema using Zod', () => {
      const invalidCube = {
        id: 'invalid',
        name: 'Invalid',
        schemaType: 'INVALID_TYPE',
        factTable: validCube.factTable,
        dimensions: validCube.dimensions,
      };

      expect(() => store.registerCube(invalidCube as Cube)).toThrow();
    });
  });

  describe('hierarchy validation', () => {
    it('should register cube with valid hierarchy', () => {
      const cubeWithHierarchy: Cube = {
        ...validCube,
        id: 'cube-with-hierarchy',
        hierarchies: [
          {
            id: 'hier-1',
            name: 'Hierarchy 1',
            dimensionId: 'dim-1',
            levels: [
              { id: 'level-1', name: 'Level 1', column: 'col1', order: 1 },
              { id: 'level-2', name: 'Level 2', column: 'col2', order: 2 },
            ],
          },
        ],
      };

      expect(() => store.registerCube(cubeWithHierarchy)).not.toThrow();
    });

    it('should throw error for hierarchy with unknown dimension', () => {
      const cubeWithInvalidHierarchy: Cube = {
        ...validCube,
        id: 'cube-invalid-hier',
        hierarchies: [
          {
            id: 'hier-1',
            name: 'Hierarchy 1',
            dimensionId: 'unknown-dim',
            levels: [
              { id: 'level-1', name: 'Level 1', column: 'col1', order: 1 },
            ],
          },
        ],
      };

      expect(() => store.registerCube(cubeWithInvalidHierarchy)).toThrow(
        "Hierarchy 'hier-1' references unknown dimension 'unknown-dim'"
      );
    });

    it('should throw error for hierarchy with duplicate level orders', () => {
      const cubeWithDuplicateOrders: Cube = {
        ...validCube,
        id: 'cube-dup-orders',
        hierarchies: [
          {
            id: 'hier-1',
            name: 'Hierarchy 1',
            dimensionId: 'dim-1',
            levels: [
              { id: 'level-1', name: 'Level 1', column: 'col1', order: 1 },
              { id: 'level-2', name: 'Level 2', column: 'col2', order: 1 },
            ],
          },
        ],
      };

      expect(() => store.registerCube(cubeWithDuplicateOrders)).toThrow(
        "Hierarchy 'hier-1' has duplicate level orders"
      );
    });

    it('should throw error for hierarchy with non-consecutive ordering', () => {
      const cubeWithGaps: Cube = {
        ...validCube,
        id: 'cube-gaps',
        hierarchies: [
          {
            id: 'hier-1',
            name: 'Hierarchy 1',
            dimensionId: 'dim-1',
            levels: [
              { id: 'level-1', name: 'Level 1', column: 'col1', order: 1 },
              { id: 'level-2', name: 'Level 2', column: 'col2', order: 3 },
            ],
          },
        ],
      };

      expect(() => store.registerCube(cubeWithGaps)).toThrow(
        "Hierarchy 'hier-1' has non-consecutive level ordering"
      );
    });
  });

  describe('query methods', () => {
    beforeEach(() => {
      const cubeWithHierarchy: Cube = {
        ...validCube,
        hierarchies: [
          {
            id: 'hier-1',
            name: 'Hierarchy 1',
            dimensionId: 'dim-1',
            levels: [
              { id: 'level-1', name: 'Level 1', column: 'col1', order: 1 },
            ],
          },
        ],
      };
      store.registerCube(cubeWithHierarchy);
    });

    it('should return all cubes', () => {
      const cubes = store.getAllCubes();
      expect(cubes).toHaveLength(1);
      expect(cubes[0].id).toBe('test-cube');
    });

    it('should return undefined for non-existent cube', () => {
      expect(store.getCube('non-existent')).toBeUndefined();
    });

    it('should return dimension by id', () => {
      const dimension = store.getDimension('test-cube', 'dim-1');
      expect(dimension).toBeDefined();
      expect(dimension?.id).toBe('dim-1');
    });

    it('should return undefined for non-existent dimension', () => {
      expect(store.getDimension('test-cube', 'non-existent')).toBeUndefined();
    });

    it('should return hierarchy by id', () => {
      const hierarchy = store.getHierarchy('test-cube', 'hier-1');
      expect(hierarchy).toBeDefined();
      expect(hierarchy?.id).toBe('hier-1');
    });

    it('should return hierarchies by dimension', () => {
      const hierarchies = store.getHierarchiesByDimension('test-cube', 'dim-1');
      expect(hierarchies).toHaveLength(1);
      expect(hierarchies[0].id).toBe('hier-1');
    });

    it('should return empty array for dimension with no hierarchies', () => {
      const hierarchies = store.getHierarchiesByDimension('test-cube', 'non-existent');
      expect(hierarchies).toEqual([]);
    });
  });
});
