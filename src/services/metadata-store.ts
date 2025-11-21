import { readFileSync } from 'fs';
import { resolve } from 'path';
import { Cube, CubeSchema, Dimension, Hierarchy } from '../domain/index.js';

export class MetadataStore {
  private cubes: Map<string, Cube> = new Map();

  registerCube(cube: Cube): void {
    const validatedCube = CubeSchema.parse(cube);
    
    if (this.cubes.has(validatedCube.id)) {
      throw new Error(`Cube with id '${validatedCube.id}' is already registered`);
    }

    this.validateDimensionReferences(validatedCube);
    this.validateHierarchyReferences(validatedCube);
    this.validateHierarchyOrdering(validatedCube);

    this.cubes.set(validatedCube.id, validatedCube);
  }

  private validateDimensionReferences(cube: Cube): void {
    const dimensionIds = new Set(cube.dimensions.map(d => d.id));
    
    for (const ref of cube.factTable.dimensionReferences) {
      if (!dimensionIds.has(ref.dimensionId)) {
        throw new Error(
          `Fact table '${cube.factTable.id}' references unknown dimension '${ref.dimensionId}'`
        );
      }
    }
  }

  private validateHierarchyReferences(cube: Cube): void {
    const dimensionIds = new Set(cube.dimensions.map(d => d.id));
    
    for (const hierarchy of cube.hierarchies) {
      if (!dimensionIds.has(hierarchy.dimensionId)) {
        throw new Error(
          `Hierarchy '${hierarchy.id}' references unknown dimension '${hierarchy.dimensionId}'`
        );
      }
    }
  }

  private validateHierarchyOrdering(cube: Cube): void {
    for (const hierarchy of cube.hierarchies) {
      const orders = hierarchy.levels.map(l => l.order);
      const sortedOrders = [...orders].sort((a, b) => a - b);
      
      const hasGaps = sortedOrders.some((order, idx) => {
        if (idx === 0) return false;
        return order !== sortedOrders[idx - 1] && order !== sortedOrders[idx - 1] + 1;
      });

      if (hasGaps) {
        throw new Error(
          `Hierarchy '${hierarchy.id}' has non-consecutive level ordering`
        );
      }

      const hasDuplicates = orders.length !== new Set(orders).size;
      if (hasDuplicates) {
        throw new Error(
          `Hierarchy '${hierarchy.id}' has duplicate level orders`
        );
      }
    }
  }

  getCube(id: string): Cube | undefined {
    return this.cubes.get(id);
  }

  getAllCubes(): Cube[] {
    return Array.from(this.cubes.values());
  }

  getDimension(cubeId: string, dimensionId: string): Dimension | undefined {
    const cube = this.cubes.get(cubeId);
    if (!cube) return undefined;
    
    return cube.dimensions.find(d => d.id === dimensionId);
  }

  getHierarchy(cubeId: string, hierarchyId: string): Hierarchy | undefined {
    const cube = this.cubes.get(cubeId);
    if (!cube) return undefined;
    
    return cube.hierarchies.find(h => h.id === hierarchyId);
  }

  getHierarchiesByDimension(cubeId: string, dimensionId: string): Hierarchy[] {
    const cube = this.cubes.get(cubeId);
    if (!cube) return [];
    
    return cube.hierarchies.filter(h => h.dimensionId === dimensionId);
  }

  loadFromFile(filePath: string): void {
    try {
      const content = readFileSync(filePath, 'utf-8');
      const data = JSON.parse(content);
      this.registerCube(data);
    } catch (error) {
      if (error instanceof Error) {
        throw new Error(`Failed to load cube from ${filePath}: ${error.message}`);
      }
      throw error;
    }
  }

  static async createFromDataDirectory(dataDir: string): Promise<MetadataStore> {
    const store = new MetadataStore();
    const schemaPath = resolve(dataDir, 'schema.json');
    store.loadFromFile(schemaPath);
    return store;
  }
}
