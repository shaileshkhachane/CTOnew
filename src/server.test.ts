import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { buildServer } from './server.js';
import { FastifyInstance } from 'fastify';

describe('Server API Integration', () => {
  let server: FastifyInstance;

  beforeAll(async () => {
    server = await buildServer();
    await server.ready();
  });

  afterAll(async () => {
    await server.close();
  });

  describe('Health and Info', () => {
    it('should return service info at root', async () => {
      const response = await server.inject({
        method: 'GET',
        url: '/',
      });

      expect(response.statusCode).toBe(200);
      const data = JSON.parse(response.body);
      expect(data.message).toBe('OLAP Backend Service');
      expect(data.version).toBe('1.0.0');
      expect(data.documentation).toBe('/docs');
    });

    it('should return health status', async () => {
      const response = await server.inject({
        method: 'GET',
        url: '/health',
      });

      expect(response.statusCode).toBe(200);
      const data = JSON.parse(response.body);
      expect(data.status).toBe('healthy');
      expect(data.cubesLoaded).toBe(1);
      expect(data.timestamp).toBeDefined();
    });
  });

  describe('Schema API', () => {
    it('should return list of cubes', async () => {
      const response = await server.inject({
        method: 'GET',
        url: '/schema/cubes',
      });

      expect(response.statusCode).toBe(200);
      const data = JSON.parse(response.body);
      expect(Array.isArray(data)).toBe(true);
      expect(data.length).toBe(1);
      expect(data[0].id).toBe('sales-cube');
      expect(data[0].dimensionCount).toBe(4);
      expect(data[0].measureCount).toBe(6);
      expect(data[0].hierarchyCount).toBe(3);
    });

    it('should return cube details', async () => {
      const response = await server.inject({
        method: 'GET',
        url: '/schema/cubes/sales-cube',
      });

      expect(response.statusCode).toBe(200);
      const data = JSON.parse(response.body);
      expect(data.id).toBe('sales-cube');
      expect(data.name).toBe('Sales Analytics Cube');
      expect(data.schemaType).toBe('STAR');
      expect(data.factTable).toBeDefined();
      expect(data.dimensions.length).toBe(4);
      expect(data.hierarchies.length).toBe(3);
    });

    it('should return 404 for non-existent cube', async () => {
      const response = await server.inject({
        method: 'GET',
        url: '/schema/cubes/non-existent',
      });

      expect(response.statusCode).toBe(404);
      const data = JSON.parse(response.body);
      expect(data.error).toBe('Cube not found');
    });

    it('should return dimension with SCD config', async () => {
      const response = await server.inject({
        method: 'GET',
        url: '/schema/cubes/sales-cube/dimensions/dim-customer',
      });

      expect(response.statusCode).toBe(200);
      const data = JSON.parse(response.body);
      expect(data.id).toBe('dim-customer');
      expect(data.name).toBe('Customer');
      expect(data.scdConfig).toBeDefined();
      expect(data.scdConfig.type).toBe('TYPE_2');
      expect(data.scdConfig.versionColumn).toBe('version');
      expect(data.scdConfig.trackedAttributes).toContain('city');
      expect(data.hierarchies).toBeDefined();
      expect(Array.isArray(data.hierarchies)).toBe(true);
    });

    it('should return dimension with SCD Type 1 config', async () => {
      const response = await server.inject({
        method: 'GET',
        url: '/schema/cubes/sales-cube/dimensions/dim-product',
      });

      expect(response.statusCode).toBe(200);
      const data = JSON.parse(response.body);
      expect(data.id).toBe('dim-product');
      expect(data.scdConfig).toBeDefined();
      expect(data.scdConfig.type).toBe('TYPE_1');
      expect(data.scdConfig.overwriteAttributes).toContain('price');
    });

    it('should return 404 for non-existent dimension', async () => {
      const response = await server.inject({
        method: 'GET',
        url: '/schema/cubes/sales-cube/dimensions/non-existent',
      });

      expect(response.statusCode).toBe(404);
      const data = JSON.parse(response.body);
      expect(data.error).toBe('Dimension not found');
    });

    it('should return hierarchy with sorted levels', async () => {
      const response = await server.inject({
        method: 'GET',
        url: '/schema/cubes/sales-cube/hierarchies/hier-calendar',
      });

      expect(response.statusCode).toBe(200);
      const data = JSON.parse(response.body);
      expect(data.id).toBe('hier-calendar');
      expect(data.name).toBe('Calendar Hierarchy');
      expect(data.dimensionId).toBe('dim-date');
      expect(data.levels.length).toBe(4);
      
      expect(data.levels[0].order).toBe(1);
      expect(data.levels[0].name).toBe('Year');
      expect(data.levels[1].order).toBe(2);
      expect(data.levels[1].name).toBe('Quarter');
      expect(data.levels[2].order).toBe(3);
      expect(data.levels[2].name).toBe('Month');
      expect(data.levels[3].order).toBe(4);
      expect(data.levels[3].name).toBe('Day');
    });

    it('should return 404 for non-existent hierarchy', async () => {
      const response = await server.inject({
        method: 'GET',
        url: '/schema/cubes/sales-cube/hierarchies/non-existent',
      });

      expect(response.statusCode).toBe(404);
      const data = JSON.parse(response.body);
      expect(data.error).toBe('Hierarchy not found');
    });
  });
});
