import { FastifyInstance, FastifyRequest } from 'fastify';
import { MetadataStore } from '../services/metadata-store.js';

interface DimensionParams {
  cubeId: string;
  dimensionId: string;
}

interface HierarchyParams {
  cubeId: string;
  hierarchyId: string;
}

export async function schemaRoutes(
  fastify: FastifyInstance,
  metadataStore: MetadataStore
) {
  fastify.get('/schema/cubes', {
    schema: {
      description: 'Get all registered cubes',
      tags: ['schema'],
      response: {
        200: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              id: { type: 'string' },
              name: { type: 'string' },
              description: { type: 'string' },
              schemaType: { type: 'string', enum: ['STAR', 'SNOWFLAKE'] },
              dimensionCount: { type: 'number' },
              measureCount: { type: 'number' },
              hierarchyCount: { type: 'number' },
            },
          },
        },
      },
    },
  }, async (_request, reply) => {
    const cubes = metadataStore.getAllCubes();
    const summary = cubes.map(cube => ({
      id: cube.id,
      name: cube.name,
      description: cube.description,
      schemaType: cube.schemaType,
      dimensionCount: cube.dimensions.length,
      measureCount: cube.factTable.measures.length,
      hierarchyCount: cube.hierarchies.length,
    }));
    return reply.send(summary);
  });

  fastify.get('/schema/cubes/:cubeId', {
    schema: {
      description: 'Get detailed cube definition',
      tags: ['schema'],
      params: {
        type: 'object',
        properties: {
          cubeId: { type: 'string' },
        },
        required: ['cubeId'],
      },
    },
  }, async (request: FastifyRequest<{ Params: { cubeId: string } }>, reply) => {
    const { cubeId } = request.params;
    const cube = metadataStore.getCube(cubeId);
    
    if (!cube) {
      return reply.code(404).send({ error: 'Cube not found' });
    }
    
    return reply.send(cube);
  });

  fastify.get('/schema/cubes/:cubeId/dimensions/:dimensionId', {
    schema: {
      description: 'Get dimension definition with SCD configuration',
      tags: ['schema'],
      params: {
        type: 'object',
        properties: {
          cubeId: { type: 'string' },
          dimensionId: { type: 'string' },
        },
        required: ['cubeId', 'dimensionId'],
      },
    },
  }, async (request: FastifyRequest<{ Params: DimensionParams }>, reply) => {
    const { cubeId, dimensionId } = request.params;
    const dimension = metadataStore.getDimension(cubeId, dimensionId);
    
    if (!dimension) {
      return reply.code(404).send({ error: 'Dimension not found' });
    }
    
    const hierarchies = metadataStore.getHierarchiesByDimension(cubeId, dimensionId);
    
    return reply.send({
      ...dimension,
      hierarchies: hierarchies.map(h => ({
        id: h.id,
        name: h.name,
        description: h.description,
      })),
    });
  });

  fastify.get('/schema/cubes/:cubeId/hierarchies/:hierarchyId', {
    schema: {
      description: 'Get hierarchy definition with ordered levels',
      tags: ['schema'],
      params: {
        type: 'object',
        properties: {
          cubeId: { type: 'string' },
          hierarchyId: { type: 'string' },
        },
        required: ['cubeId', 'hierarchyId'],
      },
    },
  }, async (request: FastifyRequest<{ Params: HierarchyParams }>, reply) => {
    const { cubeId, hierarchyId } = request.params;
    const hierarchy = metadataStore.getHierarchy(cubeId, hierarchyId);
    
    if (!hierarchy) {
      return reply.code(404).send({ error: 'Hierarchy not found' });
    }
    
    const sortedLevels = [...hierarchy.levels].sort((a, b) => a.order - b.order);
    
    return reply.send({
      ...hierarchy,
      levels: sortedLevels,
    });
  });
}
