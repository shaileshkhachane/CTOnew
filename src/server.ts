import Fastify from 'fastify';
import swagger from '@fastify/swagger';
import swaggerUi from '@fastify/swagger-ui';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import { MetadataStore } from './services/metadata-store.js';
import { schemaRoutes } from './routes/schema-routes.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const PORT = process.env.PORT ? parseInt(process.env.PORT) : 3000;
const HOST = process.env.HOST || '0.0.0.0';

async function buildServer() {
  const fastify = Fastify({
    logger: {
      level: process.env.LOG_LEVEL || 'info',
    },
  });

  await fastify.register(swagger, {
    openapi: {
      info: {
        title: 'OLAP Backend API',
        description: 'REST API for OLAP cube metadata with star schema support',
        version: '1.0.0',
      },
      servers: [
        {
          url: `http://localhost:${PORT}`,
          description: 'Development server',
        },
      ],
      tags: [
        {
          name: 'schema',
          description: 'Schema and metadata endpoints',
        },
      ],
    },
  });

  await fastify.register(swaggerUi, {
    routePrefix: '/docs',
    uiConfig: {
      docExpansion: 'list',
      deepLinking: true,
    },
  });

  const dataDir = resolve(__dirname, '../data');
  const metadataStore = await MetadataStore.createFromDataDirectory(dataDir);

  fastify.get('/', async (_request, reply) => {
    return reply.send({
      message: 'OLAP Backend Service',
      version: '1.0.0',
      documentation: '/docs',
    });
  });

  fastify.get('/health', async (_request, reply) => {
    return reply.send({
      status: 'healthy',
      timestamp: new Date().toISOString(),
      cubesLoaded: metadataStore.getAllCubes().length,
    });
  });

  await schemaRoutes(fastify, metadataStore);

  return fastify;
}

async function start() {
  try {
    const fastify = await buildServer();
    await fastify.listen({ port: PORT, host: HOST });
    
    fastify.log.info(`Server listening on http://${HOST}:${PORT}`);
    fastify.log.info(`API Documentation available at http://${HOST}:${PORT}/docs`);
  } catch (err) {
    console.error('Error starting server:', err);
    process.exit(1);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  start();
}

export { buildServer };
