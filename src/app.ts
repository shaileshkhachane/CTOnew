import express, { ErrorRequestHandler } from 'express';
import { createOlapRouter } from './routes/olap';
import { CubeManager } from './engine/cube-manager';
import { sampleCubeManager } from './engine/sample-cube';

export function createApp(manager: CubeManager = sampleCubeManager) {
  const app = express();

  app.use(express.json({ limit: '1mb' }));
  app.use('/olap', createOlapRouter(manager));

  app.get('/health', (_req, res) => {
    res.json({ status: 'ok' });
  });

  const errorHandler: ErrorRequestHandler = (err, _req, res, _next) => {
    // eslint-disable-next-line no-console
    console.error(err);
    res.status(500).json({ error: 'Unexpected server error' });
  };

  app.use(errorHandler);

  return app;
}
