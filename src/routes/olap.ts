import { Router } from 'express';
import { CubeError, CubeManager } from '../engine/cube-manager';
import { querySchema } from '../engine/query-contract';

export function createOlapRouter(manager: CubeManager) {
  const router = Router();

  router.post('/query', (req, res, next) => {
    const validation = querySchema.safeParse(req.body);
    if (!validation.success) {
      return res.status(400).json({
        error: 'Invalid query payload',
        details: validation.error.flatten()
      });
    }

    try {
      const result = manager.execute(validation.data);
      return res.json(result);
    } catch (error) {
      if (error instanceof CubeError) {
        return res.status(error.status).json({ error: error.message });
      }
      return next(error);
    }
  });

  return router;
}
