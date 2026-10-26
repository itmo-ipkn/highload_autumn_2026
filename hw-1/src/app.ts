import Fastify from 'fastify';
import { productRoutes } from './products.ts';

export function createApp(logger = false) {
  const app = Fastify({
    logger,
    exposeHeadRoutes: false,
    ajv: { customOptions: { coerceTypes: false, removeAdditional: false, useDefaults: false } },
  });
  app.get('/health', async () => ({ status: 'ok' }));
  app.register(productRoutes);
  return app;
}
