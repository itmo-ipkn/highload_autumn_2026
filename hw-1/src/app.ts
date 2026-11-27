import Fastify from "fastify";
import { productRoutes } from "./products.ts";
import type { FastifyPluginAsync } from "fastify";
import { createMonitoring, type InstanceIdentity } from "./monitoring.ts";

export function createApp(
  identity: InstanceIdentity,
  options: {
    logger?: boolean;
    businessRoutes?: FastifyPluginAsync;
  } = {},
) {
  const monitoring = createMonitoring(identity);

  const app = Fastify({
    exposeHeadRoutes: false,
    logger: true,
  });

  app.get("/health", async () => ({ status: "ok" }));

  monitoring.registerEndpoints(app);

  app.register(async (business) => {
    monitoring.registerBusinessHooks(business);
    business.register(options.businessRoutes ?? productRoutes);
  });

  return app;
}
