import { Counter, Histogram, Registry } from "@prometheus-io/client";
import type { FastifyInstance } from "fastify";

export interface InstanceIdentity {
  podId: string;
  datacenter: string;
}

export function readIdentity(env: NodeJS.ProcessEnv): InstanceIdentity {
  function required(name: string): string {
    const value = env[name]?.trim();
    if (!value) throw new Error(`${name} must be non-empty`);
    return value;
  }
  return { podId: required("POD_ID"), datacenter: required("DATACENTER") };
}

const key = (method: string, path: string) => `${method} ${path}`;

export function createMonitoring(identity: InstanceIdentity) {
  const registry = new Registry();

  const labelNames = ["method", "route", "pod_id", "datacenter"] as const;

  const requests = new Counter({
    name: "http_requests_total",
    help: "Completed business HTTP responses",
    labelNames: [...labelNames, "status_code"],
    registers: [registry],
  });

  const duration = new Histogram({
    name: "http_request_duration_seconds",
    help: "Server duration of completed business HTTP requests in seconds",
    labelNames,
    buckets: [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5],
    registers: [registry],
  });

  const handlers = new Map<string, { method: string; path: string }>();

  return {
    registerBusinessHooks(app: FastifyInstance) {
      app.addHook("onRoute", (route) => {
        const methodsArray = Array.isArray(route.method)
          ? route.method
          : [route.method];

        for (const method of methodsArray) {
          handlers.set(key(method, route.url), { method, path: route.url });
        }
      });

      app.addHook("onResponse", async (request, reply) => {
        const route = request.routeOptions.url;

        if (!route || !handlers.has(key(request.method, route))) {
          return;
        }

        const labels = {
          method: request.method,
          route,
          pod_id: identity.podId,
          datacenter: identity.datacenter,
        };

        requests.inc({ ...labels, status_code: String(reply.statusCode) });
        duration.observe(labels, reply.elapsedTime / 1000);
      });
    },
    registerEndpoints(app: FastifyInstance) {
      app.get("/metrics", async (_request, reply) => {
        return reply.type(registry.contentType).send(await registry.metrics());
      });
      app.get("/handlers", async () => ({
        handlers: [...handlers.values()].sort((a, b) =>
          key(a.method, a.path).localeCompare(key(b.method, b.path)),
        ),
      }));
    },
  };
}
