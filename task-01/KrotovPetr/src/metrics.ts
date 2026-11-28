import { Counter, Histogram, Registry } from "@prometheus-io/client";

export type InstanceIdentity = {
  podId: string;
  datacenter: string;
};

export function createRequestMetrics(identity: InstanceIdentity) {
  const registry = new Registry();
  registry.setDefaultLabels({
    pod_id: identity.podId,
    datacenter: identity.datacenter,
  });

  const requests = new Counter({
    name: "http_requests_total",
    help: "Total HTTP requests handled by an application route",
    labelNames: ["method", "route", "status_code"] as const,
    registers: [registry],
  });

  const duration = new Histogram({
    name: "http_request_duration_seconds",
    help: "HTTP request duration in seconds",
    labelNames: ["method", "route"] as const,
    buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
    registers: [registry],
  });

  return { registry, requests, duration };
}
