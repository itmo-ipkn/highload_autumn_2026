import express, {
  type NextFunction,
  type Request,
  type RequestHandler,
  type Response,
} from "express";
import { createRequestMetrics, type InstanceIdentity } from "./metrics";

type Item = {
  id: number;
  name: string;
};

type HandlerDescription = {
  method: "GET" | "POST" | "PUT";
  path: string;
};

function toTemplatePath(path: string): string {
  return path.replace(/:([A-Za-z_][A-Za-z0-9_]*)/g, "{$1}");
}

function parseItemId(raw: string | string[] | undefined): number | null {
  if (typeof raw !== "string" || !/^[1-9]\d*$/.test(raw)) {
    return null;
  }

  const id = Number(raw);
  return Number.isSafeInteger(id) ? id : null;
}

function parseItemName(body: unknown): string | null {
  if (typeof body !== "object" || body === null || Array.isArray(body)) {
    return null;
  }

  const name = (body as Record<string, unknown>).name;
  return typeof name === "string" && name.trim().length > 0 ? name.trim() : null;
}

export function createApp(identity: InstanceIdentity, demoFailures = false) {
  const app = express();
  const items = new Map<number, Item>();
  const metrics = createRequestMetrics(identity);
  const parseJson = express.json();
  const handlers: HandlerDescription[] = [];
  let nextId = 1;

  const simulateFailure: RequestHandler = (request, response, next) => {
    if (request.headers["x-demo-failure"] === "1") {
      response.status(500).json({ error: "simulated failure" });
      return;
    }
    next();
  };

  function registerHandler(
    method: HandlerDescription["method"],
    path: string,
    ...callbacks: RequestHandler[]
  ) {
    const routeCallbacks = demoFailures ? [simulateFailure, ...callbacks] : callbacks;

    switch (method) {
      case "GET":
        app.get(path, ...routeCallbacks);
        break;
      case "POST":
        app.post(path, ...routeCallbacks);
        break;
      case "PUT":
        app.put(path, ...routeCallbacks);
        break;
    }
    handlers.push({ method, path: toTemplatePath(path) });
  }

  app.disable("x-powered-by");
  app.use((request, response, next) => {
    const startedAt = process.hrtime.bigint();

    response.once("finish", () => {
      const matchedPath = request.route?.path;
      if (
        typeof matchedPath !== "string" ||
        matchedPath === "/health" ||
        matchedPath === "/metrics"
      ) {
        return;
      }

      const route = toTemplatePath(matchedPath);
      const method = request.method;
      metrics.requests.inc({ method, route, status_code: String(response.statusCode) });
      metrics.duration.observe(
        { method, route },
        Number(process.hrtime.bigint() - startedAt) / 1e9,
      );
    });

    next();
  });

  app.get("/health", (_request, response) => {
    response.status(200).json({ status: "ok" });
  });

  app.get("/metrics", async (_request, response) => {
    response.setHeader("Content-Type", metrics.registry.contentType);
    response.send(await metrics.registry.metrics());
  });

  app.get("/handlers", (_request, response) => {
    response.json({ handlers });
  });

  registerHandler("POST", "/items", parseJson, (request, response) => {
    const name = parseItemName(request.body);
    if (name === null) {
      response.status(400).json({ error: "name must be a non-empty string" });
      return;
    }

    const item: Item = { id: nextId++, name };
    items.set(item.id, item);
    response.location(`/items/${item.id}`).status(201).json(item);
  });

  registerHandler("GET", "/items/:id", (request, response) => {
    const id = parseItemId(request.params.id);
    if (id === null) {
      response.status(400).json({ error: "id must be a positive integer" });
      return;
    }

    const item = items.get(id);
    if (item === undefined) {
      response.status(404).json({ error: "item not found" });
      return;
    }

    response.json(item);
  });

  registerHandler("PUT", "/items/:id", parseJson, (request, response) => {
    const id = parseItemId(request.params.id);
    if (id === null) {
      response.status(400).json({ error: "id must be a positive integer" });
      return;
    }

    if (!items.has(id)) {
      response.status(404).json({ error: "item not found" });
      return;
    }

    const name = parseItemName(request.body);
    if (name === null) {
      response.status(400).json({ error: "name must be a non-empty string" });
      return;
    }

    const item: Item = { id, name };
    items.set(id, item);
    response.json(item);
  });

  app.use((error: unknown, _request: Request, response: Response, next: NextFunction) => {
    if (
      typeof error === "object" &&
      error !== null &&
      "type" in error &&
      error.type === "entity.parse.failed"
    ) {
      response.status(400).json({ error: "invalid JSON" });
      return;
    }

    next(error);
  });

  return app;
}
