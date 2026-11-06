export interface Handler {
  method: string;
  path: string;
}

interface HandlersResponse {
  handlers: Handler[];
}

export async function getHandlers(): Promise<Handler[]> {
  const port = process.env.API_1_PORT;
  const baseUrl = process.env.API_URL ?? (port ? `http://localhost:${port}` : undefined);

  if (!baseUrl) {
    throw new Error("API_URL or API_1_PORT must be set");
  }

  const response = await fetch(`${baseUrl}/handlers`);

  if (!response.ok) {
    throw new Error(`Failed to fetch handlers: ${response.status}`);
  }

  const data = (await response.json()) as HandlersResponse;

  return data.handlers;
}
