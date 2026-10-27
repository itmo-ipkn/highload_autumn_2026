export interface Handler {
  method: string;
  path: string;
}

interface HandlersResponse {
  handlers: Handler[];
}

export async function getHandlers(): Promise<Handler[]> {
  const port = process.env.API_1_PORT;

  if (!port) {
    throw new Error("API_1_PORT is not set");
  }

  const response = await fetch(`http://localhost:${port}/handlers`);

  if (!response.ok) {
    throw new Error(`Failed to fetch handlers: ${response.status}`);
  }

  const data = (await response.json()) as HandlersResponse;

  return data.handlers;
}
