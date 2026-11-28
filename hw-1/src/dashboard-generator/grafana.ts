const URL = `${process.env.GRAFANA_URL ?? "http://localhost:3000"}/api/dashboards/db`;

export async function saveDashboard(dashboard: object): Promise<void> {
  const username = process.env.GRAFANA_ADMIN_USER;
  const password = process.env.GRAFANA_ADMIN_PASSWORD;

  if (!username || !password) {
    throw new Error("Grafana credentials are missing");
  }

  const credentials = Buffer.from(`${username}:${password}`).toString("base64");

  const response = await fetch(URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Basic ${credentials}`,
    },
    body: JSON.stringify({
      dashboard,
      overwrite: true,
    }),
  });

  if (!response.ok) {
    throw new Error(
      `Grafana API error: ${response.status} ${await response.text()}`,
    );
  }
}
