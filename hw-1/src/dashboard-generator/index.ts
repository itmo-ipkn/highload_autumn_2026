import { getHandlers } from "./handlers.js";
import { createDashboard } from "./dashboard.js";
import { saveDashboard } from "./grafana.js";

const handlers = await getHandlers();
const dashboard = createDashboard(handlers);

await saveDashboard(dashboard);

console.log(`Dashboard generated for ${handlers.length} handlers`);
