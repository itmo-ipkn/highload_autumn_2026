import { createApp } from "./app";

const port = Number(process.env.PORT ?? "8080");
if (!Number.isInteger(port) || port < 1 || port > 65535) {
  throw new Error("PORT must be an integer between 1 and 65535");
}

const podId = process.env.POD_ID;
const datacenter = process.env.DATACENTER;
if (!podId || !datacenter) {
  throw new Error("POD_ID and DATACENTER must be set");
}

createApp({ podId, datacenter }, process.env.DEMO_FAILURES === "1").listen(
  port,
  "0.0.0.0",
  () => {
    console.log(`Items API is listening on port ${port}`);
  },
);
