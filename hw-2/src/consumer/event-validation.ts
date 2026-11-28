import { eventTypes, type UserEvent } from "../shared/event.js";

export function assertUserEvent(value: unknown): asserts value is UserEvent {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw Error("Invalid event schema");
  }

  const event = value as Record<string, unknown>;
  if (
    typeof event.event_id !== "string" ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(event.event_id) ||
    typeof event.user_id !== "string" ||
    !event.user_id ||
    !eventTypes.some((type) => type === event.type) ||
    typeof event.seq !== "number" ||
    !Number.isSafeInteger(event.seq) ||
    event.seq < 1 ||
    typeof event.produced_at !== "string" ||
    !Number.isFinite(Date.parse(event.produced_at)) ||
    !event.payload ||
    typeof event.payload !== "object" ||
    Array.isArray(event.payload)
  ) {
    throw Error("Invalid event schema");
  }
}
