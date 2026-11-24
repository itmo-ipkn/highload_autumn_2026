import { randomUUID } from "node:crypto";
export const eventTypes = ["page_view", "add_to_cart", "purchase"] as const;

export interface UserEvent {
  event_id: string;
  user_id: string;
  type: (typeof eventTypes)[number];
  seq: number;
  produced_at: string;
  payload: Record<string, unknown>;
}

export function usersFor(index: number, count: number) {
  const users: string[] = [];

  for (let n = index + 1; n <= count; n += 3) users.push(`user-${n}`);

  return users;
}

export function createGenerator(
  users: string[],
  poisonRate: number,
  random = Math.random,
) {
  const sequences = new Map<string, number>();

  let cursor = 0;

  return (): UserEvent | undefined => {
    const user = users[cursor];

    if (user === undefined) {
      return undefined;
    }

    cursor = (cursor + 1) % users.length;
    const seq = (sequences.get(user) ?? 0) + 1;

    if (!Number.isSafeInteger(seq)) {
      throw new Error(`seq exhausted for ${user}`);
    }

    sequences.set(user, seq);

    const event: UserEvent = {
      event_id: randomUUID(),
      user_id: user,
      type: eventTypes[Math.floor(random() * 3)]!,
      seq,
      produced_at: new Date().toISOString(),
      payload: {},
    };

    if (random() < poisonRate) {
      event.payload.poison = true;
    }

    return event;
  };
}

export function toMessage(event: UserEvent, mode: "user_id" | "none") {
  return {
    key: mode === "user_id" ? event.user_id : null,
    value: JSON.stringify(event),
  };
}
