export const eventTypes = ["page_view", "add_to_cart", "purchase"] as const;

export interface UserEvent {
  event_id: string;
  user_id: string;
  type: (typeof eventTypes)[number];
  seq: number;
  produced_at: string;
  payload: Record<string, unknown>;
}
