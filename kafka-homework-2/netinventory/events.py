from datetime import datetime, timezone
from uuid import uuid4

CATALOG = {
    "RTR-01": "router",
    "RTR-02": "router",
    "SW-01": "switch",
    "SW-02": "switch",
    "AP-01": "access_point",
    "AP-02": "access_point",
}
ACTIONS = ("page_view", "add_to_cart", "purchase")
ADMINS_PER_PRODUCER = 6


class UnknownDevice(ValueError):
    pass


def new_event(user_id, admin_number, seq, mode):
    request, action = divmod(seq - 1, len(ACTIONS))
    code = tuple(CATALOG)[(admin_number + request) % len(CATALOG)]
    if (seq + 7 * admin_number) % 50 == 0:
        code = "UNKNOWN-00"

    return {
        "event_id": str(uuid4()),
        "user_id": user_id,
        "type": ACTIONS[action],
        "seq": seq,
        "produced_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "payload": {
            "device_code": code,
            "request_id": f"{user_id}-{request + 1:05d}",
            "key_mode": mode,
        },
    }


def check_device(event):
    code = event["payload"]["device_code"]
    if code not in CATALOG:
        raise UnknownDevice(f"unknown device code: {code}")
    return code
