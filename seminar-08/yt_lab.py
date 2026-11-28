"""Сортировка одних заказов в Python и нативной операцией YTsaurus."""
import argparse
import logging
from time import monotonic
from uuid import uuid4

import yt.wrapper as yt
from orders import orders


def run(parts):
    logging.getLogger("Yt").setLevel(logging.WARNING)
    client = yt.YtClient(proxy="localhost:80", config={"proxy": {"enable_proxy_discovery": False}})
    root = "//tmp/itmo-s8-" + uuid4().hex
    source, dest = root + "/orders", root + "/sorted"
    keys = ["shop", "order_id"]
    rows = [dict(order_id=i, shop=s, amount=a) for i, s, a in orders()]
    start = monotonic()
    expected = sorted(rows, key=lambda r: (r["shop"], r["order_id"]))
    print(f"local rows={len(expected)} sort_ms={(monotonic() - start) * 1000:.1f}", flush=True)
    client.create("map_node", root)
    try:
        schema = [{"name": name, "type": "int64"} for name in ("order_id", "shop", "amount")]
        client.create("table", source, attributes={"schema": schema})
        client.write_table(source, rows)
        print(f"input stored_rows={client.get(source + '/@row_count')}", flush=True)
        start = monotonic()
        op = client.run_sort(source, dest, sort_by=keys, spec={"partition_count": parts}, sync=False)
        print(f"operation id={op.id} requested_parts={parts}", flush=True)
        op.wait(print_progress=False)
        elapsed = monotonic() - start
        result = client.get_operation(op.id, attributes=["state", "progress"])
        stages = sorted(set(t["job_type"] for t in result["progress"]["tasks"] if "job_type" in t))
        print(f"operation state={result['state']} job_types={','.join(stages)} wait_ms={elapsed * 1000:.0f}", flush=True)
        actual = list(client.read_table(dest))
        sorted_by = list(client.get(dest + "/@sorted_by"))
        same = actual == expected
        print(f"output rows={len(actual)} sorted_by={','.join(sorted_by)} equal={str(same).lower()}", flush=True)
        if result["state"] != "completed" or not same or sorted_by != keys:
            raise RuntimeError("Результат сортировки разошёлся с локальной проверкой")
    finally:
        client.remove(root, recursive=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parts", type=int, choices=(1, 4), default=1)
    run(parser.parse_args().parts)
