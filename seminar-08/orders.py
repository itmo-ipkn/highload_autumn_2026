"""Один заказ — одна строка. Сравнение двух способов подготовить обмен по магазину."""
import argparse
from collections import Counter
from pathlib import Path
from tempfile import TemporaryDirectory

ROW_COUNT = 12_000


def orders():
    for order_id in range(ROW_COUNT):
        slot = order_id % 100
        shop = 0 if slot < 80 else slot - 79
        yield order_id, shop, 100 + 25 * (order_id % 5)


def intermediate(batch, combine):
    if combine:
        subtotals = Counter()
        for _, shop, amount in batch:
            subtotals[shop] += amount
        yield from sorted(subtotals.items())
    else:
        for _, shop, amount in batch:
            yield shop, amount


def compare(chunks):
    purchases = list(orders())
    chunk_size = len(purchases) // chunks
    reference = Counter()
    for _, shop, amount in purchases:
        reference[shop] += amount
    print(f"input orders={len(purchases)} chunks={chunks} shops={len(reference)}", flush=True)
    with TemporaryDirectory() as work:
        path = Path(work) / "exchange.tsv"
        for combine in (False, True):
            with path.open("wb") as output:
                for offset in range(0, len(purchases), chunk_size):
                    for shop, amount in intermediate(purchases[offset:offset + chunk_size], combine):
                        output.write(f"{shop}\t{amount}\n".encode("ascii"))
            result = Counter()
            records = 0
            with path.open("rb") as source:
                for row in source:
                    shop, amount = map(int, row.split())
                    result[shop] += amount
                    records += 1
            if result != reference:
                raise RuntimeError("Суммы по магазинам разошлись")
            mode = "combined" if combine else "raw"
            print(f"{mode} records={records} bytes={path.stat().st_size} revenue={sum(result.values())}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chunks", type=int, choices=(4, 8), default=4)
    compare(parser.parse_args().chunks)
