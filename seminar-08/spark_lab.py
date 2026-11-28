"""Разбивка заказов по ключу и два физических плана одной витрины."""
import argparse
import os
import subprocess
import sys
from collections import Counter
from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.sql import SparkSession, functions as F

from orders import orders


def tree(node):
    yield node
    children = node.children().iterator()
    while children.hasNext():
        yield from tree(children.next())


def compare(spark, parts, duplicate, show_plan):
    regions = ("north", "east", "west")
    shops = [(s, regions[s % len(regions)]) for s in range(21)]
    if duplicate:
        shops.append((0, "east"))
    purchases = list(orders())
    facts = spark.createDataFrame(purchases, "order_id long, shop int, amount int")
    dimension = spark.createDataFrame(shops, "shop int, region string")
    print(f"input orders={len(purchases)} dim_rows={len(shops)} dim_keys={len(set(s for s, _ in shops))}", flush=True)

    buckets = (facts.repartition(parts, "shop").withColumn("bucket", F.spark_partition_id())
               .groupBy("bucket").agg(F.count("*").alias("rows"),
                                      F.sum(F.when(F.col("shop") == 0, 1).otherwise(0)).alias("hot")))
    sizes = [0] * parts
    hot = []
    for row in buckets.collect():
        sizes[row.bucket] = row.rows
        if row.hot:
            hot.append(row.hot)
    if hot != [9600] or sum(sizes) != len(purchases):
        raise RuntimeError("Потеря строк или нарушение распределения по ключу")
    print(f"shuffle parts={parts} rows={','.join(map(str, sizes))} hot_rows={hot[0]}", flush=True)

    expected_counts, expected_revenue = Counter(), Counter()
    for _, shop, amount in purchases:
        region = regions[shop % len(regions)]
        expected_counts[region] += 1
        expected_revenue[region] += amount
        if duplicate and shop == 0:
            expected_counts["east"] += 1
            expected_revenue["east"] += amount

    for hint, right in (("merge", dimension.hint("merge")), ("broadcast", F.broadcast(dimension))):
        summary = facts.join(right, "shop").groupBy("region").agg(
            F.count("*").alias("rows"), F.sum("amount").alias("revenue"))
        rows = summary.collect()
        if show_plan:
            summary.explain(mode="formatted")
        joins = [n for n in tree(summary._jdf.queryExecution().executedPlan())
                 if n.nodeName() in ("SortMergeJoin", "BroadcastHashJoin")]
        if len(joins) != 1:
            raise RuntimeError("В физическом плане ожидается одно соединение")
        join = joins[0]
        nodes = list(tree(join))
        hashed = sum(n.nodeName() == "Exchange" and
                     n.outputPartitioning().getClass().getSimpleName() == "HashPartitioning" for n in nodes)
        broadcasted = sum(n.nodeName() == "BroadcastExchange" for n in nodes)
        expected_plan = ("SortMergeJoin", 2, 0) if hint == "merge" else ("BroadcastHashJoin", 0, 1)
        if (join.nodeName(), hashed, broadcasted) != expected_plan:
            raise RuntimeError(f"Неожиданная стратегия соединения: {join.nodeName()}, {hashed}, {broadcasted}")
        if ({r.region: r.rows for r in rows} != expected_counts or
                {r.region: r.revenue for r in rows} != expected_revenue):
            raise RuntimeError("Витрина расходится с независимым расчётом")
        print(f"join={join.nodeName()} hash={hashed} broadcast={broadcasted}", flush=True)
        print(f"result rows={sum(r.rows for r in rows)} revenue={sum(r.revenue for r in rows)}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parts", type=int, choices=(4, 8), default=4)
    parser.add_argument("--duplicate", action="store_true", help="вторая строка магазина 0 в справочнике")
    parser.add_argument("--plan", action="store_true", help="показать физические планы обеих витрин")
    parser.add_argument("--spark-logs", action="store_true", help="показать также сообщения Java и Spark при старте")
    args = parser.parse_args()
    if not args.spark_logs:
        return run_without_startup_banner()
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["PYSPARK_PYTHON"] = sys.executable
    original = Path.cwd()
    with TemporaryDirectory() as work:
        os.chdir(work)
        try:
            spark = (SparkSession.builder.master("local[2]").appName("seminar-08")
                     .config("spark.driver.bindAddress", "127.0.0.1")
                     .config("spark.ui.enabled", "false").config("spark.ui.showConsoleProgress", "false")
                     .config("spark.sql.adaptive.enabled", "false")
                     .config("spark.sql.shuffle.partitions", str(args.parts))
                     .config("spark.sql.autoBroadcastJoinThreshold", "-1").getOrCreate())
            try:
                spark.sparkContext.setLogLevel("ERROR")
                compare(spark, args.parts, args.duplicate, args.plan)
            finally:
                spark.stop()
        finally:
            os.chdir(original)


def run_without_startup_banner():
    banner = {
        b"WARNING: Using incubator modules: jdk.incubator.vector",
        b"Using Spark's default log4j profile: org/apache/spark/log4j2-defaults.properties",
        b'Setting default log level to "WARN".',
        b"To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).",
    }
    native_fallback = (b" WARN NativeCodeLoader: Unable to load native-hadoop library for your "
                       b"platform... using builtin-java classes where applicable")
    command = [sys.executable, __file__, "--spark-logs", *sys.argv[1:]]
    with subprocess.Popen(command, stderr=subprocess.PIPE) as child:
        for line in child.stderr:
            message = line.rstrip(b"\r\n")
            if message not in banner and not message.endswith(native_fallback):
                sys.stderr.buffer.write(line)
                sys.stderr.buffer.flush()
        code = child.wait()
        return code if code >= 0 else 128 - code


if __name__ == "__main__":
    sys.exit(main())
