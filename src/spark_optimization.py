"""
Module 5: Tối ưu hóa Spark (Cache, Partition, Broadcast)
Minh họa các kỹ thuật tối ưu Spark cho báo cáo
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, broadcast
import time


def demonstrate_optimizations(spark: SparkSession, full_df: DataFrame):
    """
    Demo các kỹ thuật tối ưu Spark - dùng cho báo cáo.
    """
    print("=" * 60)
    print("  SPARK OPTIMIZATION TECHNIQUES")
    print("=" * 60)

    # ==============================
    # 1. CACHE / PERSIST
    # ==============================
    print("\n--- 1. Cache / Persist ---")

    # Không cache
    start = time.time()
    full_df.groupBy("product_category_name_english").count().collect()
    full_df.groupBy("seller_state").count().collect()
    no_cache_time = time.time() - start
    print(f"  Without cache: {no_cache_time:.2f}s")

    # Có cache
    full_df.cache()
    full_df.count()  # trigger cache

    start = time.time()
    full_df.groupBy("product_category_name_english").count().collect()
    full_df.groupBy("seller_state").count().collect()
    cache_time = time.time() - start
    print(f"  With cache:    {cache_time:.2f}s")
    print(f"  Speedup:       {no_cache_time / max(cache_time, 0.001):.1f}x")

    # ==============================
    # 2. REPARTITION
    # ==============================
    print("\n--- 2. Repartition ---")

    # Kiểm tra số partition hiện tại
    print(f"  Current partitions: {full_df.rdd.getNumPartitions()}")

    # Repartition theo seller_state (tối ưu cho group by seller_state)
    start = time.time()
    df_repartitioned = full_df.repartition(8, "seller_state")
    df_repartitioned.groupBy("seller_state").count().collect()
    repart_time = time.time() - start
    print(f"  After repartition by seller_state: {repart_time:.2f}s")
    print(f"  New partitions: {df_repartitioned.rdd.getNumPartitions()}")

    # ==============================
    # 3. BROADCAST JOIN
    # ==============================
    print("\n--- 3. Broadcast Join ---")

    # Tạo bảng nhỏ (lookup table)
    small_df = full_df.select("seller_state").distinct()
    print(f"  Small table rows: {small_df.count()}")

    # Normal join
    start = time.time()
    full_df.join(small_df, "seller_state", "inner").count()
    normal_join = time.time() - start
    print(f"  Normal join:    {normal_join:.2f}s")

    # Broadcast join
    start = time.time()
    full_df.join(broadcast(small_df), "seller_state", "inner").count()
    broadcast_time = time.time() - start
    print(f"  Broadcast join: {broadcast_time:.2f}s")
    print(f"  Speedup:        {normal_join / max(broadcast_time, 0.001):.1f}x")

    # ==============================
    # 4. COALESCE vs REPARTITION
    # ==============================
    print("\n--- 4. Coalesce vs Repartition ---")

    start = time.time()
    full_df.coalesce(4).write.mode("overwrite").format("noop").save()
    coalesce_time = time.time() - start
    print(f"  Coalesce(4):    {coalesce_time:.2f}s (no shuffle)")

    start = time.time()
    full_df.repartition(4).write.mode("overwrite").format("noop").save()
    repartition_time = time.time() - start
    print(f"  Repartition(4): {repartition_time:.2f}s (with shuffle)")

    # ==============================
    # 5. EXPLAIN PLAN
    # ==============================
    print("\n--- 5. Query Execution Plan ---")

    query = (full_df
        .filter(col("order_status") == "delivered")
        .groupBy("product_category_name_english")
        .agg({"total_item_value": "sum", "order_id": "count"})
        .orderBy(col("sum(total_item_value)").desc())
        .limit(10)
    )
    print("  Logical Plan:")
    query.explain(mode="simple")

    # Unpersist
    full_df.unpersist()

    print("\n" + "=" * 60)
    print("  OPTIMIZATION DEMO COMPLETE!")
    print("=" * 60)

    return {
        "no_cache_time": no_cache_time,
        "cache_time": cache_time,
        "broadcast_speedup": normal_join / max(broadcast_time, 0.001)
    }
