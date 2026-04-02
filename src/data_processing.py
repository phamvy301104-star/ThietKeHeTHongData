"""
Module 2: Join các bảng & Tính toán doanh thu
- Join orders + order_items + products + sellers + customers
- Tính doanh thu theo thời gian, sản phẩm, khu vực
- Phân tích tồn kho và phân phối
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, min as spark_min, max as spark_max,
    month, year, dayofweek, date_format, when, lit,
    dense_rank, percent_rank, ntile,
    round as spark_round, countDistinct, datediff,
    first, collect_list, size, expr
)
from pyspark.sql.window import Window


def join_all_tables(cleaned: dict) -> DataFrame:
    """
    Join tất cả các bảng thành 1 bảng fact chính.
    
    orders -> order_items -> products
                          -> sellers
           -> customers
           -> payments (aggregated)
    """
    print("=" * 60)
    print("  JOINING TABLES")
    print("=" * 60)

    # 1. Aggregate payments per order
    payments_agg = (cleaned["payments"]
        .groupBy("order_id")
        .agg(
            spark_sum("payment_value").alias("total_payment"),
            countDistinct("payment_type").alias("payment_methods_count"),
            first("payment_type").alias("primary_payment_type"),
            spark_sum("payment_installments").alias("total_installments")
        )
    )

    # 2. Join order_items với products
    items_products = (cleaned["order_items"]
        .join(cleaned["products"], "product_id", "left")
    )
    print(f"  order_items + products: {items_products.count():,} rows")

    # 3. Join với sellers
    items_full = (items_products
        .join(cleaned["sellers"], "seller_id", "left")
    )

    # 4. Join với orders
    orders_items = (cleaned["orders"]
        .join(items_full, "order_id", "inner")
    )
    print(f"  + orders: {orders_items.count():,} rows")

    # 5. Join với customers
    full_df = (orders_items
        .join(cleaned["customers"], "customer_id", "left")
    )

    # 6. Join với payments
    full_df = full_df.join(payments_agg, "order_id", "left")
    print(f"  + customers + payments: {full_df.count():,} rows")

    # 7. Thêm các cột thời gian
    full_df = (full_df
        .withColumn("order_year", year("order_purchase_timestamp"))
        .withColumn("order_month", month("order_purchase_timestamp"))
        .withColumn("order_dayofweek", dayofweek("order_purchase_timestamp"))
        .withColumn("order_yearmonth", date_format("order_purchase_timestamp", "yyyy-MM"))
    )

    print(f"\n  Final joined table: {full_df.count():,} rows, {len(full_df.columns)} columns")
    return full_df


def calculate_revenue_metrics(full_df: DataFrame) -> dict:
    """
    Tính toán các chỉ số doanh thu chính.
    
    Returns:
        dict chứa các DataFrame kết quả
    """
    print("\n" + "=" * 60)
    print("  CALCULATING REVENUE METRICS")
    print("=" * 60)

    results = {}

    # ====== 1. Doanh thu theo tháng ======
    monthly_revenue = (full_df
        .groupBy("order_yearmonth")
        .agg(
            spark_sum("total_item_value").alias("revenue"),
            count("order_id").alias("num_orders"),
            countDistinct("customer_unique_id").alias("unique_customers"),
            avg("total_item_value").alias("avg_order_value"),
            spark_sum("freight_value").alias("total_freight")
        )
        .orderBy("order_yearmonth")
    )
    results["monthly_revenue"] = monthly_revenue
    print("\n  1. Monthly Revenue:")
    monthly_revenue.show(5, truncate=False)

    # ====== 2. Doanh thu theo danh mục sản phẩm ======
    category_revenue = (full_df
        .groupBy("product_category_name_english")
        .agg(
            spark_sum("total_item_value").alias("revenue"),
            count("*").alias("num_items_sold"),
            countDistinct("product_id").alias("unique_products"),
            avg("price").alias("avg_price"),
            avg("freight_value").alias("avg_freight"),
            avg("product_weight_g").alias("avg_weight_g")
        )
        .orderBy(col("revenue").desc())
    )
    results["category_revenue"] = category_revenue
    print("  2. Top 10 Categories by Revenue:")
    category_revenue.show(10, truncate=False)

    # ====== 3. Doanh thu theo khu vực (seller state) ======
    seller_state_revenue = (full_df
        .groupBy("seller_state")
        .agg(
            spark_sum("total_item_value").alias("revenue"),
            count("*").alias("num_items_sold"),
            countDistinct("seller_id").alias("num_sellers"),
            avg("delivery_days").alias("avg_delivery_days")
        )
        .orderBy(col("revenue").desc())
    )
    results["seller_state_revenue"] = seller_state_revenue
    print("  3. Revenue by Seller State:")
    seller_state_revenue.show(10)

    # ====== 4. Doanh thu theo khách hàng (state) ======
    customer_state_revenue = (full_df
        .groupBy("customer_state")
        .agg(
            spark_sum("total_item_value").alias("revenue"),
            countDistinct("customer_unique_id").alias("num_customers"),
            avg("total_item_value").alias("avg_order_value")
        )
        .orderBy(col("revenue").desc())
    )
    results["customer_state_revenue"] = customer_state_revenue

    # ====== 5. Phân tích hiệu suất giao hàng ======
    delivery_performance = (full_df
        .filter(col("order_status") == "delivered")
        .groupBy("seller_state")
        .agg(
            avg("delivery_days").alias("avg_delivery_days"),
            avg("delivery_delay_days").alias("avg_delay_days"),
            spark_sum(when(col("delivery_delay_days") > 0, 1).otherwise(0)).alias("late_deliveries"),
            count("*").alias("total_deliveries")
        )
        .withColumn("late_delivery_rate",
            spark_round(col("late_deliveries") / col("total_deliveries") * 100, 2))
        .orderBy(col("total_deliveries").desc())
    )
    results["delivery_performance"] = delivery_performance
    print("  5. Delivery Performance by State:")
    delivery_performance.show(10)

    # ====== 6. Phân tích tồn kho - Top sellers ======
    seller_performance = (full_df
        .groupBy("seller_id", "seller_city", "seller_state")
        .agg(
            spark_sum("total_item_value").alias("revenue"),
            count("*").alias("items_sold"),
            countDistinct("product_id").alias("unique_products"),
            countDistinct("order_id").alias("num_orders"),
            avg("delivery_days").alias("avg_delivery_days"),
            avg("price").alias("avg_price")
        )
        .orderBy(col("revenue").desc())
    )
    results["seller_performance"] = seller_performance

    # ====== 7. Phân tích sản phẩm ======
    product_analysis = (full_df
        .groupBy("product_id", "product_category_name_english")
        .agg(
            spark_sum("total_item_value").alias("revenue"),
            count("*").alias("times_sold"),
            avg("price").alias("avg_price"),
            avg("freight_value").alias("avg_freight"),
            first("product_weight_g").alias("weight_g"),
            first("product_volume_cm3").alias("volume_cm3")
        )
        .orderBy(col("revenue").desc())
    )
    results["product_analysis"] = product_analysis

    print("\n  Tổng: 7 bảng phân tích đã tạo")
    return results


def calculate_warehouse_kpis(full_df: DataFrame) -> DataFrame:
    """
    Tính KPIs cho quản lý kho hàng.
    """
    print("\n  Calculating Warehouse KPIs...")

    # Phân tích ABC cho sản phẩm (dựa trên doanh thu)
    product_revenue = (full_df
        .groupBy("product_id", "product_category_name_english")
        .agg(
            spark_sum("total_item_value").alias("revenue"),
            count("*").alias("quantity_sold"),
            avg("freight_value").alias("avg_freight")
        )
    )

    # Tổng doanh thu
    total_rev = product_revenue.agg(spark_sum("revenue")).collect()[0][0]

    # Xếp hạng và tính phần trăm tích lũy
    window = Window.orderBy(col("revenue").desc())
    product_abc = (product_revenue
        .withColumn("rank", dense_rank().over(window))
        .withColumn("pct_rank", percent_rank().over(window))
        .withColumn("revenue_pct", spark_round(col("revenue") / lit(total_rev) * 100, 2))
        .withColumn("abc_class",
            when(col("pct_rank") <= 0.2, "A")    # Top 20% -> Class A
            .when(col("pct_rank") <= 0.5, "B")    # 20-50% -> Class B
            .otherwise("C")                         # Bottom 50% -> Class C
        )
    )

    print("  ABC Analysis Summary:")
    abc_summary = (product_abc
        .groupBy("abc_class")
        .agg(
            count("*").alias("num_products"),
            spark_sum("revenue").alias("total_revenue"),
            spark_sum("quantity_sold").alias("total_quantity")
        )
        .orderBy("abc_class")
    )
    abc_summary.show()

    return product_abc
