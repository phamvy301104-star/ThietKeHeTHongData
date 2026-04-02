"""
Module 1: Load và làm sạch dữ liệu (Data Cleaning)
- Load dữ liệu từ CSV / HDFS
- Xử lý missing values
- Chuẩn hóa kiểu dữ liệu
- Loại bỏ duplicate
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, isnan, isnull, count, trim, lower,
    to_timestamp, datediff, lit, regexp_replace, round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, DoubleType, TimestampType
)


def load_data(spark: SparkSession, data_path: str) -> dict:
    """
    Load tất cả các bảng dữ liệu từ CSV.
    
    Args:
        spark: SparkSession
        data_path: Đường dẫn tới thư mục chứa CSV 
                   (local: "./data/raw/" hoặc HDFS: "hdfs://namenode:9000/warehouse/raw/")
    
    Returns:
        dict chứa các DataFrame
    """
    print("=" * 60)
    print("  LOADING DATA")
    print("=" * 60)

    tables = {
        "orders": "olist_orders_dataset.csv",
        "order_items": "olist_order_items_dataset.csv",
        "products": "olist_products_dataset.csv",
        "sellers": "olist_sellers_dataset.csv",
        "customers": "olist_customers_dataset.csv",
        "geolocation": "olist_geolocation_dataset.csv",
        "payments": "olist_order_payments_dataset.csv",
        "reviews": "olist_order_reviews_dataset.csv",
        "category_translation": "product_category_name_translation.csv"
    }

    dataframes = {}
    for name, filename in tables.items():
        filepath = f"{data_path}/{filename}"
        df = spark.read.csv(filepath, header=True, inferSchema=True)
        dataframes[name] = df
        print(f"  Loaded {name}: {df.count():,} rows, {len(df.columns)} columns")

    print(f"\nTổng: {len(dataframes)} bảng dữ liệu")
    return dataframes


def analyze_missing_values(df: DataFrame, table_name: str):
    """Phân tích missing values trong DataFrame."""
    print(f"\n--- Missing Values: {table_name} ---")
    total_rows = df.count()

    from pyspark.sql.types import DoubleType, FloatType
    for column in df.columns:
        dtype = df.schema[column].dataType
        if isinstance(dtype, (DoubleType, FloatType)):
            null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
        else:
            null_count = df.filter(col(column).isNull()).count()
        if null_count > 0:
            pct = (null_count / total_rows) * 100
            print(f"  {column}: {null_count:,} ({pct:.1f}%)")


def clean_orders(df: DataFrame) -> DataFrame:
    """Làm sạch bảng orders."""
    print("\nCleaning: orders")

    # Chuyển đổi cột timestamp
    timestamp_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]

    for c in timestamp_cols:
        df = df.withColumn(c, to_timestamp(col(c)))

    # Loại bỏ đơn hàng trùng lặp
    before = df.count()
    df = df.dropDuplicates(["order_id"])
    after = df.count()
    print(f"  Removed {before - after} duplicate orders")

    # Chỉ giữ đơn hàng đã giao thành công cho phân tích chính
    df_delivered = df.filter(col("order_status") == "delivered")
    print(f"  Delivered orders: {df_delivered.count():,} / {after:,}")

    # Tính thời gian giao hàng (ngày)
    df = df.withColumn(
        "delivery_days",
        datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))
    )

    # Tính chênh lệch với dự kiến
    df = df.withColumn(
        "delivery_delay_days",
        datediff(col("order_delivered_customer_date"), col("order_estimated_delivery_date"))
    )

    return df


def clean_products(df: DataFrame, df_translation: DataFrame) -> DataFrame:
    """Làm sạch bảng products và dịch category sang tiếng Anh."""
    print("\nCleaning: products")

    # Join với bảng dịch category
    df = df.join(df_translation, "product_category_name", "left")

    # Điền giá trị mặc định cho missing
    df = (df
        .withColumn("product_category_name_english",
            when(col("product_category_name_english").isNull(), lit("unknown"))
            .otherwise(col("product_category_name_english")))
        .withColumn("product_weight_g",
            when(col("product_weight_g").isNull(), lit(0))
            .otherwise(col("product_weight_g")))
        .withColumn("product_length_cm",
            when(col("product_length_cm").isNull(), lit(0))
            .otherwise(col("product_length_cm")))
        .withColumn("product_height_cm",
            when(col("product_height_cm").isNull(), lit(0))
            .otherwise(col("product_height_cm")))
        .withColumn("product_width_cm",
            when(col("product_width_cm").isNull(), lit(0))
            .otherwise(col("product_width_cm")))
    )

    # Tính thể tích sản phẩm (cm³)
    df = df.withColumn(
        "product_volume_cm3",
        col("product_length_cm") * col("product_height_cm") * col("product_width_cm")
    )

    # Loại bỏ duplicate
    before = df.count()
    df = df.dropDuplicates(["product_id"])
    print(f"  Removed {before - df.count()} duplicate products")

    return df


def clean_order_items(df: DataFrame) -> DataFrame:
    """Làm sạch bảng order_items."""
    print("\nCleaning: order_items")

    # Tính tổng giá trị mỗi item (giá + phí vận chuyển)
    df = df.withColumn(
        "total_item_value",
        spark_round(col("price") + col("freight_value"), 2)
    )

    # Chuyển timestamp
    df = df.withColumn(
        "shipping_limit_date",
        to_timestamp(col("shipping_limit_date"))
    )

    # Loại bỏ giá trị bất thường (price <= 0)
    before = df.count()
    df = df.filter(col("price") > 0)
    print(f"  Removed {before - df.count()} items with invalid price")

    return df


def clean_payments(df: DataFrame) -> DataFrame:
    """Làm sạch bảng payments."""
    print("\nCleaning: payments")

    # Loại bỏ giá trị thanh toán <= 0
    df = df.filter(col("payment_value") > 0)

    return df


def clean_all_data(spark: SparkSession, data_path: str) -> dict:
    """
    Pipeline hoàn chỉnh: Load + Clean tất cả dữ liệu.
    
    Returns:
        dict chứa các DataFrame đã làm sạch
    """
    # Load
    raw = load_data(spark, data_path)

    # Phân tích missing
    for name, df in raw.items():
        analyze_missing_values(df, name)

    # Clean
    cleaned = {}
    cleaned["orders"] = clean_orders(raw["orders"])
    cleaned["products"] = clean_products(raw["products"], raw["category_translation"])
    cleaned["order_items"] = clean_order_items(raw["order_items"])
    cleaned["payments"] = clean_payments(raw["payments"])
    cleaned["sellers"] = raw["sellers"]
    cleaned["customers"] = raw["customers"]
    cleaned["geolocation"] = raw["geolocation"].dropDuplicates(["geolocation_zip_code_prefix"])
    cleaned["reviews"] = raw["reviews"]

    print("\n" + "=" * 60)
    print("  DATA CLEANING COMPLETE!")
    print("=" * 60)

    return cleaned
