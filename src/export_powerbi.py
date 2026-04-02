"""
Module 4: Export dữ liệu cho Power BI
Xuất các bảng phân tích sang CSV để import vào Power BI
"""

from pyspark.sql import DataFrame
import os


def export_for_powerbi(results: dict, predictions: DataFrame, full_df: DataFrame,
                       output_dir: str = "output/powerbi"):
    """
    Export tất cả dữ liệu cần thiết cho Power BI dashboard.
    """
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 60)
    print("  EXPORTING DATA FOR POWER BI")
    print("=" * 60)

    exports = {
        # Từ revenue metrics
        "monthly_revenue": results.get("monthly_revenue"),
        "category_revenue": results.get("category_revenue"),
        "seller_state_revenue": results.get("seller_state_revenue"),
        "customer_state_revenue": results.get("customer_state_revenue"),
        "delivery_performance": results.get("delivery_performance"),
        "seller_performance": results.get("seller_performance"),
        "product_analysis": results.get("product_analysis"),
    }

    for name, df in exports.items():
        if df is not None:
            filepath = os.path.join(output_dir, f"{name}.csv")
            df.toPandas().to_csv(filepath, index=False)
            print(f"  Exported: {name}.csv ({df.count():,} rows)")

    # Export clustering results
    if predictions is not None:
        cluster_df = predictions.select(
            "product_id", "category", "total_revenue", "times_sold",
            "avg_price", "avg_freight", "weight_g", "volume_cm3",
            "cluster", "cluster_label"
        )
        filepath = os.path.join(output_dir, "product_clusters.csv")
        cluster_df.toPandas().to_csv(filepath, index=False)
        print(f"  Exported: product_clusters.csv ({cluster_df.count():,} rows)")

    # Export bảng fact chính (sample cho Power BI)
    fact_cols = [
        "order_id", "customer_unique_id", "customer_state", "customer_city",
        "order_yearmonth", "order_year", "order_month", "order_dayofweek",
        "order_status", "product_id", "product_category_name_english",
        "seller_id", "seller_state", "seller_city",
        "price", "freight_value", "total_item_value",
        "delivery_days", "delivery_delay_days",
        "primary_payment_type", "total_payment"
    ]

    existing_cols = [c for c in fact_cols if c in full_df.columns]
    fact_export = full_df.select(existing_cols)
    filepath = os.path.join(output_dir, "fact_orders.csv")
    fact_export.toPandas().to_csv(filepath, index=False)
    print(f"  Exported: fact_orders.csv ({fact_export.count():,} rows)")

    print(f"\n  All files exported to: {output_dir}/")
    print("  Import these CSVs into Power BI Desktop")
    print("=" * 60)
