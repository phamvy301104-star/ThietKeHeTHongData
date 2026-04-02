"""
Main script - Chạy toàn bộ pipeline trên Docker cluster
"""

import sys
import os

# Thêm src vào path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from spark_config import create_spark_session
from data_cleaning import clean_all_data
from data_processing import join_all_tables, calculate_revenue_metrics, calculate_warehouse_kpis
from ml_kmeans import run_kmeans_pipeline
from export_powerbi import export_for_powerbi
from spark_optimization import demonstrate_optimizations


def main():
    print("=" * 60)
    print("  TỐI ƯU HÓA HỆ THỐNG QUẢN LÝ KHO HÀNG")
    print("  Big Data Project - PySpark + K-Means")
    print("=" * 60)

    # 1. Khởi tạo Spark
    # Đổi mode="cluster" khi chạy trên Docker
    # Đổi mode="colab" khi chạy trên Google Colab
    spark = create_spark_session(mode="local")

    # 2. Đường dẫn dữ liệu
    # Local: "data/raw"
    # HDFS:  "hdfs://namenode:9000/warehouse/raw"
    data_path = "data/raw"

    # 3. Load & Clean data
    cleaned = clean_all_data(spark, data_path)

    # 4. Join & Tính doanh thu
    full_df = join_all_tables(cleaned)
    full_df.cache()
    full_df.count()

    results = calculate_revenue_metrics(full_df)
    product_abc = calculate_warehouse_kpis(full_df)

    # 5. K-Means Clustering
    os.makedirs("output/powerbi", exist_ok=True)
    model, predictions = run_kmeans_pipeline(full_df, output_dir="output")

    # 6. Tối ưu hóa Spark (demo)
    optimize_results = demonstrate_optimizations(spark, full_df)

    # 7. Export cho Power BI
    export_for_powerbi(results, predictions, full_df, output_dir="output/powerbi")

    # Dọn dẹp
    full_df.unpersist()
    spark.stop()

    print("\n" + "=" * 60)
    print("  PIPELINE COMPLETE!")
    print("=" * 60)


if __name__ == "__main__":
    main()
