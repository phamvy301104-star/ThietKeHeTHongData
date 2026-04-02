"""
Module cấu hình SparkSession
Hỗ trợ chạy trên Docker cluster và Google Colab
"""

from pyspark.sql import SparkSession
import os


def create_spark_session(app_name="WarehouseOptimization", mode="local"):
    """
    Tạo SparkSession theo môi trường.
    
    Args:
        app_name: Tên ứng dụng Spark
        mode: "local" | "cluster" | "colab"
    
    Returns:
        SparkSession
    """
    builder = SparkSession.builder.appName(app_name)

    if mode == "cluster":
        # Kết nối Spark cluster qua Docker
        builder = (builder
            .master("spark://spark-master:7077")
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
            .config("spark.executor.memory", "2g")
            .config("spark.executor.cores", "2")
            .config("spark.driver.memory", "2g")
        )
    elif mode == "colab":
        # Google Colab - local mode
        builder = (builder
            .master("local[*]")
            .config("spark.driver.memory", "4g")
        )
    else:
        # Local mode
        builder = (builder
            .master("local[*]")
            .config("spark.driver.memory", "2g")
        )

    # Fix cho Java 17+ (getSubject not supported)
    java_opts = (
        "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
        "--add-opens=java.base/java.io=ALL-UNNAMED "
        "--add-opens=java.base/java.net=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
        "-Djava.security.manager=allow"
    )
    builder = (builder
        .config("spark.driver.extraJavaOptions", java_opts)
        .config("spark.executor.extraJavaOptions", java_opts)
    )

    # Cấu hình tối ưu chung
    builder = (builder
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", "8")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"SparkSession created: {app_name} (mode={mode})")
    print(f"Spark UI: http://localhost:4040")

    return spark
