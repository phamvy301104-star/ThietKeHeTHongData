"""
Module cấu hình SparkSession
Hỗ trợ chạy trên Docker cluster và Google Colab
Tự động tìm Java 17/21 nếu Java mặc định không tương thích
"""

from pyspark.sql import SparkSession
import os
import subprocess
import platform
import re


def _get_java_major_version(java_exe="java"):
    """Lấy major version từ java -version."""
    try:
        result = subprocess.run(
            [java_exe, "-version"],
            capture_output=True, text=True, timeout=10
        )
        output = result.stderr + result.stdout
        match = re.search(r'"(\d+)', output)
        if match:
            major = int(match.group(1))
            return 8 if major == 1 else major
    except Exception:
        pass
    return None


def _find_java_17_or_21():
    """Tìm Java 17 hoặc 21 trên máy."""
    ver = _get_java_major_version("java")
    if ver and 17 <= ver <= 24:
        return None  # Java mặc định OK

    print(f"[INFO] Java mặc định: version {ver} - cần tìm Java 17 hoặc 21...")

    search_dirs = []
    if platform.system() == "Windows":
        for base in ["C:\\Program Files", "C:\\Program Files (x86)"]:
            for vendor in ["Eclipse Adoptium", "Java", "Microsoft", "Zulu",
                          "Amazon Corretto", "BellSoft", "Eclipse Foundation"]:
                search_dirs.append(os.path.join(base, vendor))
    else:
        search_dirs.extend(["/usr/lib/jvm", "/usr/local/lib/jvm"])

    candidates = []
    for d in search_dirs:
        if not os.path.isdir(d):
            continue
        for item in os.listdir(d):
            java_home = os.path.join(d, item)
            ext = ".exe" if platform.system() == "Windows" else ""
            java_exe = os.path.join(java_home, "bin", f"java{ext}")
            if os.path.isfile(java_exe):
                v = _get_java_major_version(java_exe)
                if v and 17 <= v <= 24:
                    candidates.append((v, java_home))

    if candidates:
        # Ưu tiên 17 > 21
        candidates.sort(key=lambda x: (0 if x[0] == 17 else 1))
        return candidates[0][1]
    return None


def _setup_java():
    """Cấu hình JAVA_HOME tự động."""
    java_home = _find_java_17_or_21()
    if java_home:
        os.environ["JAVA_HOME"] = java_home
        java_bin = os.path.join(java_home, "bin")
        os.environ["PATH"] = java_bin + os.pathsep + os.environ.get("PATH", "")
        print(f"[INFO] Sử dụng Java tại: {java_home}")
    else:
        ver = _get_java_major_version("java")
        if ver and ver > 24:
            print(f"[WARNING] Java {ver} không tương thích với PySpark!")
            print(f"[WARNING] Cần cài Java 17: https://adoptium.net/temurin/releases/?version=17")
            raise RuntimeError(
                f"Java {ver} không được hỗ trợ. Cài Java 17 từ: "
                "https://adoptium.net/temurin/releases/?version=17"
            )


def create_spark_session(app_name="WarehouseOptimization", mode="local"):
    """
    Tạo SparkSession theo môi trường.
    
    Args:
        app_name: Tên ứng dụng Spark
        mode: "local" | "cluster" | "colab"
    
    Returns:
        SparkSession
    """
    # Tự động tìm Java 17/21 nếu cần
    _setup_java()

    builder = SparkSession.builder.appName(app_name)

    if mode == "cluster":
        builder = (builder
            .master("spark://spark-master:7077")
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
            .config("spark.executor.memory", "2g")
            .config("spark.executor.cores", "2")
            .config("spark.driver.memory", "2g")
        )
    elif mode == "colab":
        builder = (builder
            .master("local[*]")
            .config("spark.driver.memory", "4g")
        )
    else:
        builder = (builder
            .master("local[*]")
            .config("spark.driver.memory", "2g")
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
