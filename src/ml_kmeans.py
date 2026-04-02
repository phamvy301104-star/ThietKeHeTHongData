"""
Module 3: Machine Learning - K-Means Clustering
Phân cụm sản phẩm để tối ưu quản lý kho
- Features: doanh thu, số lần bán, giá TB, phí vận chuyển, trọng lượng, thể tích
- Áp dụng K-Means clustering
- Đánh giá và trực quan hóa kết quả
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum, first, when, lit,
    round as spark_round, log as spark_log
)
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml import Pipeline
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np


def prepare_features(full_df: DataFrame) -> DataFrame:
    """
    Chuẩn bị features cho K-Means từ bảng fact chính.
    Mỗi sản phẩm sẽ có các đặc trưng tổng hợp.
    """
    print("=" * 60)
    print("  PREPARING FEATURES FOR K-MEANS CLUSTERING")
    print("=" * 60)

    # Aggregate theo product_id
    product_features = (full_df
        .groupBy("product_id")
        .agg(
            spark_sum("total_item_value").alias("total_revenue"),
            count("*").alias("times_sold"),
            avg("price").alias("avg_price"),
            avg("freight_value").alias("avg_freight"),
            first("product_weight_g").alias("weight_g"),
            first("product_volume_cm3").alias("volume_cm3"),
            first("product_category_name_english").alias("category"),
            avg("delivery_days").alias("avg_delivery_days")
        )
    )

    # Loại bỏ null và giá trị 0
    product_features = (product_features
        .na.fill(0)
        .filter(col("total_revenue") > 0)
        .filter(col("times_sold") > 0)
    )

    # Log transform cho các feature skewed
    product_features = (product_features
        .withColumn("log_revenue", spark_log(col("total_revenue") + 1))
        .withColumn("log_times_sold", spark_log(col("times_sold") + 1))
        .withColumn("log_weight", spark_log(col("weight_g") + 1))
        .withColumn("log_volume", spark_log(col("volume_cm3") + 1))
    )

    print(f"  Products with features: {product_features.count():,}")
    product_features.describe().show()

    return product_features


def build_kmeans_pipeline(feature_cols: list, k: int = 4):
    """
    Xây dựng pipeline K-Means: VectorAssembler -> StandardScaler -> KMeans
    """
    # Gộp features thành vector
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features_raw"
    )

    # Chuẩn hóa (StandardScaler)
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withStd=True,
        withMean=True
    )

    # K-Means
    kmeans = KMeans(
        featuresCol="features",
        predictionCol="cluster",
        k=k,
        seed=42,
        maxIter=100
    )

    pipeline = Pipeline(stages=[assembler, scaler, kmeans])
    return pipeline


def find_optimal_k(product_features: DataFrame, feature_cols: list,
                   k_range: range = range(2, 11)) -> int:
    """
    Tìm số cụm tối ưu bằng Silhouette Score (Elbow Method).
    """
    print("\n  Finding optimal K...")
    print(f"  Testing K = {list(k_range)}")

    scores = []
    costs = []

    for k in k_range:
        pipeline = build_kmeans_pipeline(feature_cols, k)
        model = pipeline.fit(product_features)
        predictions = model.transform(product_features)

        # Silhouette Score
        evaluator = ClusteringEvaluator(
            featuresCol="features",
            predictionCol="cluster",
            metricName="silhouette"
        )
        score = evaluator.evaluate(predictions)
        scores.append(score)

        # Within Set Sum of Squared Errors (WSSSE)
        kmeans_model = model.stages[-1]
        cost = kmeans_model.summary.trainingCost
        costs.append(cost)

        print(f"    K={k}: Silhouette={score:.4f}, WSSSE={cost:.2f}")

    # Tìm K tốt nhất (Silhouette cao nhất)
    best_k = list(k_range)[np.argmax(scores)]
    print(f"\n  Best K = {best_k} (Silhouette = {max(scores):.4f})")

    # Vẽ biểu đồ Elbow
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    ax1.plot(list(k_range), costs, 'bo-', linewidth=2)
    ax1.set_xlabel('Số cụm (K)')
    ax1.set_ylabel('WSSSE')
    ax1.set_title('Elbow Method - WSSSE')
    ax1.grid(True, alpha=0.3)

    ax2.plot(list(k_range), scores, 'ro-', linewidth=2)
    ax2.set_xlabel('Số cụm (K)')
    ax2.set_ylabel('Silhouette Score')
    ax2.set_title('Silhouette Score theo K')
    ax2.axvline(x=best_k, color='green', linestyle='--', label=f'Best K={best_k}')
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig("output/elbow_silhouette.png", dpi=150, bbox_inches='tight')
    print("  Saved: output/elbow_silhouette.png")

    return best_k


def train_kmeans(product_features: DataFrame, feature_cols: list, k: int = 4):
    """
    Train K-Means model với K clusters.
    
    Returns:
        (model, predictions DataFrame)
    """
    print(f"\n  Training K-Means with K={k}...")

    pipeline = build_kmeans_pipeline(feature_cols, k)
    model = pipeline.fit(product_features)
    predictions = model.transform(product_features)

    # Đánh giá
    evaluator = ClusteringEvaluator(
        featuresCol="features",
        predictionCol="cluster",
        metricName="silhouette"
    )
    silhouette = evaluator.evaluate(predictions)
    print(f"  Silhouette Score: {silhouette:.4f}")

    # Thống kê từng cụm
    print("\n  Cluster Summary:")
    cluster_summary = (predictions
        .groupBy("cluster")
        .agg(
            count("*").alias("num_products"),
            spark_round(avg("total_revenue"), 2).alias("avg_revenue"),
            spark_round(avg("times_sold"), 1).alias("avg_times_sold"),
            spark_round(avg("avg_price"), 2).alias("avg_price"),
            spark_round(avg("weight_g"), 0).alias("avg_weight"),
            spark_round(avg("avg_freight"), 2).alias("avg_freight")
        )
        .orderBy("cluster")
    )
    cluster_summary.show(truncate=False)

    # Gán nhãn cho từng cụm
    predictions = label_clusters(predictions)

    return model, predictions


def label_clusters(predictions: DataFrame) -> DataFrame:
    """
    Gán nhãn ý nghĩa cho từng cụm dựa trên đặc trưng.
    """
    # Tính trung bình doanh thu và số lần bán cho mỗi cụm
    cluster_stats = (predictions
        .groupBy("cluster")
        .agg(
            avg("total_revenue").alias("avg_rev"),
            avg("times_sold").alias("avg_sold")
        )
        .collect()
    )

    # Sắp xếp cụm theo doanh thu
    sorted_clusters = sorted(cluster_stats, key=lambda x: x["avg_rev"], reverse=True)

    # Gán nhãn
    labels = [
        "Premium - Doanh thu cao",        # Cụm doanh thu cao nhất
        "Standard - Bán chạy",            # Cụm thứ 2
        "Economy - Trung bình",            # Cụm thứ 3
        "Low-value - Ít bán"              # Cụm doanh thu thấp nhất
    ]

    label_map = {}
    for i, cluster_row in enumerate(sorted_clusters):
        if i < len(labels):
            label_map[cluster_row["cluster"]] = labels[i]
        else:
            label_map[cluster_row["cluster"]] = f"Cluster {cluster_row['cluster']}"

    # Gán nhãn vào DataFrame - dynamic based on actual number of clusters
    condition = None
    for i, cluster_row in enumerate(sorted_clusters):
        label = labels[i] if i < len(labels) else f"Cluster {cluster_row['cluster']}"
        if condition is None:
            condition = when(col("cluster") == cluster_row["cluster"], lit(label))
        else:
            condition = condition.when(col("cluster") == cluster_row["cluster"], lit(label))

    predictions = predictions.withColumn(
        "cluster_label",
        condition.otherwise(lit("Other"))
    )

    print("\n  Cluster Labels:")
    predictions.groupBy("cluster", "cluster_label").count().orderBy("cluster").show(truncate=False)

    return predictions


def visualize_clusters(predictions: DataFrame, output_dir: str = "output"):
    """
    Trực quan hóa kết quả clustering.
    """
    import os
    os.makedirs(output_dir, exist_ok=True)

    # Lấy pandas DataFrame (sample nếu quá lớn)
    total_count = predictions.count()
    if total_count > 10000:
        sample_fraction = 10000 / total_count
        pdf = predictions.sample(fraction=sample_fraction, seed=42).toPandas()
    else:
        pdf = predictions.toPandas()

    # 1. Scatter: Revenue vs Times Sold
    fig, axes = plt.subplots(2, 2, figsize=(16, 14))

    colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7']

    for cluster_id in sorted(pdf['cluster'].unique()):
        mask = pdf['cluster'] == cluster_id
        label = pdf[mask]['cluster_label'].iloc[0] if 'cluster_label' in pdf.columns else f'Cluster {cluster_id}'
        c = colors[cluster_id % len(colors)]

        axes[0, 0].scatter(pdf[mask]['log_revenue'], pdf[mask]['log_times_sold'],
                          c=c, label=label, alpha=0.6, s=30)
        axes[0, 1].scatter(pdf[mask]['avg_price'], pdf[mask]['avg_freight'],
                          c=c, label=label, alpha=0.6, s=30)
        axes[1, 0].scatter(pdf[mask]['log_weight'], pdf[mask]['log_revenue'],
                          c=c, label=label, alpha=0.6, s=30)

    axes[0, 0].set_xlabel('Log Revenue')
    axes[0, 0].set_ylabel('Log Times Sold')
    axes[0, 0].set_title('Phân cụm: Doanh thu vs Số lần bán')
    axes[0, 0].legend(fontsize=8)
    axes[0, 0].grid(True, alpha=0.3)

    axes[0, 1].set_xlabel('Giá TB (BRL)')
    axes[0, 1].set_ylabel('Phí vận chuyển TB (BRL)')
    axes[0, 1].set_title('Phân cụm: Giá vs Phí vận chuyển')
    axes[0, 1].legend(fontsize=8)
    axes[0, 1].grid(True, alpha=0.3)

    axes[1, 0].set_xlabel('Log Trọng lượng')
    axes[1, 0].set_ylabel('Log Doanh thu')
    axes[1, 0].set_title('Phân cụm: Trọng lượng vs Doanh thu')
    axes[1, 0].legend(fontsize=8)
    axes[1, 0].grid(True, alpha=0.3)

    # Pie chart số lượng sản phẩm mỗi cụm
    cluster_counts = pdf.groupby('cluster_label')['product_id'].count()
    axes[1, 1].pie(cluster_counts.values, labels=cluster_counts.index,
                   autopct='%1.1f%%', colors=colors[:len(cluster_counts)],
                   startangle=90)
    axes[1, 1].set_title('Phân bổ sản phẩm theo cụm')

    plt.tight_layout()
    plt.savefig(f"{output_dir}/clustering_results.png", dpi=150, bbox_inches='tight')
    print(f"  Saved: {output_dir}/clustering_results.png")

    # 2. Box plots cho từng feature theo cụm
    fig, axes = plt.subplots(2, 3, figsize=(18, 10))
    features = ['total_revenue', 'times_sold', 'avg_price', 'avg_freight', 'weight_g', 'avg_delivery_days']
    titles = ['Doanh thu', 'Số lần bán', 'Giá TB', 'Phí vận chuyển', 'Trọng lượng (g)', 'Ngày giao TB']

    for idx, (feat, title) in enumerate(zip(features, titles)):
        ax = axes[idx // 3][idx % 3]
        if feat in pdf.columns:
            data = [pdf[pdf['cluster'] == c][feat].dropna().values for c in sorted(pdf['cluster'].unique())]
            bp = ax.boxplot(data, patch_artist=True)
            for i, patch in enumerate(bp['boxes']):
                patch.set_facecolor(colors[i % len(colors)])
            ax.set_title(title)
            ax.set_xlabel('Cluster')
            ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(f"{output_dir}/cluster_boxplots.png", dpi=150, bbox_inches='tight')
    print(f"  Saved: {output_dir}/cluster_boxplots.png")


def run_kmeans_pipeline(full_df: DataFrame, output_dir: str = "output"):
    """
    Pipeline hoàn chỉnh K-Means clustering.
    """
    import os
    os.makedirs(output_dir, exist_ok=True)

    # 1. Chuẩn bị features
    product_features = prepare_features(full_df)

    feature_cols = [
        "log_revenue", "log_times_sold", "avg_price",
        "avg_freight", "log_weight", "log_volume"
    ]

    # 2. Tìm K tối ưu
    best_k = find_optimal_k(product_features, feature_cols, k_range=range(2, 9))

    # 3. Train model
    model, predictions = train_kmeans(product_features, feature_cols, k=best_k)

    # 4. Trực quan hóa
    visualize_clusters(predictions, output_dir)

    # 5. Export kết quả
    result_pdf = predictions.select(
        "product_id", "category", "total_revenue", "times_sold",
        "avg_price", "avg_freight", "weight_g", "volume_cm3",
        "cluster", "cluster_label"
    ).toPandas()

    result_pdf.to_csv(f"{output_dir}/product_clusters.csv", index=False)
    print(f"\n  Exported: {output_dir}/product_clusters.csv")

    print("\n" + "=" * 60)
    print("  K-MEANS CLUSTERING COMPLETE!")
    print("=" * 60)

    return model, predictions
