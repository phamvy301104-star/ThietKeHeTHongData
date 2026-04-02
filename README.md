# Tối Ưu Hóa Hệ Thống Quản Lý Kho Hàng
## Đồ án Big Data - PySpark + K-Means Clustering

### Mô tả
Sử dụng dữ liệu lớn từ Brazilian E-Commerce (Olist - Kaggle) để phân tích và tối ưu hóa quản lý kho hàng, phân phối sản phẩm bằng Apache Spark và Machine Learning.

### Công nghệ sử dụng
| Công nghệ | Mục đích |
|-----------|----------|
| **Apache Spark** | Xử lý dữ liệu phân tán |
| **Hadoop (HDFS)** | Lưu trữ dữ liệu phân tán |
| **Python (PySpark)** | Ngôn ngữ lập trình chính |
| **Docker** | Container hóa cluster |
| **Power BI** | Trực quan hóa dashboard |
| **K-Means** | Phân cụm sản phẩm |

### Cấu trúc project
```
warehouse-optimization/
├── docker-compose.yml          # Docker cluster (Spark + HDFS)
├── hadoop.env                  # Cấu hình Hadoop
├── main.py                     # Script chạy toàn bộ pipeline
├── data/
│   └── raw/                    # Dữ liệu CSV từ Kaggle
├── src/
│   ├── spark_config.py         # Cấu hình SparkSession
│   ├── data_cleaning.py        # Làm sạch dữ liệu
│   ├── data_processing.py      # Join bảng & tính doanh thu
│   ├── ml_kmeans.py            # K-Means Clustering
│   ├── export_powerbi.py       # Export cho Power BI
│   └── spark_optimization.py   # Demo tối ưu Spark
├── notebooks/
│   └── warehouse_optimization_colab.ipynb  # Notebook cho Google Colab
├── scripts/
│   ├── download_data.py        # Tải dữ liệu từ Kaggle
│   └── upload_to_hdfs.sh       # Upload dữ liệu lên HDFS
└── output/
    └── powerbi/                # CSV files cho Power BI
```

### Dataset
**Brazilian E-Commerce (Olist)** - [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

| Bảng | Mô tả | Rows |
|------|--------|------|
| orders | Đơn hàng | ~100K |
| order_items | Chi tiết đơn hàng | ~113K |
| products | Sản phẩm | ~33K |
| sellers | Người bán | ~3K |
| customers | Khách hàng | ~99K |
| payments | Thanh toán | ~104K |
| reviews | Đánh giá | ~100K |
| geolocation | Vị trí | ~1M |

---

## Hướng dẫn chạy

### Cách 1: Google Colab (Đơn giản nhất)
1. Upload file `notebooks/warehouse_optimization_colab.ipynb` lên Google Colab
2. Chạy từng cell theo thứ tự
3. Tải output về máy

### Cách 2: Docker Cluster (Đầy đủ)

#### Bước 1: Tải dữ liệu
```bash
pip install kaggle
python scripts/download_data.py
```
Hoặc tải thủ công từ Kaggle và giải nén vào `data/raw/`

#### Bước 2: Khởi động Docker cluster
```bash
docker-compose up -d
```

Kiểm tra các service:
| Service | URL |
|---------|-----|
| HDFS NameNode | http://localhost:9870 |
| Spark Master | http://localhost:8080 |
| Spark Worker 1 | http://localhost:8081 |
| Spark Worker 2 | http://localhost:8082 |
| Jupyter | http://localhost:8888 |

#### Bước 3: Upload dữ liệu lên HDFS
```bash
docker exec -it namenode bash /data/upload_to_hdfs.sh
```

#### Bước 4: Chạy pipeline
```bash
# Từ Jupyter notebook trong Docker
# Hoặc submit job:
docker exec -it spark-master spark-submit /src/main.py
```

#### Bước 5: Tạo Dashboard Power BI
1. Mở Power BI Desktop
2. Get Data → CSV → import các file từ `output/powerbi/`
3. Tạo các visualizations:
   - **Trang 1**: Tổng quan doanh thu (monthly, category, state)
   - **Trang 2**: Phân tích kho & ABC Analysis
   - **Trang 3**: K-Means Clustering kết quả
   - **Trang 4**: Delivery Performance

### Cách 3: Local (không Docker)
```bash
pip install pyspark pandas matplotlib numpy
python scripts/download_data.py
python main.py
```

---

## Các bước phân tích

### 1. Thu thập dữ liệu
- Tải 9 bảng CSV từ Kaggle
- Upload lên HDFS (Docker) hoặc đọc trực tiếp (Colab)

### 2. Làm sạch dữ liệu
- Chuyển đổi kiểu timestamp
- Xử lý missing values (fill default / drop)
- Loại bỏ duplicate records
- Tính thể tích sản phẩm, thời gian giao hàng

### 3. Join & Tính doanh thu
- Join 9 bảng thành 1 bảng fact chính
- Tính doanh thu theo: tháng, danh mục, khu vực, seller
- Phân tích ABC cho sản phẩm (A: top 20%, B: 20-50%, C: bottom 50%)
- Phân tích hiệu suất giao hàng

### 4. Machine Learning - K-Means
- Features: doanh thu, số lần bán, giá TB, phí vận chuyển, trọng lượng, thể tích
- StandardScaler + K-Means clustering
- Tìm K tối ưu bằng Silhouette Score
- Phân cụm sản phẩm: Premium / Standard / Economy / Low-value

### 5. Tối ưu Spark
- **Cache/Persist**: Lưu DataFrame trong memory
- **Repartition**: Phân phối lại dữ liệu theo key
- **Broadcast Join**: Tối ưu join với bảng nhỏ
- **Adaptive Query Execution**: Tự động tối ưu

### 6. Dashboard Power BI
- Import CSV → Tạo relationships → Build visualizations
- Các trang: Overview, ABC Analysis, Clustering, Delivery

---

## Kết quả & Đề xuất

### Phân cụm sản phẩm (K-Means):
| Cụm | Đặc điểm | Đề xuất quản lý kho |
|-----|----------|---------------------|
| **Premium** | Doanh thu cao, giá cao | Tăng tồn kho, bố trí gần cửa ra |
| **Standard** | Bán chạy, giá TB | Duy trì stock ổn định, auto reorder |
| **Economy** | Trung bình | Quản lý bình thường |
| **Low-value** | Ít bán | Giảm tồn kho, đặt lô nhỏ |

### Tối ưu vận chuyển:
- Ưu tiên sellers ở SP (São Paulo) - chiếm >60% doanh thu
- Giám sát states có tỉ lệ giao trễ cao
- Tối ưu tuyến logistics cho cross-state deliveries
