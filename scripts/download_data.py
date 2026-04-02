"""
Script tải dữ liệu từ Kaggle - Brazilian E-Commerce (Olist)
Dataset chứa thông tin đơn hàng, sản phẩm, kho hàng, khách hàng
Link: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Cách sử dụng:
  1. Cài kaggle: pip install kaggle
  2. Đặt file kaggle.json vào ~/.kaggle/
  3. Chạy: python download_data.py
  
Hoặc tải thủ công từ Kaggle và giải nén vào thư mục data/raw/
"""

import os
import zipfile
import subprocess
import sys


def install_kaggle():
    """Cài đặt kaggle CLI nếu chưa có."""
    try:
        import kaggle
    except ImportError:
        print("Đang cài đặt kaggle...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "kaggle"])


def download_dataset():
    """Tải dataset từ Kaggle."""
    data_dir = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
    os.makedirs(data_dir, exist_ok=True)

    print("=" * 60)
    print("DOWNLOADING: Brazilian E-Commerce Dataset (Olist)")
    print("=" * 60)

    # Tải dataset
    cmd = [
        "kaggle", "datasets", "download",
        "-d", "olistbr/brazilian-ecommerce",
        "-p", data_dir,
        "--unzip"
    ]

    try:
        subprocess.run(cmd, check=True)
        print(f"\nTải thành công vào: {data_dir}")
    except subprocess.CalledProcessError:
        print("\nLỗi tải dataset. Vui lòng kiểm tra:")
        print("1. File kaggle.json đã được đặt đúng vị trí")
        print("2. Kết nối internet")
        print("\nHoặc tải thủ công từ:")
        print("https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce")
        print(f"Giải nén vào: {data_dir}")
        return False

    # Liệt kê các file đã tải
    print("\nCác file dữ liệu:")
    for f in sorted(os.listdir(data_dir)):
        size = os.path.getsize(os.path.join(data_dir, f))
        print(f"  - {f} ({size / 1024:.1f} KB)")

    return True


def verify_data():
    """Kiểm tra các file cần thiết đã có chưa."""
    data_dir = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
    required_files = [
        "olist_orders_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_products_dataset.csv",
        "olist_sellers_dataset.csv",
        "olist_customers_dataset.csv",
        "olist_geolocation_dataset.csv",
        "olist_order_payments_dataset.csv",
        "olist_order_reviews_dataset.csv",
        "product_category_name_translation.csv"
    ]

    missing = []
    for f in required_files:
        if not os.path.exists(os.path.join(data_dir, f)):
            missing.append(f)

    if missing:
        print(f"\nThiếu {len(missing)} file:")
        for f in missing:
            print(f"  - {f}")
        return False

    print("\nĐã có đủ tất cả file dữ liệu!")
    return True


if __name__ == "__main__":
    install_kaggle()

    if not verify_data():
        download_dataset()

    verify_data()
