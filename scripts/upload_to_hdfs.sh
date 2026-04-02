#!/bin/bash
# Script upload dữ liệu lên HDFS
# Chạy trong container namenode: docker exec -it namenode bash /data/upload_to_hdfs.sh

echo "=========================================="
echo "  UPLOAD DATA TO HDFS"
echo "=========================================="

# Tạo thư mục trên HDFS
hdfs dfs -mkdir -p /warehouse/raw
hdfs dfs -mkdir -p /warehouse/processed
hdfs dfs -mkdir -p /warehouse/output

# Upload các file CSV
echo "Uploading CSV files to HDFS..."

for file in /data/raw/*.csv; do
    filename=$(basename "$file")
    echo "  Uploading: $filename"
    hdfs dfs -put -f "$file" /warehouse/raw/
done

echo ""
echo "Verifying uploaded files..."
hdfs dfs -ls -h /warehouse/raw/

echo ""
echo "=========================================="
echo "  UPLOAD COMPLETE!"
echo "=========================================="
