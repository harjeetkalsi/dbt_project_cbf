#!/bin/bash
# ============================================================
# LOAD DATA TO BIGQUERY
# ============================================================
# This script uploads the Olist CSV files to BigQuery
#
# Prerequisites:
#   - Google Cloud SDK installed (gcloud, bq commands)
#   - Authenticated: gcloud auth login
#   - Project set: gcloud config set project YOUR_PROJECT_ID
#
# Usage:
#   chmod +x scripts/load_data_to_bigquery.sh
#   ./scripts/load_data_to_bigquery.sh
# ============================================================

set -e

# Configuration - UPDATE THESE
PROJECT_ID="${GCP_PROJECT_ID:-your-project-id}"
DATASET="raw_data"
DATA_DIR="./data"

echo "============================================"
echo "  Loading Olist Data to BigQuery"
echo "============================================"
echo "Project: $PROJECT_ID"
echo "Dataset: $DATASET"
echo ""

# Check if bq command exists
if ! command -v bq &> /dev/null; then
    echo "ERROR: bq command not found"
    echo "Install Google Cloud SDK: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Create dataset
echo "Creating dataset $DATASET..."
bq --project_id=$PROJECT_ID mk --dataset --if_not_exists $DATASET
echo "✓ Dataset ready"
echo ""

# Load each table
echo "Loading tables..."

echo "  → orders"
bq --project_id=$PROJECT_ID load --replace --source_format=CSV --skip_leading_rows=1 \
    $DATASET.orders $DATA_DIR/orders.csv \
    order_id:STRING,customer_id:STRING,order_status:STRING,order_purchase_timestamp:TIMESTAMP,order_approved_at:TIMESTAMP,order_delivered_carrier_date:TIMESTAMP,order_delivered_customer_date:TIMESTAMP,order_estimated_delivery_date:TIMESTAMP

echo "  → order_items"
bq --project_id=$PROJECT_ID load --replace --source_format=CSV --skip_leading_rows=1 \
    $DATASET.order_items $DATA_DIR/order_items.csv \
    order_id:STRING,order_item_id:INTEGER,product_id:STRING,seller_id:STRING,shipping_limit_date:TIMESTAMP,price:FLOAT,freight_value:FLOAT

echo "  → customers"
bq --project_id=$PROJECT_ID load --replace --source_format=CSV --skip_leading_rows=1 \
    $DATASET.customers $DATA_DIR/customers.csv \
    customer_id:STRING,customer_unique_id:STRING,customer_zip_code_prefix:STRING,customer_city:STRING,customer_state:STRING

echo "  → products"
bq --project_id=$PROJECT_ID load --replace --source_format=CSV --skip_leading_rows=1 \
    $DATASET.products $DATA_DIR/products.csv \
    product_id:STRING,product_category_name:STRING,product_name_lenght:INTEGER,product_description_lenght:INTEGER,product_photos_qty:INTEGER,product_weight_g:FLOAT,product_length_cm:FLOAT,product_height_cm:FLOAT,product_width_cm:FLOAT

echo "  → product_category_name_translation"
bq --project_id=$PROJECT_ID load --replace --source_format=CSV --skip_leading_rows=1 \
    $DATASET.product_category_name_translation $DATA_DIR/product_category_name_translation.csv \
    product_category_name:STRING,product_category_name_english:STRING

echo ""
echo "============================================"
echo "  ✓ All tables loaded successfully!"
echo "============================================"
echo ""
echo "Verify with: bq ls $PROJECT_ID:$DATASET"
echo "Next step:   dbt debug"
