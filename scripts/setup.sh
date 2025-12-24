#!/bin/bash
# ============================================================
# PROJECT SETUP
# ============================================================
# Run this script first to set up your environment
#
# Usage:
#   chmod +x scripts/setup.sh
#   ./scripts/setup.sh
# ============================================================

set -e

echo "============================================"
echo "  dbt Advanced Olist - Setup"
echo "============================================"
echo ""

# Check Python
echo "Checking Python..."
if command -v python3 &> /dev/null; then
    echo "✓ $(python3 --version)"
else
    echo "✗ Python 3 not found. Please install Python 3.8+"
    exit 1
fi

# Create virtual environment
echo ""
echo "Creating virtual environment..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo "✓ Created venv/"
else
    echo "✓ venv/ already exists"
fi

# Activate and install
echo ""
echo "Installing dbt-bigquery..."
source venv/bin/activate
pip install --quiet --upgrade pip
pip install --quiet dbt-bigquery==1.7.0
echo "✓ dbt-bigquery installed"

# Install packages
echo ""
echo "Installing dbt packages..."
dbt deps
echo "✓ Packages installed"

# Check profiles
echo ""
echo "Checking profiles.yml..."
if [ -f ~/.dbt/profiles.yml ]; then
    echo "✓ ~/.dbt/profiles.yml exists"
else
    echo "! No profiles.yml found"
    echo ""
    echo "ACTION REQUIRED:"
    echo "  1. Copy: cp profiles.yml.example ~/.dbt/profiles.yml"
    echo "  2. Edit ~/.dbt/profiles.yml with your GCP project ID"
    echo "  3. Update the keyfile path to your service account JSON"
fi

echo ""
echo "============================================"
echo "  Setup Complete!"
echo "============================================"
echo ""
echo "Next steps:"
echo "  1. Load data:    ./scripts/load_data_to_bigquery.sh"
echo "  2. Test connection: dbt debug"
echo "  3. Build project:   dbt build"
echo ""
