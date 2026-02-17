#!/bin/bash
# La Anónima Price Tracker - Quick Start Script

set -e

echo "=========================================="
echo "La Anónima Price Tracker - Quick Start"
echo "=========================================="
echo ""

# Check Python version
python_version=$(python3 --version 2>&1 | awk '{print $2}')
echo "Python version: $python_version"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt

# Install Playwright browsers
echo "Installing Playwright browsers..."
playwright install chromium

# Initialize directories and database
echo "Initializing tracker..."
python -m src.cli init

echo ""
echo "=========================================="
echo "Setup complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo ""
echo "1. Run a test scrape (visible browser):"
echo "   python -m src.cli scrape --no-headless"
echo ""
echo "2. Run a scrape (headless):"
echo "   python -m src.cli scrape"
echo ""
echo "3. Run analysis:"
echo "   python -m src.cli analyze"
echo ""
echo "4. Check status:"
echo "   python -m src.cli status"
echo ""
echo "5. Export data:"
echo "   python -m src.cli export --format both"
echo ""
echo "For more options:"
echo "   python -m src.cli --help"
echo ""
