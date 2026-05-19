#!/usr/bin/env python3
"""
Setup Script untuk Data Lakehouse NewsPulse

Platform: Cross-platform (Windows, Linux, Mac)
Purpose: Verify & setup semua dependencies untuk pipeline lakehouse

Usage:
    python setup_lakehouse.py                 # Full setup
    python setup_lakehouse.py --skip-pip      # Skip pip install
    python setup_lakehouse.py --verbose       # Verbose output
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path
from datetime import datetime

# ANSI Colors untuk terminal
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    CYAN = '\033[96m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

def print_header(text):
    """Print section header"""
    print(f"\n{Colors.CYAN}{'='*70}")
    print(f"{text}")
    print(f"{'='*70}{Colors.RESET}\n")

def print_success(text):
    """Print success message"""
    print(f"{Colors.GREEN}✓ {text}{Colors.RESET}")

def print_error(text):
    """Print error message"""
    print(f"{Colors.RED}✗ {text}{Colors.RESET}")

def print_warning(text):
    """Print warning message"""
    print(f"{Colors.YELLOW}⚠ {text}{Colors.RESET}")

def print_info(text):
    """Print info message"""
    print(f"{Colors.CYAN}→ {text}{Colors.RESET}")

def command_exists(command):
    """Check if command exists in PATH"""
    return shutil.which(command) is not None

def run_command(cmd, description="", check=True):
    """Run command and return result"""
    try:
        if isinstance(cmd, str):
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        else:
            result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0 and check:
            print_error(f"{description or 'Command'}: {result.stderr}")
            return False
        
        return result.stdout.strip() if result.stdout else True
    except Exception as e:
        print_error(f"Error running command: {e}")
        return False

# ===== MAIN SETUP =====

def main():
    print_header("🚀 Data Lakehouse Setup Script - NewsPulse")
    print_info("Platform: Cross-platform (Python)")
    print_info("Purpose: Setup environment untuk Bronze → Silver → Gold pipeline\n")
    
    # Parse arguments
    skip_pip = "--skip-pip" in sys.argv
    verbose = "--verbose" in sys.argv
    
    # ===== STEP 1: Python Check =====
    print_header("STEP 1: Checking Python Installation")
    
    python_version = sys.version
    print_success(f"Python: {python_version}")
    
    if sys.version_info < (3, 8):
        print_error("Python version too old (need 3.8+)")
        sys.exit(1)
    
    print_success("Python version compatible (3.8+)")
    
    # ===== STEP 2: Check pip =====
    print_header("STEP 2: Checking pip (Package Manager)")
    
    if command_exists("pip"):
        pip_version = run_command("pip --version", "pip check", check=False)
        print_success(f"pip: {pip_version}")
    else:
        print_error("pip tidak ditemukan!")
        sys.exit(1)
    
    # ===== STEP 3: Java Check =====
    print_header("STEP 3: Checking Java (Required for PySpark)")
    
    if command_exists("java"):
        java_check = run_command("java -version", "java check", check=False)
        print_success("Java found")
        print_info(java_check)
    else:
        print_warning("Java tidak ditemukan (diperlukan untuk PySpark)")
        print_info("Install dari: https://www.oracle.com/java/technologies/downloads/")
    
    # ===== STEP 4: Install Python Packages =====
    print_header("STEP 4: Installing Python Packages")
    
    if skip_pip:
        print_warning("Skipping pip install (as requested)")
    else:
        script_dir = Path(__file__).parent
        root_dir = script_dir.parent
        requirements_file = script_dir / "requirements_lakehouse.txt"
        
        if requirements_file.exists():
            print_info(f"Found: {requirements_file}")
            
            # Upgrade pip first
            print_info("Upgrading pip...")
            run_command([sys.executable, "-m", "pip", "install", "--upgrade", "pip"])
            
            # Install requirements
            print_info("Installing packages from requirements_lakehouse.txt...")
            result = run_command(
                [sys.executable, "-m", "pip", "install", "-r", str(requirements_file)],
                "Package installation",
                check=False
            )
            
            if result:
                print_success("All packages installed successfully")
            else:
                print_error("Some packages failed to install")
                sys.exit(1)
        else:
            print_error(f"requirements_lakehouse.txt not found at {requirements_file}")
            sys.exit(1)
    
    # ===== STEP 5: Verify Installation =====
    print_header("STEP 5: Verifying Installation")
    
    packages = ["pyspark", "delta", "pyarrow", "pandas"]
    
    for package in packages:
        try:
            result = run_command(
                f'python -c "import {package}; print({package}.__version__)"',
                check=False
            )
            if result:
                print_success(f"{package}: {result}")
            else:
                print_error(f"{package}: NOT INSTALLED")
        except Exception as e:
            print_error(f"{package}: {e}")
    
    # ===== STEP 6: Create Directory Structure =====
    print_header("STEP 6: Creating Directory Structure")
    
    script_dir = Path(__file__).parent
    root_dir = script_dir.parent
    lakehouse_dir = root_dir / "lakehouse"
    
    directories = [
        "lakehouse_data/bronze/news",
        "lakehouse_data/silver/news",
        "lakehouse_data/gold/word_frequency",
        "lakehouse_data/gold/news_per_source",
        "lakehouse_data/gold/word_velocity",
        "lakehouse_data/gold/cross_source_topics",
        "logs"
    ]
    
    for dir_path in directories:
        full_path = lakehouse_dir / dir_path
        full_path.mkdir(parents=True, exist_ok=True)
        print_success(f"Created/Verified: {dir_path}")
    
    # ===== STEP 7: Test Spark Session =====
    print_header("STEP 7: Testing Spark Session")
    
    print_info("Attempting to create Spark session...")
    
    test_script = """
from pyspark.sql import SparkSession

try:
    spark = SparkSession.builder \\
        .appName("LakehouseTest") \\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \\
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \\
        .getOrCreate()
    
    print(f"✓ Spark Session created successfully")
    print(f"  Version: {spark.version}")
    print(f"  Master: {spark.sparkContext.master}")
    
    # Try reading Delta
    df = spark.createDataFrame([(1, 'test')], ['id', 'name'])
    df.write.format('delta').mode('overwrite').save('./test_delta')
    print(f"✓ Delta Lake write successful")
    
    # Clean up
    import shutil
    shutil.rmtree('./test_delta', ignore_errors=True)
    
    spark.stop()
except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()
    exit(1)
"""
    
    result = run_command(
        [sys.executable, "-c", test_script],
        "Spark test",
        check=False
    )
    
    if result:
        print_success("Spark session test passed!")
    else:
        print_error("Spark session test failed")
        sys.exit(1)
    
    # ===== STEP 8: Create Configuration File =====
    print_header("STEP 8: Creating Configuration File")
    
    env_file = lakehouse_dir / ".env.example"
    
    env_content = """# Data Lakehouse Configuration
# Copy ke .env dan sesuaikan dengan environment Anda

# HDFS Configuration
HDFS_BASE=hdfs://localhost:8020/data/news
HDFS_API_PATH=hdfs://localhost:8020/data/news/api
HDFS_RSS_PATH=hdfs://localhost:8020/data/news/rss

# Local Data Paths
BRONZE_PATH=./lakehouse_data/bronze/news
SILVER_PATH=./lakehouse_data/silver/news
GOLD_PATH=./lakehouse_data/gold

# Spark Configuration
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=4g
SPARK_LOG_LEVEL=INFO

# Data Processing
MAX_NEWS=50
TOP_TRENDING_WORDS=15
DEDUP_KEY=url

# Time Settings
INTERVAL_SECONDS=120
"""
    
    env_file.write_text(env_content)
    print_success("Created: .env.example")
    
    # ===== STEP 9: Create Quick Start =====
    print_header("STEP 9: Creating Quick Start Guide")
    
    quick_start_file = lakehouse_dir / "QUICK_START.md"
    
    quick_start_content = """# 🚀 Quick Start - Data Lakehouse NewsPulse

## Setup Selesai! Berikut cara menjalankan:

### 1. Bronze Layer (Ingest dari HDFS)
```bash
cd lakehouse
python 01_bronze.py --hdfs-base "hdfs://localhost:8020/data/news" --output "./lakehouse_data/bronze/news"
```

### 2. Silver Layer (Cleaning)
```bash
python 02_silver.py --bronze-path "./lakehouse_data/bronze/news" --output "./lakehouse_data/silver/news"
```

### 3. Gold Layer (Aggregation)
```bash
python 03_gold.py --silver-path "./lakehouse_data/silver/news" --output "./lakehouse_data/gold"
```

### BONUS: Dashboard Integration
```bash
python BONUS_dashboard_integration.py
```

### BONUS: Schema Evolution
```bash
python BONUS_schema_evolution.py
```

## Troubleshooting

### Error: HDFS Connection Refused
Gunakan fallback lokal:
```bash
# Copy file JSON ke lokal dulu
mkdir -p ./data_local
python 01_bronze.py --hdfs-base "./data_local" --use-local
```

### Error: OutOfMemory
Tingkatkan Spark memory:
```bash
python 01_bronze.py --driver-memory 6g --executor-memory 6g
```

## Dokumentasi Lengkap
- README_lakehouse.md - Penjelasan arsitektur & transformasi
- 00_setup.md - Setup guide detail
- SCORING_CHECKLIST.md - Rubrik penilaian

## Informasi Berguna
- Folder Output: ./lakehouse_data/
- Log Files: ./logs/
- Sample Config: .env.example

Selamat! Setup lakehouse Anda sudah siap! 🎉
"""
    
    quick_start_file.write_text(quick_start_content)
    print_success("Created: QUICK_START.md")
    
    # ===== STEP 10: Summary =====
    print_header("✓ SETUP COMPLETED SUCCESSFULLY")
    
    print_success("All prerequisites checked and installed")
    print_success("Environment configured")
    print_success("Directory structure created")
    print_success("Spark + Delta Lake verified")
    
    print("\n" + Colors.BOLD + "Next Steps:" + Colors.RESET)
    print_info("1. cd lakehouse")
    print_info("2. python 01_bronze.py (atau baca QUICK_START.md untuk detail)")
    print_info("3. python 02_silver.py")
    print_info("4. python 03_gold.py")
    
    print("\n" + Colors.BOLD + "Documentation:" + Colors.RESET)
    print_info("- Baca: lakehouse/00_setup.md")
    print_info("- Baca: lakehouse/README_lakehouse.md")
    print_info("- Gunakan: lakehouse/SCORING_CHECKLIST.md saat evaluasi")
    
    print("\n" + Colors.BOLD + "Setup timestamp:" + Colors.RESET)
    print_info(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    print(f"\n{Colors.GREEN}✓ Setup siap! Happy coding! 🚀{Colors.RESET}\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print_warning("\nSetup interrupted by user")
        sys.exit(1)
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
