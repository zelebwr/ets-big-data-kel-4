# Setup Lakehouse - Complete Guide

**Last Updated:** May 2026  
**Status:** Ready for Production  
**Platform:** Windows, Linux, macOS

---

## 📋 System Requirements

### Minimum Hardware
- **RAM:** 8 GB (16 GB recommended for comfortable Spark execution)
- **Storage:** 5 GB free space (for lakehouse data)
- **CPU:** 2+ cores (4+ cores recommended)

### Software Requirements

| Component | Minimum | Recommended | Status |
|-----------|---------|------------|--------|
| Python | 3.8 | 3.10+ | Required ✓ |
| Java | JDK 11 | JDK 17+ | Required ✓ |
| PySpark | 3.4.0 | 3.5+ | Required ✓ |
| Delta Lake | 3.0.0 | 3.1+ | Required ✓ |
| Git | Any | Latest | Recommended |

---

## 🚀 Quick Setup (5 Minutes)

### Windows (PowerShell)

```powershell
# 1. Open PowerShell as Administrator

# 2. Navigate to scripts folder
cd scripts

# 3. Run setup script
.\setup_lakehouse.ps1

# 4. Follow the prompts
```

### Linux / macOS

```bash
# 1. Navigate to scripts folder
cd scripts

# 2. Run setup script
python setup_lakehouse.py

# 3. Follow the prompts
```

### Cross-Platform (Python)

```bash
# Works on Windows, Linux, macOS
cd scripts
python setup_lakehouse.py

# With options:
python setup_lakehouse.py --skip-pip       # Skip pip install
python setup_lakehouse.py --verbose        # Verbose output
```

---

## 📦 What Gets Installed

### Core Dependencies
```
pyspark>=3.4.0           # Apache Spark
delta-spark>=3.1.0       # Delta Lake
pyarrow>=11.0.0          # Data serialization
pandas>=1.5.0            # Data manipulation
```

### Optional Dependencies
```
flask>=2.3.0             # For dashboard integration
requests>=2.28.0         # HTTP requests
pytest>=7.2.0            # Testing
black>=23.0.0            # Code formatting
```

See `requirements_lakehouse.txt` for complete list.

---

## 📁 Directory Structure Created

```
lakehouse/
├── 00_setup.md                      # Setup documentation
├── README_lakehouse.md              # Architecture & justification
├── QUICK_START.md                   # Quick reference (auto-generated)
├── SCORING_CHECKLIST.md             # Evaluation rubric
├── .env.example                     # Configuration template (auto-generated)
│
├── 01_bronze.py                     # Bronze layer ingest
├── 02_silver.py                     # Silver layer cleaning
├── 03_gold.py                       # Gold layer aggregation
│
├── BONUS_dashboard_integration.py   # (+5 points)
├── BONUS_schema_evolution.py        # (+2 points)
│
└── lakehouse_data/                  # Data directory (auto-created)
    ├── bronze/
    │   └── news/                    # Raw ingested data
    ├── silver/
    │   └── news/                    # Cleaned data
    ├── gold/
    │   ├── word_frequency/          # Top trending words
    │   ├── news_per_source/         # Source distribution
    │   ├── word_velocity/           # Enhanced: trending detection
    │   └── cross_source_topics/     # Enhanced: cross-source join
    └── logs/                        # Execution logs
```

---

## ✅ Setup Verification Checklist

Setelah menjalankan setup script, verifikasi:

### Python & Dependencies
- [ ] `python --version` shows 3.8+
- [ ] `python -c "import pyspark; print(pyspark.__version__)"` works
- [ ] `python -c "import delta; print(delta.__version__)"` works
- [ ] `pip list` shows: pyspark, delta-spark, pyarrow, pandas

### Java
- [ ] `java -version` shows JDK installed
- [ ] `echo %JAVA_HOME%` (Windows) or `echo $JAVA_HOME` (Linux/Mac) is set

### Directories
- [ ] `lakehouse/lakehouse_data/bronze/news/` exists
- [ ] `lakehouse/lakehouse_data/silver/news/` exists
- [ ] `lakehouse/lakehouse_data/gold/` exists with 4 subdirectories
- [ ] `lakehouse/.env.example` exists

### Spark Test
- [ ] Test script creates Spark session successfully
- [ ] Can write Delta table to disk
- [ ] No error messages during test

---

## 🔧 Manual Setup (If Automatic Fails)

### Step 1: Install Python
```bash
# Windows: Use installer from python.org
# Linux: apt-get install python3.10
# macOS: brew install python@3.10

# Verify
python --version
```

### Step 2: Install Java
```bash
# Windows: choco install openjdk17
# Linux: apt-get install openjdk-17-jdk
# macOS: brew install openjdk@17

# Verify
java -version
```

### Step 3: Set Environment Variables
```bash
# Windows PowerShell
$env:JAVA_HOME = "C:\Program Files\Java\jdk-17"
$env:PATH += ";$env:JAVA_HOME\bin"

# Linux/macOS
export JAVA_HOME=/usr/libexec/java_home
export PATH="$JAVA_HOME/bin:$PATH"
```

### Step 4: Install Python Packages
```bash
# Upgrade pip
python -m pip install --upgrade pip

# Install dependencies
pip install -r scripts/requirements_lakehouse.txt

# Verify
python -c "import pyspark, delta; print('OK')"
```

### Step 5: Create Directories
```bash
mkdir -p lakehouse/lakehouse_data/{bronze,silver,gold}/news
mkdir -p lakehouse/lakehouse_data/gold/{word_frequency,news_per_source,word_velocity,cross_source_topics}
mkdir -p lakehouse/logs
```

### Step 6: Test Spark
```bash
cd lakehouse
python ../scripts/setup_lakehouse.py
```

---

## 🐛 Troubleshooting

### Issue: "Python not found"
```
Solution:
1. Install Python 3.8+ from python.org
2. Make sure "Add Python to PATH" is checked during installation
3. Restart terminal/PowerShell
4. Verify: python --version
```

### Issue: "Java not found"
```
Solution:
1. Install JDK 11+ (not JRE)
2. Set JAVA_HOME environment variable
3. Add $JAVA_HOME/bin to PATH
4. Verify: java -version
```

### Issue: "ModuleNotFoundError: No module named 'pyspark'"
```
Solution:
1. pip install --upgrade pyspark delta-spark
2. Verify installation: python -c "import pyspark"
3. If still fails, uninstall & reinstall:
   pip uninstall pyspark delta-spark -y
   pip install pyspark==3.5.0 delta-spark==3.1.0
```

### Issue: "Spark context failed to initialize"
```
Solution:
1. Verify Java: java -version
2. Set JAVA_HOME correctly
3. Increase heap size: 
   export SPARK_DRIVER_MEMORY=4g (Linux/Mac)
   $env:SPARK_DRIVER_MEMORY = "4g" (Windows)
4. Try again
```

### Issue: "Delta Lake merge schema not working"
```
Solution:
1. Make sure delta-spark>=3.0.0 installed
2. Config should have Delta extensions:
   .config("spark.sql.extensions", 
           "io.delta.sql.DeltaSparkSessionExtension")
3. Check BONUS_schema_evolution.py example
```

---

## 📊 Verify Installation Programmatically

```python
# save as test_setup.py and run: python test_setup.py

from pyspark.sql import SparkSession
import sys

print("=" * 70)
print("VERIFICATION TEST")
print("=" * 70)

# Test 1: Import checks
try:
    import pyspark
    print(f"✓ PySpark {pyspark.__version__}")
except ImportError as e:
    print(f"✗ PySpark import failed: {e}")
    sys.exit(1)

try:
    import delta
    print(f"✓ Delta Lake {delta.__version__}")
except ImportError as e:
    print(f"✗ Delta import failed: {e}")
    sys.exit(1)

try:
    import pyarrow
    print(f"✓ PyArrow {pyarrow.__version__}")
except ImportError as e:
    print(f"✗ PyArrow import failed: {e}")
    sys.exit(1)

# Test 2: Spark Session
try:
    from delta import configure_spark_with_delta_pip
    
    builder = SparkSession.builder.appName("VerificationTest") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", 
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    print(f"✓ Spark Session created (v{spark.version})")
    
    # Test 3: Create & write Delta table
    df = spark.createDataFrame([(1, "test")], ["id", "name"])
    df.write.format("delta").mode("overwrite").save("./test_verify")
    print("✓ Delta Lake table write successful")
    
    # Cleanup
    import shutil
    shutil.rmtree("./test_verify", ignore_errors=True)
    spark.stop()
    print("✓ Spark session stopped")
    
except Exception as e:
    print(f"✗ Spark test failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n" + "=" * 70)
print("✓ ALL TESTS PASSED - SETUP IS READY!")
print("=" * 70)
```

---

## 🎯 Next Steps After Setup

1. **Read Documentation**
   - `lakehouse/README_lakehouse.md` - Architecture overview
   - `lakehouse/00_setup.md` - Detailed setup guide

2. **Run Pipeline**
   - `cd lakehouse`
   - `python 01_bronze.py` - Ingest data
   - `python 02_silver.py` - Clean data
   - `python 03_gold.py` - Create aggregations

3. **Check Results**
   - Open `lakehouse_data/` to see output tables
   - Review scoring checklist: `SCORING_CHECKLIST.md`

4. **Prepare for Presentation**
   - Screenshot key outputs
   - Note improvement metrics vs ETS
   - Practice explaining Time Travel demo

---

## 📞 Support & Resources

### Local Resources
- `lakehouse/README_lakehouse.md` - Full documentation
- `lakehouse/00_setup.md` - Setup troubleshooting
- `lakehouse/QUICK_START.md` - Quick reference

### External Resources
- [Apache Spark Docs](https://spark.apache.org/docs/latest/)
- [Delta Lake Docs](https://docs.delta.io/)
- [PySpark API](https://spark.apache.org/docs/latest/api/python/)

---

**Status:** ✓ Ready for use  
**Last Verified:** May 2026
