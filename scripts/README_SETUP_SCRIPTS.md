# 📚 Lakehouse Setup Scripts - Index

**Location:** `scripts/`  
**Purpose:** Automated setup dan documentation untuk Data Lakehouse NewsPulse  
**Last Updated:** May 2026

---

## 📁 Files di Folder Ini

### 1. **setup_lakehouse.ps1** (Windows PowerShell)
**File untuk:** Windows users  
**Cara jalankan:**
```powershell
.\setup_lakehouse.ps1
```
**Fungsi:**
- Check Python 3.8+
- Check Java installation
- Install pip packages
- Create directory structure
- Test Spark + Delta Lake
- Generate configuration files

**Fitur:**
- Colorful output
- Error handling
- Environment variable setup
- Quick start guide generation

---

### 2. **setup_lakehouse.py** (Cross-platform Python)
**File untuk:** Windows, Linux, macOS  
**Cara jalankan:**
```bash
python setup_lakehouse.py
python setup_lakehouse.py --skip-pip
python setup_lakehouse.py --verbose
```
**Fungsi:**
- Same as PowerShell version
- Works everywhere (universal)
- Better error messages
- Detailed logging

**Keuntungan:**
- Konsisten di semua OS
- Mudah di-debug
- Dapat digunakan sebagai library

---

### 3. **requirements_lakehouse.txt**
**File untuk:** Python package dependencies  
**Isi:**
```
pyspark>=3.4.0          # Apache Spark
delta-spark>=3.1.0      # Delta Lake
pyarrow>=11.0.0         # Arrow serialization
pandas>=1.5.0           # Data manipulation
... dan lainnya
```
**Cara install manual:**
```bash
pip install -r requirements_lakehouse.txt
```

**Packages:**
- Core: PySpark, Delta Lake, PyArrow
- Data: pandas, numpy
- Optional: Flask (dashboard), pytest (testing)

---

### 4. **SETUP_LAKEHOUSE_GUIDE.md**
**File untuk:** Comprehensive setup documentation  
**Isi:**
- System requirements
- Quick setup instructions
- Manual setup steps (jika auto gagal)
- Troubleshooting guide
- Verification checklist
- Support resources

**Gunakan ketika:**
- Setup gagal dan perlu debug
- Perlu instalasi manual
- Perlu verify installation
- Butuh troubleshooting

---

## 🚀 Quick Start

### Option 1: Automated Setup (Recommended)

**Windows:**
```powershell
cd scripts
.\setup_lakehouse.ps1
```

**Linux/macOS/Windows:**
```bash
cd scripts
python setup_lakehouse.py
```

### Option 2: Manual Setup

```bash
# 1. Install dependencies manually
pip install -r scripts/requirements_lakehouse.txt

# 2. Create directories
mkdir -p lakehouse/lakehouse_data/{bronze,silver,gold}/news

# 3. Test installation
python -c "import pyspark, delta; print('OK')"
```

---

## 📊 What Each Script Does

### All Scripts Perform These Steps:

```
1. ✓ Python Version Check (3.8+)
   ↓
2. ✓ pip Verification
   ↓
3. ✓ Java Installation Check
   ↓
4. ✓ Install Python Packages
   ↓
5. ✓ Verify Installed Packages
   ↓
6. ✓ Create Directory Structure
   ↓
7. ✓ Test Spark Session
   ↓
8. ✓ Create .env.example
   ↓
9. ✓ Create QUICK_START.md
   ↓
10. ✓ Summary Report
```

---

## ✅ Verification Checklist

Setelah setup, verify dengan checklist ini:

```
□ Python 3.8+ installed
□ Java JDK installed
□ pyspark module imported successfully
□ delta-spark module imported successfully
□ pyarrow module imported successfully
□ pandas module imported successfully
□ Spark session created successfully
□ Delta table write successful
□ lakehouse/lakehouse_data/ structure created
□ .env.example file exists
□ QUICK_START.md file exists
```

---

## 🎯 Available Options

### setup_lakehouse.ps1 Options:
```powershell
.\setup_lakehouse.ps1 -SkipPip        # Skip pip install
.\setup_lakehouse.ps1 -SkipJava       # Skip Java check
.\setup_lakehouse.ps1 -Verbose        # Verbose output
```

### setup_lakehouse.py Options:
```bash
python setup_lakehouse.py --skip-pip  # Skip pip install
python setup_lakehouse.py --verbose   # Verbose output
```

---

## 📦 Dependencies Installed

### Required
```
pyspark>=3.4.0                  # Big Data processing
delta-spark>=3.1.0              # ACID transactions
pyarrow>=11.0.0                 # Columnar format
pandas>=1.5.0                   # Data manipulation
python-dotenv>=0.21.0           # Environment config
```

### Optional (for Bonus Features)
```
flask>=2.3.0                    # Dashboard integration
requests>=2.28.0                # HTTP requests
pytest>=7.2.0                   # Testing
black>=23.0.0                   # Code formatting
```

---

## 🔧 Customization

### Custom Python Version
```bash
# Force specific Python
python3.10 setup_lakehouse.py
```

### Custom Package Versions
Edit `requirements_lakehouse.txt`:
```
pyspark==3.5.0          # Instead of >=3.4.0
delta-spark==3.1.0      # Specific version
```

### Custom Installation Path
```bash
pip install -r requirements_lakehouse.txt --target ./custom_path
```

---

## 🐛 Common Issues & Solutions

| Issue | Solution |
|-------|----------|
| "Python not found" | Install Python 3.8+ from python.org |
| "Java not found" | Install JDK 11+ and set JAVA_HOME |
| "pip not found" | Run `python -m ensurepip --upgrade` |
| "Import error: pyspark" | `pip install --upgrade pyspark` |
| "OutOfMemory" | Set `SPARK_DRIVER_MEMORY=4g` |

See `SETUP_LAKEHOUSE_GUIDE.md` for detailed troubleshooting.

---

## 📚 Related Documentation

After setup, read these in order:

1. **QUICK_START.md** (auto-generated)
   - How to run Bronze/Silver/Gold layers
   - Quick commands reference

2. **lakehouse/00_setup.md**
   - Detailed environment setup
   - HDFS configuration
   - Advanced options

3. **lakehouse/README_lakehouse.md**
   - Architecture explanation
   - Transformation details
   - Comparison with ETS

4. **lakehouse/SCORING_CHECKLIST.md**
   - Evaluation rubric
   - Verification points
   - Point allocation

---

## 🎯 Success Indicators

Setup berhasil jika:
```
✓ Tidak ada ERROR messages
✓ "SETUP COMPLETED SUCCESSFULLY" muncul
✓ lakehouse/lakehouse_data/ dibuat dengan struktur lengkap
✓ Spark session test passed
✓ QUICK_START.md dan .env.example tercipta
```

---

## 📊 Setup Timeline

Waktu yang dibutuhkan:

| Step | Time | Notes |
|------|------|-------|
| Python check | 10s | Quick |
| pip install | 2-5 min | Depends on internet |
| Verification | 30s | Mostly I/O |
| Directory creation | 10s | Quick |
| Spark test | 30-60s | First time slower |
| **Total** | **3-7 min** | One-time setup |

---

## 🚀 After Setup

1. **Read** `lakehouse/QUICK_START.md`
2. **Run** `cd lakehouse && python 01_bronze.py`
3. **Check** output in `lakehouse/lakehouse_data/bronze/news/`
4. **Continue** with 02_silver.py → 03_gold.py
5. **Verify** using `SCORING_CHECKLIST.md`

---

## 📞 Need Help?

1. Check `SETUP_LAKEHOUSE_GUIDE.md` - Comprehensive guide
2. Read `lakehouse/00_setup.md` - Setup details
3. Review error output - Usually tells what's wrong
4. Check system requirements - Common issues

---

## 📝 Version Info

- **Created:** May 2026
- **Python Version:** 3.8+
- **PySpark Version:** 3.4+
- **Delta Lake Version:** 3.0+
- **Status:** ✓ Production Ready

---

**Last Updated:** May 2026  
**Maintained By:** BigData Course - Kelompok 4  
**Status:** ✓ All Systems Go 🚀
