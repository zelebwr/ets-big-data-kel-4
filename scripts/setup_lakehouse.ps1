# PowerShell Script: Setup Data Lakehouse Environment untuk NewsPulse
# Usage: .\setup_lakehouse.ps1
# Platform: Windows (PowerShell 5.1+)

param(
    [switch]$SkipPip = $false,
    [switch]$SkipJava = $false,
    [switch]$Verbose = $false
)

$ErrorActionPreference = "Continue"

# Colors untuk output
$colors = @{
    Success = "Green"
    Error = "Red"
    Warning = "Yellow"
    Info = "Cyan"
}

function Write-Header {
    param([string]$Text)
    Write-Host "`n" -NoNewline
    Write-Host ("=" * 70) -ForegroundColor $colors.Info
    Write-Host $Text -ForegroundColor $colors.Info
    Write-Host ("=" * 70) -ForegroundColor $colors.Info
}

function Write-Success {
    param([string]$Text)
    Write-Host "✓ $Text" -ForegroundColor $colors.Success
}

function Write-Error-Custom {
    param([string]$Text)
    Write-Host "✗ $Text" -ForegroundColor $colors.Error
}

function Write-Warning-Custom {
    param([string]$Text)
    Write-Host "⚠ $Text" -ForegroundColor $colors.Warning
}

function Write-Info {
    param([string]$Text)
    Write-Host "→ $Text" -ForegroundColor $colors.Info
}

function Test-CommandExists {
    param([string]$Command)
    $null = Get-Command $Command -ErrorAction SilentlyContinue
    return $?
}

# ===== HEADER =====
Write-Header "Data Lakehouse Setup Script - NewsPulse"
Write-Info "Platform: Windows PowerShell"
Write-Info "Purpose: Setup environment untuk Bronze → Silver → Gold pipeline`n"

# ===== STEP 1: Python Check =====
Write-Header "STEP 1: Checking Python Installation"

if (Test-CommandExists python) {
    $pythonVersion = python --version 2>&1
    Write-Success "Python found: $pythonVersion"
    
    # Check Python version (need 3.8+)
    $versionMatch = $pythonVersion -match "(\d+\.\d+)"
    if ($versionMatch) {
        $version = [version]$matches[1]
        if ($version -ge [version]"3.8") {
            Write-Success "Python version compatible (3.8+)"
        } else {
            Write-Error-Custom "Python version too old (need 3.8+, got $version)"
            exit 1
        }
    }
} else {
    Write-Error-Custom "Python tidak ditemukan!"
    Write-Info "Install dari: https://www.python.org/ (pastikan 'Add Python to PATH')"
    exit 1
}

# ===== STEP 2: Check pip =====
Write-Header "STEP 2: Checking pip (Package Manager)"

if (Test-CommandExists pip) {
    $pipVersion = pip --version
    Write-Success "pip found: $pipVersion"
} else {
    Write-Error-Custom "pip tidak ditemukan!"
    Write-Info "Jalankan: python -m ensurepip --upgrade"
    exit 1
}

# ===== STEP 3: Java Check =====
Write-Header "STEP 3: Checking Java (Required for PySpark)"

if (-not $SkipJava) {
    if (Test-CommandExists java) {
        $javaVersion = java -version 2>&1
        Write-Success "Java found"
        Write-Info $javaVersion[0]
    } else {
        Write-Warning-Custom "Java tidak ditemukan (diperlukan untuk PySpark)"
        Write-Info "Install dari: https://www.oracle.com/java/technologies/downloads/"
        Write-Info "Atau gunakan: winget install Oracle.JDK.17"
        
        $response = Read-Host "Continue without Java? (y/n)"
        if ($response -ne "y") {
            exit 1
        }
    }
}

# ===== STEP 4: Environment Variables =====
Write-Header "STEP 4: Setting Environment Variables"

# JAVA_HOME
if (Test-Path "C:\Program Files\Java") {
    $javaPath = Get-ChildItem "C:\Program Files\Java" -Directory | Select-Object -First 1 -ExpandProperty FullName
    if ($javaPath) {
        $env:JAVA_HOME = $javaPath
        Write-Success "JAVA_HOME set to: $javaPath"
    }
}

# HADOOP_HOME (if Hadoop installed)
if (Test-Path "C:\hadoop") {
    $env:HADOOP_HOME = "C:\hadoop"
    Write-Success "HADOOP_HOME set to: C:\hadoop"
} else {
    Write-Warning-Custom "HADOOP_HOME not set (optional - for HDFS access)"
}

# ===== STEP 5: Install Python Packages =====
Write-Header "STEP 5: Installing Python Packages"

if ($SkipPip) {
    Write-Warning-Custom "Skipping pip install (as requested)"
} else {
    Write-Info "Installing from requirements_lakehouse.txt..."
    
    # Get script directory
    $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
    $rootDir = Split-Path -Parent $scriptDir
    $requirementsFile = Join-Path $rootDir "scripts" "requirements_lakehouse.txt"
    
    if (Test-Path $requirementsFile) {
        Write-Info "Found: $requirementsFile"
        
        # Upgrade pip first
        Write-Info "Upgrading pip..."
        python -m pip install --upgrade pip 2>&1 | Select-String "Successfully|Requirement" | ForEach-Object { Write-Info $_ }
        
        # Install requirements
        Write-Info "Installing packages..."
        pip install -r $requirementsFile
        
        if ($LASTEXITCODE -eq 0) {
            Write-Success "All packages installed successfully"
        } else {
            Write-Error-Custom "Some packages failed to install"
            exit 1
        }
    } else {
        Write-Error-Custom "requirements_lakehouse.txt not found at $requirementsFile"
        exit 1
    }
}

# ===== STEP 6: Verify Installation =====
Write-Header "STEP 6: Verifying Installation"

$packages = @("pyspark", "delta", "pyarrow", "pandas")

foreach ($package in $packages) {
    try {
        $result = python -c "import $package; print($package.__version__)" 2>&1
        Write-Success "$package : $result"
    } catch {
        Write-Error-Custom "$package : NOT INSTALLED"
    }
}

# ===== STEP 7: Create Lakehouse Directory Structure =====
Write-Header "STEP 7: Creating Directory Structure"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$rootDir = Split-Path -Parent $scriptDir
$lakehouseDir = Join-Path $rootDir "lakehouse"

$dirs = @(
    "lakehouse_data\bronze\news",
    "lakehouse_data\silver\news",
    "lakehouse_data\gold\word_frequency",
    "lakehouse_data\gold\news_per_source",
    "lakehouse_data\gold\word_velocity",
    "lakehouse_data\gold\cross_source_topics",
    "logs"
)

foreach ($dir in $dirs) {
    $fullPath = Join-Path $lakehouseDir $dir
    if (-not (Test-Path $fullPath)) {
        New-Item -ItemType Directory -Path $fullPath -Force | Out-Null
        Write-Success "Created: $dir"
    } else {
        Write-Info "Already exists: $dir"
    }
}

# ===== STEP 8: Test Spark Session =====
Write-Header "STEP 8: Testing Spark Session"

Write-Info "Attempting to create Spark session..."

$testScript = @"
from pyspark.sql import SparkSession
try:
    spark = SparkSession.builder \
        .appName("LakehouseTest") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
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
    exit(1)
"@

$testScript | python
if ($LASTEXITCODE -eq 0) {
    Write-Success "Spark session test passed!"
} else {
    Write-Error-Custom "Spark session test failed"
    exit 1
}

# ===== STEP 9: Create Environment File =====
Write-Header "STEP 9: Creating .env Configuration File"

$envFile = Join-Path $lakehouseDir ".env.example"

$envContent = @"
# Data Lakehouse Configuration
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
"@

if (-not (Test-Path $envFile)) {
    $envContent | Set-Content $envFile
    Write-Success "Created: .env.example"
} else {
    Write-Info "Already exists: .env.example"
}

# ===== STEP 10: Create Quick Start Script =====
Write-Header "STEP 10: Creating Quick Start Guide"

$quickStartPath = Join-Path $lakehouseDir "QUICK_START.md"

$quickStartContent = @"
# 🚀 Quick Start - Data Lakehouse NewsPulse

## Setup Selesai! Berikut cara menjalankan:

### 1. Bronze Layer (Ingest dari HDFS)
\`\`\`bash
cd lakehouse
python 01_bronze.py --hdfs-base "hdfs://localhost:8020/data/news" --output "./lakehouse_data/bronze/news"
\`\`\`

### 2. Silver Layer (Cleaning)
\`\`\`bash
python 02_silver.py --bronze-path "./lakehouse_data/bronze/news" --output "./lakehouse_data/silver/news"
\`\`\`

### 3. Gold Layer (Aggregation)
\`\`\`bash
python 03_gold.py --silver-path "./lakehouse_data/silver/news" --output "./lakehouse_data/gold"
\`\`\`

### BONUS: Dashboard Integration
\`\`\`bash
python BONUS_dashboard_integration.py
\`\`\`

### BONUS: Schema Evolution
\`\`\`bash
python BONUS_schema_evolution.py
\`\`\`

## Troubleshooting

### Error: HDFS Connection Refused
Gunakan fallback lokal:
\`\`\`bash
# Copyfile JSON ke lokal dulu
mkdir -p ./data_local
python 01_bronze.py --hdfs-base "./data_local" --use-local
\`\`\`

### Error: OutOfMemory
Tingkatkan Spark memory:
\`\`\`bash
python 01_bronze.py --driver-memory 6g --executor-memory 6g
\`\`\`

## Dokumentasi Lengkap
- README_lakehouse.md - Penjelasan arsitektur & transformasi
- 00_setup.md - Setup guide detail
- SCORING_CHECKLIST.md - Rubrik penilaian

## Informasi Berguna
- Folder Output: ./lakehouse_data/
- Log Files: ./logs/
- Sample Config: .env.example

Selamat! Setup lakehouse Anda sudah siap! 🎉
"@

$quickStartContent | Set-Content $quickStartPath
Write-Success "Created: QUICK_START.md"

# ===== STEP 11: Verify Directory Structure =====
Write-Header "STEP 11: Directory Structure Summary"

Write-Info "Lakehouse directory: $lakehouseDir"
Write-Info "`nDirectories created:"

Get-ChildItem -Path "$lakehouseDir\lakehouse_data" -Recurse -Directory | ForEach-Object {
    $relative = $_.FullName -replace [regex]::Escape($lakehouseDir), ""
    Write-Info "  ✓ $relative"
}

# ===== FINAL SUMMARY =====
Write-Header "✓ SETUP COMPLETED SUCCESSFULLY"

Write-Success "All prerequisites checked and installed"
Write-Success "Environment variables configured"
Write-Success "Directory structure created"
Write-Success "Spark + Delta Lake verified"

Write-Info "`nNext Steps:"
Write-Info "1. cd lakehouse"
Write-Info "2. python 01_bronze.py (atau baca QUICK_START.md untuk detail)"
Write-Info "3. python 02_silver.py"
Write-Info "4. python 03_gold.py"

Write-Info "`nDocumentation:"
Write-Info "- Baca: lakehouse/00_setup.md"
Write-Info "- Baca: lakehouse/README_lakehouse.md"
Write-Info "- Gunakan: lakehouse/SCORING_CHECKLIST.md saat evaluasi"

Write-Info "`nSetup timestamp: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"

Write-Host "`n"
