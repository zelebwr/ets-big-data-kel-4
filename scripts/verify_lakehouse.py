#!/usr/bin/env python3
"""
Verification Script untuk Data Lakehouse Setup

Purpose: Verify semua komponen sudah terinstall dengan benar
Usage: python verify_lakehouse.py
"""

import sys
import subprocess
from pathlib import Path

class Verifier:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.warnings = 0
    
    def test(self, name, func):
        """Run single test"""
        print(f"\n  Testing: {name}...", end=" ", flush=True)
        try:
            result = func()
            if result:
                print("✓ PASS")
                self.passed += 1
            else:
                print("✗ FAIL")
                self.failed += 1
        except Exception as e:
            print(f"✗ FAIL ({e})")
            self.failed += 1
    
    def warn(self, name, func):
        """Run warning test (non-critical)"""
        print(f"\n  Checking: {name}...", end=" ", flush=True)
        try:
            result = func()
            if result:
                print("✓ OK")
            else:
                print("⚠ WARNING")
                self.warnings += 1
        except Exception as e:
            print(f"⚠ WARNING ({e})")
            self.warnings += 1
    
    def summary(self):
        """Print summary"""
        total = self.passed + self.failed
        print(f"\n\n{'='*70}")
        print(f"PASSED:  {self.passed}/{total}")
        print(f"FAILED:  {self.failed}/{total}")
        print(f"WARNINGS: {self.warnings}")
        print(f"{'='*70}")
        
        if self.failed == 0:
            print("✓ All tests passed! Setup is ready.")
            return 0
        else:
            print(f"✗ {self.failed} test(s) failed. Please fix before running pipeline.")
            return 1

def main():
    print("\n" + "="*70)
    print("🔍 DATA LAKEHOUSE VERIFICATION")
    print("="*70)
    
    v = Verifier()
    
    # ===== PYTHON =====
    print("\n[1/4] PYTHON & CORE MODULES")
    
    v.test("Python 3.8+", 
           lambda: sys.version_info >= (3, 8))
    
    v.test("pyspark", 
           lambda: __import__('pyspark').__version__)
    
    v.test("delta-spark", 
           lambda: __import__('delta').__version__)
    
    v.test("pyarrow", 
           lambda: __import__('pyarrow').__version__)
    
    v.test("pandas", 
           lambda: __import__('pandas').__version__)
    
    # ===== JAVA =====
    print("\n[2/4] JAVA ENVIRONMENT")
    
    def check_java():
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            text=True,
            timeout=5
        )
        return result.returncode == 0
    
    v.warn("Java installed", check_java)
    
    # ===== SPARK SESSION =====
    print("\n[3/4] SPARK + DELTA LAKE SESSION")
    
    def test_spark_session():
        try:
            from pyspark.sql import SparkSession
            from delta import configure_spark_with_delta_pip
            
            builder = SparkSession.builder.appName("VerificationTest") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", 
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            
            spark = configure_spark_with_delta_pip(builder).getOrCreate()
            
            # Test create DataFrame
            df = spark.createDataFrame([(1, 'test')], ['id', 'name'])
            
            # Test Delta write (in-memory only)
            import tempfile
            with tempfile.TemporaryDirectory() as tmpdir:
                df.write.format('delta').mode('overwrite').save(tmpdir)
            
            spark.stop()
            return True
        except Exception as e:
            print(f"\nError: {e}")
            return False
    
    v.test("Spark session creation & Delta write", test_spark_session)
    
    # ===== DIRECTORIES =====
    print("\n[4/4] DIRECTORY STRUCTURE")
    
    script_dir = Path(__file__).parent
    root_dir = script_dir.parent
    lakehouse_dir = root_dir / "lakehouse"
    
    required_dirs = [
        "lakehouse_data/bronze/news",
        "lakehouse_data/silver/news",
        "lakehouse_data/gold/word_frequency",
        "lakehouse_data/gold/news_per_source",
        "lakehouse_data/gold/word_velocity",
        "lakehouse_data/gold/cross_source_topics",
    ]
    
    for dir_path in required_dirs:
        full_path = lakehouse_dir / dir_path
        v.test(f"{dir_path}", lambda p=full_path: p.exists())
    
    # ===== CONFIGURATION FILES =====
    required_files = [
        "00_setup.md",
        "README_lakehouse.md",
        "SCORING_CHECKLIST.md",
        "01_bronze.py",
        "02_silver.py",
        "03_gold.py",
    ]
    
    for file_name in required_files:
        file_path = lakehouse_dir / file_name
        v.test(f"{file_name}", lambda p=file_path: p.exists())
    
    # ===== SUMMARY =====
    exit_code = v.summary()
    
    print("\n" + "="*70)
    print("NEXT STEPS:")
    print("="*70)
    
    if exit_code == 0:
        print("\n✓ Setup verified successfully!")
        print("  1. cd lakehouse")
        print("  2. python 01_bronze.py")
        print("  3. python 02_silver.py")
        print("  4. python 03_gold.py")
        print("\nFor more info: cat lakehouse/QUICK_START.md")
    else:
        print("\n✗ Some tests failed.")
        print("  Please fix the issues and run verify again.")
        print("  For troubleshooting: cat scripts/SETUP_LAKEHOUSE_GUIDE.md")
    
    print("\n" + "="*70 + "\n")
    
    return exit_code

if __name__ == "__main__":
    sys.exit(main())
