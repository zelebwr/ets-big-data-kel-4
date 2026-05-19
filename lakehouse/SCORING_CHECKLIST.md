# Scoring Checklist - Lakehouse NewsPulse

**Student Group:** [Nama Kelompok]  
**Submission Date:** [Tanggal]  
**Total Points Possible:** 100 (+ 10 bonus) = **110 Max**  

---

## 📋 Penilaian Utama (100 Poin)

### ✅ Bronze Layer (15 Poin)

Requirement: Data dari HDFS berhasil diingest ke Delta format dengan metadata `_ingested_at` & `_source`; API + RSS keduanya masuk

**Checklist:**
- [ ] File `01_bronze.py` ada di folder `lakehouse/`
- [ ] Script berjalan tanpa error dengan command: `python 01_bronze.py --hdfs-base "hdfs://localhost:8020/data/news"`
- [ ] Folder `./lakehouse_data/bronze/news/` terbentuk setelah running
- [ ] Bisa membaca Bronze table: `spark.read.format("delta").load("./lakehouse_data/bronze/news/")`
- [ ] Minimal 50 records diinggest (API + RSS combined)
- [ ] Kolom `_ingested_at` ada dan bertipe timestamp
- [ ] Kolom `_source` ada dan berisi "api" atau "rss"
- [ ] Bisa display 3 sample records dari Bronze table
- [ ] API records count > 0
- [ ] RSS records count > 0

**Evidence (Screenshot/Output):**
- [ ] Output dari `01_bronze.py` (record count, schema, sample data)
- [ ] Folder path screenshot

**Points Awarded:** ___/15

---

### ✅ Silver Layer (25 Poin)

Requirement: Minimal 3 transformasi cleaning relevan, terdokumentasi, jumlah baris berkurang dengan alasan valid

**Checklist - Transformasi 1 (DEDUPLIKASI):**
- [ ] File `02_silver.py` ada
- [ ] Script menggunakan `.dropDuplicates(["url"])`
- [ ] Alasan deduplikasi jelas (URL = unique identifier)
- [ ] Output menunjukkan jumlah duplikat yang dihapus
- [ ] Jumlah baris berkurang (Bronze count > Silver count)

**Checklist - Transformasi 2 (TYPE CASTING & TIMESTAMP):**
- [ ] Script menggunakan `to_timestamp()` untuk parsing timestamp
- [ ] Script menggunakan `hour(col("parsed_timestamp"))` untuk extract jam
- [ ] Output menunjukkan berapa % record berhasil di-parse
- [ ] Kolom `jam` ada di output (bertipe integer, 0-23)
- [ ] Kolom `parsed_timestamp` ada (bertipe timestamp)

**Checklist - Transformasi 3 (NORMALISASI & NULL HANDLING):**
- [ ] Script menggunakan `.trim()` pada kolom text
- [ ] Script menggunakan `.fillna()` atau `coalesce()` untuk null handling
- [ ] Output menunjukkan berapa null yang di-replace
- [ ] Kolom `sumber` tidak ada yang null di Silver (semua terisi)

**Checklist - Documentation:**
- [ ] README_lakehouse.md menjelaskan setiap transformasi
- [ ] README menunjukkan alasan "MENGAPA" transformasi penting
- [ ] README menampilkan jumlah baris sebelum/sesudah dengan %
- [ ] Folder `./lakehouse_data/silver/news/` terbentuk
- [ ] Bisa read Silver table tanpa error

**Evidence (Screenshot/Output):**
- [ ] Output dari `02_silver.py` dengan data quality report
- [ ] README_lakehouse.md section pada transformasi Silver
- [ ] Folder path screenshot

**Points Awarded:** ___/25

---

### ✅ Gold Layer - Reproduksi ETS (20 Poin)

Requirement: Minimal 2 tabel Gold yang mereproduksi analisis Spark ETS sebelumnya

**Checklist - Tabel 1 (word_frequency):**
- [ ] File `03_gold.py` ada
- [ ] Tabel `word_frequency` berhasil dibuat di `./lakehouse_data/gold/word_frequency/`
- [ ] Tabel berisi Top 15 kata trending
- [ ] Kolom ada: `kata`, `frekuensi`
- [ ] Data terurut by frekuensi descending
- [ ] Stopwords sudah dihapus (tidak ada "dan", "yang", "untuk", dll)
- [ ] Bisa query dan show hasil tanpa error
- [ ] Hasil lebih akurat dari ETS (duplikat sudah dihapus di Silver)

**Checklist - Tabel 2 (news_per_source):**
- [ ] Tabel `news_per_source` berhasil dibuat di `./lakehouse_data/gold/news_per_source/`
- [ ] Tabel menampilkan volume berita per sumber
- [ ] Kolom ada: `sumber`, `jumlah`
- [ ] Data terurut by jumlah descending
- [ ] Bisa query dan show hasil tanpa error
- [ ] Mencakup sumber dari API + RSS

**Checklist - Comparison vs ETS:**
- [ ] README menunjukkan perbedaan akurasi vs ETS
- [ ] README menunjukkan improvement percentage
- [ ] Contoh: "ETS: 45 artikel 'presiden', Lakehouse: 40 (5 duplikat dihapus)"

**Evidence (Screenshot/Output):**
- [ ] Output `03_gold.py` menampilkan kedua tabel
- [ ] Top 15 kata screenshot
- [ ] News per sumber screenshot
- [ ] Comparison table di README_lakehouse.md

**Points Awarded:** ___/20

---

### ✅ Gold Layer - Enhanced Analysis (20 Poin)

Requirement: Minimal 1 tabel Gold ENHANCED (Window Function, cross-source join, atau derived metric) yang tidak ada di ETS

**Checklist - Tabel 3 (word_velocity - ENHANCED):**
- [ ] Tabel `word_velocity` berhasil dibuat di `./lakehouse_data/gold/word_velocity/`
- [ ] Menggunakan Window Function: `Window.partitionBy("kata").orderBy("jam")`
- [ ] Menggunakan `lag()` untuk membandingkan dengan jam sebelumnya
- [ ] Menghitung `velocity = freq_current - freq_previous`
- [ ] Output menampilkan kata dengan velocity tertinggi (positif = trending naik)
- [ ] Tidak mungkin dilakukan di ETS (perlu timestamp terstruktur)
- [ ] Kolom ada: `kata`, `jam`, `freq_current`, `freq_previous`, `velocity`

**Checklist - Tabel 4 (cross_source_topics - ENHANCED BONUS):**
- [ ] Tabel `cross_source_topics` berhasil dibuat di `./lakehouse_data/gold/cross_source_topics/`
- [ ] Merupakan INNER JOIN antara Silver API + Silver RSS
- [ ] Mengidentifikasi topik yang muncul di kedua sumber
- [ ] Kolom ada: `topik`, `api_count`, `rss_count`, `co_occurrence_ratio`
- [ ] Menunjukkan validasi trending (bukan single-source noise)
- [ ] Tidak mungkin dilakukan di ETS (perlu clean Silver data)

**Checklist - Documentation:**
- [ ] README menjelaskan logika word_velocity dengan formula
- [ ] README menjelaskan mengapa Window Function diperlukan
- [ ] README menjelaskan benefit cross_source_topics
- [ ] README menunjukkan contoh output

**Evidence (Screenshot/Output):**
- [ ] Output tabel word_velocity (top 10 trending)
- [ ] Output tabel cross_source_topics (top 10 topics)
- [ ] README section Enhanced Analysis

**Points Awarded:** ___/20

---

### ✅ Time Travel Demo (10 Poin)

Requirement: Demonstrasi perubahan data (update/delete) dan query versi lama berhasil, output perbandingan ditampilkan

**Checklist:**
- [ ] Demo Time Travel ada di output `03_gold.py`
- [ ] Menampilkan `DeltaTable.history()` dengan columns: version, timestamp, operation
- [ ] Bisa membaca versi 0: `spark.read.format("delta").option("versionAsOf", 0).load(...)`
- [ ] Simulasi UPDATE pada Silver table (contoh: fill null sumber)
- [ ] Bandingan record count antara versi 0 vs versi latest
- [ ] Bandingan null count di kolom `sumber` sebelum/sesudah update
- [ ] Output menunjukkan perubahan yang terdeteksi
- [ ] Dokumentasi Time Travel ada di README

**Evidence (Screenshot/Output):**
- [ ] History table screenshot (versi 0, 1, 2, dst)
- [ ] Perbandingan data v0 vs latest screenshot
- [ ] Update verification screenshot
- [ ] Console output dari demo

**Points Awarded:** ___/10

---

### ✅ README & Refleksi (10 Poin)

Requirement: Diagram arsitektur sebelum/sesudah, justifikasi transformasi Silver, perbandingan hasil Gold vs ETS

**Checklist - Diagram:**
- [ ] Ada diagram arsitektur SEBELUM (ETS - JSON → Spark → Results)
- [ ] Ada diagram arsitektur SESUDAH (Lakehouse - Bronze → Silver → Gold)
- [ ] Diagram menunjukkan alur data dan transformasi

**Checklist - Justifikasi Transformasi Silver:**
- [ ] Deduplikasi: alasan + jumlah row hilang
- [ ] Type Casting: mengapa untuk temporal analysis
- [ ] Normalisasi: impact pada akurasi grouping/join

**Checklist - Perbandingan Gold vs ETS:**
- [ ] Tabel perbandingan dengan kolom: Metrik, ETS, Gold, Improvement
- [ ] Contoh real scenario dengan angka konkret
- [ ] Explanation: "Presiden ada di 45 artikel, tapi 5 duplikat = seharusnya 40"

**Checklist - Keuntungan Delta Lake:**
- [ ] ACID Transactions dijelaskan
- [ ] Time Travel dijelaskan
- [ ] Schema Evolution dijelaskan
- [ ] Data Lineage dijelaskan
- [ ] Query Performance dijelaskan

**Checklist - File Structure:**
- [ ] `README_lakehouse.md` lengkap dan terstruktur
- [ ] `00_setup.md` lengkap dengan instruksi
- [ ] Semua file `.py` memiliki docstring dan komentar
- [ ] Semua output terdokumentasi dengan screenshot

**Evidence:**
- [ ] README_lakehouse.md view/screenshot
- [ ] 00_setup.md view/screenshot
- [ ] Semua diagram terlihat jelas

**Points Awarded:** ___/10

---

## ⭐ Bonus Penilaian (Maksimal +10 Poin)

### BONUS 1: Dashboard Flask Integration (+5 Poin)

Requirement: Update dashboard/app.py untuk membaca dari Gold Delta Layer

**Checklist:**
- [ ] File `BONUS_dashboard_integration.py` ada di folder `lakehouse/`
- [ ] Script berjalan: `python BONUS_dashboard_integration.py`
- [ ] File `dashboard_data_from_delta.json` terbuat
- [ ] Output menampilkan data dari Delta Gold tables (bukan static JSON)
- [ ] Code snippet diberikan untuk integrasi di `dashboard/app.py`
- [ ] Dashboard bisa membaca `word_frequency` dari Delta
- [ ] Dashboard bisa membaca `news_per_source` dari Delta
- [ ] Dashboard bisa membaca `cross_source_topics` dari Delta (optional)

**Evidence:**
- [ ] Output script
- [ ] Generated JSON file
- [ ] Code snippet screenshot

**Bonus Points Awarded:** ___/5

---

### BONUS 2: Cross-Source Join Insight (+3 Poin)

Requirement: Cross-source join menghasilkan insight baru di Gold

**Checklist:**
- [ ] Tabel `cross_source_topics` sudah dibuat (sebagai part dari main requirement)
- [ ] Inner join berdasarkan topik yang sama
- [ ] Menampilkan count API vs count RSS
- [ ] Hitung co-occurrence ratio atau similarity metric
- [ ] Insight: Topik yang muncul di kedua sumber lebih credible

**Evidence:**
- [ ] Output tabel cross_source_topics
- [ ] Insight explanation di README

**Bonus Points Awarded:** ___/3

---

### BONUS 3: Schema Evolution Demo (+2 Poin)

Requirement: Tambah kolom baru ke Silver menggunakan mergeSchema

**Checklist:**
- [ ] File `BONUS_schema_evolution.py` ada di folder `lakehouse/`
- [ ] Script berjalan: `python BONUS_schema_evolution.py`
- [ ] Membaca Silver original schema
- [ ] Menambah kolom baru (contoh: content_length, has_image, source_channel)
- [ ] Menulis kembali dengan `mergeSchema=true`
- [ ] Verifikasi kolom baru ada di Silver table
- [ ] Menampilkan before/after schema comparison
- [ ] Mendemonstrasikan benefit: "Tidak perlu full re-ETL"

**Evidence:**
- [ ] Output script (original schema vs updated schema)
- [ ] Sample data dengan kolom baru
- [ ] Benefits explanation

**Bonus Points Awarded:** ___/2

---

## 📊 FINAL SCORING

### Penilaian Utama
| Komponen | Max | Awarded |
|----------|-----|---------|
| Bronze Layer | 15 | ___/15 |
| Silver Layer | 25 | ___/25 |
| Gold Reproduksi | 20 | ___/20 |
| Gold Enhanced | 20 | ___/20 |
| Time Travel | 10 | ___/10 |
| README & Refleksi | 10 | ___/10 |
| **SUBTOTAL** | **100** | **___/100** |

### Bonus Penilaian
| Bonus | Max | Awarded |
|-------|-----|---------|
| Dashboard Integration | 5 | ___/5 |
| Cross-Source Join | 3 | ___/3 |
| Schema Evolution | 2 | ___/2 |
| **BONUS SUBTOTAL** | **10** | **___/10** |

### 🎓 TOTAL SCORE
**Main Score:** ___/100  
**Bonus Score:** +___/10  
**FINAL SCORE:** **___/110**

---

## ✍️ Notes & Comments

**Grader Name:** _________________  
**Date:** _________________  

**Strengths:**
- 

**Areas for Improvement:**
- 

**Comments:**
- 

---

## Appendix: File Locations

Expected structure after completion:
```
lakehouse/
├── 00_setup.md                      ✓
├── README_lakehouse.md              ✓
├── 01_bronze.py                     ✓
├── 02_silver.py                     ✓
├── 03_gold.py                       ✓
├── BONUS_dashboard_integration.py   (optional)
├── BONUS_schema_evolution.py        (optional)
└── lakehouse_data/
    ├── bronze/news/                 ✓
    ├── silver/news/                 ✓
    └── gold/
        ├── word_frequency/          ✓
        ├── news_per_source/         ✓
        ├── word_velocity/           ✓
        └── cross_source_topics/     ✓
```
