#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
LAKEHOUSE_DIR = ROOT_DIR / "lakehouse"
DASHBOARD_DIR = ROOT_DIR / "dashboard" / "data"
LAKEHOUSE_OUTPUT_JSON = LAKEHOUSE_DIR / "dashboard_data_from_delta.json"
DASHBOARD_SPARK_JSON = DASHBOARD_DIR / "spark_results.json"
LIVE_API_JSON = DASHBOARD_DIR / "live_api.json"
LIVE_RSS_JSON = DASHBOARD_DIR / "live_rss.json"
PIPELINE_STATUS_JSON = DASHBOARD_DIR / "pipeline_status.json"


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return default


def build_dashboard_snapshot_from_lakehouse() -> dict:
    lakehouse_payload = load_json(LAKEHOUSE_OUTPUT_JSON, {})
    live_api = load_json(LIVE_API_JSON, [])
    live_rss = load_json(LIVE_RSS_JSON, [])

    kata_trending = lakehouse_payload.get("kata_trending", []) if isinstance(lakehouse_payload, dict) else []
    distribusi_sumber = lakehouse_payload.get("distribusi_sumber", []) if isinstance(lakehouse_payload, dict) else []
    volume_per_jam = lakehouse_payload.get("volume_per_jam", []) if isinstance(lakehouse_payload, dict) else []

    if not isinstance(volume_per_jam, list) or len(volume_per_jam) == 0:
        volume_per_jam = [{"jam": hour_index, "jumlah_berita": 0} for hour_index in range(24)]

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "source": "lakehouse_continuous_runner",
        "kata_trending": kata_trending,
        "distribusi_sumber": distribusi_sumber,
        "volume_per_jam": volume_per_jam,
        "live_news": [],
        "total_api": len(live_api),
        "total_rss": len(live_rss),
    }


def write_dashboard_snapshot() -> None:
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    snapshot = build_dashboard_snapshot_from_lakehouse()
    DASHBOARD_SPARK_JSON.write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    # Update pipeline status after snapshot
    status = {
        "phase": "gold",
        "last_run": datetime.utcnow().isoformat() + "Z",
        "last_success": True,
        "source": "run_lakehouse_continuous",
    }
    try:
        PIPELINE_STATUS_JSON.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def run_stage(stage_name: str, args: list[str], cwd: Path) -> None:
    print(f"[LAKEHOUSE] Running {stage_name}: {' '.join(args)}")
    subprocess.run(args, cwd=str(cwd), check=True)


def run_one_cycle(use_local: bool, hdfs_base: str, log_level: str) -> None:
    bronze_output = "./lakehouse_data/bronze/news"
    silver_output = "./lakehouse_data/silver/news"
    gold_output = "./lakehouse_data/gold"

    bronze_cmd = [
        sys.executable,
        "01_bronze.py",
        "--hdfs-base",
        hdfs_base,
        "--output",
        bronze_output,
        "--log-level",
        log_level,
    ]
    if use_local:
        bronze_cmd.append("--use-local")

    silver_cmd = [
        sys.executable,
        "02_silver.py",
        "--bronze-path",
        bronze_output,
        "--output",
        silver_output,
        "--log-level",
        log_level,
    ]

    gold_cmd = [
        sys.executable,
        "03_gold.py",
        "--silver-path",
        silver_output,
        "--output",
        gold_output,
        "--log-level",
        log_level,
    ]

    dashboard_cmd = [sys.executable, "BONUS_dashboard_integration.py"]

    run_stage("bronze", bronze_cmd, LAKEHOUSE_DIR)
    run_stage("silver", silver_cmd, LAKEHOUSE_DIR)
    run_stage("gold", gold_cmd, LAKEHOUSE_DIR)
    run_stage("dashboard integration", dashboard_cmd, LAKEHOUSE_DIR)

    write_dashboard_snapshot()
    print(f"[LAKEHOUSE] Snapshot updated: {DASHBOARD_SPARK_JSON}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run Lakehouse Bronze/Silver/Gold continuously and refresh dashboard data"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=120,
        help="Interval antar siklus (detik), default 120",
    )
    parser.add_argument(
        "--hdfs-base",
        default="./local_input",
        help="Base path input Bronze (HDFS atau local path)",
    )
    parser.add_argument(
        "--use-local",
        action="store_true",
        help="Paksa baca local filesystem untuk Bronze",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Jalankan satu siklus saja (untuk test)",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="ERROR",
        help="Log level Spark scripts",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    interval = max(1, args.interval)

    if args.once:
        run_one_cycle(args.use_local, args.hdfs_base, args.log_level)
        return 0

    print(f"[WATCH] Lakehouse continuous mode aktif setiap {interval} detik")
    try:
        while True:
            started = datetime.utcnow().isoformat() + "Z"
            print(f"[WATCH] Siklus dimulai: {started}")
            try:
                run_one_cycle(args.use_local, args.hdfs_base, args.log_level)
                print("[WATCH] Siklus selesai")
            except subprocess.CalledProcessError as exc:
                print(f"[WATCH] Siklus gagal: {exc}")
            except Exception as exc:
                print(f"[WATCH] Error tak terduga: {exc}")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n[WATCH] Dihentikan oleh user")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
