#!/usr/bin/env python3
"""
Pinpoint → Full Pipeline Orchestration (Staging Mode)
-----------------------------------------------------
0. Sync skills + job titles from production (pinpoint_sync_taxonomy.sh)
1. Fetch new jobs (pinpoint_fetch.py)
2. Parse with Phi-3 AI parser (pinpoint_parse_ai.py)
3. Canonical mapping to JobMinglr schema (canonical_mapper.py)
4. ✅ Skips production sync — staging-only validation

Safeguards:
 - Lock file prevents overlapping runs
 - Logs everything to /home/ubuntu/pinpoint/logs/pipeline.log
 - Tracks CPU and RAM usage
"""

import os
import sys
import subprocess
from datetime import datetime, timezone
import signal
import atexit
import psutil
import threading
import time

# ---------- CONFIG ----------
LOCK_FILE = "/tmp/pinpoint_pipeline.lock"
LOG_FILE = "/home/ubuntu/pinpoint/logs/pipeline.log"

SCRIPTS_DIR = "/home/ubuntu/pinpoint/scripts"
SYNC_TAXONOMY_SCRIPT = f"{SCRIPTS_DIR}/pinpoint_sync_taxonomy.sh"
FETCH_SCRIPT = f"{SCRIPTS_DIR}/pinpoint_fetch.py"
PARSE_SCRIPT = f"{SCRIPTS_DIR}/pinpoint_parse_ai.py"
MAP_SCRIPT = f"{SCRIPTS_DIR}/canonical_mapper.py"

PYTHON = "/home/ubuntu/pinpoint/venv/bin/python3"


# ---------- RESOURCE MONITORING ----------
class ResourceMonitor:
    """Track CPU and RAM usage during pipeline execution."""
    
    def __init__(self):
        self.process = psutil.Process(os.getpid())
        self.cpu_samples = []
        self.ram_samples = []
        self.system_cpu_samples = []
        self.system_ram_samples = []
        self.peak_ram_mb = 0
        self.peak_system_ram_percent = 0
        self.start_ram_mb = 0
        self.monitoring = False
        self._thread = None
    
    def get_system_info(self) -> dict:
        """Get current system resource info."""
        mem = psutil.virtual_memory()
        cpu_count = psutil.cpu_count()
        return {
            "total_ram_gb": round(mem.total / (1024 ** 3), 2),
            "available_ram_gb": round(mem.available / (1024 ** 3), 2),
            "used_ram_gb": round(mem.used / (1024 ** 3), 2),
            "ram_percent": mem.percent,
            "cpu_count": cpu_count,
            "cpu_percent": psutil.cpu_percent(interval=0.1)
        }
    
    def get_process_info(self) -> dict:
        """Get current process resource usage."""
        try:
            mem_info = self.process.memory_info()
            cpu_percent = self.process.cpu_percent(interval=0.1)
            return {
                "ram_mb": round(mem_info.rss / (1024 ** 2), 2),
                "ram_percent": round(mem_info.rss / psutil.virtual_memory().total * 100, 2),
                "cpu_percent": cpu_percent
            }
        except Exception:
            return {"ram_mb": 0, "ram_percent": 0, "cpu_percent": 0}
    
    def _monitor_loop(self, interval: float = 2.0):
        """Background monitoring loop."""
        while self.monitoring:
            try:
                proc_info = self.get_process_info()
                sys_info = self.get_system_info()
                
                self.cpu_samples.append(proc_info["cpu_percent"])
                self.ram_samples.append(proc_info["ram_mb"])
                self.system_cpu_samples.append(sys_info["cpu_percent"])
                self.system_ram_samples.append(sys_info["ram_percent"])
                
                if proc_info["ram_mb"] > self.peak_ram_mb:
                    self.peak_ram_mb = proc_info["ram_mb"]
                if sys_info["ram_percent"] > self.peak_system_ram_percent:
                    self.peak_system_ram_percent = sys_info["ram_percent"]
            except Exception:
                pass
            time.sleep(interval)
    
    def start(self):
        """Start background monitoring."""
        self.start_ram_mb = self.get_process_info()["ram_mb"]
        self.monitoring = True
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()
    
    def stop(self):
        """Stop background monitoring."""
        self.monitoring = False
        if self._thread:
            self._thread.join(timeout=2)
    
    def get_summary(self) -> dict:
        """Get resource usage summary."""
        avg_cpu = sum(self.cpu_samples) / len(self.cpu_samples) if self.cpu_samples else 0
        avg_ram = sum(self.ram_samples) / len(self.ram_samples) if self.ram_samples else 0
        max_cpu = max(self.cpu_samples) if self.cpu_samples else 0
        min_ram = min(self.ram_samples) if self.ram_samples else 0
        
        avg_sys_cpu = sum(self.system_cpu_samples) / len(self.system_cpu_samples) if self.system_cpu_samples else 0
        avg_sys_ram = sum(self.system_ram_samples) / len(self.system_ram_samples) if self.system_ram_samples else 0
        max_sys_cpu = max(self.system_cpu_samples) if self.system_cpu_samples else 0
        
        return {
            "start_ram_mb": self.start_ram_mb,
            "peak_ram_mb": self.peak_ram_mb,
            "avg_ram_mb": round(avg_ram, 2),
            "min_ram_mb": round(min_ram, 2),
            "avg_cpu_percent": round(avg_cpu, 2),
            "max_cpu_percent": round(max_cpu, 2),
            "avg_sys_cpu_percent": round(avg_sys_cpu, 2),
            "max_sys_cpu_percent": round(max_sys_cpu, 2),
            "avg_sys_ram_percent": round(avg_sys_ram, 2),
            "peak_sys_ram_percent": round(self.peak_system_ram_percent, 2),
            "samples_count": len(self.cpu_samples)
        }


# Initialize resource monitor
resource_monitor = ResourceMonitor()


def format_duration(seconds: float) -> str:
    """Format duration in human readable format."""
    if seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        mins = int(seconds // 60)
        secs = seconds % 60
        return f"{mins}m {secs:.2f}s"
    else:
        hours = int(seconds // 3600)
        mins = int((seconds % 3600) // 60)
        secs = seconds % 60
        return f"{hours}h {mins}m {secs:.2f}s"


def format_bytes(mb: float) -> str:
    """Format memory in human readable format."""
    if mb < 1024:
        return f"{mb:.2f} MB"
    else:
        return f"{mb / 1024:.2f} GB"


# ---------- LOGGING ----------
def log(msg: str, level: str = "INFO"):
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{timestamp}] [{level:8s}] {msg}"
    print(line)
    try:
        os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def log_separator(char: str = "=", width: int = 80):
    log(char * width)


# ---------- LOCK FILE ----------
def cleanup_lock():
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)
        log("🔓 Lock released")


if os.path.exists(LOCK_FILE):
    log("🚫 Another pipeline run is already in progress. Exiting.", "WARNING")
    sys.exit(0)

with open(LOCK_FILE, "w") as f:
    f.write(str(os.getpid()))
atexit.register(cleanup_lock)
signal.signal(signal.SIGTERM, lambda *a: cleanup_lock())
signal.signal(signal.SIGINT, lambda *a: cleanup_lock())


# ---------- STAGE STATS ----------
stage_stats = {}


# ---------- RUNNER ----------
def run_stage(name: str, cmd: str, is_python: bool = True) -> dict:
    """Run a pipeline stage and track timing."""
    start = datetime.now(timezone.utc)
    
    # Get resource snapshot before stage
    res_before = resource_monitor.get_process_info()
    sys_before = resource_monitor.get_system_info()
    
    log_separator("-")
    log(f"🧩 STAGE: {name}")
    log_separator("-")
    log(f"Script   : {cmd}")
    log(f"Started  : {start.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    log(f"RAM Before: {format_bytes(res_before['ram_mb'])} | System: {sys_before['ram_percent']:.1f}% used")
    
    try:
        if is_python:
            result = subprocess.run([PYTHON, cmd], check=True, capture_output=False)
        else:
            result = subprocess.run(["bash", cmd], check=True, capture_output=False)
        status = "SUCCESS"
    except subprocess.CalledProcessError as e:
        log(f"❌ [{name}] Failed with exit code: {e.returncode}", "ERROR")
        status = "FAILED"
        cleanup_lock()
        
        # Log final summary before exit
        end = datetime.now(timezone.utc)
        duration = (end - start).total_seconds()
        stage_stats[name] = {"status": status, "duration": duration, "start": start, "end": end}
        log_final_summary(start)
        sys.exit(1)

    end = datetime.now(timezone.utc)
    duration = (end - start).total_seconds()
    
    # Get resource snapshot after stage
    res_after = resource_monitor.get_process_info()
    sys_after = resource_monitor.get_system_info()
    
    log(f"Finished : {end.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    log(f"Duration : {format_duration(duration)}")
    log(f"RAM After : {format_bytes(res_after['ram_mb'])} | System: {sys_after['ram_percent']:.1f}% used")
    log(f"Status   : ✅ {status}")
    
    stage_stats[name] = {
        "status": status,
        "duration": duration,
        "start": start,
        "end": end,
        "ram_before_mb": res_before["ram_mb"],
        "ram_after_mb": res_after["ram_mb"],
        "sys_ram_before": sys_before["ram_percent"],
        "sys_ram_after": sys_after["ram_percent"]
    }
    return stage_stats[name]


def count_docs() -> dict:
    """Get current stage counts from MongoDB."""
    from pymongo import MongoClient
    client = MongoClient("mongodb://localhost:27017")
    db = client["jobminglr_staging"]

    counts = {
        "raw_unparsed": db["pinpoint_jobs_raw"].count_documents({"parsed": {"$ne": True}}),
        "raw_total": db["pinpoint_jobs_raw"].count_documents({}),
        "queue": db["pinpoint_jobs_queue"].count_documents({}),
        "ready_unsynced": db["pinpoint_jobs_ready"].count_documents({"validated": True, "synced": {"$ne": True}}),
        "ready_total": db["pinpoint_jobs_ready"].count_documents({})
    }
    
    client.close()
    
    log(f"📊 DB Stats → raw_unparsed={counts['raw_unparsed']} | raw_total={counts['raw_total']} | "
        f"queue={counts['queue']} | ready_unsynced={counts['ready_unsynced']} | ready_total={counts['ready_total']}")
    
    return counts


def log_final_summary(pipeline_start: datetime):
    """Log final pipeline summary with resource usage."""
    pipeline_end = datetime.now(timezone.utc)
    total_duration = (pipeline_end - pipeline_start).total_seconds()
    
    # Stop monitoring and get summary
    resource_monitor.stop()
    res_summary = resource_monitor.get_summary()
    final_sys = resource_monitor.get_system_info()
    final_proc = resource_monitor.get_process_info()
    
    log_separator("=")
    log("PIPELINE SUMMARY")
    log_separator("=")
    log(f"Pipeline Start : {pipeline_start.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    log(f"Pipeline End   : {pipeline_end.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    log(f"Total Duration : {format_duration(total_duration)}")
    log_separator("-")
    log("STAGE BREAKDOWN:")
    log_separator("-")
    
    for stage_name, stats in stage_stats.items():
        status_icon = "✅" if stats["status"] == "SUCCESS" else "❌"
        ram_diff = stats.get("ram_after_mb", 0) - stats.get("ram_before_mb", 0)
        ram_sign = "+" if ram_diff >= 0 else ""
        log(f"  {status_icon} {stage_name:20s} : {format_duration(stats['duration']):>12s} | {stats['status']} | RAM: {ram_sign}{ram_diff:.1f}MB")
    
    log_separator("-")
    log("RESOURCE USAGE (PIPELINE PROCESS)")
    log_separator("-")
    log(f"Start RAM      : {format_bytes(res_summary['start_ram_mb'])}")
    log(f"Peak RAM       : {format_bytes(res_summary['peak_ram_mb'])}")
    log(f"Avg RAM        : {format_bytes(res_summary['avg_ram_mb'])}")
    log(f"Final RAM      : {format_bytes(final_proc['ram_mb'])}")
    log(f"Avg CPU        : {res_summary['avg_cpu_percent']:.1f}%")
    log(f"Max CPU        : {res_summary['max_cpu_percent']:.1f}%")
    log_separator("-")
    log("SYSTEM RESOURCES")
    log_separator("-")
    log(f"Avg System CPU : {res_summary['avg_sys_cpu_percent']:.1f}%")
    log(f"Max System CPU : {res_summary['max_sys_cpu_percent']:.1f}%")
    log(f"Avg System RAM : {res_summary['avg_sys_ram_percent']:.1f}% used")
    log(f"Peak System RAM: {res_summary['peak_sys_ram_percent']:.1f}% used")
    log(f"Final RAM      : {final_sys['available_ram_gb']:.2f} GB available ({final_sys['ram_percent']:.1f}% used)")
    log(f"Samples        : {res_summary['samples_count']}")
    log_separator("=")


# ---------- MAIN ----------
def main():
    pipeline_start = datetime.now(timezone.utc)
    
    # Start resource monitoring
    resource_monitor.start()
    
    # Get initial system info
    sys_info = resource_monitor.get_system_info()
    proc_info = resource_monitor.get_process_info()
    
    log_separator("=")
    log("PINPOINT PIPELINE - STARTING")
    log_separator("=")
    log(f"Mode         : STAGING")
    log(f"Started      : {pipeline_start.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    log(f"PID          : {os.getpid()}")
    log(f"Scripts Dir  : {SCRIPTS_DIR}")
    log_separator("-")
    log("SYSTEM RESOURCES (INITIAL)")
    log_separator("-")
    log(f"Total RAM    : {sys_info['total_ram_gb']:.2f} GB")
    log(f"Available RAM: {sys_info['available_ram_gb']:.2f} GB ({sys_info['ram_percent']:.1f}% used)")
    log(f"CPU Cores    : {sys_info['cpu_count']}")
    log(f"System CPU   : {sys_info['cpu_percent']:.1f}%")
    log(f"Process RAM  : {format_bytes(proc_info['ram_mb'])}")
    log(f"Process CPU  : {proc_info['cpu_percent']:.1f}%")
    log_separator("=")

    # Initial DB state
    log("INITIAL DATABASE STATE:")
    initial_counts = count_docs()

    # Stage 0 - Sync Taxonomy
    run_stage("Taxonomy Sync", SYNC_TAXONOMY_SCRIPT, is_python=False)
    count_docs()

    # Stage 1 - Fetch
    run_stage("Fetch", FETCH_SCRIPT)
    count_docs()

    # Stage 2 - AI Parse (async bulk processing)
    run_stage("AI Parse", PARSE_SCRIPT)
    count_docs()

    # Stage 3 - Canonical Mapping
    run_stage("Canonical Mapping", MAP_SCRIPT)
    final_counts = count_docs()

    # # Stage 4 - Skip Production Sync
    # log_separator("-")
    # log("🛑 SKIPPING: Production sync — staging mode enabled.")

    # Final Summary
    log_final_summary(pipeline_start)
    
    # Jobs processed summary
    jobs_parsed = initial_counts["raw_unparsed"]
    jobs_ready = final_counts["ready_unsynced"]
    
    log(f"Jobs Processed : {jobs_parsed}")
    log(f"Jobs Ready     : {jobs_ready}")
    log_separator("=")
    log("🏁 PINPOINT PIPELINE - COMPLETED")
    log_separator("=")
    
    cleanup_lock()


if __name__ == "__main__":
    main()
