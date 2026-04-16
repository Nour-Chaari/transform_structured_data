"""
=============================================================================
PIPELINE METRICS EXPORTER v2+ — Optimisé pour Silver v11 + Gold v7
=============================================================================
Version améliorée avec chargement automatique du .env
Port par défaut : 8001 (évite conflit avec Gold v7 sur le port 8000)

Compatible avec ton dashboard Grafana existant.
=============================================================================
"""

import os
import re
import json
import time
import logging
import threading
from pathlib import Path
from datetime import datetime
from http.server import HTTPServer
from typing import Dict, List, Optional

# Chargement automatique du fichier .env
from dotenv import load_dotenv
load_dotenv()

from prometheus_client import (
    Counter, Gauge,
    MetricsHandler,
    REGISTRY,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pipeline_exporter_v2")

# ── Configuration depuis .env ─────────────────────────────────────────────
SILVER_LOG_DIR = Path(os.environ.get(
    "SILVER_LOG_DIR",
    r"C:\Users\chaar\Downloads\transform_structured_data\data\silver\logs"
))
GOLD_LOG_DIR = Path(os.environ.get(
    "GOLD_LOG_DIR",
    r"C:\Users\chaar\Downloads\transform_structured_data\data\gold\logs"
))
GOLD_REPORT_DIR = Path(os.environ.get(
    "GOLD_REPORT_DIR",
    r"C:\Users\chaar\Downloads\transform_structured_data\data\gold\quality_reports"
))
GOLD_PROM_DIR = Path(os.environ.get(
    "GOLD_PROM_DIR",
    r"C:\Users\chaar\Downloads\transform_structured_data\data\gold\metrics_export"
))

EXPORTER_PORT   = int(os.environ.get("EXPORTER_PORT", os.environ.get("SILVER_EXPORTER_PORT", "8001")))   # Silver exporter sur 8001
SCRAPE_INTERVAL = int(os.environ.get("SCRAPE_INTERVAL", "15"))

# ══════════════════════════════════════════════════════════════════════════
#  MÉTRIQUES PROMETHEUS (identiques à ta version originale)
# ══════════════════════════════════════════════════════════════════════════

SILVER_LABELS = ["table_name", "source_type", "pipeline"]
GOLD_LABELS   = ["table_name", "source_type", "db"]

# ── Silver ────────────────────────────────────────────────────────────────
g_rows_input       = Gauge("silver_rows_input_total",         "Lignes lues en entrée Bronze",            SILVER_LABELS)
g_rows_output      = Gauge("silver_rows_output_total",        "Lignes écrites en Silver",                SILVER_LABELS)
g_rows_quarantined = Gauge("silver_rows_quarantined_total",   "Lignes mises en quarantaine Silver",      SILVER_LABELS)
g_quarantine_rate  = Gauge("silver_quarantine_rate_percent",  "Taux de quarantaine Silver (%)",          SILVER_LABELS)
g_duration         = Gauge("silver_duration_seconds",         "Durée de traitement Silver par table (s)",SILVER_LABELS)
g_success          = Gauge("silver_pipeline_success",         "Succès (1) ou échec (0) Silver",          SILVER_LABELS)
g_last_run_ts      = Gauge("silver_last_run_timestamp",       "Timestamp UNIX dernière exécution Silver",SILVER_LABELS)

g_total_runs        = Counter("pipeline_total_runs_total",            "Nombre total d'exécutions",  ["pipeline"])
g_last_exec_secs    = Gauge("pipeline_last_execution_seconds",        "Durée totale pipeline (s)",  ["pipeline"])
g_tables_processed  = Gauge("pipeline_tables_processed_last_run",     "Tables traitées",            ["pipeline"])
g_tables_ok         = Gauge("pipeline_tables_ok_last_run",            "Tables OK",                  ["pipeline"])
g_total_rows_in     = Gauge("pipeline_total_rows_input_last_run",     "Total lignes entrée",        ["pipeline"])
g_total_rows_out    = Gauge("pipeline_total_rows_output_last_run",    "Total lignes sortie Silver", ["pipeline"])
g_total_quarantined = Gauge("pipeline_total_quarantined_last_run",    "Total lignes quarantaine",   ["pipeline"])

# ── Gold ──────────────────────────────────────────────────────────────────
g_gold_valid        = Gauge("gold_rows_valid_total",         "Lignes validées Gold",             GOLD_LABELS)
g_gold_quarantine   = Gauge("gold_rows_quarantine_total",    "Lignes rejetées Gold",             GOLD_LABELS)
g_gold_validity_rate= Gauge("gold_validity_rate_percent",    "Taux de validité Gold (%)",        GOLD_LABELS)
g_gold_quar_rate    = Gauge("gold_quarantine_rate_percent",  "Taux de quarantaine Gold (%)",     GOLD_LABELS)
g_gold_completeness = Gauge("gold_completeness_avg_percent", "Complétude moyenne colonnes (%)",  GOLD_LABELS)
g_gold_duration     = Gauge("gold_duration_seconds",         "Durée Gold par table (s)",         GOLD_LABELS)

g_gold_v_critique   = Gauge("gold_violations_critique",         "Violations CRITIQUE Gold",       GOLD_LABELS)
g_gold_v_important  = Gauge("gold_violations_important",        "Violations IMPORTANT Gold",      GOLD_LABELS)
g_gold_v_general    = Gauge("gold_violations_domain_general",   "Violations domaine général",     GOLD_LABELS)
g_gold_v_banking    = Gauge("gold_violations_domain_banking",   "Violations domaine banking",     GOLD_LABELS)
g_gold_v_islamic    = Gauge("gold_violations_domain_islamic",   "Violations domaine islamique",   GOLD_LABELS)

# ── NOUVEAU : Métriques GLOBALES Gold (sans label table) ───────────────────
g_gold_pipeline_running   = Gauge("gold_pipeline_running",          "1 si pipeline en cours")
g_gold_tables_processed   = Gauge("gold_tables_processed_total",    "Nombre de tables traitées")
g_gold_tables_skipped     = Gauge("gold_tables_skipped_total",      "Nombre de tables ignorées")
g_gold_global_validity    = Gauge("gold_global_validity_rate_percent", "Validité globale (%)")
g_gold_pipeline_duration  = Gauge("gold_pipeline_duration_seconds", "Durée totale pipeline (s)")
g_gold_rows_valid_total   = Gauge("gold_rows_valid_total_global",   "Total lignes valides (toutes tables)")
g_gold_rows_quarantine_total = Gauge("gold_rows_quarantine_total_global", "Total lignes quarantaine (toutes tables)")


# ══════════════════════════════════════════════════════════════════════════
#  PARSEURS
# ══════════════════════════════════════════════════════════════════════════

_METRICS_RE = re.compile(r'METRICS:\s*(\{.*\})')
_TOTAL_RE   = re.compile(r"Temps total d.ex.cution\s*:\s*([\d.]+)\s*s")
_RESULTS_RE = re.compile(r'RÉSULTATS\s*:\s*(\d+)\s*OK\s*/\s*(\d+)\s*ÉCHEC')


def _get_latest_file(directory: Path, pattern: str) -> Optional[Path]:
    if not directory.exists():
        return None
    files = sorted(directory.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _get_latest_log(log_dir: Path) -> Optional[Path]:
    return _get_latest_file(log_dir, "*.log")


def _parse_silver_log(log_path: Path) -> List[Dict]:
    records = []
    try:
        text = log_path.read_text(encoding="utf-8", errors="replace")
        for m in _METRICS_RE.finditer(text):
            try:
                records.append(json.loads(m.group(1)))
            except json.JSONDecodeError:
                pass
    except Exception as e:
        log.warning("Lecture log Silver impossible (%s): %s", log_path.name, e)
    return records


def _parse_silver_summary(log_path: Path) -> Dict:
    summary = {"total_duration": None, "ok": 0, "fail": 0}
    try:
        text = log_path.read_text(encoding="utf-8", errors="replace")
        m = _TOTAL_RE.search(text)
        if m:
            summary["total_duration"] = float(m.group(1))
        m2 = _RESULTS_RE.search(text)
        if m2:
            summary["ok"]   = int(m2.group(1))
            summary["fail"] = int(m2.group(2))
    except Exception as e:
        log.warning("Résumé Silver impossible (%s): %s", log_path.name, e)
    return summary


def _parse_gold_quality_reports() -> List[Dict]:
    """Priorité au rapport global de Gold v7 + extraction métriques GLOBALES"""
    if not GOLD_REPORT_DIR.exists():
        return []

    # 1. Rapport global
    global_report = _get_latest_file(GOLD_REPORT_DIR, "_global_report_*.json")
    if global_report:
        try:
            data = json.loads(global_report.read_text(encoding="utf-8"))
            table_results = data.get("table_results", [])

            # ── NOUVEAUTÉ : Mise à jour des métriques GLOBALES ─────────────
            summary = data.get("summary", {})
            if summary:
                g_gold_tables_processed.set(summary.get("tables_processed", 0))
                g_gold_tables_skipped.set(summary.get("tables_skipped", 0))
                g_gold_global_validity.set(summary.get("global_validity_rate", 0))
                g_gold_rows_valid_total.set(summary.get("total_rows_valid", 0))
                g_gold_rows_quarantine_total.set(summary.get("total_rows_quarantined", 0))
                if summary.get("pipeline_duration_seconds"):
                    g_gold_pipeline_duration.set(summary.get("pipeline_duration_seconds", 0))
                log.info("[Gold] Métriques GLOBALES mises à jour depuis _global_report")

            if table_results:
                log.info("[Gold] Rapport global chargé : %d tables", len(table_results))
                return table_results
        except Exception as e:
            log.warning("[Gold] Erreur lecture rapport global : %s", e)

    # 2. Fallback : rapports individuels
    reports = []
    for rfile in sorted(GOLD_REPORT_DIR.rglob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
        if rfile.name.startswith("_"):
            continue
        try:
            data = json.loads(rfile.read_text(encoding="utf-8"))
            metrics = data.get("metrics")
            if metrics and "rows" in metrics:
                reports.append({
                    "metrics": metrics,
                    "table": metrics.get("table"),
                    "db_name": metrics.get("db_name", ""),
                    "success": True
                })
        except Exception:
            pass
    return reports


def _parse_gold_prom_files() -> List[Dict]:
    if not GOLD_PROM_DIR.exists():
        return []

    best: Dict[str, tuple] = {}
    for pf in GOLD_PROM_DIR.glob("*.prom"):
        parts = pf.stem.rsplit("_", 2)
        table = parts[0] if parts else pf.stem
        mtime = pf.stat().st_mtime
        if table not in best or mtime > best[table][0]:
            best[table] = (mtime, pf)

    records = []
    _kv_re = re.compile(r'(\w+)\{([^}]*)\}\s+([\d.]+)')
    for _, pf in best.values():
        rec: Dict = {}
        try:
            text = pf.read_text(encoding="utf-8")
            for m in _kv_re.finditer(text):
                metric_name = m.group(1)
                labels_str  = m.group(2)
                value       = float(m.group(3))

                lbls: Dict[str, str] = {}
                for part in labels_str.split(","):
                    if "=" in part:
                        k, v = part.strip().split("=", 1)
                        lbls[k.strip()] = v.strip().strip('"')

                rec["table_name"]  = lbls.get("table", rec.get("table_name", "unknown"))
                rec["source_type"] = lbls.get("source_type", rec.get("source_type", "unknown"))
                rec["db"]          = lbls.get("db", rec.get("db", ""))
                rec[metric_name]   = value
            if rec:
                records.append(rec)
        except Exception as e:
            log.warning("[Gold/.prom] Lecture impossible (%s): %s", pf.name, e)
    return records


# ══════════════════════════════════════════════════════════════════════════
#  MISE À JOUR DES MÉTRIQUES
# ══════════════════════════════════════════════════════════════════════════

_last_processed: Dict[str, str] = {}


def _update_silver(log_path: Path):
    records = _parse_silver_log(log_path)
    if not records:
        return

    pl = "silver"
    totals = {"rows_in": 0, "rows_out": 0, "quarantined": 0}

    for rec in records:
        table    = rec.get("table_name",  "unknown")
        src_type = rec.get("source_type", "unknown")
        lbl      = {"table_name": table, "source_type": src_type, "pipeline": pl}

        rows_in   = int(rec.get("rows_input", 0))
        rows_out  = int(rec.get("rows_output", 0))
        rows_quar = int(rec.get("rows_quarantined", 0))
        
        # Compatibilité Silver v11
        duration = float(rec.get("duration_seconds") or rec.get("processing_duration_seconds") or 0)
        
        success   = 1 if rec.get("success", False) else 0
        quar_rate = round(rows_quar / max(rows_in, 1) * 100, 2)

        try:
            ts = datetime.fromisoformat(rec.get("timestamp", "").replace("Z", "+00:00")).timestamp()
        except Exception:
            ts = time.time()

        g_rows_input.labels(**lbl).set(rows_in)
        g_rows_output.labels(**lbl).set(rows_out)
        g_rows_quarantined.labels(**lbl).set(rows_quar)
        g_quarantine_rate.labels(**lbl).set(quar_rate)
        g_duration.labels(**lbl).set(duration)
        g_success.labels(**lbl).set(success)
        g_last_run_ts.labels(**lbl).set(ts)

        totals["rows_in"]     += rows_in
        totals["rows_out"]    += rows_out
        totals["quarantined"] += rows_quar

    summary = _parse_silver_summary(log_path)
    plbl = {"pipeline": pl}
    g_tables_processed.labels(**plbl).set(len(records))
    g_tables_ok.labels(**plbl).set(summary["ok"])
    g_total_rows_in.labels(**plbl).set(totals["rows_in"])
    g_total_rows_out.labels(**plbl).set(totals["rows_out"])
    g_total_quarantined.labels(**plbl).set(totals["quarantined"])

    if summary["total_duration"] is not None:
        g_last_exec_secs.labels(**plbl).set(summary["total_duration"])

    log.info("[Silver] %d tables mises à jour depuis %s", len(records), log_path.name)


def _update_gold_from_report(table_result: Dict):
    metrics  = table_result.get("metrics", {})
    if not metrics:
        return

    table    = metrics.get("table",       table_result.get("table", "unknown"))
    src_type = metrics.get("source_type", "unknown")
    db       = metrics.get("db_name",     "")
    lbl      = {"table_name": table, "source_type": src_type, "db": db}

    rows  = metrics.get("rows", {})
    viols = metrics.get("violations", {})
    v_sev = viols.get("by_severity", {})
    v_dom = viols.get("by_domain", {})
    compl = metrics.get("completeness", {})

    g_gold_valid.labels(**lbl).set(rows.get("valid", 0))
    g_gold_quarantine.labels(**lbl).set(rows.get("quarantined", 0))
    g_gold_validity_rate.labels(**lbl).set(rows.get("validity_rate", 0))
    g_gold_quar_rate.labels(**lbl).set(rows.get("quarantine_rate", 0))
    g_gold_completeness.labels(**lbl).set(compl.get("average", 0))

    g_gold_v_critique.labels(**lbl).set(v_sev.get("CRITIQUE", 0))
    g_gold_v_important.labels(**lbl).set(v_sev.get("IMPORTANT", 0))

    g_gold_v_general.labels(**lbl).set(v_dom.get("general", 0))
    g_gold_v_banking.labels(**lbl).set(v_dom.get("banking", 0))
    g_gold_v_islamic.labels(**lbl).set(v_dom.get("islamic", 0))

    duration = metrics.get("duration_seconds", 0)
    if duration:
        g_gold_duration.labels(**lbl).set(duration)


def _update_gold_from_prom(rec: Dict):
    table    = rec.get("table_name", "unknown")
    src_type = rec.get("source_type", "unknown")
    db       = rec.get("db", "")
    lbl      = {"table_name": table, "source_type": src_type, "db": db}

    mappings = {
        "gold_rows_valid_total": g_gold_valid,
        "gold_rows_quarantine_total": g_gold_quarantine,
        "gold_validity_rate_percent": g_gold_validity_rate,
        "gold_quarantine_rate_percent": g_gold_quar_rate,
        "gold_completeness_avg_percent": g_gold_completeness,
        "gold_duration_seconds": g_gold_duration,
        "gold_violations_critique": g_gold_v_critique,
        "gold_violations_important": g_gold_v_important,
        "gold_violations_domain_general": g_gold_v_general,
        "gold_violations_domain_banking": g_gold_v_banking,
        "gold_violations_domain_islamic": g_gold_v_islamic,
    }

    for metric_name, gauge in mappings.items():
        if metric_name in rec:
            gauge.labels(**lbl).set(rec[metric_name])


def _update_gold():
    table_results = _parse_gold_quality_reports()
    if table_results:
        for tr in table_results:
            _update_gold_from_report(tr)
        log.info("[Gold/JSON] %d tables mises à jour", len(table_results))
        return

    prom_records = _parse_gold_prom_files()
    if prom_records:
        for rec in prom_records:
            _update_gold_from_prom(rec)
        log.info("[Gold/.prom] %d tables mises à jour (fallback)", len(prom_records))


def refresh_all():
    """Rafraîchit toutes les métriques Silver + Gold."""

    # Silver
    silver_log = _get_latest_log(SILVER_LOG_DIR)
    if silver_log:
        key = str(silver_log)
        if key != _last_processed.get("silver"):
            _update_silver(silver_log)
            _last_processed["silver"] = key
            g_total_runs.labels(pipeline="silver").inc()

    # Gold log (durée totale)
    gold_log = _get_latest_log(GOLD_LOG_DIR)
    if gold_log:
        key = str(gold_log)
        if key != _last_processed.get("gold_log"):
            try:
                text = gold_log.read_text(encoding="utf-8", errors="replace")
                m = re.search(r"Temps total d.ex.cution\s*:\s*([\d.]+)\s*s", text)
                if m:
                    g_last_exec_secs.labels(pipeline="gold").set(float(m.group(1)))
            except Exception:
                pass
            _last_processed["gold_log"] = key
            g_total_runs.labels(pipeline="gold").inc()

    # Gold métriques qualité
    _update_gold()


def _scrape_loop():
    log.info("Scrape loop démarrée (intervalle=%ds)", SCRAPE_INTERVAL)
    while True:
        try:
            refresh_all()
        except Exception as e:
            log.error("Erreur refresh: %s", e)
        time.sleep(SCRAPE_INTERVAL)


def run_exporter():
    log.info("=" * 70)
    log.info("  PIPELINE METRICS EXPORTER v2+ (Silver v11 + Gold v7)")
    log.info("  .env chargé")
    log.info("  Silver logs   : %s", SILVER_LOG_DIR)
    log.info("  Gold reports  : %s", GOLD_REPORT_DIR)
    log.info("  Port          : %d", EXPORTER_PORT)
    log.info("  Scrape        : toutes les %ds", SCRAPE_INTERVAL)
    log.info("=" * 70)

    refresh_all()

    t = threading.Thread(target=_scrape_loop, daemon=True)
    t.start()

    server = HTTPServer(("0.0.0.0", EXPORTER_PORT), MetricsHandler)
    log.info("Exporter prêt → http://localhost:%d/metrics", EXPORTER_PORT)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Arrêt.")
        server.shutdown()


if __name__ == "__main__":
    run_exporter()