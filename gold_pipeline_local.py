"""
=============================================================================
GOLD PIPELINE v7 — LOCAL  (Python 3.12 / venv compatible)
=============================================================================
NOUVEAUTÉS v7 vs v6 :

  [INC-G-1]  État Gold persistant (gold/pipeline_state/gold_state.json) :
             Pour chaque table traitée on stocke :
               { "parquet_path", "ts", "silver_parquet_path",
                 "silver_ts", "row_count_valid", "row_count_quarantine",
                 "validity_rate", "processed_at" }
             Au démarrage, Gold lit l'état Silver (silver/pipeline_state/
             pipeline_state.json) pour savoir quels runs Silver sont nouveaux
             puis ne traite QUE les tables dont le parquet_path Silver a changé.

  [INC-G-2]  Métriques conservées pour tables inchangées : si une table
             Silver n'a pas changé, Gold log les métriques du dernier run
             Gold depuis l'état persistant (pas de retraitement).

  [INC-G-3]  Prometheus temps réel via un serveur HTTP intégré (thread
             daemon) qui expose /metrics sur localhost:8000 (configurable).
             Un Prometheus ou un curl peut scraper cette URL à tout moment.
             Le registre est mis à jour après chaque table traitée →
             monitoring table par table en temps réel.

  [INC-G-4]  Métriques globales exposées en continu :
             gold_pipeline_running (1 pendant l'exécution),
             gold_tables_processed_total, gold_tables_pending_total,
             gold_rows_valid_total, gold_quarantine_rate_percent, …

  [INC-G-5]  Tous les correctifs v6 sont conservés :
             FIX-v6-1 (latest Silver only), FIX-v6-2 (pyarrow write),
             FIX-v6-4 (logging 5 étapes), FIX-v6-5 (prom file enrichi).

USAGE PROMETHEUS :
  - Scraper http://localhost:8000/metrics depuis Prometheus / Grafana
  - Le port est configurable via GOLD_METRICS_PORT (défaut 8000)
  - Si le port est déjà pris, Gold continue sans serveur HTTP (fichiers .prom)
=============================================================================
"""

import os
import re
import sys
import json
import time
import logging
import tempfile
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from datetime import datetime, date
from typing import Optional, Union, Set, List, Dict, Any, Tuple
from dotenv import load_dotenv; load_dotenv()

os.environ["PYSPARK_PYTHON"]        = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pyspark.sql.types as T
from pyspark.sql import SparkSession, DataFrame
import pandas as pd
import numpy as np

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False
    logging.warning("pyarrow non installé — pip install pyarrow")

try:
    from groq import Groq
    GROQ_AVAILABLE = True
except ImportError:
    GROQ_AVAILABLE = False

try:
    from prometheus_client import (CollectorRegistry, Gauge, Counter,
                                   push_to_gateway, generate_latest,
                                   CONTENT_TYPE_LATEST, REGISTRY)
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False


# ═════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═════════════════════════════════════════════════════════════════════════

LOCAL_SILVER_ROOT = Path(os.environ.get(
    "LOCAL_SILVER_ROOT",
    r"C:\Users\chaar\OneDrive\Bureau\transform_structured_data\data\silver"
))
LOCAL_GOLD_ROOT = Path(os.environ.get(
    "LOCAL_GOLD_ROOT",
    r"C:\Users\chaar\OneDrive\Bureau\transform_structured_data\data\gold"
))
SILVER_STATE_FILE = LOCAL_SILVER_ROOT / "pipeline_state" / "pipeline_state.json"
GOLD_STATE_FILE   = LOCAL_GOLD_ROOT   / "pipeline_state" / "gold_state.json"


GROQ_API_KEYS = [
    os.environ.get("GROQ_API_KEY_1"),
    os.environ.get("GROQ_API_KEY_2"),
    os.environ.get("GROQ_API_KEY_3"),
]
GROQ_MODEL = "llama-3.3-70b-versatile"

PROMETHEUS_PUSHGATEWAY = os.environ.get("PROMETHEUS_PUSHGATEWAY", "localhost:9091")
GOLD_METRICS_PORT      = int(os.environ.get("GOLD_METRICS_PORT", "8000"))

MIN_CLIENT_AGE             = 18
MAX_CLIENT_AGE             = 110
MAX_INTEREST_RATE          = 18.0
MAX_BALANCE                = 10_000_000
MAX_TRANSACTION_AMOUNT     = 500_000
MAX_CREDIT_DURATION_MONTHS = 360
MIN_CREDIT_DURATION_MONTHS = 1

VALID_COUNTRY_CODES = {
    "TN","FR","DE","GB","IT","ES","MA","DZ","LY","SA","AE","QA","KW","EG",
    "US","BE","CH","NL","PT","SE","NO","DK","FI","AT","PL","RO","TR","JO",
    "LB","IQ","SY","YE","BH","OM","SD","MR","SO","DJ","KM","MU","SN","CM",
    "CI","GH","NG","ZA","KE","ET","TZ","UG","RW","MG","CV","GM","GN","ML",
    "BF","NE","TD","CF","GA","CG","CD","AO","MZ","ZM","ZW","NA","BW","LS","SZ",
}

ISLAMIC_ACCOUNT_TYPES = {"islamique_wadiah","islamique_mudaraba","wadiah","mudaraba"}
ISLAMIC_CREDIT_TYPES  = {"murabaha","ijara","istisna","salam","musharaka"}
FULLNAME_COLUMN_KEYWORDS = ["full_name","fullname","nom_complet","customer_name"]
VALID_ACCOUNT_STATUSES = {
    "actif","suspendu","cloture","clôturé","bloque","bloqué",
    "en_attente","remboursé","rembourse","en_cours","défaut","default",
    "restructuré","restructure",
}


# ═════════════════════════════════════════════════════════════════════════
#  LOGGING PERSISTANT
# ═════════════════════════════════════════════════════════════════════════

_RUN_TS = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

def _setup_logging() -> logging.Logger:
    LOCAL_GOLD_ROOT.mkdir(parents=True, exist_ok=True)
    log_dir  = LOCAL_GOLD_ROOT / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"gold_pipeline_{_RUN_TS}.log"
    fmt = logging.Formatter(fmt="%(asctime)s  %(levelname)-8s  %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    logger = logging.getLogger("gold_pipeline_v7_local")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    ch = logging.StreamHandler(sys.stdout); ch.setFormatter(fmt); logger.addHandler(ch)
    fh = logging.FileHandler(log_file, encoding="utf-8"); fh.setFormatter(fmt); logger.addHandler(fh)
    logger.info("Gold logs → %s", log_file)
    return logger

log = _setup_logging()


# ═════════════════════════════════════════════════════════════════════════
#  [INC-G-3] SERVEUR HTTP PROMETHEUS TEMPS RÉEL
# ═════════════════════════════════════════════════════════════════════════

# Registre de métriques temps réel (mis à jour après chaque table)
_RT_METRICS: Dict[str, Any] = {}          # dict brut → converti en texte Prometheus
_RT_LOCK = threading.Lock()
_HTTP_SERVER: Optional[HTTPServer] = None


def _build_prom_text() -> str:
    """Construit le texte Prometheus à partir de _RT_METRICS."""
    with _RT_LOCK:
        lines = [
            "# HELP gold_pipeline_running 1 si le pipeline est en cours",
            "# TYPE gold_pipeline_running gauge",
            f"gold_pipeline_running {_RT_METRICS.get('running', 0)}",
            "# HELP gold_tables_processed_total Tables Gold traitées",
            "# TYPE gold_tables_processed_total gauge",
            f"gold_tables_processed_total {_RT_METRICS.get('tables_processed', 0)}",
            "# HELP gold_tables_pending_total Tables Gold restant à traiter",
            "# TYPE gold_tables_pending_total gauge",
            f"gold_tables_pending_total {_RT_METRICS.get('tables_pending', 0)}",
            "# HELP gold_tables_skipped_total Tables ignorées (Silver inchangé)",
            "# TYPE gold_tables_skipped_total gauge",
            f"gold_tables_skipped_total {_RT_METRICS.get('tables_skipped', 0)}",
            "# HELP gold_rows_valid_total Lignes valides (toutes tables)",
            "# TYPE gold_rows_valid_total gauge",
            f"gold_rows_valid_total {_RT_METRICS.get('rows_valid', 0)}",
            "# HELP gold_rows_quarantine_total Lignes en quarantaine (toutes tables)",
            "# TYPE gold_rows_quarantine_total gauge",
            f"gold_rows_quarantine_total {_RT_METRICS.get('rows_quarantine', 0)}",
            "# HELP gold_global_validity_rate_percent Taux de validité global",
            "# TYPE gold_global_validity_rate_percent gauge",
            f"gold_global_validity_rate_percent {_RT_METRICS.get('validity_rate', 0.0)}",
            "# HELP gold_pipeline_duration_seconds Durée d'exécution",
            "# TYPE gold_pipeline_duration_seconds gauge",
            f"gold_pipeline_duration_seconds {_RT_METRICS.get('duration', 0.0)}",
            "# HELP gold_last_run_timestamp_seconds Timestamp Unix du dernier run",
            "# TYPE gold_last_run_timestamp_seconds gauge",
            f"gold_last_run_timestamp_seconds {_RT_METRICS.get('last_run_ts', int(time.time()))}",
        ]
        # Métriques par table
        for table_key, tm in _RT_METRICS.get("tables", {}).items():
            ls = f'table="{tm.get("table","?")}"'
            lines += [
                f'gold_table_rows_valid{{{ls}}} {tm.get("valid", 0)}',
                f'gold_table_rows_quarantine{{{ls}}} {tm.get("quarantine", 0)}',
                f'gold_table_validity_rate{{{ls}}} {tm.get("validity_rate", 0.0)}',
                f'gold_table_quarantine_rate{{{ls}}} {tm.get("quarantine_rate", 0.0)}',
                f'gold_table_completeness{{{ls}}} {tm.get("completeness", 0.0)}',
                f'gold_table_violations_critique{{{ls}}} {tm.get("violations_critique", 0)}',
                f'gold_table_violations_important{{{ls}}} {tm.get("violations_important", 0)}',
                f'gold_table_duration_seconds{{{ls}}} {tm.get("duration", 0.0)}',
                f'gold_table_last_run_ts{{{ls}}} {tm.get("last_run_ts", 0)}',
            ]
            if tm.get("precision") is not None:
                lines += [
                    f'gold_table_precision{{{ls}}} {tm["precision"]}',
                    f'gold_table_recall{{{ls}}} {tm["recall"]}',
                    f'gold_table_f1{{{ls}}} {tm["f1"]}',
                ]
    return "\n".join(lines) + "\n"


def _update_rt_table(table_name: str, metrics: Dict, duration: float):
    """Met à jour le registre temps réel pour une table."""
    rows      = metrics.get("rows", {})
    pr        = metrics.get("precision_recall", {})
    pr_m      = pr.get("metrics", {}) if pr.get("available") else {}
    viols     = metrics.get("violations", {}).get("by_severity", {})
    completeness = metrics.get("completeness", {}).get("average", 0.0)

    with _RT_LOCK:
        _RT_METRICS.setdefault("tables", {})[table_name] = {
            "table":                table_name,
            "valid":                rows.get("valid", 0),
            "quarantine":           rows.get("quarantined", 0),
            "validity_rate":        rows.get("validity_rate", 0.0),
            "quarantine_rate":      rows.get("quarantine_rate", 0.0),
            "completeness":         completeness,
            "violations_critique":  viols.get("CRITIQUE", 0),
            "violations_important": viols.get("IMPORTANT", 0),
            "duration":             round(duration, 3),
            "last_run_ts":          int(time.time()),
            "precision":            pr_m.get("precision") if pr.get("available") else None,
            "recall":               pr_m.get("recall")    if pr.get("available") else None,
            "f1":                   pr_m.get("f1_score")  if pr.get("available") else None,
        }
        # Mise à jour des totaux
        all_t  = _RT_METRICS.get("tables", {})
        total_v = sum(t.get("valid", 0)      for t in all_t.values())
        total_q = sum(t.get("quarantine", 0) for t in all_t.values())
        _RT_METRICS["rows_valid"]      = total_v
        _RT_METRICS["rows_quarantine"] = total_q
        _RT_METRICS["validity_rate"]   = round(100 * total_v / max(total_v + total_q, 1), 2)
        _RT_METRICS["last_run_ts"]     = int(time.time())


class _MetricsHandler(BaseHTTPRequestHandler):
    """Handler HTTP minimal pour exposer /metrics."""
    def do_GET(self):
        if self.path in ("/metrics", "/"):
            body = _build_prom_text().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # Silencer les logs HTTP dans la console


def _start_metrics_server(port: int) -> bool:
    """[INC-G-3] Lance le serveur HTTP Prometheus dans un thread daemon."""
    global _HTTP_SERVER
    try:
        _HTTP_SERVER = HTTPServer(("0.0.0.0", port), _MetricsHandler)
        t = threading.Thread(target=_HTTP_SERVER.serve_forever, daemon=True)
        t.start()
        log.info("[Prometheus] Serveur HTTP métriques → http://localhost:%d/metrics", port)
        return True
    except OSError as e:
        log.warning("[Prometheus] Port %d indisponible (%s) → fichiers .prom uniquement", port, e)
        return False


def _stop_metrics_server():
    global _HTTP_SERVER
    if _HTTP_SERVER:
        _HTTP_SERVER.shutdown()
        log.info("[Prometheus] Serveur HTTP arrêté.")


# ═════════════════════════════════════════════════════════════════════════
#  [INC-G-1] ÉTAT GOLD PERSISTANT
# ═════════════════════════════════════════════════════════════════════════

def load_gold_state() -> dict:
    """
    Structure gold_state.json :
    {
      "last_run": "ISO datetime",
      "tables": {
        "<source_type>|<db_name>|<table_name>": {
          "silver_parquet_path": "...",
          "silver_ts":           "20260413_123456",
          "gold_parquet_path":   "...",
          "gold_ts":             "20260413_124000",
          "row_count_valid":     9850,
          "row_count_quarantine":150,
          "validity_rate":       98.5,
          "completeness":        97.2,
          "processed_at":        "ISO datetime",
          "metrics_snapshot":    { ... }
        }
      }
    }
    """
    try:
        if GOLD_STATE_FILE.exists():
            return json.loads(GOLD_STATE_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("[GoldState] Erreur lecture : %s → état vide", e)
    return {"last_run": None, "tables": {}}


def save_gold_state(state: dict):
    state["last_run"] = datetime.utcnow().isoformat()
    GOLD_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    GOLD_STATE_FILE.write_text(
        json.dumps(state, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8"
    )
    log.info("[GoldState] État sauvegardé → %s", GOLD_STATE_FILE)


def load_silver_state() -> dict:
    """Lit l'état Silver pour connaître les derniers runs par table."""
    try:
        if SILVER_STATE_FILE.exists():
            data = json.loads(SILVER_STATE_FILE.read_text(encoding="utf-8"))
            # Compatibilité v10/v11
            if "file_hashes" in data and "bronze_hashes" not in data:
                data["bronze_hashes"] = data.pop("file_hashes")
            return data
    except Exception as e:
        log.warning("[SilverState] Erreur lecture : %s", e)
    return {"bronze_hashes": {}, "silver_outputs": {}}


def _gold_key(table_info: Dict) -> str:
    return f"{table_info.get('source_type','?')}|{table_info.get('db_name','')}|{table_info.get('table_name','?')}"


# ═════════════════════════════════════════════════════════════════════════
#  GROQ ROTATOR
# ═════════════════════════════════════════════════════════════════════════

class GroqRotator:
    def __init__(self, keys: List[str]):
        self.clients: List[Any] = []
        if GROQ_AVAILABLE:
            self.clients = [Groq(api_key=k) for k in keys if k and len(k) > 10]
        self._current = 0
        if not self.clients:
            log.warning("[Groq] Aucune clé valide.")

    def _next(self):
        self._current = (self._current + 1) % max(len(self.clients), 1)

    def complete(self, system: str, user: str, max_tokens: int = 800) -> str:
        if not self.clients:
            return ""
        for _ in range(len(self.clients) * 2):
            try:
                resp = self.clients[self._current].chat.completions.create(
                    model=GROQ_MODEL,
                    messages=[{"role":"system","content":system},{"role":"user","content":user}],
                    max_tokens=max_tokens, temperature=0.0,
                )
                return resp.choices[0].message.content.strip()
            except Exception as exc:
                err = str(exc).lower()
                if "rate" in err or "429" in err or "quota" in err:
                    log.warning("[Groq] Rate-limit clé %d → rotation", self._current)
                    self._next(); time.sleep(2)
                else:
                    log.error("[Groq] Erreur : %s", exc); return ""
        return ""


GROQ = GroqRotator(GROQ_API_KEYS)


def _parse_json_from_groq(raw: str) -> Union[Dict, List]:
    raw = re.sub(r"```json\s*", "", raw); raw = re.sub(r"```\s*", "", raw).strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"[\[{].*[\]}]", raw, re.DOTALL)
        if m:
            try: return json.loads(m.group(0))
            except Exception: pass
    return {}


# ═════════════════════════════════════════════════════════════════════════
#  SPARK SESSION
# ═════════════════════════════════════════════════════════════════════════

def create_spark() -> SparkSession:
    try:
        existing = SparkSession.getActiveSession()
        if existing: existing.stop(); time.sleep(1)
    except Exception: pass
    spark = (
        SparkSession.builder.appName("gold_pipeline_v7_local").master("local[*]")
        .config("spark.driver.memory", "6g").config("spark.executor.memory", "6g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.codegen.wholeStage", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.python.worker.reuse", "true")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.legacy.timeParserPolicy", "EXCEPTION")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.warehouse.dir",
                str(Path(tempfile.gettempdir()) / "spark-warehouse-gold7-local"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark créé : %s", spark.version)
    return spark


# ═════════════════════════════════════════════════════════════════════════
#  [INC-G-1] DÉCOUVERTE — LATEST SILVER PAR TABLE (+ filtre incrémental)
# ═════════════════════════════════════════════════════════════════════════

def discover_silver_tables(gold_state: dict, force_all: bool = False) -> Tuple[List[Dict], List[Dict]]:
    """
    Retourne (tables_to_process, tables_skipped).

    [INC-G-1] Stratégie :
    1. Lit silver/pipeline_state/pipeline_state.json pour connaître les
       derniers parquet_path par groupe Silver (v11).
    2. Si ce fichier n'existe pas, fallback sur scan filesystem (v6).
    3. Compare chaque chemin Silver avec ce que Gold a déjà traité
       dans gold_state → ne retraite que les nouvelles versions.
    [INC-G-2] Pour les tables inchangées, logue les métriques précédentes.
    """
    silver_state   = load_silver_state()
    silver_outputs = silver_state.get("silver_outputs", {})
    gold_tables    = gold_state.get("tables", {})

    SKIP_DIRS = {"quarantine", "pipeline_state", "logs", "anomalies"}

    # ── Construire la liste complète via état Silver ou filesystem ────────
    all_tables: List[Dict] = []

    if silver_outputs:
        log.info("[INC-G-1] Lecture de l'état Silver (%d groupes)", len(silver_outputs))
        for gk, info in silver_outputs.items():
            parquet_path = info.get("parquet_path", "")
            if not parquet_path or not Path(parquet_path).exists():
                log.warning("  [Skip] Parquet Silver introuvable : %s", parquet_path)
                continue
            # Reconstruire source_type / db_name / table_name depuis la clé gk
            # Format : "source_type__db_name__table_name" ou "source_type__table_name"
            parts = gk.split("__")
            if len(parts) >= 3:
                source_type, db_name, table_name = parts[0], parts[1], parts[2]
            elif len(parts) == 2:
                source_type, db_name, table_name = parts[0], "", parts[1]
            else:
                source_type, db_name, table_name = "unknown", "", parts[0]

            all_tables.append({
                "source_type":        source_type,
                "db_name":            db_name,
                "table_name":         table_name,
                "parquet_path":       parquet_path,
                "silver_ts":          info.get("ts", ""),
                "silver_row_count":   info.get("row_count", 0),
                "_silver_group_key":  gk,
            })
    else:
        # Fallback filesystem (Silver v10 sans état enrichi)
        log.info("[INC-G-1] Fallback filesystem (état Silver v10)")
        best: Dict[str, Tuple[str, Dict]] = {}
        if LOCAL_SILVER_ROOT.exists():
            for pf in LOCAL_SILVER_ROOT.rglob("*.parquet"):
                rel_parts = list(pf.relative_to(LOCAL_SILVER_ROOT).parts)
                if any(p in SKIP_DIRS for p in rel_parts): continue
                folder = pf.parent
                rel_folder = list(folder.relative_to(LOCAL_SILVER_ROOT).parts)
                if len(rel_folder) < 3: continue
                last = rel_folder[-1]
                if not last.startswith("processed_"): continue
                ts_str     = last[len("processed_"):]
                source_type = rel_folder[0]
                if source_type in SKIP_DIRS: continue
                if len(rel_folder) >= 4:
                    db_name, table_name = rel_folder[1], rel_folder[2]
                else:
                    db_name, table_name = "", rel_folder[1]
                key = f"{source_type}|{db_name}|{table_name}"
                entry = {"source_type": source_type, "db_name": db_name,
                         "table_name": table_name, "parquet_path": str(folder),
                         "silver_ts": ts_str, "silver_row_count": 0, "_silver_group_key": key}
                if key not in best or ts_str > best[key][0]:
                    best[key] = (ts_str, entry)
        all_tables = [info for _, info in best.values()]

    # ── Filtrage incrémental ──────────────────────────────────────────────
    to_process: List[Dict] = []
    skipped:    List[Dict] = []

    for t in all_tables:
        gk = _gold_key(t)

        if force_all:
            to_process.append(t)
            continue

        prev = gold_tables.get(gk, {})
        if not prev:
            t["_reason"] = "NOUVEAU"
            to_process.append(t)
        elif prev.get("silver_parquet_path") != t["parquet_path"]:
            t["_reason"] = "SILVER_UPDATED"
            to_process.append(t)
        else:
            # [INC-G-2] Table inchangée → log métriques précédentes
            log.info("  [INCHANGÉ] %s → Gold existant : ts=%s  valid=%d  quar=%d  validité=%.1f%%",
                     gk,
                     prev.get("gold_ts", "?"),
                     prev.get("row_count_valid", 0),
                     prev.get("row_count_quarantine", 0),
                     prev.get("validity_rate", 0.0))
            # Réinjecter dans le registre temps réel
            with _RT_LOCK:
                _RT_METRICS.setdefault("tables", {})[t["table_name"]] = {
                    "table":                t["table_name"],
                    "valid":                prev.get("row_count_valid", 0),
                    "quarantine":           prev.get("row_count_quarantine", 0),
                    "validity_rate":        prev.get("validity_rate", 0.0),
                    "quarantine_rate":      round(100 * prev.get("row_count_quarantine",0)
                                                  / max(prev.get("row_count_valid",0)
                                                        + prev.get("row_count_quarantine",0), 1), 2),
                    "completeness":         prev.get("completeness", 0.0),
                    "violations_critique":  0,
                    "violations_important": 0,
                    "duration":             0.0,
                    "last_run_ts":          0,
                }
            skipped.append(t)

    log.info("[INC-G-1] Tables Silver : %d total | %d à traiter | %d ignorées (inchangées)",
             len(all_tables), len(to_process), len(skipped))
    for t in to_process:
        log.info("  [%s] %s → %s", t.get("_reason","?"), _gold_key(t), t["parquet_path"])
    return to_process, skipped


# ═════════════════════════════════════════════════════════════════════════
#  NORMALISATION GROUND TRUTH
# ═════════════════════════════════════════════════════════════════════════

def _normalize_ground_truth(pdf: pd.DataFrame) -> pd.DataFrame:
    cols_lower = {c.lower(): c for c in pdf.columns}
    for candidate in ["_ground_truth", "ground_truth"]:
        real_col = cols_lower.get(candidate)
        if real_col:
            if real_col != "_ground_truth":
                pdf = pdf.rename(columns={real_col: "_ground_truth"})
            return pdf
    return pdf


# ═════════════════════════════════════════════════════════════════════════
#  GOLD-1 : ANALYSE GROQ
# ═════════════════════════════════════════════════════════════════════════

_SYSTEM_GOLD_ANALYSIS = """\
Tu es un expert en qualité de données bancaires tunisiennes (finance islamique).
On te fournit le schéma d'une table Silver et un échantillon.
Génère les règles de validation applicables UNIQUEMENT aux colonnes présentes.
Réponds en moins de 700 tokens.
RÈGLES DISPONIBLES :
GEN-001(email) GEN-002(phone) GEN-003(âge>=18) GEN-004(âge<=110) GEN-005(dob_non_future)
GEN-006(genre=m/f) GEN-007(pays_iso) GEN-008(nom_non_vide) GEN-009(date_non_future) GEN-010(full_name>=2mots)
BNK-001(iban_tn) BNK-002(solde>=0) BNK-003(montant>0) BNK-004(taux>=0) BNK-005(taux<=18%)
BNK-006(montant_crédit>0) BNK-007(durée_crédit_1-360mois) BNK-008(date_fin>date_debut)
BNK-009(solde<=10M_AML) BNK-010(montant<=500k_AML) BNK-011(statut_compte_valide)
BNK-012(référence_unique) BNK-013(plafond_virement>=0) BNK-014(capacity>0)
ISL-001(compte_islamique_taux=0) ISL-002(crédit_islamique_taux=0)
Réponds UNIQUEMENT en JSON valide :
{"detected_table_type":"transaction|client|credit|account|branch|market_data|generic",
"applicable_rules":[{"rule_id":"GEN-001","column_mapping":"email","severity":"IMPORTANT","threshold":null,"applies":true}],
"custom_rules":[],"notes":""}
"""

def analyze_table_for_validation(table_info: Dict, pdf_sample: pd.DataFrame) -> Dict:
    if not GROQ.clients:
        return {"applicable_rules": [], "custom_rules": [], "detected_table_type": "generic"}
    log.info("  [GOLD-1] Analyse Groq '%s'…", table_info.get("table_name"))
    schema_info = {}
    for c in pdf_sample.columns:
        if c.startswith("_"): continue
        nn   = pdf_sample[c].dropna()
        info = {"type": str(pdf_sample[c].dtype), "null_rate": round(100*pdf_sample[c].isna().mean(),1),
                "sample": nn.head(3).tolist() if len(nn)>0 else []}
        if len(nn) > 0:
            numeric = pd.to_numeric(nn, errors="coerce").dropna()
            if len(numeric)/max(len(nn),1) > 0.8:
                info["min"] = float(numeric.min()); info["max"] = float(numeric.max())
        schema_info[c] = info
    user_msg = (f"Table:{table_info.get('table_name')} "
                f"Source:{table_info.get('source_type')}/{table_info.get('db_name','N/A')}\n"
                f"Colonnes:{', '.join(c for c in pdf_sample.columns if not c.startswith('_'))}\n"
                f"Schéma:{json.dumps(schema_info, ensure_ascii=False, default=str)}\n"
                f"Seuils: MAX_BALANCE={MAX_BALANCE:,} MAX_TX={MAX_TRANSACTION_AMOUNT:,} "
                f"MAX_RATE={MAX_INTEREST_RATE}% MAX_CREDIT={MAX_CREDIT_DURATION_MONTHS}m")
    raw    = GROQ.complete(_SYSTEM_GOLD_ANALYSIS, user_msg, max_tokens=700)
    result = _parse_json_from_groq(raw)
    if not isinstance(result, dict): result = {}
    n_rules = len(result.get("applicable_rules",[])) + len(result.get("custom_rules",[]))
    log.info("  [GOLD-1] '%s' → type=%s, %d règles : %s",
             table_info.get("table_name"), result.get("detected_table_type","?"), n_rules,
             ", ".join(r.get("rule_id","?") for r in result.get("applicable_rules",[])))
    return result


# ═════════════════════════════════════════════════════════════════════════
#  GROQ UTILITAIRES
# ═════════════════════════════════════════════════════════════════════════

_SYSTEM_GENDER = ("Expert onomastique (noms arabes, tunisiens, français).\n"
                  "On te donne une liste JSON de prénoms/noms. Pour chacun réponds \"m\" ou \"f\".\n"
                  "Réponds UNIQUEMENT en JSON : [\"m\",\"f\",\"m\",...]\n"
                  "Si incertain → \"m\". Même longueur que l'input obligatoire.")

def infer_gender_batch(names: List[str]) -> List[str]:
    if not names or not GROQ.clients: return ["m"] * len(names)
    raw    = GROQ.complete(_SYSTEM_GENDER, json.dumps(names, ensure_ascii=False),
                           max_tokens=len(names)*4+20)
    result = _parse_json_from_groq(raw)
    if isinstance(result, list) and len(result) == len(names):
        return [("f" if str(r).strip().lower() in ("f","female","femme","féminin") else "m") for r in result]
    return ["m"] * len(names)

_SYSTEM_FULLNAME = ("Assistant nettoyage données bancaires.\n"
                    "Nom partiel + email → reconstitue \"Prénom Nom\" (2 mots min).\n"
                    "Si impossible → réponds EXACTEMENT \"IMPOSSIBLE\".")

def complete_full_name(partial_name: str, email: str = "") -> Optional[str]:
    if not GROQ.clients: return None
    result = GROQ.complete(_SYSTEM_FULLNAME, f'Nom:"{partial_name}" Email:"{email or ""}"',
                           max_tokens=50).strip()
    if not result or result.upper() == "IMPOSSIBLE": return None
    words = [w for w in result.split() if w]
    return result if len(words) >= 2 else None


# ═════════════════════════════════════════════════════════════════════════
#  CATALOGUE DES RÈGLES
# ═════════════════════════════════════════════════════════════════════════

class BusinessRule:
    CRITIQUE = "CRITIQUE"; IMPORTANT = "IMPORTANT"; AVERTISSEMENT = "AVERTISSEMENT"
    def __init__(self, rule_id, name, description, severity, domain, columns):
        self.rule_id=rule_id; self.name=name; self.description=description
        self.severity=severity; self.domain=domain; self.columns=columns

BUSINESS_RULES_CATALOG: Dict[str, BusinessRule] = {r.rule_id: r for r in [
    BusinessRule("GEN-001","Email valide","Format user@domain.ext",BusinessRule.IMPORTANT,"general",["email"]),
    BusinessRule("GEN-002","Téléphone valide","+216 + 8 chiffres",BusinessRule.IMPORTANT,"general",["phone","telephone"]),
    BusinessRule("GEN-003","Âge minimum",f">= {MIN_CLIENT_AGE} ans",BusinessRule.CRITIQUE,"general",["date_of_birth","dob"]),
    BusinessRule("GEN-004","Âge maximum",f"<= {MAX_CLIENT_AGE} ans",BusinessRule.IMPORTANT,"general",["date_of_birth","dob"]),
    BusinessRule("GEN-005","DOB non future","Date de naissance non future",BusinessRule.CRITIQUE,"general",["date_of_birth","dob"]),
    BusinessRule("GEN-006","Genre valide","m/f/male/female",BusinessRule.AVERTISSEMENT,"general",["gender"]),
    BusinessRule("GEN-007","Code pays valide","ISO-3166 alpha-2",BusinessRule.IMPORTANT,"general",["country_code","nationality"]),
    BusinessRule("GEN-008","Nom non vide","Nom non null/vide",BusinessRule.CRITIQUE,"general",["full_name","nom","customer_name"]),
    BusinessRule("GEN-009","Date non future","Dates tx/création non futures",BusinessRule.IMPORTANT,"general",["created_at","transaction_date","open_date"]),
    BusinessRule("GEN-010","Nom complet","full_name >= 2 mots",BusinessRule.IMPORTANT,"general",["full_name","customer_name"]),
    BusinessRule("BNK-001","IBAN valide","TN + 22 chiffres = 24 chars",BusinessRule.IMPORTANT,"banking",["iban"]),
    BusinessRule("BNK-002","Solde non négatif","balance >= 0",BusinessRule.CRITIQUE,"banking",["balance","solde","account_balance"]),
    BusinessRule("BNK-003","Montant tx positif","amount > 0",BusinessRule.CRITIQUE,"banking",["amount","montant","transaction_amount"]),
    BusinessRule("BNK-004","Taux non négatif","interest_rate >= 0",BusinessRule.CRITIQUE,"banking",["interest_rate","taux_interet"]),
    BusinessRule("BNK-005","Taux non usuraire",f"interest_rate <= {MAX_INTEREST_RATE}%",BusinessRule.CRITIQUE,"banking",["interest_rate","taux_interet"]),
    BusinessRule("BNK-006","Montant crédit positif","amount > 0 (crédit)",BusinessRule.CRITIQUE,"banking",["amount","montant"]),
    BusinessRule("BNK-007","Durée crédit valide",f"{MIN_CREDIT_DURATION_MONTHS}-{MAX_CREDIT_DURATION_MONTHS} mois",BusinessRule.IMPORTANT,"banking",["duration_months"]),
    BusinessRule("BNK-008","Date fin > date début","end_date > start_date",BusinessRule.CRITIQUE,"banking",["end_date","start_date"]),
    BusinessRule("BNK-009","Solde AML",f"balance <= {MAX_BALANCE:,} TND",BusinessRule.IMPORTANT,"banking",["balance","account_balance"]),
    BusinessRule("BNK-010","Montant AML",f"amount <= {MAX_TRANSACTION_AMOUNT:,} TND",BusinessRule.IMPORTANT,"banking",["amount","transaction_amount"]),
    BusinessRule("BNK-011","Statut compte valide","actif/suspendu/clôturé/bloqué/...",BusinessRule.IMPORTANT,"banking",["status","account_status","kyc_status"]),
    BusinessRule("BNK-012","Référence unique","référence unique dans la table",BusinessRule.CRITIQUE,"banking",["reference","transaction_id","credit_id","account_id"]),
    BusinessRule("BNK-013","Plafond virement","plafond_virement >= 0",BusinessRule.IMPORTANT,"banking",["plafond_virement"]),
    BusinessRule("BNK-014","Capacité agence","capacity > 0",BusinessRule.AVERTISSEMENT,"banking",["capacity"]),
    BusinessRule("ISL-001","Riba compte islamique","interest_rate = 0 si islamique",BusinessRule.CRITIQUE,"islamic",["interest_rate","taux_interet","account_type"]),
    BusinessRule("ISL-002","Riba crédit islamique","interest_rate = 0 si islamique",BusinessRule.CRITIQUE,"islamic",["interest_rate","taux_interet","product_type","credit_type"]),
]}


# ═════════════════════════════════════════════════════════════════════════
#  HELPERS DE VALIDATION
# ═════════════════════════════════════════════════════════════════════════

def _v_email(v)->bool:
    if v is None or (isinstance(v,float) and pd.isna(v)): return True
    return bool(re.match(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$",str(v).strip()))
def _v_phone(v)->bool:
    if v is None or (isinstance(v,float) and pd.isna(v)): return True
    digits=re.sub(r"[\s\-\.\(\)]","",str(v).strip())
    if   digits.startswith("+216"):                       local=digits[4:]
    elif digits.startswith("00216"):                      local=digits[5:]
    elif digits.startswith("216") and len(digits)==11:    local=digits[3:]
    else:                                                 local=digits
    if local.startswith("0") and len(local)==9: local=local[1:]
    return local.isdigit() and len(local)==8 and local[0] in "234579"
def _v_iban_tn(v)->bool:
    if v is None or (isinstance(v,float) and pd.isna(v)): return True
    s=str(v).replace(" ","").upper()
    return s.startswith("TN") and len(s)==24 and s[2:].isdigit()
def _client_age(dob_str)->Optional[float]:
    if dob_str is None or (isinstance(dob_str,float) and pd.isna(dob_str)): return None
    try:
        dob=date.fromisoformat(str(dob_str)[:10]) if isinstance(dob_str,str) else dob_str
        return (date.today()-dob).days/365.25
    except Exception: return None
def _is_future(d_str)->bool:
    if d_str is None or (isinstance(d_str,float) and pd.isna(d_str)): return False
    try:
        d=date.fromisoformat(str(d_str)[:10]) if isinstance(d_str,str) else d_str
        return d>date.today()
    except Exception: return False
def _date_a_before_b(d1,d2)->bool:
    try: return date.fromisoformat(str(d1)[:10])<date.fromisoformat(str(d2)[:10])
    except Exception: return False
def _is_valid_gender(v)->bool:
    if v is None or (isinstance(v,float) and pd.isna(v)): return True
    return str(v).lower().strip() in {"m","f","male","female"}
def _is_full_name_col(col_name:str)->bool:
    return any(kw in col_name.lower() for kw in FULLNAME_COLUMN_KEYWORDS)
def _get_col(cols_lower:Dict,name:str)->Optional[str]:
    return cols_lower.get(name.lower())


# ═════════════════════════════════════════════════════════════════════════
#  GOLD-2 : APPLICATION DES RÈGLES
# ═════════════════════════════════════════════════════════════════════════

def apply_business_rules_pandas(pdf: pd.DataFrame, table_name: str,
                                 groq_analysis: Dict) -> pd.DataFrame:
    n = len(pdf)
    errors_per_row: List[List[Dict]] = [[] for _ in range(n)]
    cols = {c.lower(): c for c in pdf.columns}

    def get(name): return _get_col(cols, name)
    def rule(rid): return BUSINESS_RULES_CATALOG.get(rid)

    ref_col     = get("transaction_id") or get("credit_id") or get("account_id") or get("reference")
    gender_col  = get("gender"); email_col = get("email")
    fullname_col: Optional[str] = None
    for fn_kw in FULLNAME_COLUMN_KEYWORDS:
        c_c = get(fn_kw)
        if c_c and c_c in pdf.columns: fullname_col = c_c; break
    balance_col = get("balance") or get("solde") or get("account_balance") or get("cust_account_balance")
    amount_col  = get("amount") or get("montant") or get("transaction_amount")
    rate_col    = get("interest_rate") or get("taux_interet")
    dob_col     = get("date_of_birth") or get("dob") or get("customerdob")
    start_col   = get("start_date"); end_col = get("end_date")
    status_col  = get("account_status") or get("kyc_status") or get("credit_status") or get("status")

    dup_ref_rows: Set[int] = set()
    if ref_col and ref_col in pdf.columns:
        seen: Dict[str,int] = {}
        for idx in range(n):
            v = pdf.at[idx, ref_col]
            if pd.notna(v):
                s = str(v).strip()
                if s in seen: dup_ref_rows.add(idx)
                else: seen[s] = idx
        if dup_ref_rows:
            log.info("  [FIX-7] %d doublons de référence", len(dup_ref_rows))

    if gender_col and gender_col in pdf.columns:
        invalid_mask = pdf[gender_col].apply(lambda v: pd.notna(v) and not _is_valid_gender(v))
        invalid_idxs = pdf.index[invalid_mask].tolist()
        if invalid_idxs:
            names_batch = [str(pdf.at[i, fullname_col]) if fullname_col and pd.notna(pdf.at[i, fullname_col]) else ""
                           for i in invalid_idxs]
            inferred = infer_gender_batch(names_batch)
            for idx, g in zip(invalid_idxs, inferred): pdf.at[idx, gender_col] = g
            log.info("  [FIX-8] %d genres corrigés en batch", len(invalid_idxs))

    for fn_key in [k for k in cols if _is_full_name_col(k)]:
        fn_col = cols[fn_key]
        if fn_col not in pdf.columns: continue
        n_fixed = n_failed = 0
        for idx in range(n):
            v = pdf.at[idx, fn_col]
            if pd.isna(v) or str(v).strip() == "" or len([w for w in str(v).split() if w]) >= 2: continue
            email_val = str(pdf.at[idx, email_col]).strip() if email_col and pd.notna(pdf.at[idx, email_col]) else ""
            completed = complete_full_name(str(v).strip(), email_val)
            if completed: pdf.at[idx, fn_col] = completed; n_fixed += 1
            else: n_failed += 1
        if n_fixed:  log.info("  [FIX-9] %d noms complétés", n_fixed)
        if n_failed: log.info("  [FIX-9] %d noms incomplets → GEN-010", n_failed)

    active_rule_ids: Set[str] = set(); groq_col_map: Dict[str,str] = {}
    for r_info in groq_analysis.get("applicable_rules",[]) + groq_analysis.get("custom_rules",[]):
        if r_info.get("applies", True):
            rid=r_info.get("rule_id",""); col_mapped=r_info.get("column_mapping","")
            active_rule_ids.add(rid)
            if col_mapped: groq_col_map[rid]=col_mapped

    log.info("  [GOLD-2] %d règles actives : %s",
             len(active_rule_ids), ", ".join(sorted(active_rule_ids)) or "(aucune)")

    def gc(rid, fb): mapped=groq_col_map.get(rid); return mapped if mapped and mapped in pdf.columns else fb
    def _num(cn): return pd.to_numeric(pdf[cn], errors="coerce") if cn and cn in pdf.columns else None

    balance_num=_num(balance_col); amount_num=_num(amount_col)
    rate_num=_num(rate_col); duration_num=_num(get("duration_months"))

    for idx in range(n):
        row_errors: List[Dict] = []
        def add_error(r: BusinessRule, detail: str):
            row_errors.append({"rule_id":r.rule_id,"name":r.name,"severity":r.severity,"domain":r.domain,"detail":detail})

        c=gc("GEN-001",get("email"))
        if c and c in pdf.columns and "GEN-001" in active_rule_ids:
            v=pdf.at[idx,c]
            if pd.notna(v) and not _v_email(v):
                r=rule("GEN-001"); r and add_error(r,f"Email invalide : '{v}'")

        for pname in ["phone","telephone","phone_contact","telephone_agence"]:
            c=gc("GEN-002",get(pname))
            if c and c in pdf.columns and "GEN-002" in active_rule_ids:
                v=pdf.at[idx,c]
                if pd.notna(v) and not _v_phone(v):
                    r=rule("GEN-002"); r and add_error(r,f"Téléphone invalide : '{v}'")
                break

        c=gc("GEN-003",dob_col)
        if c and c in pdf.columns and {"GEN-003","GEN-004","GEN-005"} & active_rule_ids:
            v=pdf.at[idx,c]
            if pd.notna(v):
                if _is_future(v):
                    r=rule("GEN-005"); r and "GEN-005" in active_rule_ids and add_error(r,f"DOB future : '{v}'")
                else:
                    age=_client_age(v)
                    if age is not None:
                        if age<MIN_CLIENT_AGE and "GEN-003" in active_rule_ids:
                            r=rule("GEN-003"); r and add_error(r,f"Client mineur : {age:.1f} ans")
                        elif age>MAX_CLIENT_AGE and "GEN-004" in active_rule_ids:
                            r=rule("GEN-004"); r and add_error(r,f"Âge improbable : {age:.1f} ans")

        for ccname in ["country_code","nationality"]:
            c=gc("GEN-007",get(ccname))
            if c and c in pdf.columns and "GEN-007" in active_rule_ids:
                v=pdf.at[idx,c]
                if pd.notna(v) and str(v).upper().strip() not in VALID_COUNTRY_CODES:
                    r=rule("GEN-007"); r and add_error(r,f"Code pays invalide : '{v}'")
                break

        for nname in ["full_name","nom","customer_name"]:
            c=gc("GEN-008",get(nname))
            if c and c in pdf.columns and "GEN-008" in active_rule_ids:
                v=pdf.at[idx,c]
                if pd.isna(v) or str(v).strip()=="":
                    r=rule("GEN-008"); r and add_error(r,"Nom vide ou null")

        for dcname in ["created_at","transaction_date","open_date","opened_date","start_date"]:
            c=gc("GEN-009",get(dcname))
            if c and c in pdf.columns and "GEN-009" in active_rule_ids:
                v=pdf.at[idx,c]
                if pd.notna(v) and _is_future(v):
                    r=rule("GEN-009"); r and add_error(r,f"Date future ({dcname}) : '{v}'")
                break

        for fn_key in [k for k in cols if _is_full_name_col(k)]:
            fn_c=cols[fn_key]
            if fn_c in pdf.columns and "GEN-010" in active_rule_ids:
                v=pdf.at[idx,fn_c]
                if pd.notna(v) and len([w for w in str(v).split() if w])<2:
                    r=rule("GEN-010"); r and add_error(r,f"Nom incomplet : '{v}'")

        c=gc("BNK-001",get("iban"))
        if c and c in pdf.columns and "BNK-001" in active_rule_ids:
            v=pdf.at[idx,c]
            if pd.notna(v) and not _v_iban_tn(v):
                r=rule("BNK-001"); r and add_error(r,f"IBAN invalide : '{v}'")

        if balance_num is not None and "BNK-002" in active_rule_ids:
            fv=balance_num.iloc[idx]
            if pd.notna(fv) and fv<0: r=rule("BNK-002"); r and add_error(r,f"Solde négatif : {fv}")

        is_tx="transaction" in table_name.lower()
        if amount_num is not None and is_tx:
            fv=amount_num.iloc[idx]
            if pd.notna(fv):
                if fv<=0 and "BNK-003" in active_rule_ids:
                    r=rule("BNK-003"); r and add_error(r,f"Montant invalide : {fv}")
                elif fv>MAX_TRANSACTION_AMOUNT and "BNK-010" in active_rule_ids:
                    r=rule("BNK-010"); r and add_error(r,f"Montant > AML : {fv}")

        if rate_num is not None and {"BNK-004","BNK-005"} & active_rule_ids:
            fv=rate_num.iloc[idx]
            if pd.notna(fv):
                if fv<0 and "BNK-004" in active_rule_ids:
                    r=rule("BNK-004"); r and add_error(r,f"Taux négatif : {fv}")
                elif fv>MAX_INTEREST_RATE and "BNK-005" in active_rule_ids:
                    r=rule("BNK-005"); r and add_error(r,f"Taux usuraire : {fv}%")

        is_credit="credit" in table_name.lower()
        if amount_num is not None and is_credit and "BNK-006" in active_rule_ids:
            fv=amount_num.iloc[idx]
            if pd.notna(fv) and fv<=0: r=rule("BNK-006"); r and add_error(r,f"Montant crédit invalide : {fv}")

        if duration_num is not None and "BNK-007" in active_rule_ids:
            fv=duration_num.iloc[idx]
            if pd.notna(fv):
                iv=int(fv)
                if not (MIN_CREDIT_DURATION_MONTHS<=iv<=MAX_CREDIT_DURATION_MONTHS):
                    r=rule("BNK-007"); r and add_error(r,f"Durée invalide : {iv} mois")

        sc=gc("BNK-008",start_col); ec=gc("BNK-008",end_col)
        if sc and ec and sc in pdf.columns and ec in pdf.columns and "BNK-008" in active_rule_ids:
            sv,ev=pdf.at[idx,sc],pdf.at[idx,ec]
            if pd.notna(sv) and pd.notna(ev) and not _date_a_before_b(sv,ev):
                r=rule("BNK-008"); r and add_error(r,f"Date fin≤début ({ev}≤{sv})")

        if balance_num is not None and "BNK-009" in active_rule_ids:
            fv=balance_num.iloc[idx]
            if pd.notna(fv) and fv>MAX_BALANCE:
                r=rule("BNK-009"); r and add_error(r,f"Solde suspect>{MAX_BALANCE:,} TND : {fv}")

        if status_col and status_col in pdf.columns and "BNK-011" in active_rule_ids:
            v=pdf.at[idx,status_col]
            if pd.notna(v) and str(v).lower().strip() not in VALID_ACCOUNT_STATUSES:
                r=rule("BNK-011"); r and add_error(r,f"Statut invalide : '{v}'")

        if idx in dup_ref_rows and "BNK-012" in active_rule_ids:
            rv=pdf.at[idx,ref_col] if ref_col else None
            r=rule("BNK-012"); r and add_error(r,f"Référence dupliquée : '{rv}'")

        c=gc("BNK-013",get("plafond_virement"))
        if c and c in pdf.columns and "BNK-013" in active_rule_ids:
            v=pdf.at[idx,c]
            if pd.notna(v):
                try:
                    if float(v)<0: r=rule("BNK-013"); r and add_error(r,f"Plafond négatif : {v}")
                except (ValueError,TypeError): pass

        at_col=get("account_type")
        if at_col and rate_col and at_col in pdf.columns and rate_col in pdf.columns:
            at,rt=pdf.at[idx,at_col],pdf.at[idx,rate_col]
            if pd.notna(at) and pd.notna(rt) and str(at).lower().strip() in ISLAMIC_ACCOUNT_TYPES:
                try:
                    if float(rt)!=0: r=rule("ISL-001"); r and add_error(r,f"Riba compte islamique ({at}), taux={rt}%")
                except (ValueError,TypeError): pass

        for ct_field in ["product_type","credit_type"]:
            ct_col=get(ct_field)
            if ct_col and rate_col and ct_col in pdf.columns and rate_col in pdf.columns:
                ct,rt=pdf.at[idx,ct_col],pdf.at[idx,rate_col]
                if pd.notna(ct) and pd.notna(rt) and str(ct).lower().strip() in ISLAMIC_CREDIT_TYPES:
                    try:
                        if float(rt)!=0: r=rule("ISL-002"); r and add_error(r,f"Riba crédit islamique ({ct}), taux={rt}%")
                    except (ValueError,TypeError): pass
                break

        errors_per_row[idx]=row_errors

    pdf["_validation_errors"]=[json.dumps(e,ensure_ascii=False) for e in errors_per_row]
    pdf["_violation_count"]=[len(e) for e in errors_per_row]
    def max_sev(errs):
        if not errs: return "NONE"
        sevs=[e["severity"] for e in errs]
        for s in (BusinessRule.CRITIQUE,BusinessRule.IMPORTANT,BusinessRule.AVERTISSEMENT):
            if s in sevs: return s
        return "NONE"
    pdf["_max_severity"]=[max_sev(e) for e in errors_per_row]
    pdf["_is_quarantined"]=[any(e["severity"] in (BusinessRule.CRITIQUE,BusinessRule.IMPORTANT) for e in errs) for errs in errors_per_row]
    pdf["_quarantine_reason"]=[("|".join(f"[{e['rule_id']}] {e['detail']}" for e in [x for x in errs if x["severity"] in (BusinessRule.CRITIQUE,BusinessRule.IMPORTANT)][:3]) or None) for errs in errors_per_row]

    rvc: Dict[str,int] = {}
    for errs in errors_per_row:
        for e in errs: rvc[e.get("rule_id","?")]=rvc.get(e.get("rule_id","?"),0)+1
    if rvc:
        log.info("  [GOLD-2] Violations par règle :")
        for rid,cnt in sorted(rvc.items(),key=lambda x:-x[1]):
            r_o=BUSINESS_RULES_CATALOG.get(rid); lbl=r_o.name if r_o else rid
            log.info("    %-10s  %4d  — %s",rid,cnt,lbl)
    else:
        log.info("  [GOLD-2] Aucune violation détectée ✓")
    return pdf


# ═════════════════════════════════════════════════════════════════════════
#  MÉTRIQUES
# ═════════════════════════════════════════════════════════════════════════

def compute_precision_recall(pdf: pd.DataFrame, table_name: str) -> Dict:
    if "_ground_truth" not in pdf.columns or "_is_quarantined" not in pdf.columns:
        return {"available": False, "note": "Colonne _ground_truth absente"}
    tp=fp=fn=tn=0
    etd: Dict[str,int]={}; etm: Dict[str,int]={}
    for _,row in pdf.iterrows():
        gt=str(row.get("_ground_truth","VALID")); q=bool(row.get("_is_quarantined",False))
        inv=not gt.startswith("VALID"); et=gt.replace("INVALID: ","")
        if inv and q:     tp+=1; etd[et]=etd.get(et,0)+1
        elif inv and not q: fn+=1; etm[et]=etm.get(et,0)+1
        elif not inv and q: fp+=1
        else:               tn+=1
    total=tp+fp+fn+tn
    p=round(tp/max(tp+fp,1)*100,2); r=round(tp/max(tp+fn,1)*100,2)
    f1=round(2*p*r/max(p+r,1),2); acc=round((tp+tn)/max(total,1)*100,2)
    if etm: log.warning("  [P/R] Erreurs manquées : %s",etm)
    log.info("  [P/R] %s : P=%.1f%%  R=%.1f%%  F1=%.1f%%  Acc=%.1f%%",table_name,p,r,f1,acc)
    def _rating(p,r,f):
        if r>=95 and p>=85 and f>=90: g,l="A","Excellent"
        elif r>=85 and p>=75 and f>=80: g,l="B","Bon"
        elif r>=75 and p>=65 and f>=70: g,l="C","Acceptable"
        elif r>=60 and f>=55: g,l="D","Insuffisant"
        else: g,l="F","Critique"
        ws=[]
        if r<80: ws.append(f"ALERTE : Rappel faible ({r}%)")
        if p<70: ws.append(f"ALERTE : Précision faible ({p}%)")
        return {"grade":g,"label":l,"warnings":ws}
    return {"available":True,"table":table_name,
            "confusion_matrix":{"TP":tp,"FP":fp,"FN":fn,"TN":tn},
            "metrics":{"precision":p,"recall":r,"f1_score":f1,"accuracy":acc},
            "error_detection":{"detected_by_type":etd,"missed_by_type":etm},
            "rating":_rating(p,r,f1)}


def compute_quality_metrics(pdf, pdf_valid, pdf_quar, pr_metrics, table_info) -> Dict:
    total=len(pdf); nv=len(pdf_valid); nq=len(pdf_quar)
    completeness={c:round(100*pdf[c].notna().sum()/max(total,1),2) for c in pdf.columns if not c.startswith("_")}
    avg_comp=round(sum(completeness.values())/max(len(completeness),1),2)
    vrr: Dict[str,int]={}; vrd: Dict[str,int]={}
    vrs={"CRITIQUE":0,"IMPORTANT":0,"AVERTISSEMENT":0}
    for ej in pdf["_validation_errors"]:
        try:
            for e in json.loads(ej or "[]"):
                rid=e.get("rule_id","?"); dom=e.get("domain","?"); sev=e.get("severity","?")
                vrr[rid]=vrr.get(rid,0)+1; vrd[dom]=vrd.get(dom,0)+1
                if sev in vrs: vrs[sev]+=1
        except (json.JSONDecodeError,TypeError): pass
    qr=round(100*nq/max(total,1),2); alerts: List[Dict]=[]
    if qr>30: alerts.append({"level":"CRITICAL","message":f"Taux quarantaine critique : {qr}%"})
    elif qr>10: alerts.append({"level":"WARNING","message":f"Taux quarantaine élevé : {qr}%"})
    if avg_comp<70: alerts.append({"level":"WARNING","message":f"Complétude faible : {avg_comp}%"})
    if pr_metrics.get("available") and pr_metrics["metrics"]["recall"]<80:
        alerts.append({"level":"CRITICAL","message":f"Rappel faible ({pr_metrics['metrics']['recall']}%)"})
    log.info("  [Métriques] total=%d  valides=%d  quarantaine=%d  (%.1f%%)",total,nv,nq,round(100*nv/max(total,1),2))
    log.info("  [Métriques] Complétude : %.1f%%  |  CRITIQUE=%d  IMPORTANT=%d  AVERT=%d",
             avg_comp,vrs.get("CRITIQUE",0),vrs.get("IMPORTANT",0),vrs.get("AVERTISSEMENT",0))
    for a in alerts: log.warning("  [ALERTE][%s] %s",a["level"],a["message"])
    return {"table":table_info.get("table_name"),"source_type":table_info.get("source_type"),
            "db_name":table_info.get("db_name",""),"timestamp":datetime.utcnow().isoformat(),
            "rows":{"total":total,"valid":nv,"quarantined":nq,
                    "validity_rate":round(100*nv/max(total,1),2),"quarantine_rate":qr},
            "completeness":{"by_column":completeness,"average":avg_comp},
            "violations":{"by_rule":vrr,"by_domain":vrd,"by_severity":vrs},
            "precision_recall":pr_metrics,"alerts":alerts}


# ═════════════════════════════════════════════════════════════════════════
#  PROMETHEUS / PROM FILE
# ═════════════════════════════════════════════════════════════════════════

def push_metrics_prometheus(metrics: Dict):
    table=metrics.get("table","unknown")
    labels={"table":table,"db":metrics.get("db_name",""),"source_type":metrics.get("source_type","")}
    pr=metrics.get("precision_recall",{}); pr_m=pr.get("metrics",{})
    viols_d=metrics.get("violations",{}).get("by_domain",{})
    vrs=metrics.get("violations",{}).get("by_severity",{})

    if PROMETHEUS_AVAILABLE:
        try:
            registry=CollectorRegistry()
            def gauge(name,desc,val):
                g=Gauge(name,desc,list(labels.keys()),registry=registry)
                g.labels(**labels).set(val)
            gauge("gold_rows_valid_total","Lignes valides",metrics["rows"]["valid"])
            gauge("gold_rows_quarantine_total","Lignes quarantine",metrics["rows"]["quarantined"])
            gauge("gold_validity_rate_percent","Taux validité %",metrics["rows"]["validity_rate"])
            gauge("gold_quarantine_rate_percent","Taux quarantaine %",metrics["rows"]["quarantine_rate"])
            gauge("gold_completeness_avg_percent","Complétude %",metrics["completeness"]["average"])
            if pr.get("available"):
                gauge("gold_precision_percent","Précision %",pr_m.get("precision",0))
                gauge("gold_recall_percent","Rappel %",pr_m.get("recall",0))
                gauge("gold_f1_score_percent","F1-Score %",pr_m.get("f1_score",0))
            push_to_gateway(PROMETHEUS_PUSHGATEWAY, job="gold_pipeline_v7",
                            grouping_key={"table":table}, registry=registry)
            log.info("  [Prometheus] Push gateway OK")
        except Exception:
            log.info("  [Prometheus] Gateway inaccessible → .prom + HTTP")
            _write_prom_file(metrics,labels,pr_m,pr,viols_d,vrs)
    else:
        _write_prom_file(metrics,labels,pr_m,pr,viols_d,vrs)


def _write_prom_file(metrics,labels,pr_m,pr,viols_by_domain,vrs):
    ls=",".join(f'{k}="{v}"' for k,v in labels.items())
    lines=[
        f'# TYPE gold_rows_valid_total gauge',
        f'gold_rows_valid_total{{{ls}}} {metrics["rows"]["valid"]}',
        f'# TYPE gold_rows_quarantine_total gauge',
        f'gold_rows_quarantine_total{{{ls}}} {metrics["rows"]["quarantined"]}',
        f'# TYPE gold_validity_rate_percent gauge',
        f'gold_validity_rate_percent{{{ls}}} {metrics["rows"]["validity_rate"]}',
        f'# TYPE gold_quarantine_rate_percent gauge',
        f'gold_quarantine_rate_percent{{{ls}}} {metrics["rows"]["quarantine_rate"]}',
        f'# TYPE gold_completeness_avg_percent gauge',
        f'gold_completeness_avg_percent{{{ls}}} {metrics["completeness"]["average"]}',
        f'gold_violations_critique{{{ls}}} {vrs.get("CRITIQUE",0)}',
        f'gold_violations_important{{{ls}}} {vrs.get("IMPORTANT",0)}',
    ]
    for domain,cnt in viols_by_domain.items():
        safe=re.sub(r"[^a-zA-Z0-9_]","_",domain)
        lines.append(f'gold_violations_domain_{safe}{{{ls}}} {cnt}')
    if pr.get("available"):
        lines+=[f'gold_precision_percent{{{ls}}} {pr_m.get("precision",0)}',
                f'gold_recall_percent{{{ls}}} {pr_m.get("recall",0)}',
                f'gold_f1_score_percent{{{ls}}} {pr_m.get("f1_score",0)}']
    prom_dir=LOCAL_GOLD_ROOT/"metrics_export"; prom_dir.mkdir(parents=True,exist_ok=True)
    fname=prom_dir/f"{metrics.get('table','unknown')}_{_RUN_TS}.prom"
    fname.write_text("\n".join(lines),encoding="utf-8")
    log.info("  [Prometheus] Métriques exportées → %s",fname)


# ═════════════════════════════════════════════════════════════════════════
#  ÉCRITURE GOLD — PYARROW DIRECT
# ═════════════════════════════════════════════════════════════════════════

def _write_parquet_pyarrow(pdf: pd.DataFrame, out_dir: Path, filename: str="data.parquet") -> str:
    out_dir.mkdir(parents=True,exist_ok=True)
    out_file=out_dir/filename
    if PYARROW_AVAILABLE:
        try:
            pdf_c=pdf.copy()
            for cn in pdf_c.select_dtypes(include=["object"]).columns:
                pdf_c[cn]=pdf_c[cn].where(pd.notnull(pdf_c[cn]),None)
            tbl=pa.Table.from_pandas(pdf_c,preserve_index=False)
            pq.write_table(tbl,str(out_file),compression="snappy",write_statistics=True)
            log.info("  [pyarrow] → %s (%d lignes)",out_file,len(pdf))
            return str(out_file)
        except Exception as e:
            log.warning("  [pyarrow] Échec (%s) → CSV fallback",e)
    csv_file=out_dir/filename.replace(".parquet",".csv")
    pdf.to_csv(str(csv_file),index=False,encoding="utf-8")
    log.info("  [CSV fallback] → %s (%d lignes)",csv_file,len(pdf))
    return str(csv_file)


def _gold_paths(table_info: Dict, ts: str) -> Tuple[Path,Path,Path]:
    st=table_info.get("source_type","unknown"); db=table_info.get("db_name",""); tn=table_info.get("table_name","unknown")
    is_db=db and st in ("db_rows","db_wrapped")
    if is_db:
        bv=LOCAL_GOLD_ROOT/"validated"/st/db/tn/f"processed_{ts}"; bq=LOCAL_GOLD_ROOT/"quarantine"/st/db/tn/f"quarantine_{ts}"
        rp=LOCAL_GOLD_ROOT/"quality_reports"/st/db/f"{tn}_{ts}.json"
    else:
        bv=LOCAL_GOLD_ROOT/"validated"/st/tn/f"processed_{ts}"; bq=LOCAL_GOLD_ROOT/"quarantine"/st/tn/f"quarantine_{ts}"
        rp=LOCAL_GOLD_ROOT/"quality_reports"/st/f"{tn}_{ts}.json"
    return bv,bq,rp


def write_gold(pdf_valid,pdf_quar,metrics,table_info) -> Dict:
    ts=datetime.utcnow().strftime("%Y%m%d_%H%M%S"); bv,bq,rp=_gold_paths(table_info,ts); op={}
    if not pdf_valid.empty:
        cols_drop=[c for c in pdf_valid.columns if c.startswith("_validation") or
                   c in {"_is_quarantined","_max_severity","_violation_count","_ground_truth","_quarantine_reason"}]
        pex=pdf_valid.drop(columns=cols_drop,errors="ignore")
        op["valid_parquet"]=_write_parquet_pyarrow(pex,bv)
        sp=bv.parent/f"processed_{ts}_sample.json"
        sp.write_text(json.dumps(pdf_valid.head(100).where(pd.notnull(pdf_valid.head(100)),None).to_dict(orient="records"),
                                  ensure_ascii=False,indent=2,default=str),encoding="utf-8")
        log.info("  [Écriture] Sample → %s",sp)
    if not pdf_quar.empty:
        op["quarantine_parquet"]=_write_parquet_pyarrow(pdf_quar,bq,"quarantine.parquet")
        rdr: Dict[str,int]={}
        for ej in pdf_quar["_validation_errors"]:
            try:
                for e in json.loads(ej or "[]"): rid=e.get("rule_id","?"); rdr[rid]=rdr.get(rid,0)+1
            except (json.JSONDecodeError,TypeError): pass
        qd=bq.parent/f"quarantine_{ts}_details.json"
        qd.write_text(json.dumps({"table":table_info.get("table_name"),"timestamp":ts,"count":len(pdf_quar),
                                    "reasons_by_rule":rdr,"sample_rows":pdf_quar.head(50).where(pd.notnull(pdf_quar.head(50)),None).to_dict(orient="records")},
                                   ensure_ascii=False,indent=2,default=str),encoding="utf-8")
    rp.parent.mkdir(parents=True,exist_ok=True)
    rp.write_text(json.dumps({"metadata":table_info,"metrics":metrics},ensure_ascii=False,indent=2,default=str),encoding="utf-8")
    log.info("  [Écriture] Rapport → %s",rp); op["quality_report"]=str(rp)
    return op


# ═════════════════════════════════════════════════════════════════════════
#  PIPELINE GOLD — TRAITEMENT D'UNE TABLE
# ═════════════════════════════════════════════════════════════════════════

def process_table(spark: SparkSession, table_info: Dict) -> Dict:
    tn=table_info.get("table_name","?"); st=table_info.get("source_type","?"); db=table_info.get("db_name","")
    pp=table_info.get("parquet_path","")
    log.info("")
    log.info("╔══════════════════════════════════════════════════════════════╗")
    log.info("║  TABLE : %-52s ║", f"{st}/{db}/{tn}" if db else f"{st}/{tn}")
    log.info("╚══════════════════════════════════════════════════════════════╝")
    log.info("  Silver : %s",pp)
    result: Dict={"table":tn,"db_name":db,"success":False}; t0=time.time()
    try:
        log.info("  [ÉTAPE 1/5] Lecture Parquet Spark…")
        df=spark.read.parquet(pp); total=df.count()
        if total==0: log.warning("  Parquet vide → ignoré"); return result
        log.info("  [ÉTAPE 1/5] ✓  %d lignes × %d colonnes",total,len(df.columns))

        pdf=df.toPandas(); pdf=_normalize_ground_truth(pdf)

        log.info("  [ÉTAPE 2/5] Analyse Groq (GOLD-1)…")
        ga=analyze_table_for_validation(table_info,pdf.head(50))
        log.info("  [ÉTAPE 2/5] ✓  Type : %s",ga.get("detected_table_type","?"))

        log.info("  [ÉTAPE 3/5] Application des règles métier…")
        pdf=apply_business_rules_pandas(pdf,tn,ga)
        pq_=pdf[pdf["_is_quarantined"]==True].copy(); pv=pdf[pdf["_is_quarantined"]!=True].copy()
        log.info("  [ÉTAPE 3/5] ✓  Valides : %d  |  Quarantaine : %d  (%.1f%%)",
                 len(pv),len(pq_),100*len(pq_)/max(total,1))

        log.info("  [ÉTAPE 4/5] Métriques de qualité…")
        pr=compute_precision_recall(pdf,tn); metrics=compute_quality_metrics(pdf,pv,pq_,pr,table_info)
        if pr.get("available"):
            rat=pr.get("rating",{})
            log.info("  [ÉTAPE 4/5] ✓  Rating : %s — %s",rat.get("grade","?"),rat.get("label",""))
        result.update({"metrics":metrics,"alerts":metrics.get("alerts",[])})

        # [INC-G-3] Mise à jour registre temps réel AVANT écriture
        duration_so_far=time.time()-t0
        _update_rt_table(tn, metrics, duration_so_far)
        push_metrics_prometheus(metrics)

        log.info("  [ÉTAPE 5/5] Écriture Gold (pyarrow)…")
        op=write_gold(pv,pq_,metrics,table_info)
        result.update({"output_paths":op,"success":True,"gold_parquet_path":op.get("valid_parquet","")})

        duration=round(time.time()-t0,2)
        # Mise à jour finale métriques temps réel avec durée exacte
        _update_rt_table(tn, metrics, duration)

        log.info("  ┌─ RÉSUMÉ ────────────────────────────────────────────────")
        log.info("  │  valid=%-8d  quarantaine=%-6d  durée=%.2fs",len(pv),len(pq_),duration)
        log.info("  │  Complétude : %.1f%%",metrics.get("completeness",{}).get("average",0))
        if pr.get("available"):
            pm=pr["metrics"]
            log.info("  │  P=%.1f%%  R=%.1f%%  F1=%.1f%%",pm["precision"],pm["recall"],pm["f1_score"])
        log.info("  └────────────────────────────────────────────────────────")

    except Exception as exc:
        log.error("  ERREUR Gold %s : %s",tn,exc,exc_info=True)
        result["error"]=str(exc)
    return result


# ═════════════════════════════════════════════════════════════════════════
#  RAPPORT GLOBAL
# ═════════════════════════════════════════════════════════════════════════

def write_global_report(all_results: List[Dict], skipped: List[Dict],
                        gold_state: dict):
    ts=_RUN_TS
    tv=sum(r.get("metrics",{}).get("rows",{}).get("valid",0) for r in all_results)
    tq=sum(r.get("metrics",{}).get("rows",{}).get("quarantined",0) for r in all_results)
    tt=tv+tq
    pr_av=[r for r in all_results if r.get("metrics",{}).get("precision_recall",{}).get("available")]
    def avg(k):
        if not pr_av: return None
        return round(sum(r["metrics"]["precision_recall"]["metrics"][k] for r in pr_av)/len(pr_av),2)
    all_alerts=[a for r in all_results for a in r.get("alerts",[])]
    report={"pipeline":"gold_pipeline_v7_local","timestamp":ts,
            "silver_root":str(LOCAL_SILVER_ROOT),"gold_root":str(LOCAL_GOLD_ROOT),
            "summary":{"tables_processed":len(all_results),"tables_skipped":len(skipped),
                       "tables_success":sum(1 for r in all_results if r.get("success")),
                       "total_rows":tt,"total_rows_valid":tv,"total_rows_quarantined":tq,
                       "global_validity_rate":round(100*tv/max(tt,1),2)},
            "pipeline_quality_metrics":{"avg_precision":avg("precision"),"avg_recall":avg("recall"),
                                        "avg_f1_score":avg("f1_score")},
            "alerts":all_alerts,"table_results":all_results}
    rp=LOCAL_GOLD_ROOT/"quality_reports"/f"_global_report_{ts}.json"
    rp.parent.mkdir(parents=True,exist_ok=True)
    rp.write_text(json.dumps(report,ensure_ascii=False,indent=2,default=str),encoding="utf-8")

    # Mise à jour état Gold
    for r in all_results:
        if r.get("success"):
            t=next((t for t in [] if t.get("table_name")==r.get("table")),"") or {}
            gk=f"{r.get('source_type','?')}|{r.get('db_name','')}|{r.get('table','?')}"
            gold_state["tables"][gk]={
                "silver_parquet_path": r.get("silver_parquet_path",""),
                "silver_ts":           r.get("silver_ts",""),
                "gold_parquet_path":   r.get("gold_parquet_path",""),
                "gold_ts":             _RUN_TS,
                "row_count_valid":     r.get("metrics",{}).get("rows",{}).get("valid",0),
                "row_count_quarantine":r.get("metrics",{}).get("rows",{}).get("quarantined",0),
                "validity_rate":       r.get("metrics",{}).get("rows",{}).get("validity_rate",0.0),
                "completeness":        r.get("metrics",{}).get("completeness",{}).get("average",0.0),
                "processed_at":        datetime.utcnow().isoformat(),
            }

    log.info("")
    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║             RAPPORT GLOBAL — GOLD v7                ║")
    log.info("╠══════════════════════════════════════════════════════╣")
    log.info("║  Traitées  : %-5d  Ignorées (inchang.) : %-5d     ║",len(all_results),len(skipped))
    log.info("║  Lignes valides   : %-8d                          ║",tv)
    log.info("║  Lignes quarant.  : %-8d                          ║",tq)
    log.info("║  Taux de validité : %-6.1f%%                         ║",report["summary"]["global_validity_rate"])
    if avg("precision") is not None:
        log.info("║  P=%-5.1f%%  R=%-5.1f%%  F1=%-5.1f%%                    ║",avg("precision"),avg("recall"),avg("f1_score"))
    log.info("╚══════════════════════════════════════════════════════╝")
    log.info("Rapport global → %s",rp)
    return report


# ═════════════════════════════════════════════════════════════════════════
#  POINT D'ENTRÉE
# ═════════════════════════════════════════════════════════════════════════

def run_gold_pipeline(target_table: Optional[str]=None, target_db: Optional[str]=None,
                      force_all: bool=False):
    pipeline_start=time.time()
    log.info("╔══════════════════════════════════════════╗")
    log.info("║  GOLD PIPELINE v7 LOCAL  —  démarrage    ║")
    log.info("╚══════════════════════════════════════════╝")
    log.info("  Silver : %s",LOCAL_SILVER_ROOT); log.info("  Gold   : %s",LOCAL_GOLD_ROOT)
    log.info("  Python : %s / %s",sys.version.split()[0],sys.executable)
    log.info("  pyarrow : %s  |  Groq : %s  |  Prometheus : %s",
             PYARROW_AVAILABLE,GROQ_AVAILABLE,PROMETHEUS_AVAILABLE)

    # [INC-G-3] Démarrer le serveur HTTP métriques
    with _RT_LOCK:
        _RT_METRICS.update({"running":1,"tables_processed":0,"tables_pending":0,
                            "tables_skipped":0,"rows_valid":0,"rows_quarantine":0,
                            "validity_rate":0.0,"duration":0.0,"last_run_ts":int(time.time()),
                            "tables":{}})
    http_ok=_start_metrics_server(GOLD_METRICS_PORT)

    gold_state=load_gold_state(); spark=create_spark()
    try:
        tables_to_process, tables_skipped = discover_silver_tables(gold_state, force_all)

        if target_table or target_db:
            tables_to_process=[t for t in tables_to_process
                               if (target_table is None or t["table_name"]==target_table)
                               and (target_db is None or t.get("db_name")==target_db)]
            if not tables_to_process:
                log.error("Aucune table trouvée (table=%s db=%s)",target_table,target_db); return

        if not tables_to_process:
            log.info("Aucune table à traiter (toutes les tables Silver sont déjà à jour).")
            log.info("  Utilisez --force-all pour retraiter malgré tout.")
            return

        with _RT_LOCK:
            _RT_METRICS["tables_pending"]=len(tables_to_process)
            _RT_METRICS["tables_skipped"]=len(tables_skipped)

        all_results=[]
        for i, t in enumerate(tables_to_process, 1):
            with _RT_LOCK:
                _RT_METRICS["tables_pending"]=len(tables_to_process)-i
                _RT_METRICS["tables_processed"]=i-1
            # Propager silver_parquet_path dans le résultat
            r=process_table(spark, t)
            r["silver_parquet_path"]=t.get("parquet_path","")
            r["silver_ts"]=t.get("silver_ts","")
            r["source_type"]=t.get("source_type","?")
            all_results.append(r)
            with _RT_LOCK: _RT_METRICS["tables_processed"]=i

        write_global_report(all_results, tables_skipped, gold_state)
        if not target_table: save_gold_state(gold_state)

        total_sec=round(time.time()-pipeline_start,2)
        with _RT_LOCK:
            _RT_METRICS["running"]=0; _RT_METRICS["duration"]=total_sec
        log.info("Temps total : %.1f s",total_sec)
        if http_ok:
            log.info("[Prometheus] Métriques finales disponibles → http://localhost:%d/metrics", GOLD_METRICS_PORT)

    finally:
        spark.stop()
        _stop_metrics_server()
        log.info("Spark arrêté.")


# ═════════════════════════════════════════════════════════════════════════
#  CLI
# ═════════════════════════════════════════════════════════════════════════

if __name__=="__main__":
    import argparse
    parser=argparse.ArgumentParser(
        description="Gold Pipeline v7 — LOCAL (incrémental + Prometheus temps réel)",
        epilog="""
Variables d'environnement :
  LOCAL_SILVER_ROOT      Dossier Silver
  LOCAL_GOLD_ROOT        Dossier Gold
  GROQ_API_KEY_1         Clé Groq principale
  PROMETHEUS_PUSHGATEWAY Adresse gateway (défaut: localhost:9091)
  GOLD_METRICS_PORT      Port HTTP métriques (défaut: 8000)

Exemples :
  python gold_pipeline_local_v7.py                   # incrémental
  python gold_pipeline_local_v7.py --force-all       # tout retraiter
  python gold_pipeline_local_v7.py --table clients_dirty
  python gold_pipeline_local_v7.py --list
  python gold_pipeline_local_v7.py --status          # état Gold persistant
  curl http://localhost:8000/metrics                 # métriques temps réel
        """)
    parser.add_argument("--table",     type=str,          default=None)
    parser.add_argument("--db",        type=str,          default=None)
    parser.add_argument("--force-all", action="store_true")
    parser.add_argument("--list",      action="store_true")
    parser.add_argument("--status",    action="store_true")
    args=parser.parse_args()

    if args.list:
        gs=load_gold_state(); sv=load_silver_state()
        tables,_=discover_silver_tables(gs)
        print(f"\nTables Silver à traiter ({len(tables)}) :")
        for t in sorted(tables,key=lambda x:(x.get("db_name",""),x["table_name"])):
            print(f"  [{t.get('_reason','?'):<16}]  {_gold_key(t):<50}  {t['parquet_path']}")

    elif args.status:
        gs=load_gold_state(); gt=gs.get("tables",{})
        print(f"\n{'='*75}")
        print(f"  ÉTAT GOLD PERSISTANT — {GOLD_STATE_FILE}")
        print(f"  Dernier run : {gs.get('last_run','jamais')}")
        print(f"{'='*75}")
        print(f"\n  {'CLÉ TABLE':<45}  {'GOLD_TS':<18}  {'VALIDES':>8}  {'QUAR.':>6}  {'VALID%':>7}")
        print(f"  {'-'*45}  {'-'*18}  {'-'*8}  {'-'*6}  {'-'*7}")
        for gk,info in sorted(gt.items()):
            print(f"  {gk:<45}  {info.get('gold_ts','?'):<18}  "
                  f"{info.get('row_count_valid',0):>8}  {info.get('row_count_quarantine',0):>6}  "
                  f"{info.get('validity_rate',0.0):>6.1f}%")
        if not gt: print("  (aucun output enregistré)")
        print(f"\n  Tables Gold enregistrées : {len(gt)}")
        print(f"{'='*75}\n")

    else:
        run_gold_pipeline(target_table=args.table, target_db=args.db, force_all=args.force_all)
