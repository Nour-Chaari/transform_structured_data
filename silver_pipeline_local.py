"""
=============================================================================
SILVER PIPELINE v11 — LOCAL  (Python 3.12 / venv compatible)
=============================================================================
NOUVEAUTÉS v11 vs v10 :

  [INC-S-1]  État Silver persistant enrichi : le fichier pipeline_state.json
             contient désormais deux sections distinctes :
               • "bronze_hashes"  : MD5 des fichiers Bronze (inchangé v10)
               • "silver_outputs" : pour chaque groupe traité avec succès,
                 on enregistre { "parquet_path", "ts", "row_count",
                 "bronze_hash", "processed_at" }
             → Gold peut lire cette section pour savoir quels runs Silver
               sont nouveaux sans rescanner tout l'arbre de fichiers.

  [INC-S-2]  Comportement Bronze inchangé : si les fichiers Bronze n'ont pas
             changé (même MD5), la table n'est PAS re-traitée ET les métriques
             précédentes sont conservées/loggées.

  [INC-S-3]  _emit_metrics enrichi : publie aussi en fichier .prom dans
             silver/metrics_export/ pour le monitoring temps réel.

  [INC-S-4]  write_silver_table_local : écriture Parquet via pyarrow direct
             (plus de Spark write → même correctif que Gold v6 FIX-v6-2).
             Spark conservé pour la lecture uniquement.

  [INC-S-5]  CLI --status : affiche l'état courant (tables traitées, hashes,
             derniers runs) sans relancer le pipeline.

Toutes les fonctionnalités v10 sont conservées.
=============================================================================
"""

import os
import io
import re
import json
import time
import hashlib
import logging
import tempfile
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from functools import reduce
from dotenv import load_dotenv; load_dotenv()

os.environ["PYSPARK_PYTHON"]        = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from groq import Groq
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lower, trim, when, lit, to_date, regexp_replace, explode
import pandas as pd
import numpy as np

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False

# ═════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═════════════════════════════════════════════════════════════════════════

LOCAL_BRONZE_ROOT = Path(os.environ.get("LOCAL_BRONZE_ROOT",
    r"C:\Users\chaar\OneDrive\Bureau\transform_structured_data\data\bronze"))
LOCAL_SILVER_ROOT = Path(os.environ.get("LOCAL_SILVER_ROOT",
    r"C:\Users\chaar\OneDrive\Bureau\transform_structured_data\data\silver"))
LOCAL_STATE_FILE  = LOCAL_SILVER_ROOT / "pipeline_state" / "pipeline_state.json"
LOCAL_LOGS_DIR    = LOCAL_SILVER_ROOT / "logs"
LOCAL_METRICS_DIR = LOCAL_SILVER_ROOT / "metrics_export"


GROQ_API_KEYS = [
    os.environ.get("GROQ_API_KEY_1"),
    os.environ.get("GROQ_API_KEY_2"),
    os.environ.get("GROQ_API_KEY_3"),
]
GROQ_MODEL           = "llama-3.3-70b-versatile"
SUPPORTED_FORMATS    = {".json", ".csv", ".xml"}
UNIX_TS_MS_THRESHOLD = 1e10
MIN_VALID_YEAR       = 1900

NULL_VALUES = {
    "", " ", "null", "none", "nan", "n/a", "na", "nil", "undefined",
    "unknown", "inconnue", "inconnu", "missing", "#n/a", "#na",
    "not available", "not applicable", "nd", "nr", "-", "--", "?",
}

NEVER_UNIX_TS_KEYWORDS = [
    "price", "change", "percent", "volume", "qty", "quantity",
    "amount", "balance", "fee", "rate", "ratio", "weight",
    "avg", "average", "sum", "total", "count", "num",
]

GENDER_COLUMN_KEYWORDS = ["gender", "sex"]

DATE_COLUMN_KEYWORDS = [
    "date", "dob", "birth", "created", "updated",
    "opened", "closed", "expire", "start", "end", "on", "at",
]

PRIORITY_DATE_FORMATS = [
    "yyyy-MM-dd", "dd/MM/yyyy", "dd/MM/yy",
    "dd-MM-yyyy", "MM/dd/yyyy", "yyyy",
]

NO_DEDUP_SOURCE_TYPES = {"api"}

META_COLS = {
    "_source_type", "_source_name", "_table_name",
    "_processed_at", "_db_name",
}

CANONICAL_MAPPING = {
    "customerid":             "customer_id",
    "cust_id":                "customer_id",
    "transactionid":          "transaction_id",
    "transactionamount__inr": "transaction_amount",
    "transaction_amount_inr": "transaction_amount",
    "transactiondate":        "transaction_date",
    "custgender":             "gender",
    "cust_gender":            "gender",
    "custlocation":           "location",
    "cust_location":          "location",
    "custaccountbalance":     "account_balance",
    "cust_account_balance":   "account_balance",
    "customerdob":            "date_of_birth",
    "cust_dob":               "date_of_birth",
    "dob":                    "date_of_birth",
}

TUNISIAN_MOBILE_PREFIXES = {"2", "3", "4", "5", "7", "9"}
TUNISIAN_PHONE_LENGTH    = 8

# ═════════════════════════════════════════════════════════════════════════
#  LOGGING PERSISTANT
# ═════════════════════════════════════════════════════════════════════════

_RUN_TS = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

def _setup_logging() -> logging.Logger:
    LOCAL_LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOCAL_LOGS_DIR / f"silver_pipeline_{_RUN_TS}.log"
    fmt = logging.Formatter(
        fmt="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger("silver_pipeline_v11_local")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.info("Silver logs → %s", log_file)
    return logger

log = _setup_logging()


# ═════════════════════════════════════════════════════════════════════════
#  GROQ CLIENT
# ═════════════════════════════════════════════════════════════════════════

class GroqRotator:
    def __init__(self, keys: List[str]):
        self.clients  = [Groq(api_key=k) for k in keys if k and len(k) > 10]
        self._current = 0
        if not self.clients:
            log.warning("Aucune clé Groq valide.")

    def _next(self):
        self._current = (self._current + 1) % max(len(self.clients), 1)

    def complete(self, system: str, user: str, max_tokens: int = 800) -> str:
        if not self.clients:
            return ""
        for _ in range(len(self.clients) * 2):
            try:
                resp = self.clients[self._current].chat.completions.create(
                    model=GROQ_MODEL,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user",   "content": user},
                    ],
                    max_tokens=max_tokens,
                    temperature=0.0,
                )
                return resp.choices[0].message.content.strip()
            except Exception as exc:
                err = str(exc).lower()
                if "rate" in err or "429" in err or "quota" in err:
                    log.warning("Groq rate-limit clé %d. Rotation...", self._current)
                    self._next()
                    time.sleep(2)
                else:
                    log.error("Groq error: %s", exc)
                    return ""
        return ""


GROQ = GroqRotator(GROQ_API_KEYS)


def _parse_json_from_groq(raw: str) -> dict:
    raw = re.sub(r"```json\s*", "", raw)
    raw = re.sub(r"```\s*",     "", raw).strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(0))
            except Exception:
                pass
    return {}


# ═════════════════════════════════════════════════════════════════════════
#  GESTION FICHIERS LOCAUX
# ═════════════════════════════════════════════════════════════════════════

def list_local_files(root: Path, prefix: str = "") -> List[Path]:
    base = root / prefix if prefix else root
    if not base.exists():
        return []
    return [p for p in base.rglob("*") if p.is_file()]

def read_local_bytes(root: Path, rel_path: str) -> bytes:
    return (root / rel_path).read_bytes()

def write_local_bytes(root: Path, rel_path: str, data: bytes,
                      create_parents: bool = True):
    target = root / rel_path
    if create_parents:
        target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)

def ensure_local_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


# ═════════════════════════════════════════════════════════════════════════
#  [INC-S-1] ÉTAT PERSISTANT — BRONZE + SILVER
# ═════════════════════════════════════════════════════════════════════════

def _md5(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def load_state() -> dict:
    """
    Charge l'état persistant depuis pipeline_state.json.
    Structure v11 :
    {
      "last_run":      "ISO datetime",
      "bronze_hashes": { "<group_key>": "<md5>" },
      "silver_outputs": {
        "<group_key>": {
          "parquet_path": "...",
          "ts":           "20260413_123456",
          "row_count":    10000,
          "bronze_hash":  "<md5>",
          "processed_at": "ISO datetime"
        }
      }
    }
    """
    try:
        if LOCAL_STATE_FILE.exists():
            data = json.loads(LOCAL_STATE_FILE.read_text(encoding="utf-8"))
            # Compatibilité v10 : renommer "file_hashes" → "bronze_hashes"
            if "file_hashes" in data and "bronze_hashes" not in data:
                data["bronze_hashes"] = data.pop("file_hashes")
            data.setdefault("silver_outputs", {})
            return data
    except Exception as e:
        log.warning("[State] Erreur lecture état : %s → état vide", e)
    return {"bronze_hashes": {}, "silver_outputs": {}, "last_run": None}


def save_state(state: dict):
    state["last_run"] = datetime.utcnow().isoformat()
    LOCAL_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    LOCAL_STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False),
                                 encoding="utf-8")
    log.info("[State] État sauvegardé → %s", LOCAL_STATE_FILE)


def group_hash(sources: List[dict]) -> str:
    parts = ""
    for s in sorted(sources, key=lambda x: x["key"]):
        try:
            data = (LOCAL_BRONZE_ROOT / s["key"]).read_bytes()
            parts += f"{s['key']}:{_md5(data)}:{len(data)};"
        except Exception:
            parts += f"{s['key']}:ERROR;"
    return _md5(parts.encode())


def detect_changes(groups: dict, prev_state: dict) -> Tuple[dict, dict]:
    """
    [INC-S-2] Compare les MD5 Bronze actuels avec l'état précédent.
    Retourne (groupes_modifiés, nouveaux_hashes).
    Les tables inchangées sont loggées avec leurs métriques précédentes.
    """
    prev_hashes = prev_state.get("bronze_hashes", {})
    prev_silver = prev_state.get("silver_outputs", {})
    new_hashes: dict  = {}
    changed: dict     = {}

    for gk, sources in groups.items():
        h = group_hash(sources)
        new_hashes[gk] = h
        if gk not in prev_hashes or prev_hashes[gk] != h:
            status = "[NOUVEAU]" if gk not in prev_hashes else "[MODIFIÉ]"
            log.info("  %s %s", status, gk)
            changed[gk] = sources
        else:
            # [INC-S-2] Fichier Bronze inchangé → logguer les métriques précédentes
            prev_out = prev_silver.get(gk, {})
            if prev_out:
                log.info("  [INCHANGÉ] %s → Silver existant : %s (%d lignes, run=%s)",
                         gk,
                         prev_out.get("parquet_path", "?"),
                         prev_out.get("row_count", 0),
                         prev_out.get("ts", "?"))
            else:
                log.info("  [INCHANGÉ] %s (pas encore de Silver enregistré)", gk)

    removed = set(prev_hashes.keys()) - set(new_hashes.keys())
    for r in removed:
        log.info("  [SUPPRIMÉ] %s", r)

    log.info("Changements : %d à traiter / %d inchangés / %d supprimés",
             len(changed), len(groups) - len(changed), len(removed))
    return changed, new_hashes


# ═════════════════════════════════════════════════════════════════════════
#  SPARK SESSION
# ═════════════════════════════════════════════════════════════════════════

def create_spark() -> SparkSession:
    try:
        existing = SparkSession.getActiveSession()
        if existing:
            existing.stop()
            time.sleep(1)
    except Exception:
        pass

    spark = (
        SparkSession.builder
        .appName("silver_pipeline_v11_local")
        .master("local[*]")
        .config("spark.driver.memory", "6g")
        .config("spark.executor.memory", "6g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.sql.codegen.wholeStage", "false")
        .config("spark.python.worker.reuse", "true")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.legacy.timeParserPolicy", "EXCEPTION")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.warehouse.dir",
                str(Path(tempfile.gettempdir()) / "spark-warehouse-silver11-local"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark créé : %s", spark.version)
    return spark


# ═════════════════════════════════════════════════════════════════════════
#  DÉCOUVERTE DES SOURCES LOCALES
# ═════════════════════════════════════════════════════════════════════════

def classify_source_local(abs_path: Path) -> Optional[dict]:
    try:
        rel = abs_path.relative_to(LOCAL_BRONZE_ROOT)
    except ValueError:
        return None
    parts = list(rel.parts)
    if not parts:
        return None
    ext = abs_path.suffix.lower()

    if parts[0] == "source_file":
        if ext == ".csv":
            table_name = abs_path.stem
            return {"source_type": "source_file",
                    "source_name": f"source_file/{table_name}",
                    "table_name":  table_name, "key": str(rel), "ext": ext}
        elif len(parts) >= 3 and parts[1] == "other" and ext in SUPPORTED_FORMATS:
            return {"source_type": "source_file",
                    "source_name": f"source_file/{abs_path.stem}",
                    "table_name":  abs_path.stem, "key": str(rel), "ext": ext}
        return None

    if parts[0] == "transactions-bancaires":
        if len(parts) >= 3 and ext == ".json":
            return {"source_type": "db_rows",
                    "source_name": f"transactions-bancaires/{parts[1]}",
                    "db_name":     "transactions-bancaires",
                    "table_name":  parts[1], "key": str(rel), "ext": ext}
        return None

    if len(parts) >= 4 and parts[1] == "public" and ext == ".json":
        return {"source_type": "db_wrapped",
                "source_name": f"{parts[0]}/{parts[2]}",
                "db_name":     parts[0],
                "table_name":  parts[2], "key": str(rel), "ext": ext}

    if ext == ".json":
        return {"source_type": "api", "source_name": parts[0],
                "table_name":  parts[0], "key": str(rel), "ext": ext}
    return None


def discover_sources_local() -> Dict[str, List[dict]]:
    groups: Dict[str, List[dict]] = {}
    if not LOCAL_BRONZE_ROOT.exists():
        log.error("Dossier Bronze introuvable : %s", LOCAL_BRONZE_ROOT)
        return groups
    for abs_path in LOCAL_BRONZE_ROOT.rglob("*"):
        if not abs_path.is_file():
            continue
        info = classify_source_local(abs_path)
        if not info:
            continue
        db = info.get("db_name", "")
        gk = (f"{info['source_type']}__{db}__{info['table_name']}" if db
              else f"{info['source_type']}__{info['table_name']}")
        groups.setdefault(gk, []).append(info)
    log.info("Sources découvertes : %d groupes, %d fichiers",
             len(groups), sum(len(v) for v in groups.values()))
    return groups


# ═════════════════════════════════════════════════════════════════════════
#  ANALYSE STRUCTURELLE
# ═════════════════════════════════════════════════════════════════════════

def _infer_column_profiles(pdf: pd.DataFrame) -> dict:
    profiles = {}
    n = max(len(pdf), 1)
    for c in pdf.columns:
        series   = pdf[c]
        nn       = series.dropna()
        null_rt  = round(100 * (1 - len(nn) / n), 1)
        sample   = nn.head(5).tolist() if len(nn) > 0 else []
        profile: dict = {"null_rate_pct": null_rt, "sample": sample}
        if len(nn) > 0:
            str_vals     = nn.astype(str).str.strip()
            numeric_s    = pd.to_numeric(
                str_vals.str.replace(r"[,\s$€£¥]", "", regex=True), errors="coerce"
            )
            numeric_rate = numeric_s.notna().mean()
            if numeric_rate > 0.8:
                vals = numeric_s.dropna()
                profile["detected_as"]    = "numeric"
                profile["min"]            = float(vals.min())
                profile["max"]            = float(vals.max())
                profile["mean"]           = round(float(vals.mean()), 4)
                profile["likely_unix_ts"] = (
                    bool(vals.min() > 1_000_000_000) and not _is_financial_column(c)
                )
            else:
                date_rate = pd.to_datetime(nn, errors="coerce").notna().mean()
                if date_rate > 0.6 or _is_date_like_column(c, ""):
                    profile["detected_as"] = "date"
                else:
                    unique_rate = len(nn.unique()) / len(nn)
                    profile["detected_as"]     = "string"
                    profile["unique_rate"]     = round(unique_rate, 3)
                    profile["max_len"]         = int(str_vals.str.len().max())
                    if unique_rate < 0.1 and len(nn.unique()) <= 20:
                        profile["likely_categorical"] = True
                        profile["categories"]         = nn.unique().tolist()[:10]
        profiles[c] = profile
    return profiles


def _detect_domain(table_name: str, columns: List[str]) -> str:
    cols_str = " ".join(columns).lower()
    name     = table_name.lower()
    if any(k in cols_str for k in ["transaction", "amount", "balance", "iban", "account"]):
        return "banking_transaction"
    if any(k in cols_str for k in ["customer", "client", "dob", "gender", "phone"]):
        return "customer_profile"
    if any(k in cols_str for k in ["price", "volume", "open", "close", "symbol"]):
        return "market_data"
    if any(k in cols_str for k in ["credit", "loan", "rate", "duration"]):
        return "credit"
    if any(k in cols_str for k in ["agence", "agency", "branch", "capacity"]):
        return "branch"
    if any(k in name for k in ["log", "event", "audit", "track"]):
        return "event_log"
    return "generic"


# ═════════════════════════════════════════════════════════════════════════
#  GROQ — PROMPT ADAPTATIF
# ═════════════════════════════════════════════════════════════════════════

_SYSTEM_BASE = """\
Tu es un expert senior Data Engineering spécialisé en nettoyage de données \
bancaires islamiques. Analyse les profils de colonnes fournis et génère des \
règles précises de nettoyage. Structure inconnue — inférer dynamiquement.

RÈGLES ABSOLUES DE TYPAGE :
1. NUMÉRIQUES : strings numériques → double. IDs alphanumériques → string.
   Colonnes price/change/percent/volume/qty/quantity/amount/balance/fee/rate/
   ratio → TOUJOURS double, JAMAIS unix_timestamp.
2. TIMESTAMPS UNIX : UNIQUEMENT colonnes likely_unix_ts=true ET nom contient
   time/timestamp/opentime/closetime. NE JAMAIS classer financier comme unix_ts.
3. DATES : colonne detected_as=date OU nom contient date/_at/_on/start/end/
   dob/birth/created/updated/opened/closed/expire → expected_type=date.
   Max 6 formats, inclure yyyy-MM-dd,dd/MM/yyyy,dd/MM/yy,dd-MM-yyyy.
4. CATÉGORIELLES : likely_categorical=true → valid_values. gender/sex → string
   + normalize_case=lower + valid_values=null.
5. snake_case : priceChange → price_change.
6. NULLS : fill_null_with uniquement pour numériques (0 ou médiane).
7. is_critical=true UNIQUEMENT pour la clé primaire unique.
ISLAMIQUE : taux_interet/interest_rate = 0 pour murabaha/ijara/istisna/salam.
Réponds en moins de 800 tokens.\
"""

_DOMAIN_HINTS = {
    "banking_transaction": "Transactionnel islamique : ID transaction critique, dates obligatoires, amount=double positif, statut→valid_values, credit_type→produit islamique.",
    "customer_profile":    "Profil client : customer_id critique, dob=date, gender=lower, phone=string (pas numérique), email=lower.",
    "market_data":         "Marché (sukuk) : open/close time→unix_timestamp, OHLC/price→double, volume→double JAMAIS unix_ts.",
    "credit":              "Crédit islamique : interest_rate=0, duration_months=integer, start/end=date, amount=double positif.",
    "generic":             "Générique : colonnes ID critiques, dates par nom, numériques par valeurs.",
}

_JSON_SCHEMA = """
Réponds UNIQUEMENT en JSON valide (pas de commentaires, pas de markdown) :
{"table_purpose":"...","detected_domain":"banking_transaction|customer_profile|market_data|credit|branch|event_log|generic","has_phone_column":true,"phone_column_names":[],"has_unix_timestamps":false,"unix_timestamp_columns":[],"column_renames":{},"columns":[{"name":"","original_name":"","expected_type":"string|integer|long|double|boolean|date|unix_timestamp","is_id":false,"is_critical":false,"nullable":true,"normalize_case":"none|lower|upper|title","date_formats_to_try":null,"valid_values":null,"min_value":null,"max_value":null,"fill_null_with":null}],"critical_columns":[],"drop_duplicates_on":null,"anomalies_detected":[]}
"""

def _build_groq_system(domain: str) -> str:
    hint = _DOMAIN_HINTS.get(domain, _DOMAIN_HINTS["generic"])
    return _SYSTEM_BASE + "\n" + hint + "\n" + _JSON_SCHEMA


def analyze_with_groq(source_info: dict, sample_rows: List[dict],
                      columns: List[str], col_profiles: dict = None) -> dict:
    if not GROQ.clients:
        return {}
    domain        = _detect_domain(source_info.get("table_name", ""), columns)
    system_prompt = _build_groq_system(domain)
    sample_str    = json.dumps(sample_rows[:15], ensure_ascii=False, default=str)
    profiles_str  = json.dumps(col_profiles or {}, ensure_ascii=False, default=str)
    user_msg = (
        f"Source:{source_info.get('source_name')} Type:{source_info.get('source_type')} "
        f"Table:{source_info.get('table_name')} Domaine:{domain}\n"
        f"Colonnes:{','.join(columns)}\n"
        f"Profils:{profiles_str}\n"
        f"Échantillon(15 lignes):{sample_str}\n"
        "INSTRUCTIONS: 1)Profils→type. 2)likely_unix_ts=true+non-financier→unix_timestamp. "
        "3)detected_as=date→date obligatoire. 4)likely_categorical→valid_values. "
        "5)null_rate>50%→nullable=true. 6)Tous les noms en snake_case."
    )
    raw   = GROQ.complete(system_prompt, user_msg, max_tokens=800)
    rules = _parse_json_from_groq(raw)
    if rules:
        rules = _postprocess_groq_rules(rules, source_info)
        log.info("[Groq/%s] %s — %d colonnes, domaine=%s",
                 domain, source_info.get("table_name"),
                 len(rules.get("columns", [])), rules.get("detected_domain", domain))
    return rules


def _postprocess_groq_rules(rules: dict, source_info: dict) -> dict:
    orig_unix  = rules.get("unix_timestamp_columns", [])
    clean_unix = [c for c in orig_unix if not _is_financial_column(c)]
    if len(clean_unix) != len(orig_unix):
        log.warning("[FIX-A] Colonnes retirées de unix_timestamp_columns : %s",
                    set(orig_unix) - set(clean_unix))
    rules["unix_timestamp_columns"] = clean_unix
    for col_rule in rules.get("columns", []):
        col_name = col_rule.get("name", "") or col_rule.get("original_name", "")
        if col_rule.get("expected_type") == "unix_timestamp" and _is_financial_column(col_name):
            log.warning("[FIX-A] %s : unix_timestamp → double", col_name)
            col_rule["expected_type"] = "double"
        if _is_gender_column(col_name):
            col_rule["expected_type"]  = "string"
            col_rule["normalize_case"] = "lower"
            col_rule["valid_values"]   = None
    return rules


# ═════════════════════════════════════════════════════════════════════════
#  HELPERS
# ═════════════════════════════════════════════════════════════════════════

def _is_financial_column(col_name: str) -> bool:
    return any(kw in col_name.lower() for kw in NEVER_UNIX_TS_KEYWORDS)

def _is_gender_column(col_name: str) -> bool:
    return any(kw in col_name.lower() for kw in GENDER_COLUMN_KEYWORDS)

def _is_date_like_column(col_name: str, exp_type: str) -> bool:
    if exp_type == "date":
        return True
    if _is_gender_column(col_name):
        return False
    lower_name    = col_name.lower()
    EXACT_KWS     = {"end", "start", "on", "at"}
    SUBSTRING_KWS = {"date","dob","birth","created","updated","opened","closed","expire"}
    if any(kw in lower_name for kw in SUBSTRING_KWS):
        return True
    return any(re.search(r'(^|_)' + re.escape(kw) + r'(_|$)', lower_name) for kw in EXACT_KWS)

def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2, default=str)


# ═════════════════════════════════════════════════════════════════════════
#  [INC-S-3] MÉTRIQUES PROMETHEUS — FICHIER .prom TEMPS RÉEL
# ═════════════════════════════════════════════════════════════════════════

PROMETHEUS_PUSHGATEWAY = os.environ.get("PROMETHEUS_PUSHGATEWAY", "localhost:9091")

try:
    from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False


def _write_silver_prom(group_key: str, table_name: str, source_type: str,
                       rows_input: int, rows_output: int, rows_quarantined: int,
                       duration: float, success: bool):
    """[INC-S-3] Écrit un fichier .prom dans silver/metrics_export/ pour scraping temps réel."""
    LOCAL_METRICS_DIR.mkdir(parents=True, exist_ok=True)
    safe_table = re.sub(r"[^a-zA-Z0-9_]", "_", table_name)
    ls = f'table="{table_name}",source_type="{source_type}",pipeline="silver"'
    lines = [
        f'# HELP silver_rows_input_total Lignes lues depuis Bronze',
        f'# TYPE silver_rows_input_total gauge',
        f'silver_rows_input_total{{{ls}}} {rows_input}',
        f'# HELP silver_rows_output_total Lignes valides écrites dans Silver',
        f'# TYPE silver_rows_output_total gauge',
        f'silver_rows_output_total{{{ls}}} {rows_output}',
        f'# HELP silver_rows_quarantined_total Lignes en quarantaine',
        f'# TYPE silver_rows_quarantined_total gauge',
        f'silver_rows_quarantined_total{{{ls}}} {rows_quarantined}',
        f'# HELP silver_processing_duration_seconds Durée de traitement',
        f'# TYPE silver_processing_duration_seconds gauge',
        f'silver_processing_duration_seconds{{{ls}}} {round(duration, 3)}',
        f'# HELP silver_success Succès du traitement (1=ok, 0=erreur)',
        f'# TYPE silver_success gauge',
        f'silver_success{{{ls}}} {1 if success else 0}',
        f'# HELP silver_last_run_timestamp_seconds Timestamp Unix du dernier run',
        f'# TYPE silver_last_run_timestamp_seconds gauge',
        f'silver_last_run_timestamp_seconds{{{ls}}} {int(time.time())}',
    ]
    prom_file = LOCAL_METRICS_DIR / f"{safe_table}.prom"
    prom_file.write_text("\n".join(lines), encoding="utf-8")
    log.info("  [Prometheus] .prom mis à jour → %s", prom_file)

    if PROMETHEUS_AVAILABLE:
        try:
            registry = CollectorRegistry()
            labels_keys = ["table", "source_type", "pipeline"]
            labels_vals = [table_name, source_type, "silver"]
            def gauge(name: str, desc: str, val: float):
                g = Gauge(name, desc, labels_keys, registry=registry)
                g.labels(*labels_vals).set(val)
            gauge("silver_rows_input_total",            "Lignes Bronze",      rows_input)
            gauge("silver_rows_output_total",           "Lignes Silver",      rows_output)
            gauge("silver_rows_quarantined_total",      "Lignes quarantaine", rows_quarantined)
            gauge("silver_processing_duration_seconds", "Durée (s)",         duration)
            gauge("silver_success",                     "Succès",            1 if success else 0)
            push_to_gateway(PROMETHEUS_PUSHGATEWAY, job="silver_pipeline_v11",
                            grouping_key={"table": table_name}, registry=registry)
            log.info("  [Prometheus] Push gateway OK (%s)", PROMETHEUS_PUSHGATEWAY)
        except Exception as e:
            log.info("  [Prometheus] Gateway inaccessible → fichier .prom uniquement")


def _emit_metrics(group_key: str, table_name: str, src: str,
                  pdf: pd.DataFrame, quarantine_dict: dict,
                  start_time: float, success: bool, error: str = ""):
    duration          = time.time() - start_time
    quarantined_total = sum(len(v) for v in quarantine_dict.values())
    rows_input  = len(pdf) + quarantined_total if success else 0
    rows_output = len(pdf) if success else 0

    metrics = {
        "event":            "silver_pipeline_metrics",
        "timestamp":        datetime.utcnow().isoformat(),
        "group_key":        group_key,
        "table_name":       table_name,
        "source_type":      src,
        "rows_input":       rows_input,
        "rows_output":      rows_output,
        "rows_quarantined": quarantined_total,
        "duration_seconds": round(duration, 3),
        "success":          success,
        "python_version":   sys.version.split()[0],
    }
    if error:
        metrics["error"] = error
    log.info("METRICS: %s", json.dumps(metrics))

    # [INC-S-3] Écriture .prom temps réel
    _write_silver_prom(group_key, table_name, src,
                       rows_input, rows_output, quarantined_total,
                       duration, success)


# ═════════════════════════════════════════════════════════════════════════
#  NETTOYAGE — FONCTIONS (inchangées vs v10, copiées intégralement)
# ═════════════════════════════════════════════════════════════════════════

def normalize_column_names(df: DataFrame) -> DataFrame:
    def to_snake(name: str) -> str:
        name = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
        name = re.sub(r'([a-z\d])([A-Z])', r'\1_\2', name)
        return re.sub(r'[\s\-\.]+', '_', name).lower().strip("_")
    renames = {c: to_snake(c) for c in df.columns if to_snake(c) != c}
    for old, new in renames.items():
        df = df.withColumnRenamed(old, new)
    return df


def apply_canonical_renames_spark(df: DataFrame):
    applied = {}
    for old_name, new_name in CANONICAL_MAPPING.items():
        if old_name in df.columns and new_name not in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
            applied[old_name] = new_name
    return df, applied


def apply_canonical_renames_pandas(pdf: pd.DataFrame):
    applied = {}
    cols_lower = {c.lower(): c for c in pdf.columns}
    for old_name_low, new_name in CANONICAL_MAPPING.items():
        orig_col = cols_lower.get(old_name_low)
        if orig_col and new_name not in pdf.columns:
            pdf = pdf.rename(columns={orig_col: new_name})
            applied[orig_col] = new_name
    return pdf, applied


def apply_column_renames(df: DataFrame, renames: dict):
    applied = {}
    for old_name, new_name in renames.items():
        if old_name in df.columns and new_name not in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
            applied[old_name] = new_name
    return df, applied


def apply_column_renames_pandas(pdf: pd.DataFrame, renames: dict):
    applied = {}
    for old_name, new_name in renames.items():
        if old_name in pdf.columns and new_name not in pdf.columns:
            pdf = pdf.rename(columns={old_name: new_name})
            applied[old_name] = new_name
    return pdf, applied


def standardize_nulls_spark(df: DataFrame) -> DataFrame:
    null_set = set(NULL_VALUES)
    conditions = reduce(
        lambda acc, c: acc.when(lower(trim(col(f"`{c}`"))).isin(null_set), None)
                          .otherwise(col(f"`{c}`")),
        df.columns,
        when(lit(False), None)
    )
    for c in df.columns:
        df = df.withColumn(
            c,
            when(lower(trim(col(f"`{c}`"))).isin(null_set), None).otherwise(col(f"`{c}`"))
        )
    return df


def standardize_nulls_pandas(pdf: pd.DataFrame) -> pd.DataFrame:
    null_set = set(NULL_VALUES)
    str_df   = pdf.astype(str).apply(lambda s: s.str.strip().str.lower())
    mask     = str_df.isin(null_set)
    pdf[mask] = None
    return pdf


def _parse_date_pandas(series: pd.Series, formats: List[str]) -> pd.Series:
    null_mask = series.isna() | series.astype(str).str.strip().str.lower().isin(
        {"none", "nat", "nan", "null", ""}
    )
    result = pd.Series([None] * len(series), index=series.index, dtype=object)

    for fmt in formats:
        py_fmt = (fmt.replace("yyyy", "%Y").replace("MM", "%m")
                     .replace("dd", "%d").replace("HH", "%H")
                     .replace("mm", "%M").replace("ss", "%S"))
        remaining = result.isna() & ~null_mask
        if not remaining.any():
            break
        parsed = pd.to_datetime(
            series[remaining].astype(str).str.strip(),
            format=py_fmt, errors="coerce"
        )
        result[remaining] = parsed.dt.strftime("%Y-%m-%d").where(parsed.notna(), None)

    remaining = result.isna() & ~null_mask
    if remaining.any():
        parsed_flex = pd.to_datetime(
            series[remaining].astype(str).str.strip(), errors="coerce"
        )
        result[remaining] = parsed_flex.dt.strftime("%Y-%m-%d").where(parsed_flex.notna(), None)
    return result


def apply_cleaning_spark(df: DataFrame, rules: dict,
                         skip_cols: set = None, applied_renames: dict = None) -> DataFrame:
    skip_cols       = skip_cols       or set()
    applied_renames = applied_renames or {}
    if not rules or "columns" not in rules:
        return df

    unix_ts_cols = set(rules.get("unix_timestamp_columns", []))
    for col_rule in rules.get("columns", []):
        col_name = col_rule.get("name") or col_rule.get("original_name", "")
        if not col_name or col_name not in df.columns or col_name in skip_cols:
            continue

        exp_type     = col_rule.get("expected_type", "string")
        norm_case    = col_rule.get("normalize_case", "none")
        valid_values = col_rule.get("valid_values")
        min_val      = col_rule.get("min_value")
        max_val      = col_rule.get("max_value")
        fill_null    = col_rule.get("fill_null_with")
        date_fmts    = col_rule.get("date_formats_to_try") or PRIORITY_DATE_FORMATS

        c_expr = col(f"`{col_name}`")

        if exp_type in ("integer", "long", "double") and not _is_financial_column(col_name):
            cleaned = regexp_replace(c_expr.cast("string"), r"[,\s$€£¥]", "")
            if exp_type == "integer":     c_expr = cleaned.cast(T.IntegerType())
            elif exp_type == "long":      c_expr = cleaned.cast(T.LongType())
            elif exp_type == "double":    c_expr = cleaned.cast(T.DoubleType())
        elif exp_type in ("integer", "long", "double") and _is_financial_column(col_name):
            cleaned = regexp_replace(c_expr.cast("string"), r"[,\s$€£¥]", "")
            c_expr  = cleaned.cast(T.DoubleType())

        if exp_type == "date" and _is_date_like_column(col_name, exp_type):
            parsed = None
            for fmt in date_fmts:
                attempt = to_date(c_expr, fmt)
                parsed  = attempt if parsed is None else when(parsed.isNull(), attempt).otherwise(parsed)
            if parsed is not None:
                c_expr = parsed.cast("string")

        if exp_type == "unix_timestamp" and col_name in unix_ts_cols:
            numeric_col = regexp_replace(c_expr.cast("string"), r"[,\s]", "").cast(T.DoubleType())
            ts_seconds  = when(numeric_col > UNIX_TS_MS_THRESHOLD, numeric_col / 1000).otherwise(numeric_col)
            c_expr = F.from_unixtime(ts_seconds, "yyyy-MM-dd HH:mm:ss")

        if norm_case == "lower":     c_expr = lower(trim(c_expr.cast("string")))
        elif norm_case == "upper":   c_expr = F.upper(trim(c_expr.cast("string")))
        elif norm_case == "title":   c_expr = F.initcap(trim(c_expr.cast("string")))

        df = df.withColumn(col_name, c_expr)

        if fill_null is not None:
            df = df.withColumn(col_name, when(col(col_name).isNull(), lit(fill_null)).otherwise(col(col_name)))

    return df


def apply_cleaning_pandas(pdf: pd.DataFrame, rules: dict,
                          skip_cols: set = None, applied_renames: dict = None) -> pd.DataFrame:
    skip_cols = skip_cols or set()
    if not rules or "columns" not in rules:
        return pdf

    for col_rule in rules.get("columns", []):
        col_name = col_rule.get("name") or col_rule.get("original_name", "")
        if not col_name or col_name not in pdf.columns or col_name in skip_cols:
            continue

        exp_type  = col_rule.get("expected_type", "string")
        norm_case = col_rule.get("normalize_case", "none")
        date_fmts = col_rule.get("date_formats_to_try") or PRIORITY_DATE_FORMATS
        fill_null = col_rule.get("fill_null_with")

        if exp_type in ("integer", "long", "double"):
            cleaned = (pdf[col_name].astype(str)
                       .str.replace(r"[,\s$€£¥]", "", regex=True)
                       .str.strip())
            numeric = pd.to_numeric(cleaned, errors="coerce")
            if exp_type == "integer": numeric = numeric.astype("Int64")
            pdf[col_name] = numeric

        if exp_type == "date" and _is_date_like_column(col_name, exp_type):
            pdf[col_name] = _parse_date_pandas(pdf[col_name], date_fmts)

        if exp_type == "unix_timestamp":
            numeric = pd.to_numeric(pdf[col_name], errors="coerce")
            ts_sec  = numeric.where(numeric <= UNIX_TS_MS_THRESHOLD, numeric / 1000)
            pdf[col_name] = pd.to_datetime(ts_sec, unit="s", errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

        if norm_case == "lower":
            pdf[col_name] = pdf[col_name].astype(str).str.lower().str.strip().where(pdf[col_name].notna(), None)
        elif norm_case == "upper":
            pdf[col_name] = pdf[col_name].astype(str).str.upper().str.strip().where(pdf[col_name].notna(), None)
        elif norm_case == "title":
            pdf[col_name] = pdf[col_name].astype(str).str.title().str.strip().where(pdf[col_name].notna(), None)

        if fill_null is not None:
            pdf[col_name] = pdf[col_name].fillna(fill_null)

    return pdf


def process_pandas_cleaning(pdf: pd.DataFrame, phone_cols: List[str],
                             unix_ts_cols: List[str]) -> Tuple[pd.DataFrame, dict]:
    quarantine_dict: dict = {}

    def _clean_phone(v):
        if v is None or (isinstance(v, float) and pd.isna(v)): return v
        digits = re.sub(r"[\s\-\.\(\)\/]", "", str(v))
        if digits.startswith("+216"):   digits = digits[4:]
        elif digits.startswith("00216"): digits = digits[5:]
        elif digits.startswith("216") and len(digits) == 11: digits = digits[3:]
        if digits.startswith("0") and len(digits) == 9: digits = digits[1:]
        return digits if (digits.isdigit() and len(digits) == TUNISIAN_PHONE_LENGTH
                          and digits[0] in TUNISIAN_MOBILE_PREFIXES) else None

    for pc in phone_cols:
        if pc in pdf.columns:
            cleaned = pdf[pc].apply(_clean_phone)
            bad     = pdf[cleaned.isna() & pdf[pc].notna()]
            if not bad.empty:
                quarantine_dict.setdefault("invalid_phone", []).extend(
                    bad.to_dict(orient="records"))
            pdf[pc] = cleaned

    for uc in unix_ts_cols:
        if uc in pdf.columns:
            numeric = pd.to_numeric(pdf[uc], errors="coerce")
            ts_sec  = numeric.where(numeric <= UNIX_TS_MS_THRESHOLD, numeric / 1000)
            pdf[uc] = pd.to_datetime(ts_sec, unit="s", errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

    return pdf, quarantine_dict


def deduplicate_pandas(pdf: pd.DataFrame, dup_cols: Optional[List[str]],
                       source_type: str = "") -> Tuple[pd.DataFrame, List[dict]]:
    if source_type in NO_DEDUP_SOURCE_TYPES:
        return pdf, []
    if dup_cols:
        valid_cols = [c for c in dup_cols if c in pdf.columns]
        if valid_cols:
            before = len(pdf)
            dups   = pdf[pdf.duplicated(subset=valid_cols, keep="first")]
            dup_rows = [r for r in dups.to_dict(orient="records")]
            pdf    = pdf.drop_duplicates(subset=valid_cols, keep="first")
            if before - len(pdf) > 0:
                log.info("  Dédupliqué sur %s : %d doublons", valid_cols, before - len(pdf))
            return pdf, dup_rows
    before = len(pdf)
    meta   = [c for c in META_COLS if c in pdf.columns]
    dedup_cols = [c for c in pdf.columns if c not in meta]
    if dedup_cols:
        dups     = pdf[pdf.duplicated(subset=dedup_cols, keep="first")]
        dup_rows = dups.to_dict(orient="records")
        pdf      = pdf.drop_duplicates(subset=dedup_cols, keep="first")
        if before - len(pdf) > 0:
            log.info("  Dédupliqué (toutes colonnes hors meta) : %d doublons", before - len(pdf))
        return pdf, dup_rows
    return pdf, []


# ═════════════════════════════════════════════════════════════════════════
#  LECTEURS
# ═════════════════════════════════════════════════════════════════════════

def read_source_csv_pandas_local(key: str) -> pd.DataFrame:
    path = LOCAL_BRONZE_ROOT / key
    encodings = ["utf-8", "utf-8-sig", "latin-1", "cp1252"]
    for enc in encodings:
        try:
            return pd.read_csv(str(path), encoding=enc, low_memory=False)
        except UnicodeDecodeError:
            continue
        except Exception as e:
            log.warning("  CSV read error (%s) with %s : %s", key, enc, e)
            break
    return pd.DataFrame()


def read_source_json_local(spark: SparkSession, key: str) -> DataFrame:
    path = LOCAL_BRONZE_ROOT / key
    raw  = path.read_text(encoding="utf-8")
    data = json.loads(raw)
    if isinstance(data, dict):
        data = [data]
    return spark.read.json(spark.sparkContext.parallelize([json.dumps(r) for r in data]))


def read_source_xml_local(spark: SparkSession, key: str) -> DataFrame:
    import xml.etree.ElementTree as ET
    path = LOCAL_BRONZE_ROOT / key
    tree = ET.parse(str(path))
    root = tree.getroot()
    rows = []
    for child in root:
        row = {sub.tag: sub.text for sub in child}
        rows.append(row)
    if not rows:
        return spark.createDataFrame([], schema=T.StructType([]))
    return spark.createDataFrame(rows)


def read_api_json_local(spark: SparkSession, keys: List[str]) -> DataFrame:
    all_rows = []
    for key in keys:
        path = LOCAL_BRONZE_ROOT / key
        raw  = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(raw, list):   all_rows.extend(raw)
        elif isinstance(raw, dict): all_rows.append(raw)
    return spark.read.json(spark.sparkContext.parallelize([json.dumps(r) for r in all_rows]))


def read_db_rows_local(spark: SparkSession, keys: List[str]) -> DataFrame:
    dfs = []
    for key in keys:
        path = LOCAL_BRONZE_ROOT / key
        data = json.loads(path.read_text(encoding="utf-8"))
        rows = data if isinstance(data, list) else data.get("rows", [data])
        if rows:
            dfs.append(spark.read.json(spark.sparkContext.parallelize([json.dumps(r) for r in rows])))
    if not dfs:
        return spark.createDataFrame([], schema=T.StructType([]))
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)


def read_db_wrapped_local(spark: SparkSession, keys: List[str]) -> DataFrame:
    dfs = []
    for key in keys:
        path = LOCAL_BRONZE_ROOT / key
        data = json.loads(path.read_text(encoding="utf-8"))
        rows = []
        if isinstance(data, dict):
            for v in data.values():
                if isinstance(v, list):       rows.extend(v)
                elif isinstance(v, dict):     rows.append(v)
        elif isinstance(data, list):
            rows = data
        if rows:
            dfs.append(spark.read.json(spark.sparkContext.parallelize([json.dumps(r) for r in rows])))
    if not dfs:
        return spark.createDataFrame([], schema=T.StructType([]))
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)


# ═════════════════════════════════════════════════════════════════════════
#  QUARANTAINE, FLATTEN, HARMONISATION
# ═════════════════════════════════════════════════════════════════════════

def _write_quarantine_local(rows: List[dict], source_info: dict,
                             ts: str, reason_type: str):
    src_type   = source_info.get("source_type", "unknown")
    table_name = source_info.get("table_name", "unknown")
    db_name    = source_info.get("db_name", "")
    qdir = (LOCAL_SILVER_ROOT / "quarantine" / src_type / db_name / table_name / f"quarantine_{ts}"
            if db_name
            else LOCAL_SILVER_ROOT / "quarantine" / src_type / table_name / f"quarantine_{ts}")
    qdir.mkdir(parents=True, exist_ok=True)
    qfile = qdir / f"{reason_type}.json"
    qfile.write_text(_json_dumps({"reason": reason_type, "count": len(rows), "rows": rows}),
                     encoding="utf-8")
    log.info("  Quarantaine [%s] : %d lignes → %s", reason_type, len(rows), qfile)


def flatten_nested(df: DataFrame, max_depth: int = 3) -> DataFrame:
    depth = 0
    while depth < max_depth:
        nested = [(f.name, f.dataType) for f in df.schema.fields
                  if isinstance(f.dataType, (T.StructType, T.ArrayType))]
        if not nested:
            break
        new_cols = []
        for field in df.schema.fields:
            if isinstance(field.dataType, T.StructType):
                for sub in field.dataType.fields:
                    new_cols.append(col(f"`{field.name}`.`{sub.name}`").alias(
                        f"{field.name}_{sub.name}"))
            elif isinstance(field.dataType, T.ArrayType):
                new_cols.append(explode(col(f"`{field.name}`")).alias(field.name))
            elif isinstance(field.dataType, T.MapType):
                new_cols.append(F.to_json(col(f"`{field.name}`")).alias(field.name))
            else:
                new_cols.append(col(f"`{field.name}`"))
        df = df.select(new_cols)
        depth += 1
    return df


def harmonize_dataframe_pandas(pdf: pd.DataFrame, source_info: dict,
                                table_name: Optional[str] = None) -> pd.DataFrame:
    t_name      = table_name or source_info.get("table_name", "unknown")
    source_type = source_info.get("source_type", "unknown")
    db_name     = source_info.get("db_name", "")
    pdf["_source_type"]  = source_type
    pdf["_source_name"]  = source_info.get("source_name", "unknown")
    pdf["_table_name"]   = t_name
    pdf["_processed_at"] = datetime.utcnow().isoformat()
    if source_type in ("db_rows", "db_wrapped") and db_name:
        pdf["_db_name"] = db_name
    return pdf


# ═════════════════════════════════════════════════════════════════════════
#  [INC-S-4] ÉCRITURE SILVER — PYARROW DIRECT
# ═════════════════════════════════════════════════════════════════════════

def _silver_paths_local(source_info: dict, table_name: str, ts: str) -> Tuple[Path, Path]:
    src_type = source_info.get("source_type", "unknown")
    db_name  = source_info.get("db_name", "")
    is_db    = src_type in ("db_rows", "db_wrapped") and db_name
    if is_db:
        base   = LOCAL_SILVER_ROOT / src_type / db_name / table_name / f"processed_{ts}"
        report = LOCAL_SILVER_ROOT / src_type / db_name / table_name / f"report_{ts}.json"
    else:
        base   = LOCAL_SILVER_ROOT / src_type / table_name / f"processed_{ts}"
        report = LOCAL_SILVER_ROOT / src_type / table_name / f"report_{ts}.json"
    return base, report


def write_silver_table_local(pdf: pd.DataFrame, source_info: dict,
                              table_name: str, ts: str, spark: SparkSession) -> str:
    """[INC-S-4] Écriture Parquet via pyarrow direct (pas de Spark write)."""
    base_path, report_path = _silver_paths_local(source_info, table_name, ts)
    base_path.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    pdf_clean = pdf.where(pd.notnull(pdf), None)

    if PYARROW_AVAILABLE:
        try:
            for c in pdf_clean.select_dtypes(include=["object"]).columns:
                pdf_clean[c] = pdf_clean[c].where(pd.notnull(pdf_clean[c]), None)
            table_pa = pa.Table.from_pandas(pdf_clean, preserve_index=False)
            pq.write_table(table_pa,
                           str(base_path / "part-00000.parquet"),
                           compression="snappy",
                           write_statistics=True)
            log.info("  [pyarrow] Silver Parquet → %s", base_path)
        except Exception as e:
            log.warning("  [pyarrow] Échec (%s) → fallback Spark", e)
            _write_silver_spark_fallback(spark, pdf_clean, base_path)
    else:
        _write_silver_spark_fallback(spark, pdf_clean, base_path)

    sampled = pdf.sample(n=min(len(pdf), 100), random_state=42).copy()
    sample  = sampled.where(pd.notnull(sampled), None).to_dict(orient="records")
    report  = {"source": source_info, "table_name": table_name, "timestamp": ts,
               "row_count": len(pdf), "columns": list(pdf.columns), "sample_rows": sample}
    report_path.write_text(_json_dumps(report), encoding="utf-8")
    log.info("  Silver → %s | Rapport → %s", base_path, report_path)
    return str(base_path)


def _write_silver_spark_fallback(spark: SparkSession, pdf_clean: pd.DataFrame, base_path: Path):
    try:
        df_spark = spark.createDataFrame(pdf_clean)
    except Exception as e:
        log.warning("createDataFrame a échoué (%s), fallback string...", e)
        df_spark = spark.createDataFrame(pdf_clean.astype(str))
    (df_spark.coalesce(1).write.mode("overwrite")
             .option("compression", "snappy").parquet(str(base_path)))


# ═════════════════════════════════════════════════════════════════════════
#  PIPELINE PRINCIPAL
# ═════════════════════════════════════════════════════════════════════════

def _resolve_special_cols(ai_rules: dict, df_cols: List[str]) -> Tuple[List, List, Optional[List]]:
    phone_cols   = []
    unix_ts_cols = []
    dup_cols     = None
    if ai_rules:
        phone_cols   = [c for c in ai_rules.get("phone_column_names", []) if c in df_cols]
        raw_unix     = ai_rules.get("unix_timestamp_columns", [])
        unix_ts_cols = [c for c in raw_unix if c in df_cols and not _is_financial_column(c)]
        dup_cols     = ai_rules.get("drop_duplicates_on")
    if not phone_cols:
        phone_cols = [c for c in df_cols
                      if any(k in c.lower() for k in ["phone","telephone","tel","mobile","gsm"])]
    return phone_cols, unix_ts_cols, dup_cols


def process_group(spark: SparkSession, group_key: str, sources: List[dict]) -> Tuple[bool, str, int]:
    """
    Retourne (success, parquet_path, row_count) pour mise à jour de l'état Silver.
    """
    start_time  = time.time()
    source_info = sources[0]
    table_name  = source_info.get("table_name", "unknown")
    src         = source_info["source_type"]
    ext         = source_info["ext"]
    keys        = [s["key"] for s in sources]
    ts          = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    quarantine_dict: dict = {}

    log.info("═══ Traitement : %s (%d fichier(s)) ═══",
             source_info.get("source_name"), len(sources))
    try:
        if src == "source_file" and ext == ".csv":
            ok, parquet_path, row_count = _process_csv_pandas_local(
                spark, source_info, keys[0], ts)
            return ok, parquet_path, row_count

        if src == "api":          df = read_api_json_local(spark, keys)
        elif src == "db_rows":    df = read_db_rows_local(spark, keys)
        elif src == "db_wrapped": df = read_db_wrapped_local(spark, keys)
        elif src == "source_file":
            df = (read_source_xml_local(spark, keys[0]) if ext == ".xml"
                  else read_source_json_local(spark, keys[0]))
        else:
            log.warning("Type inconnu : %s", src)
            return False, "", 0

        row_count = df.count()
        if row_count == 0:
            log.warning("0 lignes pour %s", source_info.get("source_name"))
            return False, "", 0
        log.info("  Lu : %d lignes × %d colonnes", row_count, len(df.columns))

        df = flatten_nested(df)
        df = normalize_column_names(df)
        df, canonical_renames = apply_canonical_renames_spark(df)

        sample_rows  = [row.asDict() for row in df.limit(30).collect()]
        sample_pdf   = pd.DataFrame(sample_rows)
        col_profiles = _infer_column_profiles(sample_pdf)
        ai_rules     = analyze_with_groq(source_info, sample_rows, list(df.columns), col_profiles)

        renames = ai_rules.get("column_renames", {}) if ai_rules else {}
        df, groq_renames  = apply_column_renames(df, renames)
        applied_renames   = {**canonical_renames, **groq_renames}

        phone_cols, unix_ts_cols, dup_cols = _resolve_special_cols(ai_rules, list(df.columns))
        log.info("  Téléphones : %s | Unix TS : %s", phone_cols, unix_ts_cols)

        df = standardize_nulls_spark(df)
        df = apply_cleaning_spark(df, ai_rules or {},
                                  skip_cols=set(phone_cols) | set(unix_ts_cols),
                                  applied_renames=applied_renames)

        pdf = df.toPandas()
        pdf, quarantine_dict = process_pandas_cleaning(pdf, phone_cols, unix_ts_cols)

        pdf, dup_rows = deduplicate_pandas(pdf, dup_cols, source_type=src)
        if dup_rows:
            quarantine_dict.setdefault("doublon", []).extend(dup_rows)

        for reason_type, rows in quarantine_dict.items():
            if rows:
                _write_quarantine_local(rows, source_info, ts, reason_type)

        pdf = pdf.where(pd.notnull(pdf), None)
        if pdf.empty:
            log.warning("  Aucune ligne valide pour %s", table_name)
            return True, "", 0

        pdf = harmonize_dataframe_pandas(pdf, source_info, table_name=table_name)
        log.info("  ✓ %d lignes → Silver | %d en quarantaine",
                 len(pdf), sum(len(v) for v in quarantine_dict.values()))
        _emit_metrics(group_key, table_name, src, pdf, quarantine_dict, start_time, True)
        parquet_path = write_silver_table_local(pdf, source_info, table_name, ts, spark)
        return True, parquet_path, len(pdf)

    except Exception as exc:
        log.error("ERREUR sur %s : %s", source_info.get("source_name"), exc, exc_info=True)
        _emit_metrics(group_key, table_name, src, pd.DataFrame(),
                      quarantine_dict, start_time, False, str(exc))
        return False, "", 0


def _process_csv_pandas_local(spark: SparkSession,
                               source_info: dict, key: str, ts: str) -> Tuple[bool, str, int]:
    start_time = time.time()
    table_name = source_info.get("table_name", "unknown")
    src        = source_info["source_type"]
    quarantine_dict: dict = {}
    parquet_path = ""
    try:
        pdf = read_source_csv_pandas_local(key)
        if pdf.empty:
            log.warning("  CSV vide : %s", key)
            return False, "", 0
        log.info("  CSV lu : %d lignes × %d colonnes", len(pdf), len(pdf.columns))

        pdf, canonical_renames = apply_canonical_renames_pandas(pdf)
        col_profiles = _infer_column_profiles(pdf.head(100))
        sample       = pdf.head(30).where(pd.notnull(pdf.head(30)), None).to_dict(orient="records")
        ai_rules     = analyze_with_groq(source_info, sample, list(pdf.columns), col_profiles)

        renames = ai_rules.get("column_renames", {}) if ai_rules else {}
        pdf, groq_renames   = apply_column_renames_pandas(pdf, renames)
        applied_renames     = {**canonical_renames, **groq_renames}

        phone_cols, unix_ts_cols, dup_cols = _resolve_special_cols(ai_rules, list(pdf.columns))
        skip_cols = set(phone_cols) | set(unix_ts_cols)

        pdf = standardize_nulls_pandas(pdf)
        pdf = apply_cleaning_pandas(pdf, ai_rules or {}, skip_cols, applied_renames)
        pdf, quarantine_dict = process_pandas_cleaning(pdf, phone_cols, unix_ts_cols)

        pdf, dup_rows = deduplicate_pandas(pdf, dup_cols, source_type=src)
        if dup_rows:
            quarantine_dict.setdefault("doublon", []).extend(dup_rows)

        for reason_type, rows in quarantine_dict.items():
            if rows:
                _write_quarantine_local(rows, source_info, ts, reason_type)

        if pdf.empty:
            log.warning("  Aucune ligne valide pour %s", table_name)
            return True, "", 0

        pdf = harmonize_dataframe_pandas(pdf, source_info, table_name=table_name)
        log.info("  ✓ %d lignes → Silver | %d en quarantaine",
                 len(pdf), sum(len(v) for v in quarantine_dict.values()))
        _emit_metrics(source_info.get("source_name"), table_name, src,
                      pdf, quarantine_dict, start_time, True)
        parquet_path = write_silver_table_local(pdf, source_info, table_name, ts, spark)
        return True, parquet_path, len(pdf)

    except Exception as exc:
        log.error("ERREUR CSV sur %s : %s", source_info.get("source_name"), exc, exc_info=True)
        _emit_metrics(source_info.get("source_name"), table_name, src,
                      pd.DataFrame(), quarantine_dict, start_time, False, str(exc))
        return False, "", 0


# ═════════════════════════════════════════════════════════════════════════
#  POINT D'ENTRÉE
# ═════════════════════════════════════════════════════════════════════════

def run_pipeline(target_group: Optional[str] = None, force_all: bool = False):
    pipeline_start = time.time()

    log.info("╔══════════════════════════════════════════╗")
    log.info("║  SILVER PIPELINE v11 LOCAL  —  démarrage ║")
    log.info("╚══════════════════════════════════════════╝")
    log.info("  Bronze  : %s", LOCAL_BRONZE_ROOT)
    log.info("  Silver  : %s", LOCAL_SILVER_ROOT)
    log.info("  State   : %s", LOCAL_STATE_FILE)
    log.info("  Run TS  : %s", _RUN_TS)
    log.info("  pyarrow : %s", PYARROW_AVAILABLE)

    spark = create_spark()
    try:
        groups = discover_sources_local()
        if not groups:
            log.error("Aucune source trouvée dans %s", LOCAL_BRONZE_ROOT)
            return

        prev_state = load_state()

        if target_group:
            groups = {k: v for k, v in groups.items() if k == target_group}
            if not groups:
                log.error("Groupe introuvable : %s", target_group)
                return
            changed_groups = groups
            new_hashes     = {k: group_hash(v) for k, v in groups.items()}
        elif force_all:
            log.info("Mode FORCE_ALL — traitement de tous les groupes")
            changed_groups = groups
            new_hashes     = {k: group_hash(v) for k, v in groups.items()}
        else:
            changed_groups, new_hashes = detect_changes(groups, prev_state)
            if not changed_groups:
                log.info("Aucun changement Bronze détecté → pipeline Skip.")
                log.info("  Utilisez --force-all pour retraiter malgré tout.")
                return

        results = {"ok": 0, "fail": 0}
        silver_outputs = prev_state.get("silver_outputs", {})

        for gk, srcs in changed_groups.items():
            ok, parquet_path, row_count = process_group(spark, gk, srcs)
            if ok:
                results["ok"] += 1
                # [INC-S-1] Enregistrer le nouvel output Silver dans l'état
                silver_outputs[gk] = {
                    "parquet_path": parquet_path,
                    "ts":           datetime.utcnow().strftime("%Y%m%d_%H%M%S"),
                    "row_count":    row_count,
                    "bronze_hash":  new_hashes.get(gk, ""),
                    "processed_at": datetime.utcnow().isoformat(),
                }
            else:
                results["fail"] += 1

        # Sauvegarder l'état enrichi
        if not target_group:
            prev_state["bronze_hashes"] = {**prev_state.get("bronze_hashes", {}), **new_hashes}
            prev_state["silver_outputs"] = silver_outputs
            save_state(prev_state)

        total_sec = round(time.time() - pipeline_start, 2)
        log.info("╔══════════════════════════════════════════╗")
        log.info("║  RÉSULTATS : %d OK  /  %d ÉCHEC(S)       ║",
                 results["ok"], results["fail"])
        log.info("║  Temps total d'exécution : %.1f s        ║", total_sec)
        log.info("╚══════════════════════════════════════════╝")
        log.info("Fichier log   : %s", LOCAL_LOGS_DIR / f"silver_pipeline_{_RUN_TS}.log")
        log.info("État persisté : %s", LOCAL_STATE_FILE)

    finally:
        spark.stop()


# ═════════════════════════════════════════════════════════════════════════
#  CLI
# ═════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Silver Pipeline v11 — LOCAL (incrémental + état persistant)",
        epilog="""
Variables d'environnement :
  LOCAL_BRONZE_ROOT  Dossier racine Bronze
  LOCAL_SILVER_ROOT  Dossier racine Silver
  PROMETHEUS_PUSHGATEWAY  Adresse gateway (défaut: localhost:9091)

Exemples :
  python silver_pipeline_local_v11.py               # incrémental (Bronze modifiés uniquement)
  python silver_pipeline_local_v11.py --force-all   # tout retraiter
  python silver_pipeline_local_v11.py --group source_file__clients_dirty
  python silver_pipeline_local_v11.py --list
  python silver_pipeline_local_v11.py --status      # afficher l'état persistant
        """)
    parser.add_argument("--group",     type=str, default=None,
                        help="Traiter un seul groupe Bronze")
    parser.add_argument("--force-all", action="store_true",
                        help="Retraiter tous les groupes même si inchangés")
    parser.add_argument("--list",      action="store_true",
                        help="Lister les groupes Bronze disponibles")
    parser.add_argument("--status",    action="store_true",
                        help="[INC-S-5] Afficher l'état Silver persistant")
    args = parser.parse_args()

    if args.list:
        gs = discover_sources_local()
        print(f"\nGroupes Bronze disponibles ({len(gs)}) :")
        for k, v in sorted(gs.items()):
            print(f"  {k:65s}  ({len(v)} fichier(s))")

    elif args.status:
        # [INC-S-5] Afficher l'état courant
        state = load_state()
        silver_out = state.get("silver_outputs", {})
        bronze_h   = state.get("bronze_hashes", {})
        print(f"\n{'='*70}")
        print(f"  ÉTAT SILVER PERSISTANT — {LOCAL_STATE_FILE}")
        print(f"  Dernier run : {state.get('last_run', 'jamais')}")
        print(f"{'='*70}")
        print(f"\n  Sorties Silver enregistrées ({len(silver_out)}) :")
        print(f"  {'GROUPE':<45}  {'TS':<18}  {'LIGNES':>8}  {'BRONZE_HASH'}")
        print(f"  {'-'*45}  {'-'*18}  {'-'*8}  {'-'*12}")
        for gk, info in sorted(silver_out.items()):
            print(f"  {gk:<45}  {info.get('ts','?'):<18}  "
                  f"{info.get('row_count',0):>8}  {info.get('bronze_hash','')[:10]}…")
        if not silver_out:
            print("  (aucun output enregistré)")
        print(f"\n  Hashes Bronze suivis : {len(bronze_h)}")
        unchanged = sum(1 for gk in bronze_h
                        if gk in silver_out and
                        silver_out[gk].get("bronze_hash") == bronze_h[gk])
        print(f"  Groupes Bronze inchangés depuis dernier run : {unchanged}")
        print(f"{'='*70}\n")
    else:
        run_pipeline(target_group=args.group, force_all=args.force_all)
