from __future__ import annotations

import os
import tempfile
import sys
import unicodedata
import hashlib
from typing import List, Dict, Optional

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import inspect

ENCODING = "windows-1252"
SEP = ";"


CSV_FILES = [
    "competence.csv",
    "pointage.csv",
    "priorite.csv",
    "tacheslignes.csv",
    "TachesSéparé.csv",
]

# Logical names for UI and per-department management
CSV_LOGICAL = [
    ("competence", "competence.csv"),
    ("pointage", "pointage.csv"),
    ("priorite", "priorite.csv"),
    ("tacheslignes", "tacheslignes.csv"),
    ("tachessepare", "TachesSéparé.csv"),
]


def _ascii_fold(s: str) -> str:
    nfkd = unicodedata.normalize('NFKD', s)
    return nfkd.encode('ASCII', 'ignore').decode('ASCII')


def sanitize_identifier(name: str) -> str:
    base = _ascii_fold(name)
    base = base.lower()
    out = []
    for ch in base:
        if ch.isalnum():
            out.append(ch)
        else:
            out.append("_")
    sanitized = "".join(out)
    while "__" in sanitized:
        sanitized = sanitized.replace("__", "_")
    return sanitized.strip("_") or "table"


def sanitize_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, Dict[str, str]]:
    mapping: Dict[str, str] = {}
    new_cols = []
    for c in df.columns:
        sani = sanitize_identifier(str(c))
        mapping[sani] = str(c)
        new_cols.append(sani)
    df = df.copy()
    df.columns = new_cols
    return df, mapping


def _norm_cell(v) -> str:
    if pd.isna(v):
        return ""
    s = str(v)
    return s.strip()


def compute_row_hash(df: pd.DataFrame) -> pd.Series:
    cols = list(df.columns)
    def _hash_row(row) -> str:
        # Join normalized values in column order with a delimiter unlikely to appear
        parts = [_norm_cell(row[c]) for c in cols]
        payload = "\u241F".join(parts)  # Unit Separator-like char
        return hashlib.md5(payload.encode('utf-8')).hexdigest()
    return df.apply(_hash_row, axis=1)


def _is_int_like(vals: List[str]) -> bool:
    if not vals:
        return False
    for s in vals:
        if s == "":
            continue
        if not (s.lstrip("-+").isdigit()):
            return False
    return True


def _is_float_like(vals: List[str]) -> bool:
    if not vals:
        return False
    for s in vals:
        if s == "":
            continue
        try:
            float(s.replace(',', '.'))
        except Exception:
            return False
    return True


def _is_date_like(vals: List[str]) -> str | None:
    # Return 'DATE' or 'DATETIME' if consistent
    if not vals:
        return None
    parsed_any = False
    has_time = False
    for s in vals:
        if not s:
            continue
        dt = pd.to_datetime(s, dayfirst=True, errors='coerce')
        if pd.isna(dt):
            return None
        parsed_any = True
        if getattr(dt, 'hour', 0) or getattr(dt, 'minute', 0) or getattr(dt, 'second', 0):
            has_time = True
    if not parsed_any:
        return None
    return 'DATETIME' if has_time else 'DATE'


def infer_mysql_types(df: pd.DataFrame) -> Dict[str, str]:
    types: Dict[str, str] = {}
    for c in df.columns:
        ser = df[c]
        vals = [_norm_cell(x) for x in ser.tolist()]
        sample = [x for x in vals if x][:1000]
        if _is_int_like(sample):
            types[c] = 'BIGINT'
            continue
        if _is_float_like(sample):
            types[c] = 'DOUBLE'
            continue
        dt_kind = _is_date_like(sample)
        if dt_kind:
            types[c] = dt_kind
            continue
        maxlen = max((len(x) for x in sample), default=0)
        if maxlen <= 255:
            types[c] = 'VARCHAR(255)'
        elif maxlen <= 1024:
            types[c] = 'VARCHAR(1024)'
        else:
            types[c] = 'TEXT'
    return types


def ensure_meta_table(engine: Engine):
    with engine.begin() as conn:
        conn.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS `ingestion_columns_meta` (
              `table_name` VARCHAR(128) NOT NULL,
              `column_name` VARCHAR(128) NOT NULL,
              `original_name` TEXT NULL,
              `mysql_type` VARCHAR(64) NULL,
              PRIMARY KEY (`table_name`, `column_name`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        )


def ensure_table(engine: Engine, table: str, columns: List[str], types: Dict[str, str], original_map: Dict[str, str]):
    inspector = inspect(engine)
    existing_cols = {c['name'] for c in inspector.get_columns(table)} if inspector.has_table(table) else set()
    # Build a de-duplicated list of data columns (excluding special ones)
    data_cols: List[str] = []
    seen = set()
    for c in columns:
        if c in seen:
            continue
        seen.add(c)
        data_cols.append(c)

    with engine.begin() as conn:
        if not inspector.has_table(table):
            # Create table with inferred types and row_hash PK
            # Always include `departement` once; exclude it from the dynamic list if present
            data_cols_no_dept = [c for c in data_cols if c != 'departement']
            cols_sql = ", ".join([f"`{c}` {types.get(c, 'TEXT')} NULL" for c in data_cols_no_dept])
            if cols_sql:
                cols_sql = ",\n  " + cols_sql
            sql = (
                f"CREATE TABLE `{table}` (\n"
                f"  `row_hash` CHAR(32) NOT NULL,\n"
                f"  `departement` VARCHAR(64) NULL"
                f"{cols_sql},\n"
                f"  `ingested_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n"
                f"  PRIMARY KEY (`row_hash`)\n"
                f") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
            )
            conn.exec_driver_sql(sql)
        else:
            # Ensure row_hash exists and is PK
            if 'row_hash' not in existing_cols:
                conn.exec_driver_sql(f"ALTER TABLE `{table}` ADD COLUMN `row_hash` CHAR(32) NOT NULL FIRST")
                conn.exec_driver_sql(f"ALTER TABLE `{table}` ADD PRIMARY KEY (`row_hash`)")
            # Ensure departement exists
            if 'departement' not in existing_cols:
                conn.exec_driver_sql(f"ALTER TABLE `{table}` ADD COLUMN `departement` VARCHAR(64) NULL AFTER `row_hash`")
            # Add any missing data columns with inferred types (skip departement to avoid duplicate)
            for c in data_cols:
                if c == 'departement':
                    continue
                if c not in existing_cols:
                    conn.exec_driver_sql(f"ALTER TABLE `{table}` ADD COLUMN `{c}` {types.get(c, 'TEXT')} NULL")
        # Update meta mapping (deduplicated, include departement once)
        ensure_meta_table(engine)
        rows = []
        meta_cols: List[str] = []
        seen_meta = set()
        for c in ['departement'] + data_cols:
            if c in seen_meta:
                continue
            seen_meta.add(c)
            meta_cols.append(c)
        for c in meta_cols:
            rows.append((table, c, original_map.get(c), types.get(c, 'TEXT')))
        if rows:
            conn.exec_driver_sql(
                """
                INSERT INTO `ingestion_columns_meta`(table_name, column_name, original_name, mysql_type)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE original_name=VALUES(original_name), mysql_type=VALUES(mysql_type)
                """,
                rows
            )


def ensure_departement_index(engine: Engine, table: str):
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql(f"CREATE INDEX IF NOT EXISTS `idx_{table}_departement` ON `{table}`(`departement`)")
    except Exception:
        try:
            with engine.begin() as conn:
                conn.exec_driver_sql(f"CREATE INDEX `idx_{table}_departement` ON `{table}`(`departement`)")
        except Exception:
            pass


def upsert_rows(engine: Engine, table: str, df: pd.DataFrame, chunk_size: int = 1000) -> int:
    # df is expected to include 'row_hash' and data columns
    cols_no_hash = [c for c in df.columns if c != 'row_hash']
    # Order of params: row_hash, then all data columns
    placeholders = ", ".join(["%s"] * (1 + len(cols_no_hash)))
    col_list = ", ".join([f"`{c}`" for c in cols_no_hash])
    update_list = ", ".join([f"`{c}`=VALUES(`{c}`)" for c in cols_no_hash])
    sql = f"INSERT INTO `{table}` (`row_hash`, {col_list}) VALUES ({placeholders}) " \
          f"ON DUPLICATE KEY UPDATE {update_list}"
    total = 0
    with engine.begin() as conn:
        for start in range(0, len(df), chunk_size):
            part = df.iloc[start:start+chunk_size]
            params = []
            for _, row in part.iterrows():
                row_vals = [_norm_cell(row[c]) for c in cols_no_hash]
                # SQLAlchemy exec_driver_sql expects a list of tuples/dicts for executemany
                params.append(tuple([_norm_cell(row['row_hash']), *row_vals]))
            if params:
                conn.exec_driver_sql(sql, params)
                total += len(params)
    return total


def load_csv_to_mysql(engine: Engine, base_dir: str | None = None, table_prefix: str | None = None, department: Optional[str] = None) -> list[dict]:
    results: list[dict] = []
    base = base_dir or os.getcwd()
    for fname in CSV_FILES:
        path = os.path.join(base, fname)
        base_table = sanitize_identifier(os.path.splitext(os.path.basename(fname))[0])
        table = f"{sanitize_identifier(table_prefix)}_{base_table}" if table_prefix else base_table
        if not os.path.exists(path):
            results.append({
                "file": fname,
                "table": table,
                "status": "missing",
                "rows": 0,
                "message": f"{path} not found",
            })
            continue
        try:
            df = pd.read_csv(path, encoding=ENCODING, sep=SEP)
        except Exception as e:
            results.append({
                "file": fname,
                "table": table,
                "status": "error",
                "rows": 0,
                "message": f"read_csv failed: {e}",
            })
            continue

        try:
            df, colmap = sanitize_columns(df)
            if department:
                df = df.copy()
                df['departement'] = str(department)
                colmap.setdefault('departement', 'departement')
            # Compute row_hash across all columns â†’ de-dupe exact rows
            if len(df) > 0:
                df = df.copy()
                df['row_hash'] = compute_row_hash(df)
            # Ensure table exists with row_hash PK and matching columns
            data_cols = list(df.columns.drop('row_hash')) if 'row_hash' in df.columns else list(df.columns)
            types = infer_mysql_types(df[data_cols]) if data_cols else {}
            types['departement'] = 'VARCHAR(64)'
            ensure_table(engine, table, data_cols, types, colmap)
            ensure_departement_index(engine, table)
            # Upsert (dedupe by all columns)
            rows_upserted = 0
            if len(df) > 0:
                # Ensure row_hash is present and upsert
                rows_upserted = upsert_rows(engine, table, df)
            results.append({
                "file": fname,
                "table": table,
                "status": "loaded",
                "rows": int(len(df)),
                "upserted": int(rows_upserted),
                "message": "ok (dedupe by departement + all columns)",
                "departement": department or "",
            })
        except Exception as e:
            results.append({
                "file": fname,
                "table": table,
                "status": "error",
                "rows": 0,
                "message": f"ingest failed: {e}",
            })
    return results


def add_parent_to_path():
    here = os.path.dirname(os.path.abspath(__file__))
    parent = os.path.abspath(os.path.join(here, os.pardir))
    if parent not in sys.path:
        sys.path.insert(0, parent)


def ingest_file(engine: Engine, path: str, table_override: str | None = None, table_prefix: str | None = None, department: Optional[str] = None) -> dict:
    """Ingest a single CSV file into MySQL using the same logic as bulk loader.

    - Infers table name from filename unless table_override provided
    - Normalizes columns, infers MySQL types, ensures/ALTERs table
    - Computes row_hash across all data columns; upserts rows
    Returns a dict summary.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    base_table = sanitize_identifier(os.path.splitext(os.path.basename(path))[0])
    table = table_override or (f"{sanitize_identifier(table_prefix)}_{base_table}" if table_prefix else base_table)
    df = pd.read_csv(path, encoding=ENCODING, sep=SEP)
    df, colmap = sanitize_columns(df)
    if department:
        df = df.copy()
        df['departement'] = str(department)
        colmap.setdefault('departement', 'departement')
    if len(df) > 0:
        df = df.copy()
        df['row_hash'] = compute_row_hash(df)
    data_cols = list(df.columns.drop('row_hash')) if 'row_hash' in df.columns else list(df.columns)
    types = infer_mysql_types(df[data_cols]) if data_cols else {}
    types['departement'] = 'VARCHAR(64)'
    ensure_table(engine, table, data_cols, types, colmap)
    ensure_departement_index(engine, table)
    upserted = 0
    if len(df) > 0:
        upserted = upsert_rows(engine, table, df)
    return {
        "file": os.path.basename(path),
        "table": table,
        "rows": int(len(df)),
        "upserted": int(upserted),
        "status": "loaded",
        "message": "ok (single file; departement aware)",
        "departement": department or "",
    }


def uploads_dir_base() -> str:
    base_env = os.environ.get("UPLOADS_DIR")
    if base_env:
        try:
            os.makedirs(base_env, exist_ok=True)
            return base_env
        except Exception:
            pass
    here = os.path.dirname(os.path.abspath(__file__))
    default = os.path.join(here, 'uploads')
    try:
        os.makedirs(default, exist_ok=True)
        return default
    except Exception:
        tmp = os.path.join(tempfile.gettempdir(), 'uploads')
        os.makedirs(tmp, exist_ok=True)
        return tmp


def resolve_dept_csv_paths(department: str, base_dir: Optional[str] = None, strict: bool = False) -> Dict[str, Optional[str]]:
    """Return mapping logical_type -> file path.

    - When strict=False (default): prefer per-dept upload then fallback to project root file
    - When strict=True: only accept per-dept upload; otherwise value is None
    """
    base = base_dir or os.getcwd()
    uploads_base = uploads_dir_base()
    uploads_dir = os.path.join(uploads_base, 'departments', department)
    paths: Dict[str, Optional[str]] = {}
    for logical, default_name in CSV_LOGICAL:
        dept_path = os.path.join(uploads_dir, f"{logical}.csv")
        default_path = os.path.join(base, default_name)
        if os.path.exists(dept_path):
            paths[logical] = dept_path
        else:
            paths[logical] = None if strict else default_path
    return paths


def load_department_bundle(engine: Engine, department: str, table_prefix: Optional[str] = None, base_dir: Optional[str] = None, strict: bool = False) -> List[dict]:
    """Load all known CSVs for a department.

    - strict=False: prefer per-dept uploads then fallback to project root
    - strict=True: only ingest per-dept uploads (report missing if absent)

    Returns list of ingestion result dicts.
    """
    results: List[dict] = []
    paths = resolve_dept_csv_paths(department, base_dir=base_dir, strict=strict)
    for logical, _default in CSV_LOGICAL:
        path = paths.get(logical)
        if not path or not os.path.exists(path):
            results.append({
                "file": os.path.basename(path) if path else logical,
                "table": (f"{sanitize_identifier(table_prefix)}_{logical}" if table_prefix else logical),
                "status": "missing",
                "rows": 0,
                "message": f"{path} not found" if path else "no path",
                "departement": department,
            })
            continue
        res = ingest_file(
            engine,
            path,
            table_override=(f"{sanitize_identifier(table_prefix)}_{logical}" if table_prefix else logical),
            table_prefix=None,
            department=department,
        )
        results.append(res)
    return results


def get_headers_for_logical(department: Optional[str], logical: str, base_dir: Optional[str] = None) -> List[str]:
    """Return header list for a logical CSV type using per-dept file if available, else default.

    Falls back to empty list if no file is found or columns cannot be read.
    """
    base = base_dir or os.getcwd()
    here = os.path.dirname(os.path.abspath(__file__))
    try:
        # Identify default file name
        default_name = dict(CSV_LOGICAL).get(logical)
        if not default_name:
            return []
        # Prefer per-department override
        if department:
            dept_path = os.path.join(uploads_dir_base(), 'departments', department, f"{logical}.csv")
            if os.path.exists(dept_path):
                df = pd.read_csv(dept_path, encoding=ENCODING, sep=SEP, nrows=0)
                return list(df.columns)
        # Fallback to default path at project root
        default_path = os.path.join(base, default_name)
        if os.path.exists(default_path):
            df = pd.read_csv(default_path, encoding=ENCODING, sep=SEP, nrows=0)
            return list(df.columns)
    except Exception:
        pass
    return []


def _find_column(df: pd.DataFrame, *keywords: str) -> Optional[str]:
    lowmap = {c: str(c).strip().lower() for c in df.columns}
    for c, low in lowmap.items():
        if all(k in low for k in keywords):
            return c
    return None


def fill_ressource_by_ligne(df: pd.DataFrame) -> pd.DataFrame:
    """Fill the 'Ressource' column by grouping on 'Ligne ds la planche'.

    - Detects resource column by name containing 'ressource'
    - Detects group column by name containing both 'ligne' and 'planche'
    - For each group, takes the first non-empty resource and fills missing ones
    """
    if df is None or df.empty:
        return df
    # Detect columns
    res_col = _find_column(df, 'ressource')
    grp_col = _find_column(df, 'ligne', 'planche')
    if res_col is None or grp_col is None:
        return df
    out = df.copy()
    def norm(x: object) -> str:
        s = '' if pd.isna(x) else str(x)
        return s.strip()
    for key, g in out.groupby(grp_col).groups.items():
        idxs = list(g)
        vals = [norm(out.at[i, res_col]) for i in idxs]
        first = next((v for v in vals if v), '')
        if first:
            for i, v in zip(idxs, vals):
                if not v:
                    out.at[i, res_col] = first
    return out


def fill_tachessepare_from_assign(df_ts: pd.DataFrame, df_assign: pd.DataFrame) -> pd.DataFrame:
    """Fill Ressource in TachesSéparé from TachesLignes_assigné by matching line numbers.

    - Detect line-number columns:
        * TachesSéparé: contains 'ligne' and 'planche'
        * TachesLignes_assigné: contains 'ligne' and not 'planche' or exact 'Ligne de planche'
    - Detect resource columns:
        * TachesSéparé: column containing 'ressource'
        * Assign: column containing 'ressource_affect'
    - Left join by line number (cast to string trimmed), fill missing resources in TS with assigned ones.
    """
    if df_ts is None or df_ts.empty or df_assign is None or df_assign.empty:
        return df_ts
    # Detect columns
    ts_line = _find_column(df_ts, 'ligne', 'planche')
    ts_res = _find_column(df_ts, 'ressource')
    asg_line = None
    for c in df_assign.columns:
        low = str(c).strip().lower()
        if 'ligne' in low and 'planche' in low:
            asg_line = c
            break
        if low == 'ligne de planche':
            asg_line = c
            break
    if asg_line is None:
        # fallback: any column named exactly 'Ligne de planche' in original header spelling
        asg_line = next((c for c in df_assign.columns if str(c).strip().lower().startswith('ligne')), None)
    asg_res = None
    for c in df_assign.columns:
        if 'ressource_affect' in str(c).strip().lower():
            asg_res = c
            break
    if ts_line is None or ts_res is None or asg_line is None or asg_res is None:
        return df_ts
    left = df_ts.copy()
    right = df_assign[[asg_line, asg_res]].copy()
    left['_ln'] = left[ts_line].astype(str).str.strip()
    right['_ln'] = right[asg_line].astype(str).str.strip()
    # Prefer first non-null per line in assignment
    right = right.sort_values(by=right.columns.tolist()).drop_duplicates(subset=['_ln'], keep='first')
    merged = left.merge(right[['_ln', asg_res]], on='_ln', how='left', suffixes=('', '_assign'))
    def pick(a, b):
        a = '' if pd.isna(a) else str(a).strip()
        b = '' if pd.isna(b) else str(b).strip()
        return a if a else b
    merged[ts_res] = [pick(a, b) for a, b in zip(merged[ts_res], merged[asg_res])]
    merged = merged.drop(columns=['_ln', asg_res])
    return merged
