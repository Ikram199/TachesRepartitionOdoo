from __future__ import annotations

import os
import time
import unicodedata
from typing import Dict, List, Optional, Tuple

import pandas as pd

# Defaults (overridden by app.py when running from the UI)
ENCODING = "windows-1252"
SEP = ";"

TACHES_PATH = "tacheslignes.csv"
POINTAGE_PATH = "pointage.csv"
COMPETENCE_PATH = "competence.csv"
PRIORITE_PATH = "priorite.csv"

# Output files (can be overridden by the app)
OUTPUT_PATH = "TachesLignes_assigne.csv"
BACKUP_FMT = "TachesLignes_backup_{ts}.csv"

# Simple guardrail for assignment capacity
MAX_ASSIGN_PER_RESOURCE_PER_DAY = 50


def _ascii_fold(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")


def _low(s: object) -> str:
    return _ascii_fold(str(s or "")).strip().lower()


def _find_col(cols: List[str], *needles: str) -> Optional[str]:
    for c in cols:
        low = _low(c)
        if all(n in low for n in needles):
            return c
    return None


def _load_csv(path: str, nrows: Optional[int] = None) -> pd.DataFrame:
    return pd.read_csv(path, encoding=ENCODING, sep=SEP, nrows=nrows)


def _extract_competences(df_comp: pd.DataFrame) -> Dict[str, set]:
    """Return mapping resource -> set(competences)."""
    if df_comp is None or df_comp.empty:
        return {}
    res_col = _find_col(list(df_comp.columns), "ressource")
    comp_col = _find_col(list(df_comp.columns), "competence")
    if res_col is None:
        return {}
    if comp_col is None:
        # If no competence column, treat as all-rounders
        return {str(r).strip(): set() for r in df_comp[res_col].dropna().unique().tolist()}
    groups: Dict[str, set] = {}
    for _, row in df_comp.iterrows():
        r = str(row.get(res_col, "")).strip()
        c = str(row.get(comp_col, "")).strip()
        if not r:
            continue
        groups.setdefault(r, set())
        if c:
            groups[r].add(c)
    return groups


def _extract_tasks(df_tasks: pd.DataFrame) -> Tuple[pd.DataFrame, Optional[str], Optional[str]]:
    """Return (df, line_col, need_comp_col)

    - line_col: column that identifies a task row number (e.g., 'Ligne de planche')
    - need_comp_col: column containing a competence requirement if present
    """
    if df_tasks is None or df_tasks.empty:
        return df_tasks, None, None
    cols = list(df_tasks.columns)
    line_col = None
    # Prefer both 'ligne' and 'planche' in the header; else any header starting by 'ligne'
    line_col = _find_col(cols, "ligne", "planche") or next(
        (c for c in cols if _low(c).startswith("ligne")), None
    )
    need_comp_col = _find_col(cols, "competence")
    return df_tasks.copy(), line_col, need_comp_col


def _sort_tasks(df: pd.DataFrame) -> pd.DataFrame:
    # Try to sort by priority if present; otherwise leave as-is
    prio = _find_col(list(df.columns), "priorite")
    if prio and df[prio].notna().any():
        try:
            return df.sort_values(by=[prio])
        except Exception:
            return df
    return df


def assign_tasks(max_assign_per_resource_per_day: int = MAX_ASSIGN_PER_RESOURCE_PER_DAY,
                 start_date: Optional[str] = None,
                 end_date: Optional[str] = None) -> str:
    """Very simple, deterministic assignment:

    - Build candidate resource set from competence.csv
    - Sort tasks by 'priorite' if present
    - For each task, pick the first resource that matches competence (if any);
      otherwise pick the least-loaded resource
    - Write an output CSV containing the original tasks plus a 'Ressource_affecte' column
    - Backup any previous output
    """
    # Load CSVs
    df_tasks = _load_csv(TACHES_PATH)
    df_comp = _load_csv(COMPETENCE_PATH) if os.path.exists(COMPETENCE_PATH) else pd.DataFrame()

    df_tasks, line_col, need_comp_col = _extract_tasks(df_tasks)
    if line_col is None:
        # Ensure there is some line identifier to help downstream joins
        line_col = "Ligne de planche"
        if line_col not in df_tasks.columns:
            df_tasks.insert(0, line_col, range(1, len(df_tasks) + 1))

    # Build resources and their competences
    res_to_comp = _extract_competences(df_comp)
    resources = list(res_to_comp.keys()) or ["R1", "R2", "R3"]
    loads: Dict[str, int] = {r: 0 for r in resources}

    # Sort tasks for deterministic output
    df_sorted = _sort_tasks(df_tasks)

    # Prepare assignment column
    out_col = "Ressource_affecte"
    if out_col in df_sorted.columns:
        out_col = out_col  # reuse
    else:
        df_sorted[out_col] = ""

    def pick_resource(required: Optional[str]) -> str:
        req = (required or "").strip()
        # 1) Try exact competence match among least-loaded
        if req:
            candidates = [r for r in resources if (not res_to_comp.get(r)) or (req in res_to_comp.get(r, set()))]
        else:
            candidates = list(resources)
        # Order by current load to keep distribution fair
        candidates.sort(key=lambda r: loads.get(r, 0))
        chosen = candidates[0] if candidates else resources[0]
        # Respect a soft cap per resource if provided
        if max_assign_per_resource_per_day and loads.get(chosen, 0) >= max_assign_per_resource_per_day:
            # choose next one with lower load
            for r in candidates:
                if loads.get(r, 0) < max_assign_per_resource_per_day:
                    chosen = r
                    break
        loads[chosen] = loads.get(chosen, 0) + 1
        return chosen

    # Perform assignment
    if need_comp_col and need_comp_col in df_sorted.columns:
        comps = df_sorted[need_comp_col].astype(str).tolist()
    else:
        comps = [None] * len(df_sorted)
    assigned: List[str] = []
    for req in comps:
        assigned.append(pick_resource(req))
    df_sorted[out_col] = assigned

    # Backup old output
    try:
        if os.path.exists(OUTPUT_PATH):
            ts = time.strftime("%Y%m%d_%H%M%S")
            backup_path = BACKUP_FMT.format(ts=ts)
            try:
                os.replace(OUTPUT_PATH, backup_path)
            except Exception:
                # Fallback to copy
                df_old = _load_csv(OUTPUT_PATH)
                df_old.to_csv(backup_path, index=False, encoding=ENCODING, sep=SEP)
    except Exception:
        pass

    # Write new output
    df_sorted.to_csv(OUTPUT_PATH, index=False, encoding=ENCODING, sep=SEP)
    return OUTPUT_PATH


if __name__ == "__main__":
    path = assign_tasks()
    print(f"Assignment written to: {path}")

