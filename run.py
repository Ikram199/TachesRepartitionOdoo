from __future__ import annotations

import os
import time
import unicodedata
from typing import Dict, List, Optional, Tuple, Set
import re

import pandas as pd

# Defaults (overridden by app.py when running from the UI)
ENCODING = "windows-1252"
SEP = ";"

TACHES_PATH = "tacheslignes.csv"
POINTAGE_PATH = "pointage.csv"
PROG_PATH = "prog.csv"
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


def _codes_from_value(v: object) -> Set[str]:
    s = str(v or "").strip()
    if not s:
        return set()
    # Split on non-alnum separators (space, /, -, , ; etc.)
    toks = re.findall(r"[A-Za-z0-9]+", s.upper())
    return set(toks)


def _extract_competences(df_comp: pd.DataFrame) -> Dict[str, set]:
    """Return mapping resource -> set(competences).

    Column heuristics:
    - Resource column: contains one of ['ressource', 'nom prenom', 'nom prénom', 'nom', 'employe', 'employé']
    - Competence columns: any column containing ['qualif', 'compet', 'certif'] (multi-columns supported)
    """
    if df_comp is None or df_comp.empty:
        return {}
    cols = list(df_comp.columns)
    # Detect resource column
    res_col = (
        _find_col(cols, "ressource")
        or _find_col(cols, "nom", "prenom")
        or _find_col(cols, "nom", "prénom")
        or _find_col(cols, "employe")
        or _find_col(cols, "employé")
        or _find_col(cols, "nom")
    )
    if res_col is None:
        return {}
    # Detect competence columns (can be multiple: Qualif 1/2/3, etc.)
    comp_cols: List[str] = []
    for c in cols:
        low = _low(c)
        if any(k in low for k in ["qualif", "compet", "certif"]):
            comp_cols.append(c)
    groups: Dict[str, set] = {}
    if not comp_cols:
        # Treat as all-rounders (no explicit competence)
        for r in df_comp[res_col].dropna().astype(str).map(str.strip).tolist():
            if r:
                groups.setdefault(r, set())
        return groups
    for _, row in df_comp.iterrows():
        r = str(row.get(res_col, "")).strip()
        if not r:
            continue
        groups.setdefault(r, set())
        for c in comp_cols:
            codes = _codes_from_value(row.get(c))
            if codes:
                groups[r].update(codes)
    return groups


def _extract_program(df_prog: pd.DataFrame) -> List[Tuple[str, str, str]]:
    """Return a list of (resource, date_str, vacation_code) tuples marking availability.

    Heuristics for columns: resource (ressource|nom prénom|employé), date (jour|date), vacation (vacation|shift|code).
    Dates are normalized as YYYY-MM-DD strings.
    """
    if df_prog is None or df_prog.empty:
        return []
    cols = list(df_prog.columns)
    res_col = (
        _find_col(cols, "ressource")
        or _find_col(cols, "nom", "prenom")
        or _find_col(cols, "nom", "prénom")
        or _find_col(cols, "employe")
        or _find_col(cols, "employé")
        or _find_col(cols, "nom")
    )
    date_col = _find_col(cols, "jour") or _find_col(cols, "date")
    vac_col = _find_col(cols, "vacation") or _find_col(cols, "shift") or _find_col(cols, "code")
    if res_col is None or date_col is None or vac_col is None:
        return []
    out: List[Tuple[str, str, str]] = []
    for _, row in df_prog.iterrows():
        r = str(row.get(res_col, "")).strip()
        if not r:
            continue
        try:
            d = pd.to_datetime(row.get(date_col)).date().isoformat()
        except Exception:
            d = str(row.get(date_col, "")).strip()
        v = str(row.get(vac_col, "")).strip()
        if not d:
            continue
        out.append((r, d, v))
    return out


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
    # Required competence: prefer Qualif 1, then Qualif 2/3; else any 'competence'
    qual_cols = [c for c in cols if "qualif" in _low(c)]
    need_comp_col = qual_cols[0] if qual_cols else _find_col(cols, "competence")
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
    df_prog = _load_csv(PROG_PATH) if os.path.exists(PROG_PATH) else pd.DataFrame()
    df_prio = _load_csv(PRIORITE_PATH) if os.path.exists(PRIORITE_PATH) else pd.DataFrame()

    df_tasks, line_col, need_comp_col = _extract_tasks(df_tasks)
    if line_col is None:
        # Ensure there is some line identifier to help downstream joins
        line_col = "Ligne de planche"
        if line_col not in df_tasks.columns:
            df_tasks.insert(0, line_col, range(1, len(df_tasks) + 1))

    # Build resources and their competences
    res_to_comp = _extract_competences(df_comp)
    resources = list(res_to_comp.keys())
    if not resources:
        # Fallback attempt: derive resources from pointage if present
        try:
            df_pt = _load_csv(POINTAGE_PATH)
            res_col = (
                _find_col(list(df_pt.columns), "ressource")
                or _find_col(list(df_pt.columns), "nom", "prenom")
                or _find_col(list(df_pt.columns), "nom", "prénom")
                or _find_col(list(df_pt.columns), "employe")
                or _find_col(list(df_pt.columns), "employé")
            )
            if res_col:
                resources = (
                    df_pt[res_col].dropna().astype(str).map(str.strip).drop_duplicates().tolist()
                )
        except Exception:
            pass
    if not resources:
        resources = ["R1", "R2", "R3"]
    loads: Dict[str, int] = {r: 0 for r in resources}

    # Sort tasks for deterministic output
    # Merge priorities if provided and not already present
    try:
        if ("priorite" not in [ _low(c) for c in df_tasks.columns ]) and not df_prio.empty:
            # Try merge by line number
            cols = list(df_prio.columns)
            pr_line = _find_col(cols, "ligne") or next((c for c in cols if _low(c).startswith("ligne")), None)
            pr_col = _find_col(cols, "priorite")
            if pr_line and pr_col:
                df_tasks = df_tasks.merge(df_prio[[pr_line, pr_col]], left_on=line_col, right_on=pr_line, how='left')
    except Exception:
        pass

    df_sorted = _sort_tasks(df_tasks)

    # Prepare assignment column
    out_col = "Ressource_affecte"
    if out_col in df_sorted.columns:
        out_col = out_col  # reuse
    else:
        df_sorted[out_col] = ""

    # Build availability index from program
    prog_index = set(_extract_program(df_prog)) if not df_prog.empty else set()
    date_col = _find_col(list(df_sorted.columns), "jour") or _find_col(list(df_sorted.columns), "date")
    vac_col = _find_col(list(df_sorted.columns), "vacation") or _find_col(list(df_sorted.columns), "shift") or _find_col(list(df_sorted.columns), "code")

    # Track loads per resource per day
    loads: Dict[Tuple[str, str], int] = {}

    def pick_resource(required: Optional[str], date_str: Optional[str], vacation: Optional[str]) -> str:
        req = (required or "").strip()
        # 1) Try exact competence match among least-loaded
        if req:
            candidates = [r for r in resources if (not res_to_comp.get(r)) or (req in res_to_comp.get(r, set()))]
        else:
            candidates = list(resources)
        # 2) Filter by program availability if provided
        if prog_index and date_str:
            cand2 = []
            for r in candidates:
                key_any = (r, date_str, "")
                has_day = any((rr == r and dd == date_str) for rr, dd, _ in prog_index)
                ok = False
                if vacation:
                    ok = (r, date_str, vacation) in prog_index
                ok = ok or (has_day and not vacation)
                if ok:
                    cand2.append(r)
            if cand2:
                candidates = cand2
        # Order by current load to keep distribution fair
        key_date = date_str or ""
        candidates.sort(key=lambda r: loads.get((r, key_date), 0))
        chosen = candidates[0] if candidates else resources[0]
        # Respect a soft cap per resource/day if provided
        cap_key = (chosen, key_date)
        if max_assign_per_resource_per_day and loads.get(cap_key, 0) >= max_assign_per_resource_per_day:
            # choose next one with lower load
            for r in candidates:
                if loads.get((r, key_date), 0) < max_assign_per_resource_per_day:
                    chosen = r
                    break
        loads[cap_key] = loads.get(cap_key, 0) + 1
        return chosen

    # Perform assignment
    # Build required competence sets per task (union of non-empty Qualif cols)
    req_sets: List[Set[str]] = []
    qual_cols = [c for c in df_sorted.columns if "qualif" in _low(c)]
    if qual_cols:
        for _, row in df_sorted.iterrows():
            codes: Set[str] = set()
            for c in qual_cols:
                codes.update(_codes_from_value(row.get(c)))
            req_sets.append(codes)
    elif need_comp_col and need_comp_col in df_sorted.columns:
        for _, row in df_sorted.iterrows():
            req_sets.append(_codes_from_value(row.get(need_comp_col)))
    else:
        req_sets = [set() for _ in range(len(df_sorted))]
    else:
        comps = [None] * len(df_sorted)
    assigned: List[str] = []
    for idx, req_set in enumerate(req_sets):
        dstr = None
        vcode = None
        if date_col and date_col in df_sorted.columns:
            try:
                dstr = pd.to_datetime(df_sorted.iloc[idx][date_col]).date().isoformat()
            except Exception:
                dstr = str(df_sorted.iloc[idx][date_col]).strip()
        if vac_col and vac_col in df_sorted.columns:
            vcode = str(df_sorted.iloc[idx][vac_col]).strip()
        # Transform set to something pick_resource understands: keep set
        def pick_with_set(required: Set[str], date_str: Optional[str], vacation: Optional[str]) -> str:
            # Build candidate list using intersect logic
            if required:
                candidates = []
                for r in resources:
                    comps = res_to_comp.get(r, set())
                    if not comps:
                        candidates.append(r)  # all-rounder
                    elif comps.intersection(required):
                        candidates.append(r)
            else:
                candidates = list(resources)
            # Apply program availability filter and load balancing using pick_resource core
            # Reuse pick_resource logic by passing one arbitrary code from required (or None)
            any_code = next(iter(required)) if required else None
            # Temporarily narrow global resources to candidates
            saved = list(resources)
            try:
                nonlocal_resources = candidates
            except Exception:
                nonlocal_resources = None
            # Inline simplified selection using same mechanics
            # Order by per-day load
            key_date = date_str or ""
            # Filter by program
            if prog_index and date_str:
                cand2 = []
                for r in candidates:
                    has_day = any((rr == r and dd == date_str) for rr, dd, _ in prog_index)
                    ok = False
                    if vacation:
                        ok = (r, date_str, vacation) in prog_index
                    ok = ok or (has_day and not vacation)
                    if ok:
                        cand2.append(r)
                if cand2:
                    candidates = cand2
            if not candidates:
                candidates = saved
            candidates.sort(key=lambda r: loads.get((r, key_date), 0))
            chosen = candidates[0]
            cap_key = (chosen, key_date)
            if max_assign_per_resource_per_day and loads.get(cap_key, 0) >= max_assign_per_resource_per_day:
                for r in candidates:
                    if loads.get((r, key_date), 0) < max_assign_per_resource_per_day:
                        chosen = r
                        break
            loads[cap_key] = loads.get(cap_key, 0) + 1
            return chosen

        assigned.append(pick_with_set(req_set, dstr, vcode))
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
