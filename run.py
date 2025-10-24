#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Algorithm module loaded by the app to assign tasks to resources.

Implements the logic you provided:
- Detect target dates from 'Jour' in tacheslignes
- Build availability by shift from Pointage
- Validate competencies from competence
- Order tasks by priorities from priorite
- Respect a per‑resource daily cap

Encoding: windows-1252, separator: ';'
The app overrides the module constants below before calling assign_tasks().
"""

from __future__ import annotations

import re
import os
import io
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from typing import Dict, List, Optional, Tuple

import pandas as pd

# Defaults (overridden by the app before calling assign_tasks)
TACHES_PATH = 'tacheslignes.csv'
POINTAGE_PATH = 'pointage.csv'
COMPETENCE_PATH = 'competence.csv'
PRIORITE_PATH = 'priorite.csv'
OUTPUT_PATH = 'TachesLignes_assigne.csv'  # keep ASCII filename for portability
BACKUP_FMT = 'TachesLignes_backup_{ts}.csv'
ENCODING = 'windows-1252'
SEP = ';'

# Max assignments per resource per day (change as needed)
MAX_ASSIGN_PER_RESOURCE_PER_DAY = 1


def _norm_name(n: object) -> str:
    if pd.isna(n):
        return ''
    return re.sub(r"\s+", " ", str(n).strip()).upper()


def _detect_date_from_value(v: object) -> Optional[datetime.date]:
    if pd.isna(v):
        return None
    s = str(v)
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        d, mo, y = m.groups()
        try:
            return datetime(int(y), int(mo), int(d)).date()
        except Exception:
            return None
    dt = pd.to_datetime(s, dayfirst=True, errors='coerce')
    if pd.isna(dt):
        return None
    return dt.date()


def _norm_row_date(v: object) -> Optional[datetime.date]:
    try:
        return _detect_date_from_value(v)
    except Exception:
        return None


def load_competences(path: str = COMPETENCE_PATH) -> Dict[str, set]:
    """Return mapping NORMALIZED_EMPLOYEE_NAME -> set of competence codes.

    Heuristics:
    - employee column: contains 'emp', 'nom', 'employ'
    - competence columns: any column containing 'comp' or 'qual', excluding those with 'type'
    Multiple columns and multiple codes per cell are supported (split by non-alnum).
    """
    d: Dict[str, set] = {}
    try:
        df = pd.read_csv(path, encoding=ENCODING, sep=SEP)
    except Exception:
        return d

    emp_col = next((c for c in df.columns if any(k in str(c).lower() for k in ('emp', 'nom', 'employ'))), None)
    comp_cols = [c for c in df.columns if (('comp' in str(c).lower() or 'qual' in str(c).lower()) and 'type' not in str(c).lower())]

    if emp_col is None:
        if len(df.columns) >= 1:
            emp_col = df.columns[0]
        else:
            return d
    if not comp_cols:
        if len(df.columns) >= 2:
            comp_cols = [df.columns[1]]
        else:
            return d

    for _, r in df.iterrows():
        try:
            nom = _norm_name(r[emp_col])
            if not nom or nom.upper() == 'NAN':
                continue
            for c in comp_cols:
                raw = r.get(c, '')
                if pd.isna(raw):
                    continue
                # split on non-alphanumeric
                codes = set(re.findall(r"[A-Za-z0-9]+", str(raw).upper()))
                if codes:
                    d.setdefault(nom, set()).update(codes)
        except Exception:
            continue
    return d


def load_priorites(path: str = PRIORITE_PATH) -> Dict[str, int]:
    """Map competence code -> priority (int, lower is higher priority)."""
    pr: Dict[str, int] = {}
    try:
        df = pd.read_csv(path, encoding=ENCODING, sep=SEP)
    except Exception:
        return pr
    key_col = next((c for c in df.columns if any(k in str(c).lower() for k in ('nom', 'code', 'comp'))), None)
    val_col = next((c for c in df.columns if any(k in str(c).lower() for k in ('prior', 'val', 'niveau'))), None)
    if key_col is None or val_col is None:
        if len(df.columns) >= 2:
            key_col, val_col = df.columns[0], df.columns[1]
        else:
            return pr
    for _, r in df.iterrows():
        try:
            k = str(r[key_col]).strip().upper()
            v = r[val_col]
            if not k or pd.isna(v):
                continue
            try:
                v = int(v)
            except Exception:
                try:
                    v = int(float(v))
                except Exception:
                    continue
            pr[k] = v
        except Exception:
            continue
    return pr


def build_available_from_pointage(pointage_path: str = POINTAGE_PATH, target_date: Optional[datetime.date] = None) -> Tuple[Dict[str, List[str]], Dict[str, str]]:
    """Return (shift -> [normalized resources], normalized->original name mapping) for the target date."""
    try:
        df = pd.read_csv(pointage_path, encoding=ENCODING, sep=SEP)
    except Exception:
        return {}, {}

    # Detect columns
    date_col = next((c for c in df.columns if 'date' in str(c).lower()), None)
    res_cols = [c for c in df.columns if ('ressource' in str(c).lower() or str(c).lower() in ('res', 'resource', 'employe', 'employé', 'nom', 'nom prénom', 'nom_prénom'))]
    shift_col = next((c for c in df.columns if (str(c).lower() in ('shift', 'nom shift', 'nom_shift') or 'shift' in str(c).lower() or 'vacation' in str(c).lower())), None)
    if date_col is None or not res_cols or shift_col is None:
        return {}, {}

    def _score_res_col(col) -> float:
        cnt, total = 0, 0
        for v in df[col].dropna().head(200):
            s = str(v)
            total += 1
            if re.search(r"[A-Za-zÀ-ÖØ-öø-ÿ]", s):
                cnt += 1
        return cnt / total if total else 0

    res_col = max(res_cols, key=_score_res_col)
    df[date_col] = pd.to_datetime(df[date_col], dayfirst=True, errors='coerce')
    df['__date_only'] = df[date_col].dt.date

    if target_date is None:
        dates = df['__date_only'].dropna().unique()
        target_date = dates[0] if len(dates) > 0 else None
    if target_date is None:
        return {}, {}

    df_day = df[df['__date_only'] == target_date]
    shift_to_resources: Dict[str, List[str]] = defaultdict(list)
    norm_to_orig: Dict[str, str] = {}
    for _, r in df_day.iterrows():
        try:
            shift = str(r[shift_col]).strip()
            res = str(r[res_col]).strip()
            if not shift or not res or shift.lower() == 'nan' or res.lower() == 'nan':
                continue
            n = _norm_name(res)
            norm_to_orig.setdefault(n, res)
            if n not in shift_to_resources[shift]:
                shift_to_resources[shift].append(n)
        except Exception:
            continue
    return dict(shift_to_resources), norm_to_orig


def assign_tasks(max_assign_per_resource_per_day: int = MAX_ASSIGN_PER_RESOURCE_PER_DAY,
                 start_date: Optional[str] = None,
                 end_date: Optional[str] = None,
                 return_bytes: bool = False) -> str | bytes:
    # Load tasks
    df_t = pd.read_csv(TACHES_PATH, encoding=ENCODING, sep=SEP)

    # Normalize per-row dates
    if 'Jour' not in df_t.columns:
        raise ValueError("Colonne 'Jour' manquante dans tacheslignes")
    df_t['__date_only'] = df_t['Jour'].apply(_norm_row_date)

    # Build list of dates to process
    if start_date and end_date:
        # Accept 'dd/mm/YYYY' or ISO 'YYYY-mm-dd'
        def _to_date(s: str) -> datetime.date:
            try:
                return datetime.strptime(s, '%d/%m/%Y').date()
            except Exception:
                return datetime.fromisoformat(s).date()
        dt_start = _to_date(start_date)
        dt_end = _to_date(end_date)
        dates = []
        cur = dt_start
        while cur <= dt_end:
            dates.append(cur)
            cur = cur + timedelta(days=1)
    else:
        uniques = sorted([d for d in df_t['__date_only'].unique() if d is not None])
        if not uniques:
            raise ValueError('Impossible de détecter des dates dans tacheslignes')
        dates = uniques

    emp_to_comps = load_competences(COMPETENCE_PATH)
    priorites = load_priorites(PRIORITE_PATH)

    # Build competence->resources reverse map
    comp_to_emps: Dict[str, List[str]] = defaultdict(list)
    for emp, comps in emp_to_comps.items():
        for c in comps:
            comp_to_emps[c].append(emp)

    # Identify qualif columns
    qual_keys = [c for c in df_t.columns if str(c).strip().lower().startswith('qualif')]

    def _qual_list_for_row(row) -> List[str]:
        quals: List[str] = []
        for k in qual_keys:
            v = row.get(k, '')
            if pd.isna(v):
                continue
            parts = re.findall(r"[A-Za-z0-9]+", str(v).upper())
            quals.extend(parts)
        return [q for q in quals if q]

    def _task_priority(quals: List[str]) -> int:
        if not quals:
            return 999
        vals = [priorites[q] for q in quals if q in priorites]
        return min(vals) if vals else 999

    # Prepare output container
    df_out = df_t.copy()
    if 'Ressource_affectee' not in df_out.columns:
        df_out['Ressource_affectee'] = ''

    total_assigned = 0

    for target in dates:
        # Availability for target date
        shift_to_resources, norm_to_orig = build_available_from_pointage(POINTAGE_PATH, target)

        # Build tasks for target
        tasks: List[Dict] = []
        for idx, row in df_t[df_t['__date_only'] == target].iterrows():
            quals = _qual_list_for_row(row)
            # Detect shift/vacation column
            shift = None
            for col in ['Vacation', 'Shift', 'Nom Shift', 'Nom_Shift']:
                if col in df_t.columns:
                    shift = row.get(col)
                    break
            if shift is None:
                for c in df_t.columns:
                    if ('vac' in str(c).lower()) or ('shift' in str(c).lower()):
                        shift = row.get(c)
                        break
            shift = '' if pd.isna(shift) else str(shift).strip()
            prio = _task_priority(quals)
            tasks.append({'idx': idx, 'quals': quals, 'shift': shift, 'prio': prio})

        # Sort by priority then input order
        tasks.sort(key=lambda x: (x['prio'], x['idx']))

        # Per-day counters
        assigned_counts: Counter = Counter()
        rr_pointers: Dict[Tuple[Tuple[str, ...], str], int] = defaultdict(int)
        assigned_local: Dict[int, str] = {}

        progress = True
        passes = 0
        max_passes = 10
        while progress and passes < max_passes:
            progress = False
            passes += 1
            for t in tasks:
                if t['idx'] in assigned_local:
                    continue
                quals = t['quals']
                shift = t['shift']

                # Determine required qualifications
                uniq_quals = [q for q in dict.fromkeys(quals)]
                need_all = set(uniq_quals)
                require_all = len(need_all) >= 2  # AND logic when multiple qualifs present
                if not require_all:
                    if uniq_quals and priorites:
                        ranked = sorted([(priorites.get(q, 999), q) for q in uniq_quals])
                        target_code = ranked[0][1]
                        need = [target_code]
                    else:
                        need = uniq_quals
                else:
                    need = uniq_quals

                # Build candidate resources
                possible_resources: List[str] = []
                if require_all:
                    # Resource must have ALL required qualifs
                    for e, comps in emp_to_comps.items():
                        if need_all.issubset(comps):
                            if shift and shift in shift_to_resources:
                                if e in shift_to_resources[shift]:
                                    possible_resources.append(e)
                            else:
                                if any(e in lst for lst in shift_to_resources.values()):
                                    possible_resources.append(e)
                else:
                    # Single-qualif path (or none)
                    for q in need:
                        emps = comp_to_emps.get(q, [])
                        for e in emps:
                            if shift and shift in shift_to_resources:
                                if e in shift_to_resources[shift]:
                                    possible_resources.append(e)
                            else:
                                for lst in shift_to_resources.values():
                                    if e in lst:
                                        possible_resources.append(e)

                # Unique and respect per-resource/day cap
                seen = set()
                candidates: List[str] = []
                for r in possible_resources:
                    if r not in seen:
                        seen.add(r)
                        candidates.append(r)
                candidates = [c for c in candidates if assigned_counts[c] < max_assign_per_resource_per_day]

                if not candidates:
                    continue

                key_rr = (tuple(need), shift)
                ptr = rr_pointers[key_rr] % len(candidates)
                chosen = candidates[ptr]
                rr_pointers[key_rr] = (rr_pointers[key_rr] + 1) % (len(candidates) or 1)

                assigned_local[t['idx']] = chosen
                assigned_counts[chosen] += 1
                progress = True

        # Write local assignments
        for idx, res in assigned_local.items():
            orig = norm_to_orig.get(res, res)
            df_out.at[idx, 'Ressource_affectee'] = orig
        total_assigned += len(assigned_local)

    # Emit result
    if return_bytes:
        buf = io.StringIO()
        df_out.to_csv(buf, index=False, sep=SEP, encoding=ENCODING)
        data = buf.getvalue().encode(ENCODING, errors='ignore')
        return data
    # Otherwise backup and write to disk
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_name = BACKUP_FMT.format(ts=ts)
    try:
        pd.read_csv(TACHES_PATH, encoding=ENCODING, sep=SEP).to_csv(backup_name, index=False, sep=SEP, encoding=ENCODING)
    except Exception:
        pass
    df_out.to_csv(OUTPUT_PATH, index=False, sep=SEP, encoding=ENCODING)
    return OUTPUT_PATH


if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--start', help='start date dd/mm/YYYY or YYYY-mm-dd', default=None)
    p.add_argument('--end', help='end date dd/mm/YYYY or YYYY-mm-dd', default=None)
    p.add_argument('--max', help='max assignments per resource per day', type=int, default=MAX_ASSIGN_PER_RESOURCE_PER_DAY)
    args = p.parse_args()
    print(assign_tasks(max_assign_per_resource_per_day=args.max, start_date=args.start, end_date=args.end))
