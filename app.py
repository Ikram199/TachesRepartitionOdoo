import os
import json
from flask import Flask, request, jsonify, render_template, send_file, redirect, url_for, flash, session
from dotenv import load_dotenv

from db import (
    get_engine, test_connection, ensure_database_exists, set_current_database,
    get_server_engine, qualify_db_name, is_allowed_db, db_namespace,
    is_allowed_department, allowed_departments, derive_departments_from_databases
)
from models import init_db
from loader import load_csv_to_mysql, add_parent_to_path
from loader import sanitize_identifier as _sanitize_id
from loader import ingest_file as ingest_single_file
from loader import load_department_bundle, CSV_LOGICAL, resolve_dept_csv_paths, get_headers_for_logical, fill_ressource_by_ligne, fill_tachessepare_from_assign, uploads_dir_base


# Load environment variables: prefer .env, fallback to config.env if present
_base_dir = os.path.dirname(__file__)
_loaded = load_dotenv(os.path.join(_base_dir, ".env"), override=False)
if not _loaded:
    load_dotenv(os.path.join(_base_dir, "config.env"), override=False)

add_parent_to_path()
try:
    import run as assign_module
except Exception as e:
    assign_module = None
    ASSIGN_IMPORT_ERROR = str(e)
else:
    ASSIGN_IMPORT_ERROR = None


app = Flask(__name__, static_folder="static", template_folder="templates")
# Configure secret key for sessions/flash; read from env or use a dev default
app.secret_key = (
    os.environ.get("FLASK_SECRET_KEY")
    or os.environ.get("SECRET_KEY")
    or "dev-change-me"
)
# Ensure JSON responses keep accents (UTF-8) instead of ASCII escaping
app.config['JSON_AS_ASCII'] = False
# -------------------- Auth --------------------
def _allowed_path(path: str) -> bool:
    # Public endpoints
    if path.startswith('/static'):
        return True
    if path in {'/login', '/health'}:
        return True
    return False

@app.before_request
def require_login():
    if _allowed_path(request.path):
        return
    if not session.get('auth'):
        return redirect(url_for('login', next=request.path))

@app.get('/')
def login():
    return render_template('login.html')

@app.post('/login')
def do_login():
    user = request.form.get('username', '').strip()
    pwd = request.form.get('password', '').strip()
    u_cfg = os.environ.get('ADMIN_USER', 'admin')
    p_cfg = os.environ.get('ADMIN_PASSWORD', 'admin')
    if user == u_cfg and pwd == p_cfg:
        session['auth'] = user
        flash('Connexion réussie', 'success')
        nxt = request.args.get('next') or url_for('index')
        return redirect(nxt)
    flash('Identifiants invalides', 'error')
    return redirect(url_for('login'))

@app.get('/logout')
def logout():
    session.clear()
    flash('Déconnecté', 'success')
    return redirect(url_for('login'))


def _truthy(s: str | None) -> bool:
    if s is None:
        return False
    return str(s).strip().lower() in {"1", "true", "yes", "on"}


def file_only_mode() -> bool:
    # When true, UI should hide DB features and routes may no-op for DB
    return _truthy(os.environ.get("FILE_ONLY")) or _truthy(os.environ.get("DISABLE_DB"))

def single_db_mode() -> bool:
    # In single-db mode, we never create/switch per-department databases
    return _truthy(os.environ.get("SINGLE_DB"))

@app.get("/health")
def health():
    if file_only_mode():
        db_status = "disabled"
    else:
        try:
            test_connection()
            db_status = "ok"
        except Exception as e:
            db_status = f"error: {e}"
    return jsonify({
        "status": "ok",
        "db": db_status,
        "assign_import_error": ASSIGN_IMPORT_ERROR,
    })


@app.get("/")
def index():
    if not session.get("auth"): 
        return redirect(url_for("login"))
    return render_template("index.html", file_only=file_only_mode())
# -------------------- New DB-first flow --------------------

def _db_folder(dbname: str) -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(here, 'uploads', 'databases', dbname)


def _short_dept_from_db(dbname: str) -> str:
    ns = db_namespace()
    pref = f"{ns}_"
    low = (dbname or '').lower()
    if low.startswith(pref):
        return dbname[len(pref):]
    return dbname


def _resolve_db_csv_paths(dbname: str) -> dict:
    base = _db_folder(dbname)
    os.makedirs(base, exist_ok=True)
    paths = {}
    for logical, _default in CSV_LOGICAL:
        paths[logical] = os.path.join(base, f"{logical}.csv")
    return paths


@app.get("/databases")
def ui_databases():
    # List user databases under namespace
    try:
        eng = get_server_engine()
        names = []
        with eng.connect() as conn:
            res = conn.exec_driver_sql("SHOW DATABASES")
            for (dbname,) in res:
                names.append(dbname)
        system = {"information_schema", "mysql", "performance_schema", "sys"}
        user_dbs = [n for n in names if n not in system and is_allowed_db(n)]
        user_dbs = sorted(user_dbs)
    except Exception as e:
        user_dbs = []
    # Fallback: if nothing is listed (e.g., shared hosting restricts SHOW DATABASES),
    # propose the configured MYSQL_DB so the user can proceed.
    if not user_dbs:
        fallback_db = os.environ.get("MYSQL_DB")
        if fallback_db:
            user_dbs = [fallback_db]
    return render_template("databases.html", databases=user_dbs, namespace=db_namespace())


@app.post("/databases/select")
def db_select():
    name = request.form.get('db') or (request.get_json(silent=True) or {}).get('db')
    if not name:
        flash("Choisissez une base", "error")
        return redirect(url_for('ui_databases'))
    # Ensure folder exists for this DB
    os.makedirs(_db_folder(name), exist_ok=True)
    return redirect(url_for('ui_db_csv', db=name))


@app.get("/databases/<db>/csv")
def ui_db_csv(db: str):
    paths = _resolve_db_csv_paths(db)
    rows = []
    for logical, _ in CSV_LOGICAL:
        p = paths.get(logical)
        rows.append({
            'logical': logical,
            'path': p if os.path.exists(p) else '',
            'exists': os.path.exists(p),
        })
    return render_template("db_csv.html", db=db, files=rows, paths=paths)


@app.post("/databases/<db>/csv/upload")
def db_csv_upload(db: str):
    logical = request.args.get('type') or request.form.get('type')
    if not logical or logical not in dict(CSV_LOGICAL):
        return jsonify({"ok": False, "error": "Type invalide"}), 400
    if 'file' not in request.files:
        return jsonify({"ok": False, "error": "Aucun fichier"}), 400
    f = request.files['file']
    if not f.filename:
        return jsonify({"ok": False, "error": "Nom vide"}), 400
    folder = _db_folder(db)
    os.makedirs(folder, exist_ok=True)
    save_path = os.path.join(folder, f"{logical}.csv")
    f.save(save_path)  # overwrite
    flash(f"Fichier '{logical}' importé pour {db}", "success")
    return redirect(url_for('ui_db_csv', db=db))


@app.post("/databases/<db>/csv/load")
def db_csv_load(db: str):
    # Set current DB and ingest only present CSVs in its folder
    try:
        ensure_database_exists(db)
        set_current_database(db)
        engine = get_engine()
        paths = _resolve_db_csv_paths(db)
        dept = _short_dept_from_db(db)
        results = []
        for logical, _ in CSV_LOGICAL:
            p = paths.get(logical)
            if not (p and os.path.exists(p)):
                results.append({
                    'file': logical,
                    'table': logical,
                    'status': 'missing',
                    'rows': 0,
                    'message': 'no path',
                    'departement': dept,
                })
                continue
            try:
                res = ingest_single_file(engine, p, table_override=logical, table_prefix=None, department=dept)
                results.append(res)
            except Exception as e:
                results.append({'file': os.path.basename(p), 'table': logical, 'status': 'error', 'rows': 0, 'message': str(e), 'departement': dept})
        ok = all(r.get('status') == 'loaded' for r in results if r.get('status') != 'missing')
        return render_template("db_csv.html", db=db, files=[{'logical': l, 'path': paths[l], 'exists': os.path.exists(paths[l])} for l, _ in CSV_LOGICAL], paths=paths, results=results, ok=ok)
    except Exception as e:
        flash(f"Erreur: {e}", "error")
        return redirect(url_for('ui_db_csv', db=db))


@app.post("/databases/<db>/assign")
def db_assign(db: str):
    # Run assignment using files from the DB's folder
    if assign_module is None:
        return jsonify({"ok": False, "error": f"Cannot import run.py: {ASSIGN_IMPORT_ERROR}"}), 500
    folder = _db_folder(db)
    paths = {logical: os.path.join(folder, f"{logical}.csv") for logical, _ in CSV_LOGICAL}
    required = ['tacheslignes', 'pointage', 'competence', 'priorite']
    missing = [k for k in required if not os.path.exists(paths[k])]
    if missing:
        return jsonify({"ok": False, "error": "Fichiers manquants", "missing": missing, "folder": folder}), 400
    try:
        assign_module.TACHES_PATH = paths['tacheslignes']
        assign_module.POINTAGE_PATH = paths['pointage']
        assign_module.COMPETENCE_PATH = paths['competence']
        assign_module.PRIORITE_PATH = paths['priorite']
        # Optional program file\n        # Force a stable ASCII filename to avoid OS/encoding surprises
        assign_module.OUTPUT_PATH = os.path.join(folder, 'TachesLignes_assigne.csv')
        assign_module.BACKUP_FMT = os.path.join(folder, 'TachesLignes_backup_{ts}.csv')
        max_per = request.form.get('max') or (request.get_json(silent=True) or {}).get('max')
        if max_per is None:
            max_per = assign_module.MAX_ASSIGN_PER_RESOURCE_PER_DAY
        else:
            max_per = int(max_per)
        assign_module.assign_tasks(max_assign_per_resource_per_day=max_per)
        return jsonify({"ok": True, "db": db, "output": assign_module.OUTPUT_PATH})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/databases/<db>/tachessepare/fill")
def db_fill_tachessepare(db: str):
    # Fill tachessepare.csv's ressource column using assignment in the same folder
    try:
        folder = _db_folder(db)
        ts_path = os.path.join(folder, 'tachessepare.csv')
        # Robustly locate assignment file in the folder
        candidates = [
            'TachesLignes_assigne.csv',
            'TachesLignes_assigne.csv',
            'TachesLignes_assigne.csv',
            'TachesLignes_assign.csv',
            'TachesLignes_assignÇ¸.csv',
        ]
        asg_path = None
        for name in candidates:
            p = os.path.join(folder, name)
            if os.path.exists(p):
                asg_path = p
                break
        if not os.path.exists(ts_path):
            return jsonify({"ok": False, "error": "tachessepare.csv introuvable"}), 404
        if not asg_path:
            return jsonify({"ok": False, "error": "Fichier d'assignation introuvable"}), 404
        import pandas as pd, io
        df_ts = pd.read_csv(ts_path, encoding='windows-1252', sep=';')
        df_asg = pd.read_csv(asg_path, encoding='windows-1252', sep=';')
        df_out = fill_tachessepare_from_assign(df_ts, df_asg)
        buf = io.StringIO()
        df_out.to_csv(buf, index=False, sep=';', encoding='windows-1252')
        data = buf.getvalue().encode('windows-1252', errors='ignore')
        return send_file(io.BytesIO(data), mimetype='text/csv; charset=windows-1252', as_attachment=True, download_name='tachessepare_filled.csv')
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/databases/<db>/download/assign")
def db_download_assign(db: str):
    """Télécharger le fichier d'assignation gÃ©nÃ©rÃ© pour cette base."""
    folder = _db_folder(db)
    # Try common assignment filenames within the DB folder
    candidates = [
        'TachesLignes_assigne.csv',
        'TachesLignes_assigne.csv',
        'TachesLignes_assigne.csv',
        'TachesLignes_assign.csv',
        'TachesLignes_assignÇ¸.csv',
    ]
    path = None
    for name in candidates:
        p = name if os.path.isabs(name) else os.path.join(folder, name)
        if os.path.exists(p) and os.path.isfile(p):
            path = p
            break
    if not path:
        return jsonify({"ok": False, "error": "Fichier d'assignation introuvable", "folder": folder}), 404
    try:
        return send_file(path, mimetype='text/csv; charset=windows-1252', as_attachment=True, download_name=os.path.basename(path))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/databases/<db>/download/<logical>")
def db_download_logical(db: str, logical: str):
    """Télécharger un CSV logique depuis le dossier de la base."""
    allowed = set(dict(CSV_LOGICAL).keys()) | { 'tachessepare_filled' }
    if logical not in allowed:
        return jsonify({"ok": False, "error": "Type invalide"}), 400
    folder = _db_folder(db)
    # Map filled alias to a concrete file if present
    filename = 'tachessepare_filled.csv' if logical == 'tachessepare_filled' else f"{logical}.csv"
    path = os.path.join(folder, filename)
    if not os.path.exists(path):
        return jsonify({"ok": False, "error": "Fichier introuvable", "path": path}), 404
    try:
        return send_file(path, mimetype='text/csv; charset=windows-1252', as_attachment=True, download_name=os.path.basename(path))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/databases/<db>/csv/template")
def db_csv_template(db: str):
    logical = request.args.get('type')
    try:
        rows = int(request.args.get('rows', '5'))
    except Exception:
        rows = 5
    if not logical or logical not in dict(CSV_LOGICAL):
        return jsonify({"ok": False, "error": "Type invalide"}), 400
    # Try to infer headers from existing CSV in the DB folder, otherwise return only an informational first line
    import io, csv
    folder = _db_folder(db)
    path = os.path.join(folder, f"{logical}.csv")
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=';')
    try:
        if os.path.exists(path):
            import pandas as pd
            df = pd.read_csv(path, encoding='windows-1252', sep=';', nrows=0)
            headers = list(df.columns)
            if headers:
                w.writerow(headers)
                for _ in range(max(0, rows)):
                    w.writerow([''] * len(headers))
            else:
                w.writerow([f"No headers available for {logical}"])
        else:
            w.writerow([f"No headers available for {logical}"])
    except Exception:
        w.writerow([f"No headers available for {logical}"])
    data = buf.getvalue().encode('windows-1252', errors='ignore')
    from flask import Flask, request, jsonify, render_template, send_file, redirect, url_for, flash, session
    return _send(io.BytesIO(data), mimetype='text/csv; charset=windows-1252', as_attachment=True, download_name=f"{logical}_modele.csv")


@app.post("/load-csvs")
@app.get("/load-csvs")
def load_csvs():
    ensure_database_exists()
    engine = get_engine()
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    prefix = request.args.get("prefix")
    results = load_csv_to_mysql(engine, base_dir=base_dir, table_prefix=prefix)
    ok = all(r.get("status") == "loaded" for r in results if r.get("status") != "missing")
    return jsonify({
        "ok": ok,
        "results": results,
        "prefix": prefix or "",
    })


@app.post("/assign")
@app.get("/assign")
def run_assign():
    if assign_module is None:
        return jsonify({
            "ok": False,
            "error": f"Cannot import run.py: {ASSIGN_IMPORT_ERROR}",
        }), 500

    if request.method == 'GET':
        start = request.args.get("start")
        end = request.args.get("end")
        max_per = request.args.get("max")
    else:
        data = request.get_json(silent=True) or {}
        start = data.get("start")
        end = data.get("end")
        max_per = data.get("max")
    try:
        if max_per is not None:
            max_per = int(max_per)
        else:
            max_per = assign_module.MAX_ASSIGN_PER_RESOURCE_PER_DAY
    except Exception:
        return jsonify({"ok": False, "error": "Invalid max (must be integer)"}), 400

    try:
        assign_module.assign_tasks(max_assign_per_resource_per_day=max_per,
                                   start_date=start,
                                   end_date=end)
        return jsonify({"ok": True, "message": "assignment complete"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/download")
@app.get("/download/assigned")
@app.get("/download/assignment")
@app.get("/download/tacheslignes_assigne.csv")
def download_assigned():
    # Determine likely locations of the output file
    filename = None
    expected_name = None
    if assign_module is not None and getattr(assign_module, "OUTPUT_PATH", None):
        expected_name = assign_module.OUTPUT_PATH
    else:
        expected_name = "TachesLignes_assigne.csv"

    here = os.path.dirname(os.path.abspath(__file__))
    parent = os.path.abspath(os.path.join(here, os.pardir))
    dept = request.args.get('dept')
    candidates = []
    if dept:
        dept_dir = os.path.join(uploads_dir_base(), 'departments', dept)
        candidates.append(os.path.join(dept_dir, expected_name))
    candidates.extend([
        os.path.join(here, expected_name),
        os.path.join(parent, expected_name),
    ])
    for p in candidates:
        if os.path.exists(p) and os.path.isfile(p):
            filename = p
            break
    if not filename:
        return jsonify({"ok": False, "error": "Fichier non trouvé"}), 404

    try:
        return send_file(
            filename,
            mimetype="text/csv; charset=windows-1252",
            as_attachment=True,
            download_name=os.path.basename(filename),
        )
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/init-db")
@app.get("/init-db")
def init_db_route():
    try:
        ensure_database_exists()
        engine = get_engine()
        init_db(engine)
        # Return current tables to confirm
        from sqlalchemy import inspect
        insp = inspect(engine)
        return jsonify({
            "ok": True,
            "message": "Tables created (if not exist)",
            "tables": insp.get_table_names(),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/db/tables")
def db_tables():
    try:
        ensure_database_exists()
        engine = get_engine()
        from sqlalchemy import inspect, text
        insp = inspect(engine)
        tables = insp.get_table_names()
        counts = {}
        with engine.connect() as conn:
            for t in tables:
                try:
                    res = conn.execute(text(f"SELECT COUNT(*) FROM `{t}`"))
                    counts[t] = int(list(res)[0][0])
                except Exception as e:
                    counts[t] = f"error: {e}"
        # Expose minimal DB info (no password)
        import os as _os
        info = {
            "host": _os.environ.get("MYSQL_HOST"),
            "port": _os.environ.get("MYSQL_PORT"),
            "user": _os.environ.get("MYSQL_USER"),
            "db": _os.environ.get("MYSQL_DB"),
            "namespace": db_namespace(),
        }
        return jsonify({"ok": True, "db": info, "tables": counts})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/tables")
def ui_tables():
    ensure_database_exists()
    engine = get_engine()
    from sqlalchemy import inspect, text
    insp = inspect(engine)
    rows = []
    for t in insp.get_table_names():
        try:
            with engine.connect() as conn:
                cnt = conn.execute(text(f"SELECT COUNT(*) FROM `{t}`")).scalar() or 0
        except Exception:
            cnt = "?"
        rows.append({"name": t, "count": cnt})
    return render_template("tables.html", tables=rows)


@app.get("/table/<name>")
def ui_table_view(name: str):
    ensure_database_exists()
    engine = get_engine()
    page = max(int(request.args.get("page", 1)), 1)
    size = max(min(int(request.args.get("size", 50)), 500), 1)
    offset = (page - 1) * size
    from sqlalchemy import text, inspect
    insp = inspect(engine)
    if name not in insp.get_table_names():
        flash(f"Table inconnue: {name}", "error")
        return redirect(url_for("ui_tables"))
    with engine.connect() as conn:
        cols = [c['name'] for c in insp.get_columns(name)]
        total = conn.execute(text(f"SELECT COUNT(*) FROM `{name}`")).scalar() or 0
        rs = conn.execute(text(f"SELECT * FROM `{name}` LIMIT :lim OFFSET :off"), {"lim": size, "off": offset})
        rows = [dict(r._mapping) for r in rs]
    pages = max((total + size - 1) // size, 1)
    return render_template("table_view.html", name=name, cols=cols, rows=rows, page=page, size=size, total=total, pages=pages)


@app.get("/export/<name>.csv")
def export_table(name: str):
    ensure_database_exists()
    engine = get_engine()
    import io, csv
    from sqlalchemy import text, inspect
    insp = inspect(engine)
    if name not in insp.get_table_names():
        return jsonify({"ok": False, "error": "Table inconnue"}), 404
    output = io.StringIO()
    with engine.connect() as conn:
        cols = [c['name'] for c in insp.get_columns(name)]
        writer = csv.writer(output, delimiter=';')
        writer.writerow(cols)
        rs = conn.execute(text(f"SELECT * FROM `{name}`"))
        for r in rs:
            writer.writerow([r._mapping.get(c, "") for c in cols])
    output.seek(0)
    return send_file(io.BytesIO(output.getvalue().encode('windows-1252', errors='ignore')),
                     mimetype="text/csv; charset=windows-1252",
                     as_attachment=True,
                     download_name=f"{name}.csv")


@app.get("/upload")
def ui_upload():
    return render_template("upload.html")


@app.post("/upload")
def upload_post():
    ensure_database_exists()
    engine = get_engine()
    if 'file' not in request.files:
        flash("Aucun fichier fourni", "error")
        return redirect(url_for('ui_upload'))

    # Nouveau flux: sauvegarde orientÃƒÂ©e dÃƒÂ©partement et ingestion
    f = request.files['file']
    if not f.filename:
        flash("Nom de fichier vide", "error")
        return redirect(url_for('ui_upload'))

    table_name = request.form.get('table') or _sanitize_id(os.path.splitext(f.filename)[0])
    table_name = _sanitize_id(table_name) if table_name else None
    prefix = request.form.get('prefix')
    dept = request.form.get('dept')
    logical = request.form.get('type')

    here = os.path.dirname(__file__)
    base_uploads = os.path.join(here, 'uploads')
    save_dir = base_uploads
    save_name = f.filename

    try:
        if dept and is_allowed_department(dept):
            save_dir = os.path.join(base_uploads, 'departments', dept)
            os.makedirs(save_dir, exist_ok=True)
            valid_types = set(dict(CSV_LOGICAL).keys())
            if logical and logical in valid_types:
                save_name = f"{logical}.csv"
    except Exception:
        save_dir = base_uploads

    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, save_name)
    f.save(save_path)

    try:
        res = ingest_single_file(
            engine,
            save_path,
            table_override=table_name,
            table_prefix=prefix,
            department=(dept if dept else None),
        )
        final_table = res.get('table') or (
            f"{_sanitize_id(prefix)}_{table_name}" if prefix and table_name else (
                table_name or _sanitize_id(os.path.splitext(f.filename)[0])
            )
        )
        flash(f"Ingestion OK: {res}", "success")
        return redirect(url_for('ui_table_view', name=final_table))
    except Exception as e:
        flash(f"Erreur ingestion: {e}", "error")
        return redirect(url_for('ui_upload'))


# Mode simple: choisir/crÃƒÂ©er la base du dÃƒÂ©partement et charger tous les CSV
@app.post("/simple/load")
@app.get("/simple/load")
def simple_load():
    try:
        payload = request.get_json(silent=True) or {}
        dept = request.args.get("dept") or payload.get("dept")
        if not dept:
            return jsonify({"ok": False, "error": "Paramètre 'dept' requis"}), 400
        if single_db_mode():
            qname = os.environ.get("MYSQL_DB") or ""
            engine = get_engine()
        else:
            qname = qualify_db_name(dept)
            ensure_database_exists(qname)
            set_current_database(qname)
            engine = get_engine()
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
        # Ingestion stricte depuis le dossier du département (évite mélanges)
        results = load_department_bundle(engine, department=dept, table_prefix=None, base_dir=base_dir, strict=True)
        ok = all(r.get("status") == "loaded" for r in results if r.get("status") != "missing")
        return jsonify({
            "ok": ok,
            "db": qname,
            "namespace": db_namespace(),
            "departement": dept,
            "results": results,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# Departments management pages
@app.get("/departments")
def ui_departments():
    # List allowed departments from config or derive from db-list
    try:
        allowed = allowed_departments()
    except Exception:
        allowed = []
    if not allowed:
        derived = derive_departments_from_databases()
    else:
        derived = []
    depts = allowed or derived
    return render_template("departments.html", departments=depts, namespace=db_namespace())


@app.get("/db/derive-departments")
def db_derive_departments():
    try:
        depts = derive_departments_from_databases()
        return jsonify({"ok": True, "namespace": db_namespace(), "departments": depts})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/departments/<dept>/csv")
def ui_department_csv(dept: str):
    if not is_allowed_department(dept):
        flash("Département non autorisé", "error")
        return redirect(url_for('ui_departments'))
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    # Ensure the department upload directory exists so uploads work immediately (respects UPLOADS_DIR)
    dept_dir = os.path.join(uploads_dir_base(), 'departments', dept)
    os.makedirs(dept_dir, exist_ok=True)
    paths = resolve_dept_csv_paths(dept, base_dir=base_dir, strict=True)
    return render_template("department_csv.html", dept=dept, files=CSV_LOGICAL, paths=paths, file_only=file_only_mode(), single_db=single_db_mode())


@app.post("/departments/<dept>/csv/upload")
def department_csv_upload(dept: str):
    if not is_allowed_department(dept):
        return jsonify({"ok": False, "error": "Département non autorisé"}), 403
    logical = request.args.get('type') or request.form.get('type')
    if not logical or logical not in dict(CSV_LOGICAL):
        return jsonify({"ok": False, "error": "Type invalide"}), 400
    if 'file' not in request.files:
        return jsonify({"ok": False, "error": "Aucun fichier"}), 400
    f = request.files['file']
    if not f.filename:
        return jsonify({"ok": False, "error": "Nom vide"}), 400
    uploads_dir = os.path.join(uploads_dir_base(), 'departments', dept)
    os.makedirs(uploads_dir, exist_ok=True)
    save_path = os.path.join(uploads_dir, f"{logical}.csv")
    f.save(save_path)
    flash(f"Fichier '{logical}' importé pour {dept}", "success")
    return redirect(url_for('ui_department_csv', dept=dept))


@app.post("/departments/<dept>/csv/load")
def department_csv_load(dept: str):
    if not is_allowed_department(dept):
        return jsonify({"ok": False, "error": "Département non autorisé"}), 403
    if single_db_mode():
        engine = get_engine()
    else:
        ensure_database_exists(qualify_db_name(dept))
        set_current_database(qualify_db_name(dept))
        engine = get_engine()
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    try:
        results = load_department_bundle(engine, department=dept, table_prefix=None, base_dir=base_dir, strict=True)
        ok = all(r.get('status') == 'loaded' for r in results if r.get('status') != 'missing')
        flash("Chargement effectué", "success" if ok else "error")
        return render_template("department_csv.html", dept=dept, files=CSV_LOGICAL, paths=resolve_dept_csv_paths(dept, base_dir=base_dir, strict=True), results=results)
    except Exception as e:
        flash(f"Erreur: {e}", "error")
        return redirect(url_for('ui_department_csv', dept=dept))

@app.get("/departments/open")
def departments_open():
    name = request.args.get('dept')
    if not name:
        flash("Saisissez un nom de département", "error")
        return redirect(url_for('ui_departments'))
    if not is_allowed_department(name):
        flash("DÇ¸partement non autorisÇ¸", "error")
        return redirect(url_for('ui_departments'))
    return redirect(url_for('ui_department_csv', dept=name))


@app.post("/departments/<dept>/assign")
@app.get("/departments/<dept>/assign")
def department_assign(dept: str):
    if not is_allowed_department(dept):
        return jsonify({"ok": False, "error": "Département non autorisé"}), 403
    if assign_module is None:
        return jsonify({"ok": False, "error": f"Cannot import run.py: {ASSIGN_IMPORT_ERROR}"}), 500
    # Compute department-specific file paths (strict: no fallback to root)
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    dept_dir = os.path.join(uploads_dir_base(), 'departments', dept)
    os.makedirs(dept_dir, exist_ok=True)
    required = {
        'tacheslignes': os.path.join(dept_dir, 'tacheslignes.csv'),
        'pointage': os.path.join(dept_dir, 'pointage.csv'),
        'competence': os.path.join(dept_dir, 'competence.csv'),
        'priorite': os.path.join(dept_dir, 'priorite.csv'),
    }
    missing = [k for k, p in required.items() if not os.path.exists(p)]
    if missing:
        return jsonify({
            "ok": False,
            "error": "Fichiers manquants dans le dossier du département",
            "dept": dept,
            "dir": dept_dir,
            "missing": missing,
            "expected": required,
        }), 400
    # Map logicals to run.py constants
    taches = required['tacheslignes']
    pointage = required['pointage']
    competence = required['competence']
    priorite = required['priorite']
    output = os.path.join(dept_dir, getattr(assign_module, 'OUTPUT_PATH', 'TachesLignes_assignÇ¸.csv'))
    backup_fmt = getattr(assign_module, 'BACKUP_FMT', 'TachesLignes_backup_{ts}.csv')
    # Override module-level paths for this call
    try:
        assign_module.TACHES_PATH = taches
        assign_module.POINTAGE_PATH = pointage
        assign_module.COMPETENCE_PATH = competence
        assign_module.PRIORITE_PATH = priorite
        assign_module.OUTPUT_PATH = output
        # Parse optional params
        if request.method == 'GET':
            start = request.args.get('start'); end = request.args.get('end'); max_per = request.args.get('max')
        else:
            data = request.get_json(silent=True) or {}; start = data.get('start'); end = data.get('end'); max_per = data.get('max')
        if max_per is None:
            max_per = assign_module.MAX_ASSIGN_PER_RESOURCE_PER_DAY
        else:
            max_per = int(max_per)
        assign_module.assign_tasks(max_assign_per_resource_per_day=max_per, start_date=start, end_date=end)
        return jsonify({"ok": True, "dept": dept, "output": output})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/departments/<dept>/csv/template")
def department_csv_template(dept: str):
    if not is_allowed_department(dept):
        return jsonify({"ok": False, "error": "Département non autorisé"}), 403
    logical = request.args.get('type')
    try:
        rows = int(request.args.get('rows', '5'))
    except Exception:
        rows = 5
    if not logical or logical not in dict(CSV_LOGICAL):
        return jsonify({"ok": False, "error": "Type invalide"}), 400
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    headers = get_headers_for_logical(dept, logical, base_dir=base_dir)
    # Build CSV content
    import io, csv
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=';')
    if headers:
        w.writerow(headers)
        for _ in range(max(0, rows)):
            w.writerow([''] * len(headers))
    else:
        # no headers known; write just a comment-like first line
        w.writerow([f"No headers available for {logical}"])
    data = buf.getvalue().encode('windows-1252', errors='ignore')
    from flask import Flask, request, jsonify, render_template, send_file, redirect, url_for, flash, session
    return send_file(io.BytesIO(data), mimetype='text/csv; charset=windows-1252', as_attachment=True, download_name=f"{logical}_modele.csv")


@app.get("/departments/<dept>/csv/fill-download")
def department_csv_fill_download(dept: str):
    if not is_allowed_department(dept):
        return jsonify({"ok": False, "error": "Département non autorisé"}), 403
    logical = request.args.get('type', 'tachessepare')
    if logical not in dict(CSV_LOGICAL):
        return jsonify({"ok": False, "error": "Type invalide"}), 400
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    paths = resolve_dept_csv_paths(dept, base_dir=base_dir)
    path = paths.get(logical)
    if not path or not os.path.exists(path):
        return jsonify({"ok": False, "error": f"Fichier introuvable: {logical}"}), 404
    try:
        import io
        import pandas as pd
        df = pd.read_csv(path, encoding='windows-1252', sep=';')
        source = request.args.get('source')
        if source == 'assign':
            # Load assignment file from the department upload folder
            dept_dir = os.path.join(uploads_dir_base(), 'departments', dept)
            candidates = [
                'TachesLignes_assigne.csv',
                'TachesLignes_assigne.csv',
                'TachesLignes_assign.csv',
                'TachesLignes_assigne.csv',
                'TachesLignes_assignÇ¸.csv',
            ]
            assign_path = None
            for name in candidates:
                p = os.path.join(dept_dir, name)
                if os.path.exists(p):
                    assign_path = p
                    break
            if not assign_path:
                return jsonify({"ok": False, "error": "Fichier d'assignation introuvable", "folder": dept_dir}), 404
            df_asg = pd.read_csv(assign_path, encoding='windows-1252', sep=';')
            df2 = fill_tachessepare_from_assign(df, df_asg)
        else:
            df2 = fill_ressource_by_ligne(df)
        buf = io.StringIO()
        df2.to_csv(buf, index=False, sep=';', encoding='windows-1252')
        data = buf.getvalue().encode('windows-1252', errors='ignore')
        from flask import Flask, request, jsonify, render_template, send_file, redirect, url_for, flash, session
        suffix = 'filled_assign' if source == 'assign' else 'filled'
        return send_file(io.BytesIO(data), mimetype='text/csv; charset=windows-1252', as_attachment=True, download_name=f"{logical}_{suffix}.csv")
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/db/create-and-init")
@app.get("/db/create-and-init")
def db_create_and_init():
    try:
        name = request.args.get("name") if request.method == 'GET' else (request.get_json(silent=True) or {}).get("name")
        if not name:
            return jsonify({"ok": False, "error": "Paramètre 'name' requis"}), 400
        if not is_allowed_department(name):
            return jsonify({"ok": False, "error": "Département non autorisé"}), 403
        qname = qualify_db_name(name)
        ensure_database_exists(qname)
        set_current_database(qname)
        engine = get_engine()
        init_db(engine)
        from sqlalchemy import inspect
        insp = inspect(engine)
        return jsonify({"ok": True, "db": qname, "namespace": db_namespace(), "tables": insp.get_table_names()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/db/switch")
@app.get("/db/switch")
def db_switch():
    try:
        name = request.args.get("name") if request.method == 'GET' else (request.get_json(silent=True) or {}).get("name")
        if not name:
            return jsonify({"ok": False, "error": "Paramètre 'name' requis"}), 400
        if not is_allowed_department(name):
            return jsonify({"ok": False, "error": "Département non autorisé"}), 403
        qname = qualify_db_name(name)
        set_current_database(qname)
        # Probe connection
        engine = get_engine()
        from sqlalchemy import text
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return jsonify({"ok": True, "db": qname, "namespace": db_namespace()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/db/list")
def db_list():
    try:
        eng = get_server_engine()
        names = []
        with eng.connect() as conn:
            res = conn.exec_driver_sql("SHOW DATABASES")
            for (dbname,) in res:
                names.append(dbname)
        # Filter system schemas and enforce namespace policy
        system = {"information_schema", "mysql", "performance_schema", "sys"}
        user_dbs = [n for n in names if n not in system and is_allowed_db(n)]
        return jsonify({"ok": True, "namespace": db_namespace(), "databases": sorted(user_dbs)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/db/copy-schema")
@app.get("/db/copy-schema")
def db_copy_schema():
    try:
        source = request.args.get("source") if request.method == 'GET' else (request.get_json(silent=True) or {}).get("source")
        target = request.args.get("target") if request.method == 'GET' else (request.get_json(silent=True) or {}).get("target")
        if not source or not target:
            return jsonify({"ok": False, "error": "Paramètres 'source' et 'target' requis"}), 400
        if not (is_allowed_department(source) and is_allowed_department(target)):
            return jsonify({"ok": False, "error": "Départements non autorisés"}), 403
        # Qualify within namespace and validate allowed
        src_q = qualify_db_name(source)
        dst_q = qualify_db_name(target)
        if not (is_allowed_db(src_q) and is_allowed_db(dst_q)):
            return jsonify({"ok": False, "error": "Bases hors namespace interdites"}), 400
        ensure_database_exists(dst_q)
        eng = get_server_engine()
        from sqlalchemy import text
        with eng.begin() as conn:
            # List base tables in source schema
            tables = []
            q = text("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=:src AND TABLE_TYPE='BASE TABLE'")
            for (tname,) in conn.execute(q, {"src": src_q}):
                tables.append(tname)
            created = []
            for t in tables:
                # CREATE TABLE target.t LIKE source.t; (structure only)
                sql = f"CREATE TABLE IF NOT EXISTS `{dst_q}`.`{t}` LIKE `{src_q}`.`{t}`"
                conn.exec_driver_sql(sql)
                created.append(t)
        return jsonify({"ok": True, "source": src_q, "target": dst_q, "namespace": db_namespace(), "tables": created})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/db/allowed")
def db_allowed():
    try:
        return jsonify({"ok": True, "namespace": db_namespace(), "departments": allowed_departments()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/db/drop")
@app.get("/db/drop")
def db_drop():
    try:
        raw = request.args.get("name") if request.method == 'GET' else (request.get_json(silent=True) or {}).get("name")
        if not raw:
            return jsonify({"ok": False, "error": "Paramètre 'name' requis"}), 400
        ns = db_namespace()
        low = raw.strip().lower()
        prefix = f"{ns}_"
        short = raw
        if low.startswith(prefix):
            short = raw[len(prefix):]
        if not is_allowed_department(short):
            return jsonify({"ok": False, "error": "Département non autorisé"}), 403
        qname = qualify_db_name(short)
        # Safety: only allow dropping databases within namespace
        if not is_allowed_db(qname):
            return jsonify({"ok": False, "error": "Base hors namespace interdite"}), 400
        # Execute DROP via server engine (not bound to target DB)
        eng = get_server_engine()
        from sqlalchemy import text
        with eng.begin() as conn:
            conn.execute(text(f"DROP DATABASE IF EXISTS `{qname}`"))
        return jsonify({"ok": True, "dropped": qname})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.post("/departments/<dept>/prepare")
def department_prepare(dept: str):
    if not is_allowed_department(dept):
        return jsonify({"ok": False, "error": "Département non autorisé"}), 403
    try:
        dept_dir = os.path.join(uploads_dir_base(), 'departments', dept)
        os.makedirs(dept_dir, exist_ok=True)
        expected = {logical: os.path.join(dept_dir, f"{logical}.csv") for logical, _ in CSV_LOGICAL}
        exists = {k: os.path.exists(p) for k, p in expected.items()}
        return jsonify({"ok": True, "dept": dept, "directory": dept_dir, "expected": expected, "exists": exists})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == "__main__":
    # Bind to 0.0.0.0 and PORT for PaaS (Railway/Heroku)
    host = os.environ.get("HOST") or os.environ.get("FLASK_HOST") or "0.0.0.0"
    port = int(os.environ.get("PORT") or os.environ.get("FLASK_PORT") or 8080)
    app.run(host=host, port=port)





