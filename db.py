import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
import re


def _env(name: str, default: str | None = None) -> str | None:
    return os.environ.get(name, default)


def make_mysql_url() -> str:
    host = _env("MYSQL_HOST", "127.0.0.1")
    port = _env("MYSQL_PORT", "3306")
    user = _env("MYSQL_USER", "root")
    password = _env("MYSQL_PASSWORD", "")
    db = _env("MYSQL_DB", "test")
    return f"mysql+pymysql://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{db}?charset=utf8mb4"


def make_server_url() -> str:
    host = _env("MYSQL_HOST", "127.0.0.1")
    port = _env("MYSQL_PORT", "3306")
    user = _env("MYSQL_USER", "root")
    password = _env("MYSQL_PASSWORD", "")
    return f"mysql+pymysql://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/?charset=utf8mb4"


def get_engine(echo: bool = False):
    url = make_mysql_url()
    engine = create_engine(url, echo=echo, pool_pre_ping=True, pool_recycle=3600)
    return engine


def test_connection():
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    return True


def ensure_database_exists(db_name: str | None = None):
    """Create the target database if it does not exist.

    If db_name is provided, it is qualified within the namespace policy.
    """
    db = qualify_db_name(db_name) if db_name else _env("MYSQL_DB", "test")
    server_engine = create_engine(make_server_url(), pool_pre_ping=True)
    with server_engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{db}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"))
    server_engine.dispose()
    return True


def set_current_database(db_name: str):
    """Override current DB for this process via environment variable.

    Applies namespace qualification policy.
    """
    os.environ["MYSQL_DB"] = qualify_db_name(db_name)
    return True


def get_server_engine(echo: bool = False):
    url = make_server_url()
    return create_engine(url, echo=echo, pool_pre_ping=True, pool_recycle=3600)


def db_namespace() -> str:
    """Return the namespace/prefix used to scope allowed databases."""
    ns = _env("DB_NAMESPACE", "departement_tasks")
    # normalize: lowercase and sanitize
    ns = re.sub(r"[^A-Za-z0-9_]", "_", ns.strip()).lower() or "departement_tasks"
    return ns


def sanitize_db_identifier(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", name.strip())


def qualify_db_name(name: str) -> str:
    """Ensure a DB name is qualified within the namespace.

    If name already starts with '<ns>_' it is returned sanitized.
    Otherwise returns '<ns>_<name>' sanitized.
    """
    if not name:
        return db_namespace()
    ns = db_namespace()
    raw = sanitize_db_identifier(name)
    low = raw.lower()
    prefix = f"{ns}_"
    if low.startswith(prefix):
        return raw
    return f"{ns}_{raw}"


def is_allowed_db(name: str) -> bool:
    low = (name or "").lower()
    return low.startswith(f"{db_namespace()}_")


def allowed_departments() -> list:
    raw = _env("ALLOWED_DEPARTMENTS", "").strip()
    if not raw:
        return []
    items = [sanitize_db_identifier(x) for x in raw.split(",")]
    # drop empties, normalize to lowercase
    items = [x for x in (i.strip() for i in items) if x]
    return sorted({x.lower() for x in items})


def is_allowed_department(dept: str) -> bool:
    allowed = allowed_departments()
    if not allowed:
        return True  # no explicit restriction configured
    return sanitize_db_identifier(dept).lower() in allowed


def derive_departments_from_databases() -> list:
    """Infer department names from existing databases in the namespace.

    Looks at SHOW DATABASES, keeps those starting with '<namespace>_', and
    returns the part after the prefix as department names (sanitized, lowercase).
    """
    ns = db_namespace()
    prefix = f"{ns}_"
    try:
        eng = get_server_engine()
        names = []
        with eng.connect() as conn:
            res = conn.exec_driver_sql("SHOW DATABASES")
            for (dbname,) in res:
                names.append(dbname)
        depts = []
        for n in names:
            if isinstance(n, str) and n.lower().startswith(prefix):
                depts.append(n[len(prefix):])
        # sanitize and dedupe
        depts = [sanitize_db_identifier(d).lower() for d in depts if d]
        return sorted({d for d in depts if d})
    except Exception:
        return []
