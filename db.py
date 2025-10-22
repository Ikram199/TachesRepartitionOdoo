import os
from functools import lru_cache
from typing import Optional, List

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# -------------------------------
# Configuration & utilitaires
# -------------------------------

# Base courante choisie par l'app (si None -> on prend MYSQL_DB depuis l'env)
_CURRENT_DB: Optional[str] = None

def db_namespace() -> str:
    """
    Namespace logique pour les bases "par département".
    Par défaut 'dept'. Configurable via APP_NAMESPACE.
    Exemple: APP_NAMESPACE=tasks -> bases autorisées: tasks_<dept>
    """
    return os.environ.get("APP_NAMESPACE", "dept").strip().lower()

def _get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip()

def _mysql_params_from_env():
    """
    Lis les variables d'env MYSQL_*.
    """
    return {
        "host": _get_env("MYSQL_HOST", "sme-ramh.com"),
        "port": int(_get_env("MYSQL_PORT", "3306") or "3306"),
        "user": _get_env("MYSQL_USER", "u385695825_usertasks"),
        "password": _get_env("MYSQL_PASSWORD", "ZaidZwin123"),
        "db": _get_env("MYSQL_DB", "u385695825_demo"),
    }

def _build_sqlalchemy_url(user: str, password: str, host: str, port: int, db: Optional[str]) -> str:
    """
    Construit une URL SQLAlchemy en PyMySQL (recommandé sur PaaS).
    Si db est None, on omet la partie base pour faire un 'server engine'.
    """
    auth = f"{user}:{password}" if password else user
    if db:
        return f"mysql+pymysql://{auth}@{host}:{port}/{db}?charset=utf8mb4"
    else:
        return f"mysql+pymysql://{auth}@{host}:{port}/?charset=utf8mb4"

def qualify_db_name(short: str) -> str:
    """
    Applique le namespace à un nom court de département.
    Ex.: 'prod' -> 'dept_prod' si APP_NAMESPACE=dept
    Si le nom commence déjà par '<ns>_', on le laisse.
    """
    ns = db_namespace()
    low = (short or "").strip().lower()
    pref = f"{ns}_"
    return low if low.startswith(pref) else f"{ns}_{low}"

def is_allowed_db(dbname: str) -> bool:
    """
    Autorise uniquement les bases qui appartiennent au namespace courant.
    """
    if not dbname:
        return False
    return dbname.lower().startswith(f"{db_namespace()}_")

def allowed_departments() -> List[str]:
    """
    Retourne la liste des départements explicitement autorisés,
    via la variable ALLOWED_DEPARTMENTS (séparée par virgules).
    Si non définie, retourne [] -> on pourra dériver dynamiquement.
    """
    raw = os.environ.get("ALLOWED_DEPARTMENTS", "").strip()
    if not raw:
        return []
    return [x.strip().lower() for x in raw.split(",") if x.strip()]

def is_allowed_department(name: str) -> bool:
    """
    Vérifie si un département est autorisé.
    - Si ALLOWED_DEPARTMENTS est vide -> on accepte tous (contrôle par is_allowed_db lors de la création).
    - Sinon, le nom doit apparaître dans ALLOWED_DEPARTMENTS.
    """
    if not name:
        return False
    allow = allowed_departments()
    return True if not allow else name.strip().lower() in allow

# -------------------------------
# Engines (server / database)
# -------------------------------

@lru_cache(maxsize=1)
def _get_server_engine_cached() -> Engine:
    """
    Engine 'serveur' sans base sélectionnée.
    Sert à CREATE DATABASE, SHOW DATABASES, etc.
    Priorité à DATABASE_URL si elle ne contient PAS de base ;
    sinon on reconstruit une URL sans base depuis MYSQL_*.
    """
    url = os.environ.get("DATABASE_URL")
    if url:
        # Si l'URL contient déjà une base, on la retire pour un "server engine".
        # Cas typique : mysql+pymysql://user:pwd@host:port/db -> on veut /?
        try:
            # Parser minimal sans dépendance externe
            # On coupe au dernier '/' pour enlever le nom de base éventuel
            head, _, tail = url.partition("://")
            if "/" in tail:
                before_db, _, after = tail.rpartition("/")
                # Si after contient un '?' c'est bien une DB explicite
                if "?" in after or after:
                    # reconstruit sans /<db> (garde les query params si existants)
                    base = before_db.split("?")[0]
                    q = ""
                    if "?" in tail:
                        q = "?" + tail.split("?", 1)[1]
                    url_no_db = f"{head}://{base}/{q}".replace("//?", "/?")
                    if url_no_db.endswith("/"):
                        return create_engine(url_no_db + "?charset=utf8mb4" if "?" not in url_no_db else url_no_db)
                    return create_engine(url_no_db)
        except Exception:
            # Au moindre doute, on reconstruit depuis MYSQL_*
            url = None

    if not url:
        p = _mysql_params_from_env()
        url = _build_sqlalchemy_url(p["user"], p["password"], p["host"], p["port"], db=None)

    return create_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=1800,
    )

def get_server_engine() -> Engine:
    return _get_server_engine_cached()

def _current_db_name() -> str:
    """
    Renvoie le nom de base 'courant' :
    - _CURRENT_DB si set_current_database() a été appelé
    - sinon MYSQL_DB depuis l'env
    """
    global _CURRENT_DB
    if _CURRENT_DB:
        return _CURRENT_DB
    return _mysql_params_from_env()["db"]

def set_current_database(dbname: str) -> None:
    """
    Définit la base 'courante' pour les futurs get_engine().
    On ne modifie pas les variables d'env ; on reste côté process.
    """
    global _CURRENT_DB
    _CURRENT_DB = dbname

@lru_cache(maxsize=32)
def _make_db_engine_cached(dbname: str) -> Engine:
    """
    Engine lié à une base précise.
    Mis en cache par nom (utile quand on bascule entre bases).
    """
    # 1) DATABASE_URL prioritaire si elle pointe déjà sur la base demandée
    url_env = os.environ.get("DATABASE_URL")
    if url_env:
        # Si l'URL contient un nom de base, on la garde telle quelle :
        # dans ce cas, on ignore dbname param. Sinon, on reconstruit.
        if "/" in url_env.rsplit("@", 1)[-1]:
            # contient probablement '/<db>' -> on utilise tel quel
            url = url_env
        else:
            # URL sans DB -> on ajoute dbname
            try:
                if url_env.endswith("/"):
                    url = url_env + dbname
                else:
                    # si déjà des query params, on insère '/dbname' avant '?'
                    if "?" in url_env:
                        u, q = url_env.split("?", 1)
                        url = f"{u.rstrip('/')}/{dbname}?{q}"
                    else:
                        url = f"{url_env.rstrip('/')}/{dbname}"
            except Exception:
                url = None
    else:
        url = None

    # 2) Sinon, reconstruit depuis MYSQL_*
    if not url:
        p = _mysql_params_from_env()
        url = _build_sqlalchemy_url(p["user"], p["password"], p["host"], p["port"], dbname)

    return create_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=1800,
    )

def get_engine() -> Engine:
    """
    Engine rattaché à la base 'courante'.
    """
    return _make_db_engine_cached(_current_db_name())

# -------------------------------
# Opérations DB de haut niveau
# -------------------------------

def ensure_database_exists(name: Optional[str] = None) -> None:
    """
    Crée la base si elle n'existe pas (charset utf8mb4).
    - name: si None -> on prend la base courante (MYSQL_DB ou set_current_database()).
    - applique la politique de namespace si c'est une base 'département'.
    """
    target = name or _current_db_name()
    if not target:
        raise RuntimeError("No database name provided or configured (MYSQL_DB).")

    # Sécurité namespace : si on essaie de créer une base 'département',
    # elle doit respecter le préfixe <ns>_*
    if "_" in target and not is_allowed_db(target):
        raise ValueError(f"Database '{target}' is outside allowed namespace '{db_namespace()}_*'.")

    eng = get_server_engine()
    sql = text(
        f"CREATE DATABASE IF NOT EXISTS `{target}` "
        "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
    )
    with eng.begin() as conn:
        conn.execute(sql)

def test_connection() -> None:
    """
    Vérifie que la connexion à la base 'courante' fonctionne.
    Lève une exception sinon (capturée dans /health).
    """
    eng = get_engine()
    with eng.connect() as conn:
        conn.execute(text("SELECT 1"))

def derive_departments_from_databases() -> List[str]:
    """
    Parcourt SHOW DATABASES et renvoie la liste des départements
    déduits des bases '<ns>_<dept>'.
    """
    ns = db_namespace()
    pref = f"{ns}_"
    eng = get_server_engine()
    depts: List[str] = []
    with eng.connect() as conn:
        res = conn.exec_driver_sql("SHOW DATABASES")
        for (dbname,) in res:
            d = str(dbname)
            if d.startswith(pref):
                depts.append(d[len(pref):])
    depts.sort()
    return depts
