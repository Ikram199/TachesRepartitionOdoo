App Web – CSV to MySQL + Assignment (underscore folder)

Overview
- Identique à "app web/" mais dans un dossier sans espace pour Windows.
- Ingestion améliorée: toutes les colonnes sont prises en compte pour la déduplication via un `row_hash` (MD5 de toutes les valeurs de colonnes). Les insertions utilisent `INSERT ... ON DUPLICATE KEY UPDATE` pour assurer l'idempotence.
- Endpoints Flask:
  - `GET /health`
  - `POST /load-csvs` (ingestion incrémentale: déduplication par toutes les colonnes)
  - `POST /assign`

Setup
1) Copier `config.example.env` en `.env` et renseigner:
   - `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DB`
2) Installer les dépendances (PowerShell):
   - `py -m pip install -r app_web/requirements.txt`
   - (ou depuis le dossier) `py -m pip install -r requirements.txt`
3) Lancer l'app:
   - `python app.py`
   - ou `python -m flask --app app.py run --port 5050`

Initialiser/Créer dynamiquement la base et les tables
- Dans l'UI, section "Base de données":
  - Saisissez le nom (ex: tasks_asign)
  - Cliquez "Créer la base + tables" → crée la base si absente, la sélectionne, puis crée les tables ORM
- Endpoints:
  - `GET/POST /db/create-and-init?name=tasks_asign`
  - `GET/POST /db/switch?name=tasks_asign` (bascule sans créer)

Initialiser les tables (models ORM)
- Cliquez sur le bouton "Créer les tables (models)" dans l'UI, ou bien:
  - `curl -X POST http://127.0.0.1:5050/init-db`
  - Crée (si absentes) les tables ORM: competence, pointage, priorite, prog, tacheslignes, tachessepare
  - Schéma: `row_hash` (PK), `data` (JSON), `ingested_at` (timestamp)

Espace sécurisé de bases
- Les bases visibles/créées sont limitées au namespace `DB_NAMESPACE` (par défaut: `departement_tasks`).
- Exemple: si vous demandez `dep_a`, la base effective sera `departement_tasks_dep_a`.
- Lister/choisir (`/db/list`) ne renvoie que les bases du namespace.

Usage rapide
- UI: http://127.0.0.1:5050/
- API: `POST /load-csvs`, `POST /assign`, `GET /download`

Notes ingestion (toutes les colonnes)
- Pour chaque CSV, les colonnes sont normalisées (noms sans accents/espaces) et un `row_hash` est calculé à partir de toutes les valeurs de colonnes.
- La table cible est créée/ajustée si besoin (colonnes `TEXT`, clé primaire `row_hash`).
- Les lignes sont insérées avec `ON DUPLICATE KEY UPDATE` pour être idempotentes: 
  - Une ligne identique (mêmes valeurs sur toutes les colonnes) est ignorée.
  - Si une valeur change, son `row_hash` change: la ligne est insérée comme une nouvelle version. Vous pouvez ensuite gérer l'historique via `ingested_at`.

Remarque sur les modèles ORM vs tables "larges"
- Les modèles ORM fournis stockent la ligne brute dans la colonne JSON `data` pour être robustes aux changements de colonnes.
- Le chargeur CSV continue à maintenir des tables "larges" (colonnes explicites) et à les faire évoluer automatiquement.
- Vous pouvez choisir d'utiliser l'un ou l'autre selon vos besoins d'accès et de reporting.
