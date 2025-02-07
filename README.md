# Migration JSON to PostgreSQL
Projet de migration de données JSON vers une base de données PostgreSQL.

## Prérequis
- Ubuntu/WSL
- PostgreSQL 16.6
- Python 3.10.6 (lewagon - https://github.com/lewagon/data-setup/blob/master/WINDOWS.md)
- VSCode

## Installation

### PostgreSQL
```bash
sudo apt install postgresql postgresql-contrib
sudo systemctl status postgresql
psql --version
```

### Base de données
```bash
sudo -u postgres psql
CREATE DATABASE dealinka_migration;
ALTER USER postgres WITH PASSWORD 'your_password';
```

### Environnement Python
```bash
# Création environnement virtuel
python -m venv .venv
source .venv/bin/activate

# Installation dépendances
pip install -r requirements.txt
```

### Fichier requirements.txt
```
psycopg2-binary==2.9.9
python-dotenv==1.0.0
```

## Configuration

### Gestion Utilisateurs PostgreSQL
```sql
-- Création utilisateurs
CREATE USER user1 WITH PASSWORD 'secure_password1';
CREATE USER user2 WITH PASSWORD 'secure_password2';

-- Attribution droits
GRANT ALL PRIVILEGES ON DATABASE dealinka_migration TO user1, user2;

-- Droits sur schéma
\c dealinka_migration
GRANT ALL PRIVILEGES ON SCHEMA public TO user1, user2;
```

### Structure des Tables
```python
# Configuration connexion
conn = psycopg2.connect(
    dbname="dealinka_migration",
    user="postgres",
    password="your_password",
    host="localhost",
    port="5432"
)

# Création tables
tables_creation = """
CREATE TABLE companies (
    co_id SERIAL PRIMARY KEY,
    co_business_name VARCHAR(255) NOT NULL,
    co_siren VARCHAR(25) NOT NULL,
    co_siret VARCHAR(25) NOT NULL,
    co_vat VARCHAR(20),
    co_code_ent VARCHAR(50) UNIQUE,
    co_head_office_address VARCHAR(255),
    cp_head_office_city VARCHAR(255),
    cp_head_office_postal_code VARCHAR(255),
    co_legal_form VARCHAR(100),
    fk_us INTEGER DEFAULT 0,
    "user" INTEGER,
    stocks INTEGER[],
    logistic_address INTEGER[],
    contacts INTEGER[]
);

CREATE TABLE companies_duplicates (
    id SERIAL PRIMARY KEY,
    co_code_ent VARCHAR(50),
    duplicate_count INTEGER,
    original_data JSONB
);

CREATE TABLE companies_null_issues (
    id SERIAL PRIMARY KEY,
    co_code_ent VARCHAR(50),
    field_name VARCHAR(100),
    expected_type VARCHAR(50),
    original_value TEXT,
    replaced_value TEXT
);
"""
```

### Configuration VSCode
1. Installation extension PostgreSQL (Chris Kolkman)
2. Configuration connexion :
   - Host: localhost
   - Database: dealinka_migration
   - User: postgres
   - Port: 5432

## Gestion des Données

### Valeurs NULL
- VARCHAR → "."
- INTEGER → 0
- BOOLEAN → false

### Commandes PostgreSQL Utiles
```bash
# Connexion
sudo -u postgres psql dealinka_migration

# Commandes psql
\dt                   # Liste tables
\du                   # Liste utilisateurs
\d table_name         # Structure table

# Backup/Restore
sudo -u postgres pg_dump dealinka_migration > dealinka_migration.sql
psql -U postgres dealinka_migration < dealinka_migration.sql
```

## Bonnes Pratiques
- Utiliser un fichier .env pour les credentials
- Ajouter .sql au .gitignore
- Vérifier les droits utilisateurs avant accès distant
```



# Troubleshooting Extensions VSCode

## Extensions à Désinstaller
```
SQLTools
Database Client
```

## Extension Recommandée
```
PostgreSQL (Chris Kolkman)
```

## Configuration PostgreSQL Extension
1. Extensions (Ctrl+Shift+X)
2. Installer "PostgreSQL"
3. Connexion :
   - Nom: dealinka_migration
   - Host: localhost
   - Database: dealinka_migration
   - User: postgres
   - Port: 5432

## Points Importants
- Éviter les doublons d'extensions
- Redémarrer VSCode après désinstallation/installation
- Une seule extension de base de données suffit
- Extension PostgreSQL offre visualisation dynamique des tables

## Commandes VSCode
```
Ctrl+Shift+P : Ouvrir la palette de commandes
"PostgreSQL: Add Connection" : Ajouter connexion
"PostgreSQL: Refresh" : Actualiser vue
```
