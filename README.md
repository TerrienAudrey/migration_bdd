# Migration JSON to PostgreSQL

Projet de migration de données JSON vers une base de données PostgreSQL avec nettoyage et validation des données.

## Prérequis

* Ubuntu/WSL
* PostgreSQL 16.6
* Python 3.10.6 (lewagon - https://github.com/lewagon/data-setup/blob/master/WINDOWS.md)
* VSCode

## Structure du Projet

```
MIGRATION_BDD [WSL: UBUNTU]
├── .venv/                      # Environnement virtuel Python
├── .vscode/                    # Configuration VSCode
├── data/                       # Données
│   ├── cleaned_json/          # JSON nettoyés
│   │   ├── companies_*.json
│   │   ├── logistic_addresses_*.json
│   │   ├── organizations_*.json
│   │   ├── stock_imports_*.json
│   │   └── stocks_*.json
│   ├── info_cleaned_json/     # Informations de nettoyage
│   │   ├── *_duplicates_*.json
│   │   ├── *_missing_columns_*.json
│   │   └── *_null_issues_*.json
│   ├── raw_json/              # JSON bruts
│   │   ├── companies.json
│   │   ├── logistic_address.json
│   │   ├── organizations.json
│   │   ├── stock_import.json
│   │   └── stocks.json
│   └── transformed_json/      # JSON transformés
├── src/
│   ├── cleaning/              # Scripts de nettoyage
│   │   ├── clean_companies.py
│   │   ├── clean_logistic_adresses.py
│   │   ├── clean_organizations.py
│   │   ├── clean_stock_import.py
│   │   └── clean_stocks.py
│   └── transformation/        # Scripts de transformation
├── .gitignore
├── dealinka_migration.sql
├── LICENSE
├── python-version
└── requirements.txt
```

## Installation

### PostgreSQL

```bash
sudo apt install postgresql postgresql-contrib
sudo systemctl status postgresql
psql --version
```

### Base de données

```sql
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

## Scripts de Nettoyage

### Fonctionnalités Communes

Chaque script de nettoyage (`clean_*.py`) implémente :

1. Validation des données selon un schéma défini
2. Détection et gestion des doublons
3. Gestion des valeurs manquantes
4. Génération de rapports détaillés

### Gestion des Valeurs NULL

* VARCHAR → "."
* INTEGER → 0
* BOOLEAN → false
* Arrays → []
* JSON → null
* DateTime → null (si optionnel)

### Structure des Fichiers de Sortie

Pour chaque type de données, trois fichiers sont générés :

1. Données nettoyées (`cleaned_json/*.json`)
2. Rapports de doublons (`info_cleaned_json/*_duplicates_*.json`)
3. Colonnes manquantes (`info_cleaned_json/*_missing_columns_*.json`)
4. Problèmes de valeurs NULL (`info_cleaned_json/*_null_issues_*.json`)

## Configuration VSCode

### Extensions

1. Désinstaller :
   * SQLTools Database Client

2. Installer :
   * PostgreSQL (Chris Kolkman)

### Configuration PostgreSQL

1. Extensions (Ctrl+Shift+X)
2. Installer "PostgreSQL"
3. Configuration connexion :
   * Host: localhost
   * Database: dealinka_migration
   * User: postgres
   * Port: 5432

### Commandes Utiles VSCode

* `Ctrl+Shift+P` : Ouvrir la palette de commandes
* `PostgreSQL: Add Connection` : Ajouter connexion
* `PostgreSQL: Refresh` : Actualiser vue

## Bonnes Pratiques

1. Gestion des Fichiers
   * Utiliser un fichier .env pour les credentials
   * Ajouter .sql au .gitignore
   * Sauvegarder régulièrement les données nettoyées

2. Validation des Données
   * Vérifier la structure des données avant import
   * Valider les types de données
   * Gérer les cas spéciaux (NULL, doublons)

3. Logging et Monitoring
   * Générer des rapports détaillés
   * Tracer les modifications
   * Surveiller les erreurs

4. Sécurité
   * Vérifier les droits utilisateurs
   * Sécuriser les connexions
   * Protéger les données sensibles
