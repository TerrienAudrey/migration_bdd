# Migration JSON to PostgreSQL

Projet de migration de données JSON vers une base de données PostgreSQL avec nettoyage et validation des données.

## Prérequis

* Ubuntu/WSL
* PostgreSQL 16.6
* Python 3.10.6
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
python3.10 -m venv .venv
source .venv/bin/activate

# Installation dépendances
pip install -r requirements.txt
```

## Scripts de Nettoyage

### Fonctionnalités Communes

Les schémas de table sont définis à partir de ce [schéma prisma](https://github.com/SandboxDealinka/APP_DEALINKA_BACK/blob/dev_cloe_stock_fix/prisma/schema.prisma).

Chaque script de nettoyage (`clean_*.py`) implémente :

1. Configuration initiale
   - Définition des constantes (SPECIAL_VALUES, TIMESTAMP_FORMAT)
   - Configuration des chemins
   - Configuration du logging

2. Validation préliminaire
   - Vérification existence fichier d'entrée
   - Vérification/création des répertoires de sortie

3. Définition des structures
   - Règles de validation pour chaque champ (type, obligatoire, valeurs par défaut)
   - Structure pour le suivi des problèmes
   - Typage des données

4. Chargement des données
   - Lecture du fichier JSON
   - Gestion des erreurs de parsing

5. Validation des colonnes
   - Identification des colonnes manquantes
   - Journalisation des colonnes manquantes

6. Nettoyage des données (pour chaque entrée)
   a. Validation des chaînes
      - Normalisation des espaces
      - Vérification longueur maximale
      - Détection caractères spéciaux
      - Troncature si nécessaire

   b. Validation des entiers
      - Conversion des types
      - Vérification plage de valeurs
      - Application valeurs par défaut

   c. Validation des dates
      - Parsing du format
      - Normalisation timezone
      - Conversion en format ISO

   d. Validation des tableaux
      - Vérification du type
      - Application tableau vide par défaut

7. Gestion des doublons
   - Détection basée sur clé unique
   - Journalisation des doublons
   - Sélection première occurrence

8. Génération des fichiers de sortie
   - Création fichier données nettoyées
   - Création fichiers d'information par type de problème
   - Horodatage des fichiers

9. Génération des statistiques
   - Comptage par type de problème
   - Comptage entrées uniques
   - Résumé des modifications

10. Étapes cachées
    - Gestion mémoire pour grands fichiers
    - Validation des types à chaque étape
    - Préservation des ID originaux
    - Gestion encodage caractères
    - Maintenance ordre des champs
    - Gestion des exceptions à chaque niveau

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

### Résultats des nettoyages

#### Résumé du traitement transports :
- Entrées transports uniques : 13
- Doublons trouvés : 0
- Colonnes manquantes : 2
- Valeurs NULL corrigées : 0 (voir transport_null_issues.json pour les détails)

#### Résumé du traitement stock_imports :
- Entrées stock_imports uniques : 0
- Doublons trouvés : 477
- Colonnes manquantes : 21
- Valeurs NULL corrigées : 0 (voir stock_import_null_issues.json pour les détails)

#### Résumé du traitement logistic_addresses :
- Unique addresses: 431
- Duplicates: 674
- Missing columns: 6
- NULL values corrected: 3318
- String issues corrected: 0
- Boolean issues corrected: 0

#### Résumé du traitement organizations:
- Entrées organizations uniques : 477
- Doublons trouvés : 0
- Colonnes manquantes : 17
- Valeurs NULL corrigées : 0 (voir organization_null_issues.json pour les détails)

#### Résumé du traitement stocks:
- Entrées stocks uniques : 421
- Doublons trouvés : 0
- Colonnes manquantes : 15
- Valeurs NULL corrigées : 6 (voir stocks_null_issues.json pour les détails)

#### Résumé du traitement companies :
- Unique companies: 267
- Strict duplicates: 6
- Case duplicates: 0
- Missing columns: 9
- NULL values corrected: 33
- String issues corrected: 0

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
