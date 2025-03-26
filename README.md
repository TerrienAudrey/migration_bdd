# Projet de Transformation de Données Multi-Tables

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/downloads/)
[![Pandas](https://img.shields.io/badge/pandas-2.0%2B-green.svg)](https://pandas.pydata.org/)
[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-brightgreen.svg)](https://github.com/username/repo/graphs/commit-activity)

Un système modulaire et hautement configurable pour la transformation, la validation et le nettoyage de données structurées multi-tables.

## 📋 Table des matières

- [Vue d'ensemble](#vue-densemble)
- [Fonctionnalités](#fonctionnalités)
- [Configuration](#configuration)
- [Utilisation](#utilisation)
- [Structure du projet](#structure-du-projet)
- [Architecture de transformation](#architecture-de-transformation)
- [Traitement par table](#traitement-par-table)
- [Gestion des erreurs](#gestion-des-erreurs)
- [Dépannage](#dépannage)
- [Développement](#développement)
- [Licence](#licence)

## Vue d'ensemble

Ce projet propose un cadre complet pour le traitement ETL (Extract, Transform, Load) de données structurées multi-tables. Il est conçu pour valider, nettoyer, normaliser et transformer des données JSON dans une structure définie conforme aux modèles de données cibles.

Le système applique une approche compartimentée où chaque table est traitée indépendamment par des modules dédiés, conformément au principe de responsabilité unique.

## Fonctionnalités

- **Transformations modulaires** : Chaque table dispose de ses propres modules de transformation spécifiques
- **Validation rigoureuse** : Validation structurelle et sémantique à l'entrée et à la sortie
- **Gestion des erreurs** : Rapports d'erreurs détaillés au format Excel
- **Application de correctifs** : Mécanisme flexible pour appliquer des corrections aux données
- **Archivage automatique** : Conservation des versions historiques des données transformées
- **Logs détaillés** : Journalisation complète pour faciliter le débogage et l'audit
- **Extensibilité** : Architecture permettant l'ajout facile de nouvelles tables et règles de transformation

## Configuration

### Prérequis

- Environnement Ubuntu
- Python 3.8 ou supérieur
- pip (gestionnaire de paquets Python)

### Installation

1. Cloner le dépôt
   ```bash
   git clone https://github.com/username/data-transformation-project.git
   cd data-transformation-project
   ```

2. Créer et activer un environnement virtuel (recommandé)
   ```bash
    # Créer un environnement virtuel avec pyenv
    pyenv virtualenv 3.10.6 .0_MIGRATION_BDD

    # Activer automatiquement l'environnement dans ce dossier
    pyenv local .0_MIGRATION_BDD

    # Vérifier que l'environnement est actif
    python --version
   ```

3. Installer les dépendances
   ```bash
   pip install -r requirements.txt
   ```

### Structure des dossiers

Assurez-vous que les dossiers suivants existent ou créez-les :

```bash
mkdir -p data/{raw,clean,archive,patches,error_report} logs
```

## Utilisation

### Exécution simple

Pour transformer toutes les tables disponibles :

```bash
python main.py
```

### Options spécifiques

Pour transformer une table spécifique :

```bash
python main.py --table companies
```

Pour utiliser un fichier d'entrée spécifique :

```bash
python main.py --table stocks --input data/raw/custom_stocks.json
```

Pour désactiver l'archivage automatique :

```bash
python main.py --no-archive
```

### Exemples de flux de travail

1. **Traitement complet par lots :**
   ```bash
   python main.py
   ```
   Cette commande traitera toutes les tables disponibles, archivera les fichiers précédents et générera des rapports d'erreurs le cas échéant.

2. **Traitement d'une seule table avec fichier personnalisé :**
   ```bash
   python main.py --table companies --input data/specific/new_companies.json
   ```

3. **Application de correctifs puis transformation :**
   1. Placez les fichiers de correctifs dans `data/patches/`
   2. Exécutez `python main.py --table companies`

## Structure du projet

```
project_root/
│
├── data/                          # Tous les fichiers de données
│   ├── raw/                       # Données d'entrée brutes
│   ├── clean/                     # Données transformées
│   ├── archive/                   # Versions précédentes archivées
│   ├── patches/                   # Fichiers de correctifs
│   └── error_report/              # Rapports d'erreurs générés
│
├── logs/                          # Journaux d'exécution
│
├── src/                           # Code source
│   ├── tables/                    # Modules spécifiques aux tables
│   │   ├── companies/             # Traitement des données d'entreprises
│   │   ├── logistic_address/      # Traitement des adresses logistiques
│   │   ├── organizations/         # Traitement des organisations
│   │   ├── stock_import/          # Traitement des imports de stocks
│   │   ├── stocks/                # Traitement des stocks
│   │   └── transports/            # Traitement des transports
│   │
│   └── utils/                     # Utilitaires partagés
│       └── logging_manager.py     # Gestionnaire de logs
│
├── main.py                        # Point d'entrée principal
└── requirements.txt               # Dépendances du projet
```

### Détail d'un module de table

Chaque module de table (par exemple, `companies/`) suit la même structure :

```
companies/
├── __init__.py
├── clean_companies.py             # Fonction principale d'orchestration
├── input_structure.py             # Définition de la structure d'entrée
├── output_structure.py            # Définition de la structure de sortie
├── error_reporting/               # Génération de rapports d'erreurs
└── transformations/               # Modules de transformation
    ├── normalize_text.py          # Normalisation du texte
    ├── validate_input_structure.py # Validation de structure
    └── ...                        # Autres transformations spécifiques
```

## Architecture de transformation

Le système utilise une architecture par étapes (pipeline) où chaque table traverse une séquence de transformations :

1. **Validation de structure d'entrée** : Vérifie que les données respectent le schéma attendu
2. **Normalisation de texte** : Standardise les chaînes (majuscules, espaces, etc.)
3. **Normalisation des caractères spéciaux** : Traite les accents, caractères spéciaux, etc.
4. **Validations spécifiques** : Valide les identifiants, codes postaux, etc.
5. **Transformations métier** : Applique les transformations spécifiques à chaque table
6. **Application de correctifs** : Applique les corrections depuis les fichiers de patches
7. **Préparation du modèle final** : Finalize la structure de données
8. **Génération de rapport d'erreurs** : Si des erreurs sont détectées

Chaque étape est implémentée comme une fonction pure qui prend un DataFrame et retourne un DataFrame transformé plus une liste d'erreurs.

## Traitement par table

### Companies

Le module `companies` se concentre sur les données d'entreprises avec des validations spécifiques pour :
- Numéros SIREN et SIRET (algorithme de Luhn)
- Numéros de TVA
- Adresses et codes postaux
- Relations entre identifiants (SIREN dans SIRET, etc.)

### Logistic Address

Le module `logistic_address` traite les adresses logistiques avec :
- Extraction des composants d'adresse (numéro, rue, etc.)
- Validation des codes postaux
- Normalisation des noms de ville
- Gestion des indicateurs d'accessibilité

### Organizations

Le module `organizations` gère les données des organisations avec :
- Validation des numéros RNA
- Validation des adresses
- Normalisation des dénominations

### Stock Import

Le module `stock_import` traite les importations de stock avec :
- Validation des dates
- Traitement des champs JSON
- Validation des types de données

### Stocks

Le module `stocks` gère les données de stocks avec :
- Gestion des commissions
- Vérification des stock_import
- Validation des dates
- Génération de statistiques

### Transports

Le module `transports` traite les données de transport avec :
- Déduplication des identifiants stock_import
- Validation des dénominations
- Normalisation des textes

## Gestion des erreurs

Le système utilise trois niveaux de sévérité pour les erreurs :

- **info** : Modifications informatives (normalisation, etc.)
- **warning** : Problèmes potentiels nécessitant attention
- **error** : Problèmes critiques nécessitant correction

Les erreurs sont collectées tout au long du processus de transformation et compilées dans un rapport Excel structuré contenant :

- Un onglet de résumé avec des statistiques globales
- Des onglets détaillés par catégorie d'erreur
- Les données originales pour référence

## Dépannage

### Problèmes courants

| Problème | Cause possible | Solution |
|----------|----------------|----------|
| `Missing column error` | Structure du fichier d'entrée incorrecte | Vérifiez la structure JSON contre le schéma défini dans `input_structure.py` |
| `SIREN/SIRET validation errors` | Identifiants d'entreprise incorrects | Vérifiez les identifiants et utilisez un fichier de correctifs si nécessaire |
| `File not found` | Chemin de fichier incorrect | Vérifiez que les fichiers existent dans les emplacements attendus |
| `Import error` | Problème de dépendance ou chemin Python | Vérifiez que toutes les dépendances sont installées et que la structure de dossiers est correcte |

### Génération de correctifs

Pour créer un fichier de correctifs pour des données incorrectes :

1. Identifiez les entrées à corriger dans le rapport d'erreurs
2. Créez un fichier JSON dans `data/patches/` avec le format approprié
   ```json
   [
     {
       "co_id": 123,
       "patches": {
         "co_siren": "123456789",
         "co_siret": "12345678901234"
       }
     }
   ]
   ```
3. Nommez le fichier selon la convention `<table>_<type>.json` (ex: `companies_siret_manquant.json`)
4. Exécutez à nouveau le processus de transformation

## Développement

### Principes de codage

Ce projet suit ces principes de développement :

- **Responsabilité unique** : Chaque module/fichier a une seule responsabilité
- **Immuabilité** : Les fonctions ne modifient pas leurs entrées mais retournent de nouvelles valeurs
- **Type hints** : Annotations de type strictes pour tous les paramètres et retours
- **Documentation** : Docstrings complets pour toutes les fonctions et modules
- **Gestion explicite des erreurs** : Capture et journalisation de toutes les exceptions

### Ajout d'une nouvelle table

Pour ajouter une nouvelle table :

1. Créez un nouveau dossier sous `src/tables/` (ex: `new_table/`)
2. Implémentez la structure standard avec les fichiers minimaux requis :
   - `__init__.py`
   - `clean_new_table.py`
   - `input_structure.py`
   - `output_structure.py`
   - `transformations/validate_input_structure.py`
   - `error_reporting/generate_error_report.py`
3. Ajoutez l'import dans `main.py`

### Exécution des tests

Des tests unitaires seront ajoutés dans une future version.

## Licence

Ce projet est sous licence [LICENSE]. Voir le fichier LICENSE pour plus de détails.

---

Développé par Audrey TERRIEN/ Dealinka 

Pour toute question ou support, contactez [audrey.terrien@dealinka.com]