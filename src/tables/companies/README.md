# Module de Transformation des Données Companies

Ce module permet de transformer, nettoyer et valider les données d'entreprises à partir d'un fichier JSON d'entrée, indépendamment des autres modules du projet.

## Structure des dossiers

Pour utiliser ce module de manière autonome, créez la structure de dossiers suivante :

```
companies_transformation/
├── data/
│   ├── raw/           # Fichiers JSON d'entrée
│   ├── clean/         # Fichiers JSON transformés
│   ├── patches/       # Correctifs spécifiques
│   └── error_report/  # Rapports d'erreurs Excel
├── logs/              # Logs d'exécution
└── src/
    ├── utils/
    │   ├── __init__.py
    │   └── logging_manager.py
    └── tables/
        ├── __init__.py
        └── companies/
            ├── __init__.py
            ├── clean_companies.py
            ├── input_structure.py
            ├── output_structure.py
            ├── transformations/
            │   ├── __init__.py
            │   ├── validate_input_structure.py
            │   ├── normalize_text.py
            │   ├── normalize_special_chars.py
            │   ├── clean_punctuation.py
            │   ├── validate_id_relationships.py
            │   ├── validate_postal_code.py
            │   ├── split_address.py
            │   ├── patch_data.py
            │   ├── prepare_final_model.py
            │   └── validate_identifiers/
            │       ├── __init__.py
            │       ├── validate_siren.py
            │       ├── validate_siret.py
            │       └── validate_vat.py
            └── error_reporting/
                ├── __init__.py
                └── generate_error_report.py
```

## Configuration

1. Créez un environnement virtuel Python (version ≥ 3.8 recommandée) :
   ```bash
   python -m venv env
   source env/bin/activate  # ou env\Scripts\activate sur Windows
   ```

2. Installez les dépendances :
   ```bash
   pip install pandas numpy jsonschema unidecode openpyxl xlsxwriter
   ```

## Utilisation

### 1. Préparation des données

Placez votre fichier JSON de données Companies dans le dossier `data/raw/`.

Format attendu des données d'entrée :
```json
[
  {
    "co_id": 175,
    "co_business_name": "LUCAS FRANCE",
    "co_siren": "513703389",
    "co_siret": "51370338900017",
    "co_vat": "FR78513703389",
    "co_code_ent": "821747",
    "co_head_office_address": "ZONE ARTISANALE 2 GUILLEME",
    "co_head_office_city": "BAZAS",
    "co_head_office_postal_code": "33430",
    "co_legal_form": "SAS, société par actions simplifiée",
    "fk_us": 0,
    "co_head_office_additional_address": ""
  }
]
```

### 2. Correctifs (facultatif)

Si vous disposez de correctifs spécifiques, créez un fichier `companies_patches.json` dans le dossier `data/patches/` :

```json
[
  {
    "co_id": 175,
    "patches": {
      "co_siren": "513703389",
      "co_business_name": "LUCAS FRANCE SAS"
    }
  }
]
```

### 3. Exécution

Créez un script Python `run_transformation.py` à la racine du projet :

```python
import os
import sys

# Ajouter le répertoire racine au chemin Python
sys.path.append(os.path.abspath('.'))

from src.tables.companies.clean_companies import clean_companies_data

if __name__ == "__main__":
    # Configurer les chemins
    input_file = "data/raw/companies.json"
    output_file = "data/clean/companies_transformed.json"
    
    # Exécuter la transformation
    success, error_report = clean_companies_data(
        input_file_path=input_file,
        output_file_path=output_file
    )
    
    # Afficher le résultat
    if success:
        print(f"Transformation réussie. Fichier de sortie : {output_file}")
        if error_report:
            print(f"Des erreurs ont été détectées. Rapport : {error_report}")
    else:
        print("Échec de la transformation.")
        if error_report:
            print(f"Consultez le rapport d'erreurs : {error_report}")
```

Exécutez le script :
```bash
python run_transformation.py
```

## Transformations appliquées

Le module effectue les transformations suivantes :

1. **Validation de la structure d'entrée**
   - Vérification des champs obligatoires
   - Validation des types de données

2. **Normalisation du texte**
   - Suppression des espaces inutiles
   - Conversion en majuscules
   - Standardisation des espaces

3. **Normalisation des caractères spéciaux**
   - Suppression des accents
   - Standardisation des caractères spéciaux

4. **Nettoyage de la ponctuation**
   - Suppression des signes de ponctuation non significatifs
   - Conservation des formats spéciaux (ex: S.A.R.L.)

5. **Validation des identifiants**
   - SIREN (9 chiffres + algorithme de Luhn)
   - SIRET (14 chiffres + cohérence avec SIREN)
   - Numéro de TVA (format FR + clé + SIREN)

6. **Validation des adresses**
   - Format des codes postaux
   - Décomposition des adresses en numéro et nom de rue

7. **Application des correctifs**
   - Corrections manuelles via fichier de patches

8. **Préparation du modèle final**
   - Ajout des champs de validation
   - Finalisation de la structure

## Gestion des erreurs

Un rapport d'erreurs Excel est généré si des anomalies sont détectées. Il contient :

- Un onglet de résumé
- Des onglets détaillés par type d'erreur
- Un onglet de référence avec les données originales

Les niveaux de sévérité sont :
- **error** : Problème critique nécessitant correction
- **warning** : Problème potentiel à examiner
- **info** : Information sur les transformations effectuées

## Exemple de données de sortie

```json
{
  "co_id": 0,
  "co_business_name": "4HOME",
  "co_siren": "841634835",
  "co_siret": "84163483500018",
  "co_vat": "FR46841634835",
  "co_code_ent": "823494",
  "co_head_office_city": "CAHORS",
  "co_head_office_postal_code": "46000",
  "co_legal_form": "SAS, société par actions simplifiée",
  "fk_us": 0,
  "co_head_office_additional_address": "ZONE INDUSTRIELLE LA BEYNE",
  "co_desactivation_date": null,
  "stocks": [],
  "logistic_address": [],
  "contacts": [],
  "users": [],
  "co_head_office_country": "FRANCE",
  "co_head_office_number": "445",
  "co_head_office_street": "CHE DE BELLE CROIX"
}
```

## Dépannage

1. **ImportError** : Vérifiez que les chemins d'import sont corrects et que tous les fichiers `__init__.py` sont présents.

2. **Erreurs de lecture du fichier JSON** : Vérifiez que le fichier d'entrée est un JSON valide.

3. **Problèmes d'encodage** : Assurez-vous que vos fichiers sont encodés en UTF-8.

## Limitations connues

- Le module est optimisé pour les entreprises françaises. La validation des identifiants internationaux est limitée.
- Les adresses complexes peuvent ne pas être correctement décomposées.