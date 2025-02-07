# Importation des librairies standards
import json
from collections import defaultdict
from datetime import datetime

def process_companies_data(input_file, output_directory, output_info_directory):
    # Structure de sortie pour les données nettoyées
    output_data = {
        "companies": [],          # Entrées uniques avec toutes les colonnes
    }

    # Structure de sortie pour les informations
    output_info = {
        "company_duplicates": [],      # Entrées en double
        "company_missing_columns": [],  # Colonnes qui n'existaient pas dans le fichier source
        "companies_null_issues": []     # Colonnes avec des nulles là où il ne devrait pas en avoir
    }

    # 1. Chargement des données
    with open(input_file, 'r') as file:
        data = json.load(file)

    # 2. Définition des règles de validation et valeurs par défaut pour chaque champ
    field_rules = {
        # Champs String obligatoires (pas de ?)
        'co_business_name': {'type': 'string', 'required': True, 'default': ''},
        'co_legal_form': {'type': 'string', 'required': True, 'default': ''},
        'co_siret': {'type': 'string', 'required': True, 'default': ''},
        'co_siren': {'type': 'string', 'required': True, 'default': ''},
        'co_code_ent': {'type': 'string', 'required': True, 'default': ''},

        # Champs String optionnels (avec ?)
        'co_vat': {'type': 'string', 'required': False, 'default': None},
        'co_head_office_additional_address': {'type': 'string', 'required': False, 'default': None},
        'co_head_office_city': {'type': 'string', 'required': False, 'default': None},
        'co_head_office_number': {'type': 'string', 'required': False, 'default': None},
        'co_head_office_country': {'type': 'string', 'required': False, 'default': None},
        'co_head_office_postal_code': {'type': 'string', 'required': False, 'default': None},
        'co_head_office_street': {'type': 'string', 'required': False, 'default': None},

        # Champs Int obligatoires
        'fk_us': {'type': 'integer', 'required': True, 'default': 0},

        # Champs relations (arrays)
        'stocks': {'type': 'array', 'required': True, 'default': []},
        'logistic_address': {'type': 'array', 'required': True, 'default': []},
        'contacts': {'type': 'array', 'required': True, 'default': []}
    }

    # 3. Vérification des colonnes existantes dans le fichier source
    if len(data) > 0:
        existing_columns = set(data[0].keys())
        all_columns = set(field_rules.keys())
        missing_columns = all_columns - existing_columns

        # Enregistrement des colonnes manquantes
        for column in missing_columns:
            output_info["company_missing_columns"].append({
                "column_name": column,
                "type": field_rules[column]['type'],
                "default_value": field_rules[column]['default']
            })

    # 4. Détection des doublons basée sur co_code_ent qui doit être unique
    duplicates_dict = defaultdict(list)
    for item in data:
        key = item.get('co_code_ent', '')
        duplicates_dict[key].append(item)

    # 5. Traitement des entrées
    for key, items in duplicates_dict.items():
        # Si on a des doublons
        if len(items) > 1:
            for item in items:
                processed_item = process_single_item(item, field_rules, output_info)
                output_info["company_duplicates"].append({
                    "duplicate_key": key,
                    "duplicate_count": len(items),
                    "original_data": processed_item
                })
        else:
            # Entrée unique
            processed_item = process_single_item(items[0], field_rules, output_info)
            output_data["companies"].append(processed_item)

    # 6. Sauvegarde des fichiers JSON
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Sauvegarde des données nettoyées
    companies_filename = f"{output_directory}/companies_{timestamp}.json"
    with open(companies_filename, 'w', encoding='utf-8') as f:
        json.dump(output_data["companies"], f, ensure_ascii=False, indent=2)
    print(f"Fichier {companies_filename} créé avec {len(output_data['companies'])} entrées")

    # Sauvegarde des informations
    for key, data in output_info.items():
        info_filename = f"{output_info_directory}/{key}_{timestamp}.json"
        with open(info_filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Fichier {info_filename} créé avec {len(data)} entrées")

    return {
        "companies_count": len(output_data["companies"]),
        "duplicates_count": len(output_info["company_duplicates"]),
        "missing_columns_count": len(output_info["company_missing_columns"]),
        "null_issues_count": len(output_info["companies_null_issues"])
    }

def process_single_item(item, field_rules, output_info):
    """Traite un élément individuel en appliquant les règles de validation"""
    processed_item = {}

    # Copie de l'ID si présent
    if 'co_id' in item:
        processed_item['co_id'] = item['co_id']

    # Traitement de tous les champs définis
    for field_name, rules in field_rules.items():
        # Si le champ n'existe pas dans les données source
        if field_name not in item:
            processed_item[field_name] = rules['default']
            continue

        value = item.get(field_name)

        # Vérification des champs obligatoires avec valeurs nulles
        if value is None and rules['required']:
            # Stocker l'info dans companies_null_issues AVANT modification
            output_info["companies_null_issues"].append({
                "co_code_ent": item.get("co_code_ent", "unknown"),
                "column_name": field_name,
                "original_value": None,
                "replaced_by": rules['default']
            })
            # Correction dans cleaned_json
            processed_item[field_name] = rules['default']
        else:
            processed_item[field_name] = value

    return processed_item

if __name__ == "__main__":
    # Configuration
    input_file = '../../data/raw_json/companies.json'
    output_directory = '../../data/cleaned_json'
    output_info_directory = '../../data/info_cleaned_json'

    try:
        results = process_companies_data(input_file, output_directory, output_info_directory)
        print("\nRésumé du traitement :")
        print(f"- Entrées companies uniques : {results['companies_count']}")
        print(f"- Doublons trouvés : {results['duplicates_count']}")
        print(f"- Colonnes manquantes : {results['missing_columns_count']}")
        print(f"- Valeurs NULL corrigées : {results['null_issues_count']} (voir companies_null_issues.json pour les détails)")
        print("\nTraitement terminé avec succès !")

    except Exception as e:
        print(f"Une erreur s'est produite : {e}")
