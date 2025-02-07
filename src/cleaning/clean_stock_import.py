# import des bibliothèques standards
import json
from collections import defaultdict
from datetime import datetime

def process_stock_imports_data(input_file, output_directory, output_info_directory):
    # Structure de sortie pour les données nettoyées
    output_data = {
        "stock_imports": [],          # Entrées uniques avec toutes les colonnes
    }

    # Structure de sortie pour les informations
    output_info = {
        "stock_import_duplicates": [],      # Entrées en double
        "stock_import_missing_columns": [],  # Colonnes qui n'existaient pas dans le fichier source
        "stock_import_null_issues": []      # Colonnes avec des nulles là où il ne devrait pas en avoir
    }

    # 1. Chargement des données
    print("Chargement des données...")
    with open(input_file, 'r') as file:
        data = json.load(file)
    print(f"Données chargées avec succès : {len(data)} entrées trouvées")

    # 2. Définition des règles de validation et valeurs par défaut pour chaque champ
    field_rules = {
        # Champs String obligatoires
        'si_file': {'type': 'string', 'required': True, 'default': ''},
        'si_filename': {'type': 'string', 'required': True, 'default': ''},
        'si_packaging_method': {'type': 'array', 'required': True, 'default': []},

        # Champs String optionnels
        'si_gpt_file_id': {'type': 'string', 'required': False, 'default': None},
        'si_gpt_thread_matching_id': {'type': 'string', 'required': False, 'default': None},
        'si_gpt_thread_category_id': {'type': 'string', 'required': False, 'default': None},

        # Champs DateTime optionnels
        'si_date_process': {'type': 'datetime', 'required': False, 'default': None},
        'si_date_removal': {'type': 'datetime', 'required': False, 'default': None},
        'si_date_alert_removal': {'type': 'datetime', 'required': False, 'default': None},
        'si_date_delivery': {'type': 'datetime', 'required': False, 'default': None},
        'si_date_alert_delivery': {'type': 'datetime', 'required': False, 'default': None},

        # Champs Numériques obligatoires
        'si_quantity': {'type': 'integer', 'required': True, 'default': 0},
        'si_total_price': {'type': 'float', 'required': True, 'default': 0.0},

        # Champs Numériques optionnels
        'si_quantity_stackable': {'type': 'integer', 'required': False, 'default': None},

        # Champs Boolean avec default false
        'si_is_pallet': {'type': 'boolean', 'required': True, 'default': False},
        'si_is_ready': {'type': 'boolean', 'required': True, 'default': False},
        'si_is_dangerous': {'type': 'boolean', 'required': True, 'default': False},

        # Champs JSON optionnels
        'si_gpt_response_matching_json': {'type': 'json', 'required': False, 'default': None},
        'si_gpt_response_category_json': {'type': 'json', 'required': False, 'default': None},

        # Clés étrangères optionnelles
        'fk_st': {'type': 'integer', 'required': False, 'default': None},
        'fk_la': {'type': 'integer', 'required': False, 'default': None},
        'fk_tra': {'type': 'integer', 'required': False, 'default': None},

        # Champs relations (arrays)
        'items': {'type': 'array', 'required': True, 'default': []},
        'positioning': {'type': 'array', 'required': True, 'default': []}
    }

    # 3. Vérification des colonnes existantes dans le fichier source
    print("\nVérification des colonnes...")
    if len(data) > 0:
        existing_columns = set(data[0].keys())
        all_columns = set(field_rules.keys())
        missing_columns = all_columns - existing_columns

        # Enregistrement des colonnes manquantes
        for column in missing_columns:
            output_info["stock_import_missing_columns"].append({
                "column_name": column,
                "type": field_rules[column]['type'],
                "default_value": field_rules[column]['default']
            })

        if missing_columns:
            print(f"Colonnes manquantes détectées : {len(missing_columns)}")
        else:
            print("Aucune colonne manquante détectée")

    # 4. Détection des doublons (basé sur si_file et si_filename qui devraient être uniques ensemble)
    print("\nAnalyse des doublons...")
    duplicates_dict = defaultdict(list)
    for item in data:
        key = f"{item.get('si_file', '')}_{item.get('si_filename', '')}"
        duplicates_dict[key].append(item)

    duplicate_count = sum(1 for items in duplicates_dict.values() if len(items) > 1)
    if duplicate_count > 0:
        print(f"Doublons détectés : {duplicate_count} entrées")
    else:
        print("Aucun doublon détecté")

    # 5. Traitement des entrées
    print("\nTraitement des entrées...")
    for key, items in duplicates_dict.items():
        if len(items) > 1:
            for item in items:
                processed_item = process_single_item(item, field_rules, output_info)
                output_info["stock_import_duplicates"].append({
                    "duplicate_key": key,
                    "duplicate_count": len(items),
                    "original_data": processed_item
                })
        else:
            processed_item = process_single_item(items[0], field_rules, output_info)
            output_data["stock_imports"].append(processed_item)

    # 6. Sauvegarde des fichiers JSON
    print("\nSauvegarde des fichiers...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Sauvegarde des données nettoyées
    stock_imports_filename = f"{output_directory}/stock_imports_{timestamp}.json"
    with open(stock_imports_filename, 'w', encoding='utf-8') as f:
        json.dump(output_data["stock_imports"], f, ensure_ascii=False, indent=2)
    print(f"Fichier {stock_imports_filename} créé avec {len(output_data['stock_imports'])} entrées")

    # Sauvegarde des informations
    for key, data in output_info.items():
        info_filename = f"{output_info_directory}/{key}_{timestamp}.json"
        with open(info_filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Fichier {info_filename} créé avec {len(data)} entrées")

    return {
        "stock_imports_count": len(output_data["stock_imports"]),
        "duplicates_count": len(output_info["stock_import_duplicates"]),
        "missing_columns_count": len(output_info["stock_import_missing_columns"]),
        "null_issues_count": len(output_info["stock_import_null_issues"])
    }

def process_single_item(item, field_rules, output_info):
    """Traite un élément individuel en appliquant les règles de validation"""
    processed_item = {}

    # Copie de l'ID si présent
    if 'si_id' in item:
        processed_item['si_id'] = item['si_id']

    # Traitement de tous les champs définis
    for field_name, rules in field_rules.items():
        # Si le champ n'existe pas dans les données source
        if field_name not in item:
            processed_item[field_name] = rules['default']
            continue

        value = item.get(field_name)

        # Vérification des champs obligatoires avec valeurs nulles
        if value is None and rules['required']:
            # Stocker l'info dans stock_import_null_issues AVANT modification
            output_info["stock_import_null_issues"].append({
                "si_id": item.get("si_id", "unknown"),
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
    input_file = '../../data/raw_json/stock_import.json'
    output_directory = '../../data/cleaned_json'
    output_info_directory = '../../data/info_cleaned_json'

    try:
        print("Début du traitement des données...")
        results = process_stock_imports_data(input_file, output_directory, output_info_directory)
        print("\nRésumé du traitement :")
        print(f"- Entrées stock_imports uniques : {results['stock_imports_count']}")
        print(f"- Doublons trouvés : {results['duplicates_count']}")
        print(f"- Colonnes manquantes : {results['missing_columns_count']}")
        print(f"- Valeurs NULL corrigées : {results['null_issues_count']} (voir stock_import_null_issues.json pour les détails)")
        print("\nTraitement terminé avec succès !")

    except Exception as e:
        print(f"Une erreur s'est produite : {e}")
