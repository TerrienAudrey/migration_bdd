import json
from collections import defaultdict
from datetime import datetime

def process_logistic_addresses_data(input_file, output_directory, output_info_directory):
    # Structure de sortie pour les données nettoyées
    output_data = {
        "logistic_addresses": [],     # Entrées uniques avec toutes les colonnes
    }

    # Structure de sortie pour les informations
    output_info = {
        "logistic_address_duplicates": [],      # Entrées en double
        "logistic_address_missing_columns": [],  # Colonnes qui n'existaient pas dans le fichier source
        "logistic_address_null_issues": []      # Colonnes avec des nulles là où il ne devrait pas en avoir
    }

    # 1. Chargement des données
    print("Chargement des données...")
    with open(input_file, 'r') as file:
        data = json.load(file)
    print(f"Données chargées avec succès : {len(data)} entrées trouvées")

    # 2. Définition des règles de validation et valeurs par défaut pour chaque champ
    field_rules = {
        # Champs String obligatoires
        'la_postal_code': {'type': 'string', 'required': True, 'default': ''},

        # Champs String optionnels
        'la_house_number': {'type': 'string', 'required': False, 'default': None},
        'la_street': {'type': 'string', 'required': False, 'default': None},
        'la_city': {'type': 'string', 'required': False, 'default': None},
        'la_additional_address': {'type': 'string', 'required': False, 'default': None},

        # Champs Boolean obligatoires
        'la_truck_access': {'type': 'boolean', 'required': True, 'default': False},
        'la_loading_dock': {'type': 'boolean', 'required': True, 'default': False},
        'la_forklift': {'type': 'boolean', 'required': True, 'default': False},
        'la_pallet': {'type': 'boolean', 'required': True, 'default': False},
        'la_fenwick': {'type': 'boolean', 'required': True, 'default': False},
        'la_isactive': {'type': 'boolean', 'required': True, 'default': True},

        # Champs Integer optionnels
        'la_palet_capacity': {'type': 'integer', 'required': False, 'default': None},
        'la_longitude': {'type': 'integer', 'required': False, 'default': None},
        'la_latitude': {'type': 'integer', 'required': False, 'default': None},

        # Clés étrangères optionnelles avec valeurs par défaut
        'fk_cou': {'type': 'integer', 'required': False, 'default': 1},

        # Clés étrangères optionnelles sans valeur par défaut
        'fk_or': {'type': 'integer', 'required': False, 'default': None},
        'fk_re': {'type': 'integer', 'required': False, 'default': None},
        'fk_co': {'type': 'integer', 'required': False, 'default': None},
        'fk_con': {'type': 'integer', 'required': False, 'default': None},

        # Champs relations (arrays)
        'opening_hour': {'type': 'array', 'required': True, 'default': []},
        'stock_import': {'type': 'array', 'required': True, 'default': []},
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
            output_info["logistic_address_missing_columns"].append({
                "column_name": column,
                "type": field_rules[column]['type'],
                "default_value": field_rules[column]['default']
            })

        if missing_columns:
            print(f"Colonnes manquantes détectées : {len(missing_columns)}")
        else:
            print("Aucune colonne manquante détectée")

    # 4. Détection des doublons (basé sur combinaison d'adresse unique)
    print("\nAnalyse des doublons...")
    duplicates_dict = defaultdict(list)
    for item in data:
        # Création d'une clé unique basée sur les éléments d'adresse
        key = f"{item.get('la_street', '')}_{item.get('la_postal_code', '')}_{item.get('la_city', '')}"
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
                output_info["logistic_address_duplicates"].append({
                    "duplicate_key": key,
                    "duplicate_count": len(items),
                    "original_data": processed_item
                })
        else:
            processed_item = process_single_item(items[0], field_rules, output_info)
            output_data["logistic_addresses"].append(processed_item)

    # 6. Sauvegarde des fichiers JSON
    print("\nSauvegarde des fichiers...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Sauvegarde des données nettoyées
    logistic_addresses_filename = f"{output_directory}/logistic_addresses_{timestamp}.json"
    with open(logistic_addresses_filename, 'w', encoding='utf-8') as f:
        json.dump(output_data["logistic_addresses"], f, ensure_ascii=False, indent=2)
    print(f"Fichier {logistic_addresses_filename} créé avec {len(output_data['logistic_addresses'])} entrées")

    # Sauvegarde des informations
    for key, data in output_info.items():
        info_filename = f"{output_info_directory}/{key}_{timestamp}.json"
        with open(info_filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Fichier {info_filename} créé avec {len(data)} entrées")

    return {
        "logistic_addresses_count": len(output_data["logistic_addresses"]),
        "duplicates_count": len(output_info["logistic_address_duplicates"]),
        "missing_columns_count": len(output_info["logistic_address_missing_columns"]),
        "null_issues_count": len(output_info["logistic_address_null_issues"])
    }

def process_single_item(item, field_rules, output_info):
    """Traite un élément individuel en appliquant les règles de validation"""
    processed_item = {}

    # Copie de l'ID si présent
    if 'la_id' in item:
        processed_item['la_id'] = item['la_id']

    # Traitement de tous les champs définis
    for field_name, rules in field_rules.items():
        # Si le champ n'existe pas dans les données source
        if field_name not in item:
            processed_item[field_name] = rules['default']
            continue

        value = item.get(field_name)

        # Vérification des champs obligatoires avec valeurs nulles
        if value is None and rules['required']:
            # Stocker l'info dans logistic_address_null_issues AVANT modification
            output_info["logistic_address_null_issues"].append({
                "la_id": item.get("la_id", "unknown"),
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
    input_file = '../../data/raw_json/logistic_address.json'
    output_directory = '../../data/cleaned_json'
    output_info_directory = '../../data/info_cleaned_json'

    try:
        print("Début du traitement des données...")
        results = process_logistic_addresses_data(input_file, output_directory, output_info_directory)
        print("\nRésumé du traitement :")
        print(f"- Entrées logistic_addresses uniques : {results['logistic_addresses_count']}")
        print(f"- Doublons trouvés : {results['duplicates_count']}")
        print(f"- Colonnes manquantes : {results['missing_columns_count']}")
        print(f"- Valeurs NULL corrigées : {results['null_issues_count']} (voir logistic_address_null_issues.json pour les détails)")
        print("\nTraitement terminé avec succès !")

    except Exception as e:
        print(f"Une erreur s'est produite : {e}")
