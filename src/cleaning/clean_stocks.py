import json
from collections import defaultdict
from datetime import datetime

def process_stocks_data(input_file, output_directory, output_info_directory):
    # Structure de sortie pour les données nettoyées
    output_data = {
        "stocks": [],          # Entrées uniques avec toutes les colonnes
    }

    # Structure de sortie pour les informations
    output_info = {
        "stock_duplicates": [],      # Entrées en double
        "stock_missing_columns": [],  # Colonnes qui n'existaient pas dans le fichier source
        "stocks_null_issues": []      # Colonnes avec des nulles là où il ne devrait pas en avoir
    }

    # 1. Chargement des données
    with open(input_file, 'r') as file:
        data = json.load(file)

    # 2. Définition des règles de validation et valeurs par défaut pour chaque champ
    field_rules = {
        # Champs String obligatoires (pas de ?)
        'st_io': {'type': 'string', 'required': True, 'default': ''},
        'st_transportby': {'type': 'string', 'required': True, 'default': ''},

        # Champs String optionnels (avec ?)
        'st_commentary': {'type': 'string', 'required': False, 'default': None},
        'st_instructions': {'type': 'string', 'required': False, 'default': None},

        # Champs Float optionnels
        'st_commission': {'type': 'float', 'required': False, 'default': None},

        # Champs Boolean avec default false
        'st_is_freetransport': {'type': 'boolean', 'required': True, 'default': False},
        'st_is_standby': {'type': 'boolean', 'required': True, 'default': False},
        'st_is_taxreceiptdeadline': {'type': 'boolean', 'required': True, 'default': False},
        'st_is_runoffdeadline': {'type': 'boolean', 'required': True, 'default': False},
        'st_is_showorganization': {'type': 'boolean', 'required': True, 'default': False},

        # Champs DateTime optionnels
        'st_use_by_date': {'type': 'datetime', 'required': False, 'default': None},
        'st_creation_date': {'type': 'datetime', 'required': False, 'default': None},
        'st_ending_date': {'type': 'datetime', 'required': False, 'default': None},

        # Champs Int optionnels et obligatoires
        'fk_sta': {'type': 'integer', 'required': True, 'default': 1},
        'fk_us': {'type': 'integer', 'required': False, 'default': None},
        'fk_co': {'type': 'integer', 'required': False, 'default': None},
        'fk_sav': {'type': 'integer', 'required': False, 'default': None},

        # Champs relations (arrays)
        'positioning': {'type': 'array', 'required': True, 'default': []},
        'stock_status_history': {'type': 'array', 'required': True, 'default': []},
        'stock_import': {'type': 'array', 'required': True, 'default': []},
        'stock_sav_history': {'type': 'array', 'required': True, 'default': []}
    }

    # 3. Vérification des colonnes existantes dans le fichier source
    if len(data) > 0:
        existing_columns = set(data[0].keys())
        all_columns = set(field_rules.keys())
        missing_columns = all_columns - existing_columns

        # Enregistrement des colonnes manquantes
        for column in missing_columns:
            output_info["stock_missing_columns"].append({
                "column_name": column,
                "type": field_rules[column]['type'],
                "default_value": field_rules[column]['default']
            })

    # 4. Détection des doublons
    duplicates_dict = defaultdict(list)
    for item in data:
        # Création d'une clé unique basée sur st_io et fk_co
        key = f"{item.get('st_io', '')}_{item.get('fk_co', '')}"
        duplicates_dict[key].append(item)

    # 5. Traitement des entrées
    for key, items in duplicates_dict.items():
        # Si on a des doublons
        if len(items) > 1:
            for item in items:
                processed_item = process_single_item(item, field_rules, output_info)
                output_info["stock_duplicates"].append({
                    "duplicate_key": key,
                    "duplicate_count": len(items),
                    "original_data": processed_item
                })
        else:
            # Entrée unique
            processed_item = process_single_item(items[0], field_rules, output_info)
            output_data["stocks"].append(processed_item)

    # 6. Sauvegarde des fichiers JSON
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Sauvegarde des données nettoyées
    stocks_filename = f"{output_directory}/stocks_{timestamp}.json"
    with open(stocks_filename, 'w', encoding='utf-8') as f:
        json.dump(output_data["stocks"], f, ensure_ascii=False, indent=2)
    print(f"Fichier {stocks_filename} créé avec {len(output_data['stocks'])} entrées")

    # Sauvegarde des informations
    for key, data in output_info.items():
        info_filename = f"{output_info_directory}/{key}_{timestamp}.json"
        with open(info_filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Fichier {info_filename} créé avec {len(data)} entrées")

    return {
        "stocks_count": len(output_data["stocks"]),
        "duplicates_count": len(output_info["stock_duplicates"]),
        "missing_columns_count": len(output_info["stock_missing_columns"])
    }

def process_single_item(item, field_rules, output_info):
    """Traite un élément individuel en appliquant les règles de validation"""
    processed_item = {}

    # Copie de l'ID si présent
    if 'st_id' in item:
        processed_item['st_id'] = item['st_id']

    # Traitement de tous les champs définis
    for field_name, rules in field_rules.items():
        # Si le champ n'existe pas dans les données source
        if field_name not in item:
            processed_item[field_name] = rules['default']
            continue

        value = item.get(field_name)

        # Vérification des champs obligatoires avec valeurs nulles
        if value is None and rules['required']:
            # Stocker l'info dans stocks_null_issues AVANT modification
            output_info["stocks_null_issues"].append({
                "st_id": item.get("st_id", "unknown"),
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
    input_file = '../../data/raw_json/stocks.json'
    output_directory = '../../data/cleaned_json'
    output_info_directory = '../../data/info_cleaned_json'

    try:
        results = process_stocks_data(input_file, output_directory, output_info_directory)
        print("\nRésumé du traitement :")
        print(f"- Entrées stocks uniques : {results['stocks_count']}")
        print(f"- Doublons trouvés : {results['duplicates_count']}")
        print(f"- Colonnes manquantes : {results['missing_columns_count']}")
        print("\nTraitement terminé avec succès !")

    except Exception as e:
        print(f"Une erreur s'est produite : {e}")
