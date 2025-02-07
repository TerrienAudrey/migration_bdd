# Importation des bibliothèques standards
import json
from collections import defaultdict
from datetime import datetime

def process_transports_data(input_file, output_directory, output_info_directory):
    # Structure de sortie pour les données nettoyées
    output_data = {
        "transports": [],          # Entrées uniques avec toutes les colonnes
    }

    # Structure de sortie pour les informations
    output_info = {
        "transport_duplicates": [],      # Entrées en double
        "transport_missing_columns": [],  # Colonnes qui n'existaient pas dans le fichier source
        "transport_null_issues": []      # Colonnes avec des nulles là où il ne devrait pas en avoir
    }

    # 1. Chargement des données
    print("Chargement des données...")
    with open(input_file, 'r') as file:
        data = json.load(file)
    print(f"Données chargées avec succès : {len(data)} entrées trouvées")

    # 2. Définition des règles de validation et valeurs par défaut pour chaque champ
    field_rules = {
        # Champs String obligatoires (@unique)
        'tra_denomination': {'type': 'string', 'required': True, 'default': ''},

        # Champs Integer optionnels
        'fk_con': {'type': 'integer', 'required': False, 'default': None},

        # Champs relations (arrays)
        'deliveries': {'type': 'array', 'required': True, 'default': []},
        'stock_import': {'type': 'array', 'required': True, 'default': []}
    }

    # 3. Vérification des colonnes existantes dans le fichier source
    print("\nVérification des colonnes...")
    if len(data) > 0:
        existing_columns = set(data[0].keys())
        all_columns = set(field_rules.keys())
        missing_columns = all_columns - existing_columns

        # Enregistrement des colonnes manquantes
        for column in missing_columns:
            output_info["transport_missing_columns"].append({
                "column_name": column,
                "type": field_rules[column]['type'],
                "default_value": field_rules[column]['default']
            })

        if missing_columns:
            print(f"Colonnes manquantes détectées : {len(missing_columns)}")
        else:
            print("Aucune colonne manquante détectée")

    # 4. Détection des doublons basée sur tra_denomination qui est @unique
    print("\nAnalyse des doublons...")
    duplicates_dict = defaultdict(list)
    for item in data:
        key = item.get('tra_denomination', '')
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
                output_info["transport_duplicates"].append({
                    "duplicate_key": key,
                    "duplicate_count": len(items),
                    "original_data": processed_item
                })
        else:
            processed_item = process_single_item(items[0], field_rules, output_info)
            output_data["transports"].append(processed_item)

    # 6. Sauvegarde des fichiers JSON
    print("\nSauvegarde des fichiers...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Sauvegarde des données nettoyées
    transports_filename = f"{output_directory}/transports_{timestamp}.json"
    with open(transports_filename, 'w', encoding='utf-8') as f:
        json.dump(output_data["transports"], f, ensure_ascii=False, indent=2)
    print(f"Fichier {transports_filename} créé avec {len(output_data['transports'])} entrées")

    # Sauvegarde des informations
    for key, data in output_info.items():
        info_filename = f"{output_info_directory}/{key}_{timestamp}.json"
        with open(info_filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Fichier {info_filename} créé avec {len(data)} entrées")

    return {
        "transports_count": len(output_data["transports"]),
        "duplicates_count": len(output_info["transport_duplicates"]),
        "missing_columns_count": len(output_info["transport_missing_columns"]),
        "null_issues_count": len(output_info["transport_null_issues"])
    }

def process_single_item(item, field_rules, output_info):
    """Traite un élément individuel en appliquant les règles de validation"""
    processed_item = {}

    # Copie de l'ID si présent
    if 'tra_id' in item:
        processed_item['tra_id'] = item['tra_id']

    # Traitement de tous les champs définis
    for field_name, rules in field_rules.items():
        # Si le champ n'existe pas dans les données source
        if field_name not in item:
            processed_item[field_name] = rules['default']
            continue

        value = item.get(field_name)

        # Vérification des champs obligatoires avec valeurs nulles
        if value is None and rules['required']:
            # Stocker l'info dans transport_null_issues AVANT modification
            output_info["transport_null_issues"].append({
                "tra_id": item.get("tra_id", "unknown"),
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
    input_file = '../../data/raw_json/transports.json'
    output_directory = '../../data/cleaned_json'
    output_info_directory = '../../data/info_cleaned_json'

    try:
        print("Début du traitement des données...")
        results = process_transports_data(input_file, output_directory, output_info_directory)
        print("\nRésumé du traitement :")
        print(f"- Entrées transports uniques : {results['transports_count']}")
        print(f"- Doublons trouvés : {results['duplicates_count']}")
        print(f"- Colonnes manquantes : {results['missing_columns_count']}")
        print(f"- Valeurs NULL corrigées : {results['null_issues_count']} (voir transport_null_issues.json pour les détails)")
        print("\nTraitement terminé avec succès !")

    except Exception as e:
        print(f"Une erreur s'est produite : {e}")
