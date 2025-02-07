import json
from collections import defaultdict
from datetime import datetime

def process_companies_data(input_file, output_directory, output_info_directory):
    """
    Traite les données des entreprises avec les étapes suivantes:
    1. Chargement des données et définition des règles
    2. Vérification des colonnes manquantes
    3. Premier nettoyage et normalisation des données
    4. Détection et gestion des doublons stricts
    5. Détection et gestion des doublons après normalisation
    6. Sélection des meilleures entrées
    7. Sauvegarde des résultats
    """
    # Structures de sortie
    output_data = {"companies": []}
    output_info = {
        "company_strict_duplicates": [],      # Doublons exacts
        "company_case_duplicates": [],        # Doublons après normalisation
        "company_missing_columns": [],        # Colonnes manquantes
        "companies_null_issues": [],          # Problèmes de valeurs nulles
        "company_string_issues": []           # Problèmes de chaînes de caractères
        "company_cleaned_cases": []           # Pour tracer les normalisations de casse
    }

    # 1. Chargement des données et définition des règles
    with open(input_file, 'r') as file:
        data = json.load(file)

    field_rules = {
        # Champs String obligatoires
        'co_business_name': {'type': 'string', 'required': True, 'default': ''},
        'co_legal_form': {'type': 'string', 'required': True, 'default': ''},
        'co_siret': {'type': 'string', 'required': True, 'default': ''},
        'co_siren': {'type': 'string', 'required': True, 'default': ''},
        'co_code_ent': {'type': 'string', 'required': True, 'default': ''},

        # Champs String optionnels
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

    # 2. Vérification des colonnes manquantes
    if len(data) > 0:
        existing_columns = set(data[0].keys())
        missing_columns = set(field_rules.keys()) - existing_columns
        for column in missing_columns:
            output_info["company_missing_columns"].append({
                "column_name": column,
                "type": field_rules[column]['type'],
                "default_value": field_rules[column]['default']
            })

    # 3. Premier nettoyage et normalisation
    normalized_data = []
    for item in data:
        processed_item = process_single_item(item, field_rules, output_info)
        normalized_data.append(processed_item)

    # 4. Détection et gestion des doublons stricts
    strict_duplicates_dict = defaultdict(list)
    for item in normalized_data:
        key = item.get('co_code_ent', '')
        if key:  # Ignore les clés vides
            strict_duplicates_dict[key].append(item)

    # Traitement des doublons stricts
    unique_data = []
    for key, items in strict_duplicates_dict.items():
        if len(items) > 1:
            # Log tous les doublons stricts
            for item in items:
                output_info["company_strict_duplicates"].append({
                    "duplicate_key": key,
                    "duplicate_count": len(items),
                    "original_data": item
                })
            # Garde une seule copie pour l'étape suivante
            unique_data.append(items[0])
        else:
            unique_data.append(items[0])

    # 5. Détection des doublons après normalisation
    normalized_dict = defaultdict(list)
    for item in unique_data:
        normalized_key = str(item.get('co_code_ent', '')).lower().strip()
        if normalized_key:  # Ignore les clés vides
            normalized_dict[normalized_key].append(item)

    # 6. Sélection des meilleures entrées
    final_data = []
    for key, items in normalized_dict.items():
        if len(items) > 1:
            # Sélection de l'entrée avec le meilleur formatage
            selected_item = select_best_case_item(items)
            final_data.append(selected_item)

            # Log des entrées ignorées
            for item in items:
                if item != selected_item:
                    output_info["company_case_duplicates"].append({
                        "kept_entry": selected_item.get('co_code_ent'),
                        "removed_entry": item.get('co_code_ent'),
                        "normalized_key": key,
                        "removed_data": item
                    })
        else:
            final_data.append(items[0])

    # 7. Sauvegarde des résultats
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Sauvegarde des données nettoyées
    with open(f"{output_directory}/companies_{timestamp}.json", 'w', encoding='utf-8') as f:
        json.dump(final_data, f, ensure_ascii=False, indent=2)

    # Sauvegarde des informations de nettoyage
    for key, data in output_info.items():
        with open(f"{output_info_directory}/{key}_{timestamp}.json", 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    # Retourne les statistiques
    return {
        "companies_count": len(final_data),
        "strict_duplicates_count": len(output_info["company_strict_duplicates"]),
        "case_duplicates_count": len(output_info["company_case_duplicates"]),
        "missing_columns_count": len(output_info["company_missing_columns"]),
        "null_issues_count": len(output_info["companies_null_issues"]),
        "string_issues_count": len(output_info["company_string_issues"])
    }

def validate_and_clean_value(value, rules, field_name, item, output_info):
    """Valide et nettoie une valeur selon les règles définies"""
    special_values = {".", "x", "X", "/", "0", ","}

    def log_issue(original, replaced, issue_type, mod_type="type_validation"):
        output_info[issue_type].append({
            "co_code_ent": item.get("co_code_ent", "unknown"),
            "column_name": field_name,
            "original_value": original,
            "replaced_by": replaced,
            "modification_type": mod_type
        })

    # Gestion des valeurs nulles
    if value is None:
        if rules['required']:
            log_issue(None, rules['default'], "companies_null_issues")
        return rules['default']

    # Nettoyage selon le type
    if rules['type'] == 'integer':
        if isinstance(value, str):
            try:
                cleaned_value = int(float(value.strip().replace(',', '.')))
                log_issue(value, cleaned_value, "companies_null_issues", "string_to_int_conversion")
                return cleaned_value
            except (ValueError, TypeError):
                log_issue(value, 0, "companies_null_issues", "invalid_type_to_zero")
                return 0
        elif isinstance(value, float):
            return int(value)
        elif not isinstance(value, int):
            return 0

    elif rules['type'] == 'string':
        if not isinstance(value, str):
            value = str(value)
        # Nettoyage des espaces
        cleaned_value = ' '.join(value.split())
        if cleaned_value != value:
            log_issue(value, cleaned_value, "company_string_issues", "space_normalization")
            value = cleaned_value
        # Gestion des caractères spéciaux
        if value in special_values:
            log_issue(value, "", "company_string_issues", "special_char_removal")
            return ""
        return value

    elif rules['type'] == 'array' and not isinstance(value, list):
        log_issue(value, rules['default'], "companies_null_issues", "invalid_type_to_empty_array")
        return rules['default']

    return value

def process_single_item(item, field_rules, output_info):
    """Traite un élément individuel en appliquant les règles de validation"""
    processed_item = {}

    # Copie de l'ID si présent
    if 'co_id' in item:
        processed_item['co_id'] = item['co_id']

    # Traitement de tous les champs définis
    for field_name, rules in field_rules.items():
        if field_name not in item:
            processed_item[field_name] = rules['default']
            continue

        value = item.get(field_name)
        processed_item[field_name] = validate_and_clean_value(
            value, rules, field_name, item, output_info
        )

    return processed_item

def select_best_case_item(items):
    """Sélectionne l'entrée avec le meilleur formatage de casse"""
    def case_score(item):
        score = 0
        for key, value in item.items():
            if isinstance(value, str):
                score += sum(1 for c in value if c.isupper())
        return score
    return max(items, key=case_score)

if __name__ == "__main__":
    # Configuration
    input_file = '../../data/raw_json/companies.json'
    output_directory = '../../data/cleaned_json'
    output_info_directory = '../../data/info_cleaned_json'

    try:
        print("Début du traitement des données...")
        results = process_companies_data(input_file, output_directory, output_info_directory)

        print("\nRésumé du traitement :")
        print(f"- Entrées companies uniques : {results['companies_count']}")
        print(f"- Doublons stricts trouvés : {results['strict_duplicates_count']}")
        print(f"- Doublons après normalisation : {results['case_duplicates_count']}")
        print(f"- Colonnes manquantes : {results['missing_columns_count']}")
        print(f"- Valeurs NULL corrigées : {results['null_issues_count']}")
        print(f"- Problèmes de chaînes corrigés : {results['string_issues_count']}")
        print("\nTraitement terminé avec succès !")

    except Exception as e:
        print(f"Une erreur s'est produite : {e}")
