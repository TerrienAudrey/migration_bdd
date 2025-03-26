import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Union, Type
from collections import defaultdict

from src.config.constantes import TIMESTAMP_FORMAT, REQUIRED_DIRS
from src.core.validateurs.base_validateur import (
    BaseValidateur,
    ValidateurChaines,
    ValidateurNumerique,
    ValidateurBooleen,
    ValidateurDateTime,
    ValidateurTableaux,
    ValidateurJSON
)

class ProcesseurDonnees:
    """Classe de base pour le traitement des données"""

    def __init__(self, prefix: str):
        self.prefix = prefix
        self.validators = {
            'string': ValidateurChaines,
            'integer': ValidateurNumerique,
            'float': ValidateurNumerique,
            'boolean': ValidateurBooleen,
            'datetime': ValidateurDateTime,
            'array': ValidateurTableaux,
            'json': ValidateurJSON
        }

    def validate_paths(self, paths: Dict[str, str]) -> None:
        """Valide l'existence des chemins d'entrée/sortie"""
        if not os.path.isfile(paths['input_file']):
            raise FileNotFoundError(f"Fichier d'entrée non trouvé : {paths['input_file']}")

        for dir_name in REQUIRED_DIRS:
            dir_path = paths[dir_name]
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)

    def get_validator(self, type_name: str) -> Type[BaseValidateur]:
        """Récupère le validateur approprié pour un type donné"""
        validator = self.validators.get(type_name)
        if not validator:
            raise ValueError(f"Type inconnu : {type_name}")
        return validator

    def validate_and_clean_value(
        self,
        value: Any,
        rules: Dict[str, Any],
        field_name: str,
        item: Dict[str, Any],
        output_info: Dict[str, List[Dict[str, Any]]]
    ) -> Any:
        """Valide et nettoie une valeur unique"""
        context = {
            'field_name': field_name,
            'item': item,
            'output_info': output_info,
            'issue_type': f'{self.prefix}_null_issues',
            'prefix': self.prefix
        }

        # Si la valeur est None pour un champ requis
        if value is None and rules.get('required', False):
            default_value = rules.get('default')
            if default_value is not None:
                self.log_issue(context, None, default_value, "null_to_default")
                return default_value
            else:
                self.log_issue(context, None, None, "required_field_missing")
                return None

        # Si la valeur est None pour un champ optionnel
        if value is None and not rules.get('required', False):
            return None

        validator_class = self.get_validator(rules['type'])
        validator = validator_class(rules, context)
        try:
            return validator.clean(value)
        except Exception as e:
            logging.error(f"Erreur lors de la validation du champ {field_name}: {str(e)}")
            return rules.get('default')

    def log_issue(self, context: Dict[str, Any], original: Any, replaced: Any, mod_type: str) -> None:
        """Journalise les problèmes de nettoyage des données"""
        issue_data = {
            "field_name": context['field_name'],
            "original_value": original,
            "replaced_by": replaced,
            "modification_type": mod_type
        }

        # Ajout de l'ID si disponible
        id_field = f"{context['prefix']}_id"
        if id_field in context['item']:
            issue_data[id_field] = context['item'][id_field]

        context['output_info'][context['issue_type']].append(issue_data)

    def process_single_item(
        self,
        item: Dict[str, Any],
        field_rules: Dict[str, Dict[str, Any]],
        output_info: Dict[str, List[Dict[str, Any]]]
    ) -> Dict[str, Any]:
        """Traite un élément de données unique"""
        processed = {}
        id_field = f"{self.prefix}_id"
        if id_field in item:
            processed[id_field] = item[id_field]

        for field_name, rules in field_rules.items():
            processed[field_name] = self.validate_and_clean_value(
                item.get(field_name), rules, field_name, item, output_info
            )

        return processed

    def check_missing_columns(
        self,
        data: List[Dict[str, Any]],
        field_rules: Dict[str, Dict[str, Any]],
        output_info: Dict[str, List[Dict[str, Any]]]
    ) -> None:
        """Vérifie les colonnes manquantes"""
        if not data:
            return

        existing_columns = set(data[0].keys())
        missing_columns = set(field_rules.keys()) - existing_columns

        for column in missing_columns:
            output_info[f"{self.prefix}_missing_columns"].append({
                "column_name": column,
                "type": field_rules[column]['type'],
                "default_value": field_rules[column].get('default')
            })

    def remove_duplicates(
        self,
        data: List[Dict[str, Any]],
        create_key_func,
        output_info: Dict[str, List[Dict[str, Any]]]
    ) -> List[Dict[str, Any]]:
        """Supprime les doublons basés sur une fonction de clé"""
        duplicates = defaultdict(list)
        for item in data:
            key = create_key_func(item)
            duplicates[key].append(item)

        unique_data = []
        for key, items in duplicates.items():
            if len(items) > 1:
                for item in items:
                    output_info[f"{self.prefix}_duplicates"].append({
                        "duplicate_key": key,
                        "duplicate_count": len(items),
                        "original_data": item
                    })
                unique_data.append(items[0])
            else:
                unique_data.append(items[0])

        return unique_data

    def save_output_files(
        self,
        data: Dict[str, Any],
        paths: Dict[str, str],
        timestamp: str
    ) -> None:
        """Sauvegarde tous les fichiers de sortie"""
        try:
            output_path = f"{paths['output_directory']}/{self.prefix}s_{timestamp}.json"
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data['final_data'], f, ensure_ascii=False, indent=2)

            for key, content in data['output_info'].items():
                info_path = f"{paths['output_info_directory']}/{key}_{timestamp}.json"
                with open(info_path, 'w', encoding='utf-8') as f:
                    json.dump(content, f, ensure_ascii=False, indent=2)
        except IOError as e:
            raise IOError(f"Erreur lors de la sauvegarde des fichiers : {str(e)}")

    def generate_statistics(
        self,
        final_data: List[Dict[str, Any]],
        output_info: Dict[str, List[Dict[str, Any]]]
    ) -> Dict[str, int]:
        """Génère des statistiques de traitement"""
        stats = {
            f"{self.prefix}s_count": len(final_data),
            "duplicates_count": len(output_info[f"{self.prefix}_duplicates"]),
            "missing_columns_count": len(output_info[f"{self.prefix}_missing_columns"]),
            "null_issues_count": len(output_info[f"{self.prefix}_null_issues"]),
            "string_issues_count": len(output_info[f"{self.prefix}_string_issues"]),
            "integer_issues_count": len(output_info[f"{self.prefix}_integer_issues"]),
            "length_issues_count": len(output_info[f"{self.prefix}_length_issues"]),
            "special_char_issues_count": len(output_info[f"{self.prefix}_special_char_issues"])
        }

        return stats

    def process_data(
        self,
        input_file: str,
        output_directory: str,
        output_info_directory: str,
        field_rules: Dict[str, Dict[str, Any]],
        create_key_func,
        initialize_output_info
    ) -> Dict[str, int]:
        """Fonction principale de traitement des données"""
        paths = {
            'input_file': input_file,
            'output_directory': output_directory,
            'output_info_directory': output_info_directory
        }
        self.validate_paths(paths)

        try:
            with open(input_file, 'r', encoding='utf-8') as file:
                data = json.load(file)
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON invalide dans le fichier d'entrée : {str(e)}")

        output_info = initialize_output_info()
        self.check_missing_columns(data, field_rules, output_info)

        processed_data = [
            self.process_single_item(item, field_rules, output_info)
            for item in data
        ]

        final_data = self.remove_duplicates(processed_data, create_key_func, output_info)

        timestamp = datetime.now().strftime(TIMESTAMP_FORMAT)
        output = {
            'final_data': final_data,
            'output_info': output_info
        }
        self.save_output_files(output, paths, timestamp)

        return self.generate_statistics(final_data, output_info)
