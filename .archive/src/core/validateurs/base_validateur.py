from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from src.config.constantes import SPECIAL_VALUES

class BaseValidateur(ABC):
    """Classe de base abstraite pour tous les validateurs"""

    def __init__(self, rules: Dict[str, Any], context: Dict[str, Any]):
        self.rules = rules
        self.context = context

    def log_issue(self, original: Any, replaced: Any, mod_type: str) -> None:
        """Journalise les problèmes de nettoyage de données"""
        self.context['output_info'][self.context['issue_type']].append({
            "id": self.context['item'].get(f"{self.context['prefix']}_id", "unknown"),
            "column_name": self.context['field_name'],
            "original_value": original,
            "replaced_by": replaced,
            "modification_type": mod_type
        })

    @abstractmethod
    def clean(self, value: Any) -> Any:
        """Méthode abstraite pour nettoyer et valider une valeur"""
        pass

    def handle_null(self, value: Any) -> Optional[Any]:
        """Gère les valeurs nulles selon les règles définies"""
        if value is None:
            if self.rules.get('required', False):
                self.log_issue(None, self.rules.get('default'), "null_to_default")
                return self.rules.get('default')
            return None
        return value

class ValidateurChaines(BaseValidateur):
    """Validateur pour les chaînes de caractères"""

    def clean(self, value: Any) -> Optional[str]:
        self.context['issue_type'] = f"{self.context['prefix']}_string_issues"

        value = self.handle_null(value)
        if value is None:
            return None

        if not isinstance(value, str):
            value = str(value)

        cleaned = ' '.join(value.split())
        if cleaned != value:
            self.log_issue(value, cleaned, "space_normalization")

        if cleaned.lower() in SPECIAL_VALUES:
            self.context['issue_type'] = f"{self.context['prefix']}_special_char_issues"
            self.log_issue(cleaned, "", "special_char_removal")
            return ""

        if 'max_length' in self.rules and len(cleaned) > self.rules['max_length']:
            self.context['issue_type'] = f"{self.context['prefix']}_length_issues"
            truncated = cleaned[:self.rules['max_length']]
            self.log_issue(cleaned, truncated, "string_truncation")
            return truncated

        return cleaned

class ValidateurNumerique(BaseValidateur):
    """Validateur pour les nombres (entiers et flottants)"""

    def clean(self, value: Any) -> Optional[Union[int, float]]:
        self.context['issue_type'] = f"{self.context['prefix']}_{self.rules['type']}_issues"

        # Traitement spécial pour les clés primaires
        if self.rules.get('primary_key'):
            try:
                value = int(str(value).strip())
                if value < 1:
                    self.log_issue(value, None, "invalid_primary_key_value")
                    raise ValueError("Primary key must be positive")
                return value
            except (ValueError, TypeError):
                self.log_issue(value, None, "invalid_primary_key_type")
                raise ValueError(f"Invalid primary key value: {value}")

        value = self.handle_null(value)
        if value is None:
            return None

        try:
            if isinstance(value, str):
                value = float(value.strip().replace(',', '.'))
            elif isinstance(value, (int, float)):
                value = float(value)
            else:
                raise ValueError

            if self.rules['type'] == 'integer':
                value = int(value)

            if 'min_value' in self.rules and value < self.rules['min_value']:
                self.log_issue(value, self.rules.get('default'), "below_minimum_value")
                return self.rules.get('default')

            return value
        except (ValueError, TypeError):
            self.log_issue(value, self.rules.get('default'), "invalid_type_to_default")
            return self.rules.get('default')

class ValidateurBooleen(BaseValidateur):
    """Validateur pour les booléens"""

    def clean(self, value: Any) -> bool:
        self.context['issue_type'] = f"{self.context['prefix']}_boolean_issues"

        if isinstance(value, bool):
            return value

        if isinstance(value, str):
            value = value.lower()
            if value in ('true', '1', 'yes', 'y'):
                return True
            if value in ('false', '0', 'no', 'n'):
                return False

        self.log_issue(value, self.rules.get('default'), "invalid_type_to_default_boolean")
        return self.rules.get('default', False)

class ValidateurDateTime(BaseValidateur):
    """Validateur pour les dates et heures"""

    def clean(self, value: Any) -> Optional[str]:
        self.context['issue_type'] = f"{self.context['prefix']}_datetime_issues"

        value = self.handle_null(value)
        if value is None:
            return None

        try:
            if isinstance(value, str):
                cleaned_value = value.replace('Z', '+00:00')
                datetime.fromisoformat(cleaned_value)
                return cleaned_value
            elif isinstance(value, datetime):
                return value.isoformat()
        except (ValueError, TypeError):
            self.log_issue(value, self.rules.get('default'), "invalid_datetime_to_default")
            return self.rules.get('default')

class ValidateurTableaux(BaseValidateur):
    """Validateur pour les tableaux avec validation de type des éléments"""

    def validate_element(self, element: Any, element_type: str) -> bool:
        """Valide le type d'un élément du tableau"""
        if element_type == 'integer':
            return isinstance(element, int)
        elif element_type == 'float':
            return isinstance(element, (int, float))
        elif element_type == 'string':
            return isinstance(element, str)
        elif element_type == 'boolean':
            return isinstance(element, bool)
        return True

    def clean(self, value: Any) -> List:
        if not isinstance(value, list):
            self.log_issue(value, self.rules.get('default'), "invalid_type_to_empty_array")
            return self.rules.get('default', [])

        if 'element_type' in self.rules:
            invalid_elements = [
                (idx, element) for idx, element in enumerate(value)
                if not self.validate_element(element, self.rules['element_type'])
            ]

            if invalid_elements:
                self.log_issue(
                    [f"Index {idx}: {element}" for idx, element in invalid_elements],
                    self.rules.get('default'),
                    f"invalid_element_type_{self.rules['element_type']}"
                )
                return self.rules.get('default', [])

        return value

class ValidateurJSON(BaseValidateur):
    """Validateur pour les champs JSON"""

    def clean(self, value: Any) -> Optional[Dict]:
        self.context['issue_type'] = f"{self.context['prefix']}_json_issues"

        value = self.handle_null(value)
        if value is None:
            return None

        try:
            if isinstance(value, (dict, list)):
                return value
            elif isinstance(value, str):
                import json
                return json.loads(value)
            else:
                raise ValueError("Invalid JSON value")
        except Exception:
            self.log_issue(value, self.rules.get('default'), "invalid_json_to_default")
            return self.rules.get('default')
