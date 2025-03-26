from typing import Dict, List, Union
from src.core.processeurs.processeur_donnees import ProcesseurDonnees

class ProcesseurTransport(ProcesseurDonnees):
    """Processeur spécifique pour les données de transport"""

    def __init__(self):
        super().__init__(prefix='tra')

    @staticmethod
    def get_field_rules() -> Dict[str, Dict[str, Union[str, bool, any]]]:
        """Définit les règles de validation pour les champs de transport"""
        return {
            # Primary Key - autoincrement
            'tra_id': {'type': 'integer', 'required': True, 'min_value': 0, 'unique': True, 'primary_key': True},

            # Required string field (unique)
            'tra_denomination': {'type': 'string','required': True, 'max_length': 100,'default': '', 'unique': True},

            # Optional integer field
            'fk_con': {'type': 'integer','required': False, 'min_value': 0, 'default': None},

            # Required array fields
            'contacts' :  {'type': 'array', 'required': True, 'default': [], 'element_type': 'integer'},
            'deliveries': {'type': 'array', 'required': True, 'default': [], 'element_type': 'integer'},
            'stock_import': {'type': 'array','required': True,'default': [], 'element_type': 'integer'}
        }

    @staticmethod
    def initialize_output_info() -> Dict[str, List[Dict[str, any]]]:
        """Initialise la structure d'information de sortie"""
        return {
            "tra_duplicates": [],
            "tra_missing_columns": [],
            "tra_null_issues": [],
            "tra_string_issues": [],
            "tra_integer_issues": [],
            "tra_length_issues": [],
            "tra_special_char_issues": [],
            "tra_cleaned_cases": [],
            "tra_uniqueness_violations": []
        }

    @staticmethod
    def create_key(item: Dict) -> str:
        """Crée une clé unique pour le transport"""
        return item.get('tra_denomination', '')
