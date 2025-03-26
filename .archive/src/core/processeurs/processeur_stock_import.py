from typing import Dict, List, Union
from datetime import datetime
from src.core.processeurs.processeur_donnees import ProcesseurDonnees

class ProcesseurStockImport(ProcesseurDonnees):
    """Processeur spécifique pour les données de stock_import"""

    def __init__(self):
        super().__init__(prefix='si')

    @staticmethod
    def get_field_rules() -> Dict[str, Dict[str, Union[str, bool, any]]]:
        """Définit les règles de validation pour les champs de stock_import basées sur le modèle Prisma"""
        return {
            # Primary Key - autoincrement
            'si_id': {'type': 'integer', 'required': True, 'min_value': 0,'unique': True, 'primary_key': True},

            # Required string fields
            'si_file': {'type': 'string','required': True,'max_length': 255, 'default': ''},
            'si_filename': { 'type': 'string','required': True,  'max_length': 255,'default': ''},

            # Optional datetime fields
            'si_date_process': {'type': 'datetime', 'required': False, 'default': None},

            # Required integer fields
            'si_quantity': {'type': 'integer', 'required': True, 'min_value': 0, 'default': 0},

            # Optional integer fields
            'si_quantity_stackable': {'type': 'integer', 'required': False,'min_value': 0, 'default': None},

            # Required float fields
            'si_total_price': {'type': 'float', 'required': True, 'min_value': 0.0, 'default': 0.0},

            # String array field
            'si_packaging_method': {'type': 'array', 'required': True, 'element_type': 'string', 'default': []},

            # Optional datetime fields
            'si_date_removal': {'type': 'datetime', 'required': False, 'default': None},
            'si_date_alert_removal': {'type': 'datetime', 'required': False, 'default': None},
            'si_date_delivery': {'type': 'datetime', 'required': False, 'default': None},
            'si_date_alert_delivery': { 'type': 'datetime', 'required': False, 'default': None},

            # Boolean fields with defaults
            'si_is_pallet': {'type': 'boolean', 'required': True, 'default': False },
            'si_is_ready': {'type': 'boolean','required': True, 'default': False},
            'si_is_dangerous': {'type': 'boolean', 'required': True, 'default': False},

            # Optional string fields for GPT
            'si_gpt_file_id': {'type': 'string', 'required': False,'max_length': 255, 'default': None},
            'si_gpt_thread_matching_id': {'type': 'string','required': False, 'max_length': 255, 'default': None},
            'si_gpt_thread_category_id': {'type': 'string', 'required': False, 'max_length': 255,'default': None},

            # Optional JSON fields
            'si_gpt_response_matching_json': { 'type': 'json', 'required': False, 'default': None},
            'si_gpt_response_category_json': { 'type': 'json','required': False, 'default': None},

            # Optional foreign keys
            'fk_st': { 'type': 'integer', 'required': False,'min_value': 1,  'default': None},
            'fk_la': {'type': 'integer','required': False,'min_value': 1,'default': None },
            'fk_tra': {'type': 'integer', 'required': False, 'min_value': 1, 'default': None}
        }

    @staticmethod
    def initialize_output_info() -> Dict[str, List[Dict[str, any]]]:
        """Initialise la structure d'information de sortie"""
        return {
            "si_duplicates": [],
            "si_missing_columns": [],
            "si_null_issues": [],
            "si_string_issues": [],
            "si_float_issues": [],
            "si_json_issues": [],
            "si_boolean_issues": [],
            "si_datetime_issues": [],
            "si_integer_issues": [],
            "si_array_type_issues": [],
            "si_length_issues": [],
            "si_special_char_issues": [],
            "si_reference_issues": [],
            "si_cleaned_cases": []
        }

    @staticmethod
    def create_key(item: Dict) -> str:
        """Crée une clé unique pour le stock_import basée sur si_id"""
        return str(item.get('si_id', ''))
