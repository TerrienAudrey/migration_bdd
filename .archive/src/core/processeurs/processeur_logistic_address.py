from typing import Dict, List, Union
from datetime import datetime
from src.core.processeurs.processeur_donnees import ProcesseurDonnees

class ProcesseurLogisticAddress(ProcesseurDonnees):
    """Processeur spécifique pour les données d'adresse logistique"""

    def __init__(self):
        super().__init__(prefix='la')

    @staticmethod
    def get_field_rules() -> Dict[str, Dict[str, Union[str, bool, any]]]:
        """Définit les règles de validation pour les champs d'adresse logistique basées sur le modèle Prisma"""
        return {
            # Primary Key - autoincrement
            'la_id': {'type': 'integer','required': True,'min_value': 0,'unique': True,'primary_key': True},

            # Optional string fields with specific lengths (VarChar)
            'la_house_number': {'type': 'string','required': False,'max_length': 10,'default': None},
            'la_street': {'type': 'string','required': False,'max_length': 255,'default': None},
            'la_city': {'type': 'string','required': False,'max_length': 100,'default': None},
            'la_additional_address': {'type': 'string','required': False,'max_length': 255,'default': None },

            # Required string field
            'la_postal_code': {'type': 'string','required': True,'max_length': 10,'default': ''},

            # Required boolean fields
            'la_truck_access': {'type': 'boolean','required': True,'default': False },
            'la_loading_dock': {'type': 'boolean','required': True,'default': False},
            'la_forklift': {'type': 'boolean','required': True,'default': False },
            'la_pallet': {'type': 'boolean','required': True,'default': False},
            'la_fenwick': {'type': 'boolean','required': True,'default': False},

            # Boolean with default true
            'la_isactive': {'type': 'boolean','required': True,'default': True},

            # Optional integer fields
            'la_palet_capacity': {'type': 'integer','required': False,'min_value': 0,'default': None},
            'la_longitude': {'type': 'integer','required': False,'default': None},
            'la_latitude': {'type': 'integer','required': False,'default': None },

            # Optional foreign keys with default for fk_cou
            'fk_cou': {'type': 'integer','required': False,'min_value': 1,'default': 1},
            'fk_or': {'type': 'integer','required': False,'min_value': 0,'default': None},
            'fk_re': {'type': 'integer','required': False,'min_value': 0,'default': None},
            'fk_co': {'type': 'integer','required': False,'min_value': 0,'default': None},
            'fk_con': {'type': 'integer','required': False,'min_value': 0,'default': None},

            # Arrays (relations)
            'opening_hour': {'type': 'array','required': True,'element_type': 'integer','default': []},
            'stock_import': {'type': 'array','required': True,'element_type': 'integer','default': []},
            'positioning': {'type': 'array','required': True,'element_type': 'integer','default': []}
        }

    @staticmethod
    def initialize_output_info() -> Dict[str, List[Dict[str, any]]]:
        """Initialise la structure d'information de sortie"""
        return {
            "la_duplicates": [],
            "la_missing_columns": [],
            "la_null_issues": [],
            "la_string_issues": [],
            "la_boolean_issues": [],
            "la_integer_issues": [],
            "la_length_issues": [],
            "la_special_char_issues": [],
            "la_reference_issues": [],
            "la_array_type_issues": [],
            "la_unique_constraint_violations": [],
            "la_cleaned_cases": []
        }

    @staticmethod
    def create_key(item: Dict) -> str:
        """Crée une clé unique pour l'adresse logistique basée sur la_id"""
        return str(item.get('la_id', ''))
