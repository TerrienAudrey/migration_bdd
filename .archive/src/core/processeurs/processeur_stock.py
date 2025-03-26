from typing import Dict, List, Union
from datetime import datetime
from src.core.processeurs.processeur_donnees import ProcesseurDonnees

class ProcesseurStock(ProcesseurDonnees):
    """Processeur spécifique pour les données de stock"""

    def __init__(self):
        super().__init__(prefix='st')

    @staticmethod
    def get_field_rules() -> Dict[str, Dict[str, Union[str, bool, any]]]:
        """Définit les règles de validation pour les champs de stock basées sur le modèle Prisma"""
        return {
            # Primary Key - autoincrement
            'st_id': {'type': 'integer', 'required': True, 'min_value': 0, 'unique': True, 'primary_key': True},

            # Required string fields
            'st_io': {'type': 'string', 'required': True, 'max_length': 255, 'default': ''},
            'st_transportby': {'type': 'string','required': True, 'max_length': 255, 'default': ''},

            # Optional float field
            'st_commission': {'type': 'float','required': False, 'min_value': 0.0,'default': None},

            # Boolean fields with default false
            'st_is_freetransport': {'type': 'boolean','required': True,'default': False},
            'st_is_standby': {'type': 'boolean', 'required': True, 'default': False},
            'st_is_taxreceiptdeadline': {'type': 'boolean', 'required': True, 'default': False},
            'st_is_runoffdeadline': {'type': 'boolean', 'required': True, 'default': False},
            'st_is_showorganization': {'type': 'boolean','required': True, 'default': False},

            # Optional string fields
            'st_commentary': {'type': 'string', 'required': False,'default': None},
            'st_instructions': {'type': 'string', 'required': False, 'max_length': 100, 'default': None },

            # Optional datetime fields
            'st_use_by_date': {'type': 'datetime', 'required': False, 'default': None},
            'st_creation_date': {'type': 'datetime', 'required': False, 'default': datetime.now().isoformat()},
            'st_ending_date': {'type': 'datetime', 'required': False, 'default': None},

            # Enum field with default
            'st_step_planning': {'type': 'string', 'required': True,'default': 'NOTSTARTED'},

            # Optional foreign keys with default for fk_sta
            'fk_sta': {'type': 'integer', 'required': False, 'min_value': 1, 'default': 1 },
            'fk_us': {'type': 'integer',  'required': False, 'min_value': 1, 'default': None},
            'fk_co': {'type': 'integer', 'required': False, 'min_value': 1, 'default': None },
            'fk_sav': {'type': 'integer',  'required': False, 'min_value': 1, 'default': None },

            # Array fields containing integers
            'positioning': {'type': 'array', 'required': True, 'default': [],'element_type': 'integer' },
            'stock_status_history': {'type': 'array', 'required': True,'default': [],'element_type': 'integer'},
            'stock_import': {'type': 'array','required': True, 'default': [],'element_type': 'integer'},
            'stock_sav_history': {'type': 'array','required': True, 'default': [], 'element_type': 'integer'}
        }

    @staticmethod
    def initialize_output_info() -> Dict[str, List[Dict[str, any]]]:
        """Initialise la structure d'information de sortie"""
        return {
            "st_duplicates": [],
            "st_missing_columns": [],
            "st_null_issues": [],
            "st_string_issues": [],
            "st_float_issues": [],
            "st_boolean_issues": [],
            "st_datetime_issues": [],
            "st_integer_issues": [],
            "st_enum_issues": [],
            "st_length_issues": [],
            "st_special_char_issues": [],
            "st_array_type_issues": [],
            "st_reference_issues": [],  # Pour les violations de clés étrangères
            "st_cleaned_cases": []
        }

    @staticmethod
    def create_key(item: Dict) -> str:
        """Crée une clé unique pour le stock basée sur st_id"""
        return str(item.get('st_id', ''))
