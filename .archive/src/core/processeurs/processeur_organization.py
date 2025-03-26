from typing import Dict, List, Union
from datetime import datetime
from src.core.processeurs.processeur_donnees import ProcesseurDonnees

class ProcesseurOrganization(ProcesseurDonnees):
    """Processeur spécifique pour les données d'organisation"""

    def __init__(self):
        super().__init__(prefix='or')

    @staticmethod
    def get_field_rules() -> Dict[str, Dict[str, Union[str, bool, any]]]:
        """Définit les règles de validation pour les champs d'organisation basées sur le modèle Prisma"""
        return {
            # Primary Key - autoincrement
            'or_id': { 'type': 'integer','required': True, 'min_value': 0, 'unique': True,'primary_key': True },

            # Required string fields with specific lengths (VarChar)
            'or_denomination': {'type': 'string','required': True, 'max_length': 100, 'default': '' },
            'or_rna': { 'type': 'string','required': True, 'max_length': 15,  'default': ''  },
            'or_house_number': { 'type': 'string', 'required': True, 'max_length': 10,'default': '' },
            'or_street': {'type': 'string', 'required': True, 'max_length': 100, 'default': '' },
            'or_postal_code': {'type': 'string','required': True,'max_length': 10, 'default': '' },
            'or_city': { 'type': 'string', 'required': True, 'max_length': 100, 'default': '' },
            'or_country': {'type': 'string','required': True, 'max_length': 100,'default': ''},
            'or_state': { 'type': 'string', 'required': True, 'max_length': 100, 'default': '' },

            # Optional string fields
            'or_logo': { 'type': 'string', 'required': False,'max_length': 100, 'default': None},
            'or_additionnal_information': {'type': 'string', 'required': False, 'max_length': 500, 'default': None },

            # Required integer for address ID
            'or_id_address': {'type': 'integer', 'required': True,'min_value': 1,'unique': True },

            # Required datetime with default now()
            'or_creation_date': {'type': 'datetime', 'required': True, 'default': datetime.now().isoformat() },

            # Required foreign keys
            'fk_us': {'type': 'integer', 'required': True,'min_value': 0},
            'fk_ot': { 'type': 'integer', 'required': True,'min_value': 0},
            'fk_ovs': {'type': 'integer', 'required': True,'min_value': 0},
            'fk_cou': {'type': 'integer', 'required': True,  'min_value': 0},

            # Optional foreign key
            'fileFi_id': {'type': 'integer', 'required': False,'min_value': 0, 'default': None},

            # Arrays (relations)
            'categories': {'type': 'array', 'required': True, 'element_type': 'integer', 'default': []},
            'logistic_address': {'type': 'array', 'required': True,'element_type': 'integer', 'default': []},
            'origin_approvals': { 'type': 'array', 'required': True, 'element_type': 'integer', 'default': []},
            'positioning': { 'type': 'array', 'required': True, 'element_type': 'integer', 'default': [] },
            'organization_categories': {'type': 'array', 'required': True, 'element_type': 'integer','default': []}
        }

    @staticmethod
    def initialize_output_info() -> Dict[str, List[Dict[str, any]]]:
        """Initialise la structure d'information de sortie"""
        return {
            "or_duplicates": [],
            "or_missing_columns": [],
            "or_null_issues": [],
            "or_string_issues": [],
            "or_datetime_issues": [],
            "or_integer_issues": [],
            "or_length_issues": [],
            "or_special_char_issues": [],
            "or_reference_issues": [],
            "or_array_type_issues": [],
            "or_unique_constraint_violations": [],
            "or_cleaned_cases": []
        }

    @staticmethod
    def create_key(item: Dict) -> str:
        """Crée une clé unique pour l'organisation basée sur or_id"""
        return str(item.get('or_id', ''))
