from typing import Dict, List, Union
from src.core.processeurs.processeur_donnees import ProcesseurDonnees

class ProcesseurCompany(ProcesseurDonnees):
    """Processeur spécifique pour les données des entreprises"""

    def __init__(self):
        super().__init__(prefix='co')

    @staticmethod
    def get_field_rules() -> Dict[str, Dict[str, Union[str, bool, any]]]:
        """Définit les règles de validation pour les champs d'entreprise basées sur le modèle Prisma"""
        return {
            # Primary Key - autoincrement
            'co_id': {'type': 'integer','required': True,'min_value': 0,'unique': True,'primary_key': True},

            # Required string fields with specific lengths (VarChar)
            'co_business_name': {'type': 'string','required': True,'max_length': 255,'default': ''},
            'co_legal_form': {'type': 'string','required': True,'max_length': 100,'default': ''},
            'co_siret': {'type': 'string','required': True,'max_length': 25,'default': ''},
            'co_siren': {'type': 'string','required': True,'max_length': 25,'default': ''},
            'co_vat': {'type': 'string','required': True,'max_length': 20,'default': ''},

            # Address fields - all required
            'co_head_office_additional_address': {'type': 'string','required': True,'max_length': 255,'default': ''},
            'co_head_office_city': {'type': 'string','required': True,'max_length': 255,'default': ''},
            'co_head_office_number': {'type': 'string','required': True,'max_length': 255,'default': ''},
            'co_head_office_country': {'type': 'string','required': True,'max_length': 255,'default': ''},
            'co_head_office_postal_code': {'type': 'string','required': True,'max_length': 255,'default': ''},
            'co_head_office_street': {'type': 'string','required': True,'max_length': 255,'default': ''},

            # Unique code field
            'co_code_ent': {'type': 'string','required': True,'max_length': 50,'unique': True,'default': ''},

            # Required foreign key
            'fk_us': {'type': 'integer','required': True,'min_value': 0},

            # Arrays (relations)
            'stocks': {'type': 'array','required': True,'element_type': 'integer','default': []},
            'logistic_address': {'type': 'array','required': True,'element_type': 'integer','default': []},
            'contacts': {'type': 'array','required': True,'element_type': 'integer','default': []}
        }

    @staticmethod
    def initialize_output_info() -> Dict[str, List[Dict[str, any]]]:
        """Initialise la structure d'information de sortie"""
        return {
            "co_duplicates": [],
            "co_missing_columns": [],
            "co_null_issues": [],
            "co_string_issues": [],
            "co_integer_issues": [],
            "co_length_issues": [],
            "co_special_char_issues": [],
            "co_reference_issues": [],
            "co_array_type_issues": [],
            "co_unique_constraint_violations": [],
            "co_siret_format_issues": [],
            "co_siren_format_issues": [],
            "co_vat_format_issues": [],
            "co_cleaned_cases": []
        }

    @staticmethod
    def create_key(item: Dict) -> str:
        """Crée une clé unique pour l'entreprise basée sur co_id et co_code_ent"""
        return f"{item.get('co_id', '')}_{item.get('co_code_ent', '')}"

    def validate_siret(self, siret: str) -> bool:
        """Valide le format du numéro SIRET"""
        # Retire les espaces et tirets éventuels
        siret = siret.replace(' ', '').replace('-', '')

        # Vérifie la longueur (14 chiffres)
        if not siret.isdigit() or len(siret) != 14:
            return False

        # Algorithme de validation Luhn
        total = 0
        for i, digit in enumerate(reversed(siret)):
            n = int(digit)
            if i % 2 == 1:
                n *= 2
                if n > 9:
                    n -= 9
            total += n
        return total % 10 == 0

    def validate_siren(self, siren: str) -> bool:
        """Valide le format du numéro SIREN"""
        # Retire les espaces et tirets éventuels
        siren = siren.replace(' ', '').replace('-', '')

        # Vérifie la longueur (9 chiffres)
        if not siren.isdigit() or len(siren) != 9:
            return False

        # Algorithme de validation Luhn
        total = 0
        for i, digit in enumerate(reversed(siren)):
            n = int(digit)
            if i % 2 == 1:
                n *= 2
                if n > 9:
                    n -= 9
            total += n
        return total % 10 == 0

    def validate_vat(self, vat: str) -> bool:
        """Valide le format du numéro de TVA intracommunautaire"""
        # Format basique pour la France (FR + 2 chiffres + SIREN)
        if not vat.startswith('FR'):
            return False

        # Retire le préfixe FR
        vat_number = vat[2:]

        # Vérifie que le reste est composé de chiffres
        if not vat_number.isdigit() or len(vat_number) != 11:
            return False

        return True
