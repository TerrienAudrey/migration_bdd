"""
Définition de la structure d'entrée pour les données 'companies'.
Ce module contient le schéma de validation JSON pour les données brutes.
"""

from typing import Dict, Any

# Schéma de validation pour les données d'entrée
INPUT_SCHEMA: Dict[str, Any] = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "co_id": {"type": "integer"},
            "co_business_name": {"type": "string"},
            "co_siren": {"type": ["string", "null"]},
            "co_siret": {"type": ["string", "null"]},
            "co_vat": {"type": ["string", "null"]},
            "co_code_ent": {"type": ["string", "null"]},
            "co_head_office_address": {"type": ["string", "null"]},
            "co_head_office_city": {"type": ["string", "null"]},
            "co_head_office_postal_code": {"type": ["string", "null"]},
            "co_legal_form": {"type": ["string", "null"]},
            "fk_us": {"type": "integer"},
            "co_head_office_additional_address": {"type": ["string", "null"]}
        },
        "required": ["co_id", "co_business_name", "fk_us"],
        "additionalProperties": False
    }
}

# Définition des champs obligatoires et optionnels
REQUIRED_FIELDS = ["co_id", "co_business_name", "fk_us"]
OPTIONAL_FIELDS = [
    "co_siren", 
    "co_siret", 
    "co_vat", 
    "co_code_ent",
    "co_head_office_address", 
    "co_head_office_city", 
    "co_head_office_postal_code",
    "co_legal_form", 
    "co_head_office_additional_address"
]

# Structure des types de données attendus pour la validation
FIELD_TYPES = {
    "co_id": int,
    "co_business_name": str,
    "co_siren": str,
    "co_siret": str,
    "co_vat": str,
    "co_code_ent": str,
    "co_head_office_address": str,
    "co_head_office_city": str,
    "co_head_office_postal_code": str,
    "co_legal_form": str,
    "fk_us": int,
    "co_head_office_additional_address": str
}

# Règles de validation spécifiques par champ
VALIDATION_RULES = {
    "co_siren": {
        "pattern": r"^\d{9}$",
        "error_message": "Le SIREN doit contenir exactement 9 chiffres"
    },
    "co_siret": {
        "pattern": r"^\d{14}$",
        "error_message": "Le SIRET doit contenir exactement 14 chiffres"
    },
    "co_vat": {
        "pattern": r"^FR\d{11}$",
        "error_message": "Le numéro de TVA doit être au format FR suivi de 11 chiffres"
    },
    "co_head_office_postal_code": {
        "pattern": r"^\d{5}$",
        "error_message": "Le code postal doit contenir exactement 5 chiffres"
    }
}

# Documentation des champs pour référence
FIELD_DESCRIPTIONS = {
    "co_id": "Identifiant unique de l'entreprise",
    "co_business_name": "Nom commercial de l'entreprise",
    "co_siren": "Numéro SIREN (9 chiffres)",
    "co_siret": "Numéro SIRET (14 chiffres)",
    "co_vat": "Numéro de TVA intracommunautaire",
    "co_code_ent": "Code entreprise interne",
    "co_head_office_address": "Adresse du siège social",
    "co_head_office_city": "Ville du siège social",
    "co_head_office_postal_code": "Code postal du siège social",
    "co_legal_form": "Forme juridique de l'entreprise",
    "fk_us": "Clé étrangère vers la table utilisateurs",
    "co_head_office_additional_address": "Complément d'adresse du siège social"
}

# Répertoire des erreurs communes pour uniformisation des messages
COMMON_ERRORS = {
    "missing_required": "Champ obligatoire manquant",
    "invalid_type": "Type de données incorrect",
    "invalid_format": "Format de données incorrect",
    "empty_string": "Chaîne de caractères vide"
}