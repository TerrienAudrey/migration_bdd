"""
Définition de la structure d'entrée pour les données 'organizations'.
Ce module contient le schéma de validation JSON pour les données brutes.
"""

from typing import Dict, Any

# Schéma de validation pour les données d'entrée
INPUT_SCHEMA: Dict[str, Any] = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "or_id": {"type": "integer"},
            "or_denomination": {"type": "string"},
            "or_rna": {"type": ["string", "null"]},
            "or_id_address": {"type": "integer"},
            "or_house_number": {"type": ["string", "null"]},
            "or_street": {"type": ["string", "null"]},
            "or_postal_code": {"type": ["string", "null"]},
            "or_city": {"type": ["string", "null"]},
            "or_state": {"type": ["string", "null"]},
            "or_additional_address": {"type": ["string", "null"]}
        },
        "required": ["or_id", "or_denomination", "or_id_address"],
        "additionalProperties": False
    }
}

# Définition des champs obligatoires et optionnels
REQUIRED_FIELDS = ["or_id", "or_denomination", "or_id_address"]
OPTIONAL_FIELDS = [
    "or_rna", 
    "or_house_number", 
    "or_street", 
    "or_postal_code",
    "or_city", 
    "or_state", 
    "or_additional_address"
]

# Structure des types de données attendus pour la validation
FIELD_TYPES = {
    "or_id": int,
    "or_denomination": str,
    "or_rna": str,
    "or_id_address": int,
    "or_house_number": str,
    "or_street": str,
    "or_postal_code": str,
    "or_city": str,
    "or_state": str,
    "or_additional_address": str
}

# Règles de validation spécifiques par champ
VALIDATION_RULES = {
    "or_rna": {
        "pattern": r"^W\d{9}$",
        "error_message": "Le RNA doit être au format W suivi de 9 chiffres"
    },
    "or_postal_code": {
        "pattern": r"^\d{5}$",
        "error_message": "Le code postal doit contenir exactement 5 chiffres"
    }
}

# Répertoire des erreurs communes pour uniformisation des messages
COMMON_ERRORS = {
    "missing_required": "Champ obligatoire manquant",
    "invalid_type": "Type de données incorrect",
    "invalid_format": "Format de données incorrect",
    "empty_string": "Chaîne de caractères vide",
    "value_too_long": "Valeur trop longue pour le champ",
    "invalid_rna": "Format de RNA invalide",
    "invalid_postal_code": "Format de code postal invalide",
    "invalid_city_name": "Format de nom de ville invalide",
    "invalid_state_name": "Format de nom de région invalide",
    "duplicate_entry": "Entrée en doublon"
}