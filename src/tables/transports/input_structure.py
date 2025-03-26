"""
Définition de la structure d'entrée pour les données 'transports'.
Ce module contient le schéma de validation JSON pour les données brutes.
"""

from typing import Dict, Any, List

# Schéma de validation pour les données d'entrée
INPUT_SCHEMA: Dict[str, Any] = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "tra_id": {"type": "integer"},
            "tra_denomination": {"type": ["string", "null"]},
            "stock_import": {"type": ["array", "null"], "items": {"type": ["integer", "string"]}}
        },
        "required": ["tra_id"],
        "additionalProperties": True
    }
}

# Définition des champs obligatoires et optionnels
REQUIRED_FIELDS = ["tra_id"]
OPTIONAL_FIELDS = [
    "tra_denomination",
    "stock_import"
]

# Structure des types de données attendus pour la validation
FIELD_TYPES = {
    "tra_id": int,
    "tra_denomination": str,
    "stock_import": list
}

# Règles de validation spécifiques par champ
VALIDATION_RULES = {
    "tra_denomination": {
        "pattern": r"^[A-Za-zÀ-ÿ0-9\s\-'&.]+$",
        "error_message": "La dénomination ne doit contenir que des lettres, chiffres, espaces, tirets, apostrophes, ampersands et points"
    }
}

# Documentation des champs pour référence
FIELD_DESCRIPTIONS = {
    "tra_id": "Identifiant unique du transporteur",
    "tra_denomination": "Nom du transporteur",
    "stock_import": "Tableau d'identifiants d'imports de stock"
}

# Répertoire des erreurs communes pour uniformisation des messages
COMMON_ERRORS = {
    "missing_required": "Champ obligatoire manquant",
    "invalid_type": "Type de données incorrect",
    "invalid_format": "Format de données incorrect",
    "empty_string": "Chaîne de caractères vide",
    "invalid_array": "Tableau invalide",
    "duplicate_entries": "Valeurs en double détectées",
    "duplicate_denomination": "Dénomination en double détectée"
}