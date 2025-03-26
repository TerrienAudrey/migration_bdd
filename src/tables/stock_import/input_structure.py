"""
Définition de la structure d'entrée pour les données 'stock_import'.
Ce module contient le schéma de validation JSON pour les données brutes.
"""

from typing import Dict, Any, List

# Schéma de validation pour les données d'entrée
INPUT_SCHEMA: Dict[str, Any] = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "si_id": {"type": "integer"},
            "id_ope": {"type": ["string", "null"]},
            "si_io": {"type": ["string", "null"]},
            "si_date_removal": {"type": ["string", "null"]},
            "si_date_delivery": {"type": ["string", "null"]},
            "si_total_price": {"type": ["number", "null"]},
            "fk_st": {"type": ["integer", "null"]},
            "fk_co": {"type": ["integer", "null"]}
        },
        "required": ["si_id"],
        "additionalProperties": True
    }
}

# Définition des champs obligatoires et optionnels
REQUIRED_FIELDS = ["si_id"]
OPTIONAL_FIELDS = [
    "id_ope",
    "si_io",
    "si_date_removal",
    "si_date_delivery",
    "si_total_price",
    "fk_st",
    "fk_co"
]

# Structure des types de données attendus pour la validation
FIELD_TYPES = {
    "si_id": int,
    "id_ope": str,
    "si_io": str,
    "si_date_removal": str,
    "si_date_delivery": str,
    "si_total_price": float,
    "fk_st": int,
    "fk_co": int
}

# Règles de validation spécifiques par champ
VALIDATION_RULES = {
    "si_date_removal": {
        "pattern": r"^\d{4}-\d{2}-\d{2}$",
        "error_message": "La date doit être au format YYYY-MM-DD"
    },
    "si_date_delivery": {
        "pattern": r"^\d{4}-\d{2}-\d{2}$",
        "error_message": "La date doit être au format YYYY-MM-DD"
    }
}

# Documentation des champs pour référence
FIELD_DESCRIPTIONS = {
    "si_id": "Identifiant unique de l'import de stock",
    "id_ope": "Identifiant de l'opération",
    "si_io": "Identifiant spécifique de l'import",
    "si_date_removal": "Date de retrait",
    "si_date_delivery": "Date de livraison",
    "si_total_price": "Prix total",
    "fk_st": "Clé étrangère vers la table stocks",
    "fk_co": "Clé étrangère vers la table companies"
}

# Répertoire des erreurs communes pour uniformisation des messages
COMMON_ERRORS = {
    "missing_required": "Champ obligatoire manquant",
    "invalid_type": "Type de données incorrect",
    "invalid_format": "Format de données incorrect",
    "empty_string": "Chaîne de caractères vide",
    "invalid_date": "Format de date invalide",
    "invalid_json": "Format JSON invalide"
}