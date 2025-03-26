"""
Définition de la structure d'entrée pour les données 'stocks'.
Ce module contient le schéma de validation JSON pour les données brutes.
"""

from typing import Dict, Any, List

# Schéma de validation pour les données d'entrée
INPUT_SCHEMA: Dict[str, Any] = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "st_id": {"type": "integer"},
            "st_io": {"type": "string"},
            "co_code_ent": {"type": ["string", "null"]},
            "st_commission_%": {"type": ["number", "string", "null"]},
            "st_commission": {"type": ["number", "string", "null"]},
            "st_creation_date": {"type": ["string", "null"]},
            "st_transportby": {"type": ["string", "null"]},
            "st_commentary": {"type": ["string", "null"]},
            "st_is_freetransport": {"type": ["boolean", "integer", "string", "null"]},
            "fk_sta": {"type": ["integer", "null"]},
            "fk_co": {"type": ["integer", "null"]},
            "stock_import": {"type": ["array", "null"], "items": {"type": ["integer", "string"]}}
        },
        "required": ["st_id", "st_io"],
        "additionalProperties": True
    }
}

# Définition des champs obligatoires et optionnels
REQUIRED_FIELDS = ["st_id", "st_io"]
OPTIONAL_FIELDS = [
    "co_code_ent",
    "st_commission_%",
    "st_commission",
    "st_creation_date",
    "st_transportby",
    "st_commentary",
    "st_is_freetransport",
    "fk_sta",
    "fk_co",
    "stock_import"
]

# Structure des types de données attendus pour la validation
FIELD_TYPES = {
    "st_id": int,
    "st_io": str,
    "co_code_ent": str,
    "st_commission_%": float,
    "st_commission": float,
    "st_creation_date": str,
    "st_transportby": str,
    "st_commentary": str,
    "st_is_freetransport": bool,
    "fk_sta": int,
    "fk_co": int,
    "stock_import": list
}

# Règles de validation spécifiques par champ
VALIDATION_RULES = {
    "st_io": {
        "pattern": r"^\d{8}-[-\d]{3}-\d{3}(-\d+)?$",
        "error_message": "Le format de st_io doit être YYYYMMDD-XXX-YYY ou YYYYMMDD-XXX-YYY-Z"
    },
    "st_commission_%": {
        "range": [0, 1],
        "error_message": "La commission doit être un ratio entre 0 et 1"
    },
    "st_creation_date": {
        "pattern": r"^\d{4}-\d{2}-\d{2}$",
        "error_message": "Le format de date doit être YYYY-MM-DD"
    }
}

# Documentation des champs pour référence
FIELD_DESCRIPTIONS = {
    "st_id": "Identifiant unique du stock",
    "st_io": "Identifiant métier du stock",
    "co_code_ent": "Code entreprise",
    "st_commission_%": "Pourcentage de commission (à renommer en st_commission_percent)",
    "st_commission": "Montant de la commission",
    "st_creation_date": "Date de création du stock",
    "st_transportby": "Moyen de transport",
    "st_commentary": "Commentaires sur le stock",
    "st_is_freetransport": "Indicateur de transport gratuit",
    "fk_sta": "Clé étrangère vers la table status",
    "fk_co": "Clé étrangère vers la table companies",
    "stock_import": "Tableau d'identifiants des imports"
}

# Répertoire des erreurs communes pour uniformisation des messages
COMMON_ERRORS = {
    "missing_required": "Champ obligatoire manquant",
    "invalid_type": "Type de données incorrect",
    "invalid_format": "Format de données incorrect",
    "empty_string": "Chaîne de caractères vide",
    "invalid_boolean": "Valeur booléenne invalide",
    "invalid_float": "Valeur flottante invalide",
    "invalid_integer": "Valeur entière invalide",
    "invalid_array": "Tableau invalide",
    "invalid_date": "Format de date invalide",
    "duplicate_st_io": "Identifiant st_io en doublon",
    "duplicate_stock_import": "Valeurs en doublon dans stock_import"
}

# Champs à renommer (source -> cible)
FIELDS_TO_RENAME = {
    "st_commission_%": "st_commission_percent"
}