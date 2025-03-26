"""
Définition de la structure d'entrée pour les données 'logistic_address'.
Ce module contient le schéma de validation JSON pour les données brutes.
"""

from typing import Dict, Any, List

# Schéma de validation pour les données d'entrée
INPUT_SCHEMA: Dict[str, Any] = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "la_id": {"type": "integer"},
            "la_house_number": {"type": ["string", "null"]},
            "la_street": {"type": ["string", "null"]},
            "la_additional_address": {"type": ["string", "null"]},
            "la_postal_code": {"type": ["string", "null"]},
            "la_city": {"type": ["string", "null"]},
            "la_truck_access": {"type": ["boolean", "integer", "string", "null"]},
            "la_loading_dock": {"type": ["boolean", "integer", "string", "null"]},
            "la_forklift": {"type": ["boolean", "integer", "string", "null"]},
            "la_pallet": {"type": ["boolean", "integer", "string", "null"]},
            "la_fenwick": {"type": ["boolean", "integer", "string", "null"]},
            "la_palet_capacity": {"type": ["integer", "string", "null"]},
            "la_longitude": {"type": ["integer", "number", "string", "null"]},
            "la_latitude": {"type": ["integer", "number", "string", "null"]},
            "la_isactive": {"type": ["boolean", "integer", "string", "null"]},
            "fk_co": {"type": ["integer", "null"]},
            "fk_or": {"type": ["integer", "null"]},
            "fk_re": {"type": ["integer", "null"]},
            "fk_con": {"type": ["integer", "null"]},
            "fk_cou": {"type": ["integer", "null"]},
            "stock_import": {"type": ["array", "null"], "items": {"type": "string"}}
        },
        "required": ["la_id"],
        "additionalProperties": True
    }
}

# Définition des champs obligatoires et optionnels
REQUIRED_FIELDS = ["la_id"]
OPTIONAL_FIELDS = [
    "la_house_number",
    "la_street",
    "la_additional_address",
    "la_postal_code",
    "la_city",
    "la_truck_access",
    "la_loading_dock",
    "la_forklift",
    "la_pallet",
    "la_fenwick",
    "la_palet_capacity",
    "la_longitude",
    "la_latitude",
    "la_isactive",
    "fk_co",
    "fk_or",
    "fk_re",
    "fk_con",
    "fk_cou",
    "stock_import"
]

# Structure des types de données attendus pour la validation
FIELD_TYPES = {
    "la_id": int,
    "la_house_number": str,
    "la_street": str,
    "la_additional_address": str,
    "la_postal_code": str,
    "la_city": str,
    "la_truck_access": bool,
    "la_loading_dock": bool,
    "la_forklift": bool,
    "la_pallet": bool,
    "la_fenwick": bool,
    "la_palet_capacity": int,
    "la_longitude": float,
    "la_latitude": float,
    "la_isactive": bool,
    "fk_co": int,
    "fk_or": int,
    "fk_re": int,
    "fk_con": int,
    "fk_cou": int,
    "stock_import": list
}

# Règles de validation spécifiques par champ
VALIDATION_RULES = {
    "la_postal_code": {
        "pattern": r"^\d{5}$",
        "error_message": "Le code postal doit contenir exactement 5 chiffres"
    },
    "la_city": {
        "pattern": r"^[A-Za-zÀ-ÿ\s\-']+$",
        "error_message": "Le nom de ville ne doit contenir que des lettres, espaces, tirets et apostrophes"
    },
    "stock_import": {
        "pattern": r"^\d{8}-+\d{3}-\d{3}-\d+$",
        "error_message": "Format de stock_import invalide"
    }
}

# Documentation des champs pour référence
FIELD_DESCRIPTIONS = {
    "la_id": "Identifiant unique de l'adresse logistique",
    "la_house_number": "Numéro de rue",
    "la_street": "Nom de la rue",
    "la_additional_address": "Informations complémentaires d'adresse",
    "la_postal_code": "Code postal",
    "la_city": "Ville",
    "la_truck_access": "Indicateur d'accessibilité pour les camions",
    "la_loading_dock": "Indicateur de présence d'un quai de chargement",
    "la_forklift": "Indicateur de disponibilité d'un chariot élévateur",
    "la_pallet": "Indicateur de disponibilité de palettes",
    "la_fenwick": "Indicateur de disponibilité d'un Fenwick",
    "la_palet_capacity": "Capacité de stockage en palettes",
    "la_longitude": "Coordonnée longitudinale",
    "la_latitude": "Coordonnée latitudinale",
    "la_isactive": "Indicateur d'activité de l'adresse",
    "fk_co": "Clé étrangère vers la table companies",
    "fk_or": "Clé étrangère vers la table organizations",
    "fk_re": "Clé étrangère vers la table recycling",
    "fk_con": "Clé étrangère vers la table contacts",
    "fk_cou": "Clé étrangère vers la table countries",
    "stock_import": "Tableau de chaînes identifiant les imports de stock"
}

# Répertoire des erreurs communes pour uniformisation des messages
COMMON_ERRORS = {
    "missing_required": "Champ obligatoire manquant",
    "invalid_type": "Type de données incorrect",
    "invalid_format": "Format de données incorrect",
    "empty_string": "Chaîne de caractères vide",
    "invalid_boolean": "Valeur booléenne invalide",
    "invalid_integer": "Valeur entière invalide",
    "invalid_array": "Tableau invalide",
    "invalid_postal_code": "Code postal invalide",
    "invalid_city_name": "Nom de ville invalide",
    "combined_address": "Adresse complète mise dans un seul champ"
}