"""
Définition de la structure de sortie pour les données 'organizations'.
Ce module décrit la structure finale attendue après transformation.
"""

from typing import Dict, Any, List
from datetime import datetime

# Schéma de validation pour les données de sortie
OUTPUT_SCHEMA: Dict[str, Any] = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "or_id": {"type": "integer"},
            "or_denomination": {"type": "string", "maxLength": 255},
            "or_rna": {"type": "string", "maxLength": 255},
            "or_id_address": {"type": "integer"},
            "or_house_number": {"type": "string", "maxLength": 255},
            "or_street": {"type": "string", "maxLength": 255},
            "or_postal_code": {"type": "string", "maxLength": 255},
            "or_city": {"type": "string", "maxLength": 255},
            "or_country": {"type": "string", "maxLength": 255, "default": "FRANCE"},
            "or_state": {"type": "string", "maxLength": 255},
            "or_logo": {"type": ["string", "null"], "maxLength": 100},
            "or_creation_date": {"type": ["object", "null"]},  # datetime object
            "or_additionnal_information": {"type": ["string", "null"], "maxLength": 500},
            "fk_us": {"type": "integer"},
            "fk_ot": {"type": "integer"},
            "fk_ovs": {"type": "integer"},
            "fk_cou": {"type": "integer"},
            "fileFi_id": {"type": ["integer", "null"]},
            "or_desactivation_date": {"type": ["object", "null"]},  # datetime object
            "logistic_address": {"type": "array", "items": {}},
            "organization_categories": {"type": "array", "items": {}},
            "origin_approvals": {"type": "array", "items": {}},
            "positioning": {"type": "array", "items": {}},
            "categories": {"type": "array", "items": {}}
        },
        "required": [
            "or_id", 
            "or_denomination", 
            "or_id_address",
            "or_country"
        ],
        "additionalProperties": False
    }
}

# Champs à ajouter avec leurs valeurs par défaut
DEFAULT_FIELDS = {
    "or_country": "FRANCE",
    "fk_us": 0,  # À adapter selon votre logique métier
    "fk_ot": 1,  # À adapter selon votre logique métier
    "fk_ovs": 1,  # À adapter selon votre logique métier
    "fk_cou": 75,  # À adapter selon votre logique métier
    "or_desactivation_date": None,
    "or_logo": None,
    "or_creation_date": None,  # Sera défini par défaut à now() dans la base de données
    "fileFi_id": None,
    "logistic_address": [],
    "organization_categories": [],
    "origin_approvals": [],
    "positioning": [],
    "categories": []
}

# Informations sur les contraintes de longueur des champs
FIELD_LENGTH_CONSTRAINTS = {
    "or_denomination": 255,
    "or_rna": 255,
    "or_house_number": 255,
    "or_street": 255,
    "or_postal_code": 255,
    "or_city": 255,
    "or_country": 255,
    "or_state": 255,
    "or_logo": 100,
    "or_additionnal_information": 500
}

# Informations sur les types de champs spéciaux
SPECIAL_FIELD_TYPES = {
    "or_creation_date": datetime,
    "or_desactivation_date": datetime,
    "fileFi_id": int
}