"""
Définition de la structure de sortie pour les données 'companies'.
Ce module décrit la structure finale attendue après transformation.
"""

from typing import Dict, Any, List

# Schéma de validation pour les données de sortie
# Maintient la même structure que l'entrée mais avec des contraintes plus strictes
OUTPUT_SCHEMA: Dict[str, Any] = {
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
            "co_head_office_additional_address": {"type": ["string", "null"]},
            # Champs additionnels après transformation
            "co_head_office_street_number": {"type": ["string", "null"]},
            "co_head_office_street_name": {"type": ["string", "null"]},
            "co_normalized_business_name": {"type": "string"},
            "co_validation_status": {"type": "object"}
        },
        "required": [
            "co_id", 
            "co_business_name", 
            "fk_us", 
            "co_normalized_business_name",
            "co_validation_status"
        ],
        "additionalProperties": False
    }
}

# Définition des nouveaux champs ajoutés par la transformation
ADDED_FIELDS = [
    "co_head_office_street_number",  # Numéro de rue extrait de l'adresse
    "co_head_office_street_name",    # Nom de rue extrait de l'adresse
    "co_normalized_business_name",   # Nom d'entreprise normalisé
    "co_validation_status"           # Statut de validation avec détails par champ
]

# Structure du champ de statut de validation
VALIDATION_STATUS_STRUCTURE = {
    "is_valid": bool,                # Validité globale de l'enregistrement
    "field_status": Dict[str, bool], # Statut par champ
    "error_details": List[Dict]      # Détails des erreurs si présentes
}

# Format attendu pour les champs normalisés
NORMALIZED_FORMATS = {
    "co_business_name": "Majuscules, sans accents, espaces simples",
    "co_head_office_address": "Majuscules, sans accents, espaces simples",
    "co_head_office_city": "Majuscules, sans accents, espaces simples",
    "co_head_office_postal_code": "5 chiffres sans espaces",
    "co_siren": "9 chiffres sans espaces",
    "co_siret": "14 chiffres sans espaces",
    "co_vat": "Format FR suivi de 11 chiffres"
}

# Relation entre champs pour la validation de cohérence
FIELD_RELATIONSHIPS = [
    {
        "fields": ["co_siren", "co_siret"],
        "rule": "co_siret doit commencer par co_siren",
        "validation": lambda siren, siret: siret.startswith(siren) if siren and siret else True
    },
    {
        "fields": ["co_siren", "co_vat"],
        "rule": "co_vat doit contenir co_siren après les deux premières lettres",
        "validation": lambda siren, vat: vat[2:].startswith(siren) if siren and vat and vat.startswith("FR") else True
    }
]

# Documentation des champs de sortie pour référence
OUTPUT_FIELD_DESCRIPTIONS = {
    "co_id": "Identifiant unique de l'entreprise",
    "co_business_name": "Nom commercial de l'entreprise (normalisé)",
    "co_siren": "Numéro SIREN (9 chiffres, validé)",
    "co_siret": "Numéro SIRET (14 chiffres, validé)",
    "co_vat": "Numéro de TVA intracommunautaire (validé)",
    "co_code_ent": "Code entreprise interne",
    "co_head_office_address": "Adresse du siège social (normalisée)",
    "co_head_office_city": "Ville du siège social (normalisée)",
    "co_head_office_postal_code": "Code postal du siège social (validé)",
    "co_legal_form": "Forme juridique de l'entreprise",
    "fk_us": "Clé étrangère vers la table utilisateurs",
    "co_head_office_additional_address": "Complément d'adresse du siège social",
    "co_head_office_street_number": "Numéro de rue extrait de l'adresse",
    "co_head_office_street_name": "Nom de rue extrait de l'adresse",
    "co_normalized_business_name": "Nom d'entreprise normalisé pour recherche",
    "co_validation_status": "Statut détaillé de validation de l'enregistrement"
}