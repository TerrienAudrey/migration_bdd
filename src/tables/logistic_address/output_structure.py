"""
Définition de la structure de sortie pour les données 'logistic_address'.
Ce module décrit la structure finale attendue après transformation.
"""

from typing import Dict, Any, List

# Schéma de validation pour les données de sortie
OUTPUT_SCHEMA: Dict[str, Any] = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "la_id": {"type": "integer"},
            "la_house_number": {"type": ["string", "null"], "maxLength": 255},
            "la_street": {"type": ["string", "null"], "maxLength": 255},
            "la_additional_address": {"type": ["string", "null"], "maxLength": 255},
            "la_postal_code": {"type": ["string", "null"], "maxLength": 255},
            "la_city": {"type": ["string", "null"], "maxLength": 255},
            "la_truck_access": {"type": "boolean"},
            "la_loading_dock": {"type": "boolean"},
            "la_forklift": {"type": "boolean"},
            "la_pallet": {"type": "boolean"},
            "la_fenwick": {"type": "boolean"},
            "la_palet_capacity": {"type": ["integer", "null"]},
            "la_longitude": {"type": ["number", "null"]},
            "la_latitude": {"type": ["number", "null"]},
            "la_isactive": {"type": "boolean"},
            "fk_co": {"type": ["integer", "null"]},
            "fk_or": {"type": ["integer", "null"]},
            "fk_re": {"type": ["integer", "null"]},
            "fk_con": {"type": ["integer", "null"]},
            "fk_cou": {"type": ["integer", "null"], "default": 75},
            "stock_import": {"type": "array", "items": {"type": "string"}},
            "opening_hour": {"type": "array", "items": {}},
            "positioning": {"type": "array", "items": {}},
            "la_validation_status": {"type": "object"}
        },
        "required": [
            "la_id",
            "la_truck_access",
            "la_loading_dock",
            "la_forklift",
            "la_pallet",
            "la_fenwick",
            "la_isactive",
            "stock_import",
            "opening_hour"
        ],
        "additionalProperties": False
    }
}

# Champs à ajouter avec leurs valeurs par défaut
DEFAULT_FIELDS = {
    "fk_cou": 75,  # France par défaut
    "la_truck_access": False,
    "la_loading_dock": False,
    "la_forklift": False,
    "la_pallet": False,
    "la_fenwick": False,
    "la_palet_capacity": 0,
    "la_longitude": 0,
    "la_latitude": 0,
    "la_isactive": False,
    "fk_re": None,
    "fk_con": None,
    "opening_hour": [],
    "positioning": []
}

# Informations sur les contraintes de longueur des champs
FIELD_LENGTH_CONSTRAINTS = {
    "la_house_number": 255,
    "la_street": 255,
    "la_additional_address": 255,
    "la_postal_code": 255,
    "la_city": 255
}

# Définition des nouveaux champs ajoutés par la transformation
ADDED_FIELDS = [
    "opening_hour",  # Tableau vide par défaut
    "positioning",   # Tableau vide par défaut
    "la_validation_status"  # Statut de validation avec détails par champ
]

# Structure du champ de statut de validation
VALIDATION_STATUS_STRUCTURE = {
    "is_valid": bool,                # Validité globale de l'enregistrement
    "field_status": Dict[str, bool], # Statut par champ
    "error_details": List[Dict]      # Détails des erreurs si présentes
}

# Format attendu pour les champs normalisés
NORMALIZED_FORMATS = {
    "la_house_number": "Chaîne alphanumérique sans espaces en début/fin",
    "la_street": "Majuscules, sans accents, espaces simples",
    "la_additional_address": "Majuscules, sans accents, espaces simples",
    "la_postal_code": "5 chiffres sans espaces",
    "la_city": "Majuscules, sans accents, espaces simples, sans mention d'arrondissement"
}

# Documentation des champs de sortie pour référence
OUTPUT_FIELD_DESCRIPTIONS = {
    "la_id": "Identifiant unique de l'adresse logistique",
    "la_house_number": "Numéro de rue (normalisé)",
    "la_street": "Nom de la rue (normalisé)",
    "la_additional_address": "Informations complémentaires d'adresse (normalisé)",
    "la_postal_code": "Code postal (validé)",
    "la_city": "Ville (normalisée, sans mention d'arrondissement)",
    "la_truck_access": "Indicateur d'accessibilité pour les camions (booléen)",
    "la_loading_dock": "Indicateur de présence d'un quai de chargement (booléen)",
    "la_forklift": "Indicateur de disponibilité d'un chariot élévateur (booléen)",
    "la_pallet": "Indicateur de disponibilité de palettes (booléen)",
    "la_fenwick": "Indicateur de disponibilité d'un Fenwick (booléen)",
    "la_palet_capacity": "Capacité de stockage en palettes (entier)",
    "la_longitude": "Coordonnée longitudinale (nombre)",
    "la_latitude": "Coordonnée latitudinale (nombre)",
    "la_isactive": "Indicateur d'activité de l'adresse (booléen)",
    "fk_co": "Clé étrangère vers la table companies",
    "fk_or": "Clé étrangère vers la table organizations",
    "fk_re": "Clé étrangère vers la table recycling",
    "fk_con": "Clé étrangère vers la table contacts",
    "fk_cou": "Clé étrangère vers la table countries (defaulted to 75 for France)",
    "stock_import": "Tableau de chaînes identifiant les imports de stock",
    "opening_hour": "Tableau d'heures d'ouverture (initialisé vide)",
    "positioning": "Tableau de positionnements (initialisé vide)",
    "la_validation_status": "Statut détaillé de validation de l'enregistrement"
}