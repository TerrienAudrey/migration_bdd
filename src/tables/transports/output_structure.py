"""
Définition de la structure de sortie pour les données 'transports'.
Ce module décrit la structure finale attendue après transformation.
"""

from typing import Dict, Any, List

# Schéma de validation pour les données de sortie
OUTPUT_SCHEMA: Dict[str, Any] = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "tra_id": {"type": "integer"},
            "tra_denomination": {"type": "string", "maxLength": 255},
            "stock_import": {"type": "array", "items": {"type": "integer"}},
            "deliveries": {"type": "array", "items": {}},
            "contacts": {"type": "array", "items": {}},
            "fk_con": {"type": ["integer", "null"]},
            "tra_validation_status": {"type": "object"}
        },
        "required": [
            "tra_id",
            "tra_denomination",
            "stock_import",
            "deliveries",
            "contacts"
        ],
        "additionalProperties": False
    }
}

# Champs à ajouter avec leurs valeurs par défaut
DEFAULT_FIELDS = {
    "fk_con": None,
    "deliveries": [],
    "contacts": []
}

# Informations sur les contraintes de longueur des champs
FIELD_LENGTH_CONSTRAINTS = {
    "tra_denomination": 255
}

# Définition des nouveaux champs ajoutés par la transformation
ADDED_FIELDS = [
    "deliveries",   # Tableau vide par défaut
    "contacts",     # Tableau vide par défaut
    "tra_validation_status"  # Statut de validation avec détails par champ
]

# Structure du champ de statut de validation
VALIDATION_STATUS_STRUCTURE = {
    "is_valid": bool,                # Validité globale de l'enregistrement
    "field_status": Dict[str, bool], # Statut par champ
    "error_details": List[Dict]      # Détails des erreurs si présentes
}

# Format attendu pour les champs normalisés
NORMALIZED_FORMATS = {
    "tra_denomination": "Majuscules, sans accents, espaces simples",
}

# Documentation des champs de sortie pour référence
OUTPUT_FIELD_DESCRIPTIONS = {
    "tra_id": "Identifiant unique du transporteur",
    "tra_denomination": "Nom du transporteur (normalisé)",
    "stock_import": "Tableau d'identifiants d'imports de stock (dédupliqué)",
    "deliveries": "Tableau vide pour les livraisons (initialisé vide)",
    "contacts": "Tableau vide pour les contacts (initialisé vide)",
    "fk_con": "Clé étrangère vers la table contacts (peut être null)",
    "tra_validation_status": "Statut détaillé de validation de l'enregistrement"
}