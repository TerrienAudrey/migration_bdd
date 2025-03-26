"""
Définition de la structure de sortie pour les données 'stock_import'.
Ce module décrit la structure finale attendue après transformation.
"""

from typing import Dict, Any, List

# Schéma de validation pour les données de sortie
OUTPUT_SCHEMA: Dict[str, Any] = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "si_id": {"type": "integer"},
            "si_date_removal": {"type": ["string", "null"]},
            "si_date_delivery": {"type": ["string", "null"]},
            "si_total_price": {"type": "number"},
            "fk_st": {"type": ["integer", "null"]},
            "fk_co": {"type": ["integer", "null"]},
            "si_is_pallet": {"type": "boolean"},
            "si_is_ready": {"type": "boolean"},
            "si_is_dangerous": {"type": "boolean"},
            "si_quantity": {"type": "integer"},
            "si_quantity_stackable": {"type": ["integer", "null"]},
            "si_gpt_response_matching_json": {"type": ["string", "object"]},
            "si_gpt_response_category_json": {"type": ["string", "object"]},
            "si_date_alert_removal": {"type": ["string", "null"]},
            "si_date_alert_delivery": {"type": ["string", "null"]},
            "si_date_process": {"type": ["string", "null"]},
            "si_packaging_method": {"type": "array"},
            "positioning": {"type": "array"},
            "fk_la": {"type": ["integer", "null"]},
            "fk_tra": {"type": ["integer", "null"]},
            "si_file": {"type": "string"},
            "si_filename": {"type": "string"},
            "si_gpt_file_id": {"type": "string"},
            "si_gpt_thread_matching_id": {"type": "string"},
            "si_gpt_thread_category_id": {"type": "string"},
            "logistic_address": {"type": ["object", "null"]},
            "stock": {"type": ["object", "null"]},
            "transports": {"type": ["object", "null"]},
            "si_validation_status": {"type": "object"}
        },
        "required": [
            "si_id",
            "si_total_price",
            "si_is_pallet",
            "si_is_ready",
            "si_is_dangerous",
            "si_quantity",
            "si_file",
            "si_filename",
            "si_packaging_method",
            "positioning"
        ],
        "additionalProperties": False
    }
}

# Champs à ajouter avec leurs valeurs par défaut
DEFAULT_FIELDS = {
    "si_is_pallet": False,
    "si_is_ready": False,
    "si_is_dangerous": False,
    "si_quantity": 0,
    "si_quantity_stackable": 0,
    "si_gpt_response_matching_json": "{}",
    "si_gpt_response_category_json": "{}",
    "si_date_alert_removal": None,
    "si_date_alert_delivery": None,
    "si_date_process": None,
    "fk_la": None,
    "fk_tra": None,
    "si_file": "",
    "si_filename": "",
    "si_gpt_file_id": "",
    "si_gpt_thread_matching_id": "",
    "si_gpt_thread_category_id": "",
    "logistic_address": None,
    "stock": None,
    "transports": None
}

# Informations sur les contraintes de longueur des champs
FIELD_LENGTH_CONSTRAINTS = {
    "si_file": 255,
    "si_filename": 255,
    "si_gpt_file_id": 255,
    "si_gpt_thread_matching_id": 255,
    "si_gpt_thread_category_id": 255
}

# Définition des nouveaux champs ajoutés par la transformation
ADDED_FIELDS = [
    "si_is_pallet",
    "si_is_ready",
    "si_is_dangerous",
    "si_quantity",
    "si_quantity_stackable",
    "si_gpt_response_matching_json",
    "si_gpt_response_category_json",
    "si_date_alert_removal",
    "si_date_alert_delivery",
    "si_date_process",
    "si_packaging_method",
    "positioning",
    "fk_la",
    "fk_tra",
    "si_file",
    "si_filename",
    "si_gpt_file_id",
    "si_gpt_thread_matching_id",
    "si_gpt_thread_category_id",
    "logistic_address",
    "stock",
    "transports",
    "si_validation_status"
]

# Structure du champ de statut de validation
VALIDATION_STATUS_STRUCTURE = {
    "is_valid": bool,                # Validité globale de l'enregistrement
    "field_status": Dict[str, bool], # Statut par champ
    "error_details": List[Dict]      # Détails des erreurs si présentes
}

# Documentation des champs de sortie pour référence
OUTPUT_FIELD_DESCRIPTIONS = {
    "si_id": "Identifiant unique de l'import de stock",
    "si_date_removal": "Date de retrait",
    "si_date_delivery": "Date de livraison",
    "si_total_price": "Prix total",
    "fk_st": "Clé étrangère vers la table stocks",
    "fk_co": "Clé étrangère vers la table companies",
    "si_is_pallet": "Indicateur si l'import est sur palette",
    "si_is_ready": "Indicateur si l'import est prêt",
    "si_is_dangerous": "Indicateur si l'import contient des matières dangereuses",
    "si_quantity": "Quantité d'items",
    "si_quantity_stackable": "Quantité empilable",
    "si_gpt_response_matching_json": "Réponse GPT pour le matching au format JSON",
    "si_gpt_response_category_json": "Réponse GPT pour la catégorisation au format JSON",
    "si_date_alert_removal": "Date d'alerte pour le retrait",
    "si_date_alert_delivery": "Date d'alerte pour la livraison",
    "si_date_process": "Date de traitement",
    "si_packaging_method": "Méthodes d'emballage",
    "positioning": "Positions",
    "fk_la": "Clé étrangère vers la table logistic_address",
    "fk_tra": "Clé étrangère vers la table transports",
    "si_file": "Fichier associé",
    "si_filename": "Nom du fichier",
    "si_gpt_file_id": "ID du fichier GPT",
    "si_gpt_thread_matching_id": "ID du thread GPT pour le matching",
    "si_gpt_thread_category_id": "ID du thread GPT pour la catégorisation",
    "logistic_address": "Adresse logistique associée",
    "stock": "Stock associé",
    "transports": "Transport associé"
}