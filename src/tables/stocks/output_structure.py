"""
Définition de la structure de sortie pour les données 'stocks'.
Ce module décrit la structure finale attendue après transformation.
"""

from typing import Dict, Any, List

# Schéma de validation pour les données de sortie
OUTPUT_SCHEMA: Dict[str, Any] = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "st_id": {"type": "integer"},
            "st_io": {"type": "string"},
            "st_transportby": {"type": ["string", "null"]},
            "st_is_standby": {"type": "boolean", "default": False},
            "st_is_taxreceiptdeadline": {"type": "boolean", "default": False},
            "st_is_runoffdeadline": {"type": "boolean", "default": False},
            "st_is_showorganization": {"type": "boolean", "default": False},
            "st_commentary": {"type": ["string", "null"]},
            "st_instructions": {"type": ["string", "null"]},
            "st_use_by_date": {"type": ["string", "null"], "format": "date-time"},
            "st_creation_date": {"type": ["string", "null"], "format": "date-time"},
            "st_ending_date": {"type": ["string", "null"], "format": "date-time"},
            "fk_sta": {"type": ["integer", "null"], "default": 1},
            "fk_us": {"type": ["integer", "null"]},
            "fk_co": {"type": ["integer", "null"]},
            "fk_sav": {"type": ["integer", "null"]},
            "st_is_freetransport": {"type": "boolean", "default": False},
            "st_commission": {"type": ["number", "null"]},
            "st_commission_percent": {"type": ["number", "null"]},
            "st_step_planning": {"type": "string", "default": "NOTSTARTED"},
            "st_has_priority": {"type": "boolean", "default": False},
            "stock_import": {"type": "array", "items": {"type": "integer"}},
            "positioning": {"type": "array", "items": {}},
            "stock_sav_history": {"type": "array", "items": {}},
            "stock_status_history": {"type": "array", "items": {}}
        },
        "required": [
            "st_id",
            "st_io",
            "st_is_freetransport",
            "st_is_standby",
            "st_is_taxreceiptdeadline",
            "st_is_runoffdeadline",
            "st_is_showorganization",
            "st_step_planning",
            "st_has_priority"
        ],
        "additionalProperties": False
    }
}

# Champs à ajouter avec leurs valeurs par défaut
DEFAULT_FIELDS = {
    "st_is_standby": False,
    "st_is_taxreceiptdeadline": False,
    "st_is_runoffdeadline": False,
    "st_is_showorganization": False,
    "st_is_freetransport": False,
    "st_has_priority": False,
    "st_step_planning": "NOTSTARTED",
    "fk_sta": 1,
    "st_instructions": None,
    "st_use_by_date": None,
    "st_ending_date": None,
    "fk_us": None,
    "fk_sav": None
}

# Définition des nouveaux champs ajoutés par la transformation
ADDED_FIELDS = [
    "st_is_standby",
    "st_is_taxreceiptdeadline",
    "st_is_runoffdeadline",
    "st_is_showorganization",
    "st_instructions",
    "st_use_by_date",
    "st_ending_date",
    "fk_us",
    "fk_sav",
    "st_step_planning",
    "st_has_priority",
    "positioning",
    "stock_sav_history",
    "stock_status_history"
]

# Champs qui sont des listes - ils doivent être traités différemment
ARRAY_FIELDS = {
    "positioning": [],
    "stock_sav_history": [],
    "stock_status_history": []
}

# Structure du champ de statut de validation
VALIDATION_STATUS_STRUCTURE = {
    "is_valid": bool,                # Validité globale de l'enregistrement
    "field_status": Dict[str, bool], # Statut par champ
    "error_details": List[Dict]      # Détails des erreurs si présentes
}

# Format attendu pour les champs normalisés
NORMALIZED_FORMATS = {
    "st_transportby": "Chaîne en majuscules, sans accents, espaces simples",
    "st_commentary": "Chaîne en majuscules, sans accents, espaces simples",
    "st_instructions": "Chaîne en majuscules, sans accents, espaces simples"
}

# Valeurs possibles pour st_step_planning
STEP_PLANNING_VALUES = [
    "NOTSTARTED",
    "PLANNED",
    "INPROGRESS",
    "COMPLETED"
]

# Documentation des champs de sortie pour référence
OUTPUT_FIELD_DESCRIPTIONS = {
    "st_id": "Identifiant unique du stock",
    "st_io": "Identifiant métier du stock",
    "st_transportby": "Moyen de transport (normalisé)",
    "st_is_standby": "Indicateur de mise en attente",
    "st_is_taxreceiptdeadline": "Indicateur de délai pour reçu fiscal",
    "st_is_runoffdeadline": "Indicateur de délai d'écoulement",
    "st_is_showorganization": "Indicateur d'affichage de l'organisation",
    "st_commentary": "Commentaires sur le stock (normalisé)",
    "st_instructions": "Instructions spécifiques au stock",
    "st_use_by_date": "Date limite d'utilisation",
    "st_creation_date": "Date de création du stock",
    "st_ending_date": "Date de fin du stock",
    "fk_sta": "Clé étrangère vers la table status",
    "fk_us": "Clé étrangère vers la table users",
    "fk_co": "Clé étrangère vers la table companies",
    "fk_sav": "Clé étrangère vers la table stock_sav",
    "st_is_freetransport": "Indicateur de transport gratuit",
    "st_commission": "Montant de la commission",
    "st_commission_percent": "Pourcentage de commission (renommé depuis st_commission_%)",
    "st_step_planning": "Étape de planification (NOTSTARTED, PLANNED, INPROGRESS, COMPLETED)",
    "st_has_priority": "Indicateur de priorité",
    "stock_import": "Tableau d'identifiants des imports (dédupliqué)",
    "positioning": "Tableau de positionnements",
    "stock_sav_history": "Historique des SAV",
    "stock_status_history": "Historique des statuts"
}