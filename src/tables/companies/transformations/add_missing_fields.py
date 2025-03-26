"""
Module pour ajouter les champs manquants au modèle Companies.
"""

from typing import Dict, List, Tuple, Any

import pandas as pd


def add_missing_fields(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Ajoute les champs manquants requis par le modèle Prisma.
    
    Args:
        df: DataFrame contenant les données Companies
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les champs manquants ajoutés
        - La liste des informations sur les modifications
    """
    errors = []
    result_df = df.copy()
    
    # Liste des champs à ajouter s'ils sont manquants
    missing_fields = {
        "co_head_office_country": "FRANCE",  # Valeur par défaut pour la France
        "co_desactivation_date": None        # Valeur par défaut à null
    }
    
    # Ajout des champs manquants
    for field, default_value in missing_fields.items():
        if field not in result_df.columns:
            result_df[field] = default_value
            errors.append({
                "type": "field_added",
                "severity": "info",
                "field": field,
                "default_value": str(default_value),
                "message": f"Champ '{field}' ajouté avec la valeur par défaut"
            })
    
    return result_df, errors