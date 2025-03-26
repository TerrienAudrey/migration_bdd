"""
Module pour ajouter les champs manquants au modèle logistic_address.
"""

from typing import Dict, List, Tuple, Any

import pandas as pd


def add_missing_fields(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Ajoute les champs manquants requis par le modèle Prisma.
    
    Args:
        df: DataFrame contenant les données logistic_address
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les champs manquants ajoutés
        - La liste des informations sur les modifications
    """
    errors = []
    result_df = df.copy()


    # Liste des champs à ajouter s'ils sont manquants
    missing_fields = {
        "fk_cou": 1,  # Valeur par défaut pour la France
        "fk_re": None,
        "fk_con": None
    }
    
    # Champs qui sont des listes - ils doivent être traités différemment
    array_fields = {
        "opening_hour": [],
        "positioning": []
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
        elif result_df[field].isna().all():
            # Si le champ existe mais toutes les valeurs sont NA, définir la valeur par défaut
            result_df[field] = default_value
            errors.append({
                "type": "field_default_applied",
                "severity": "info",
                "field": field,
                "default_value": str(default_value),
                "message": f"Valeur par défaut appliquée au champ '{field}' car toutes les valeurs étaient NA"
            })
    
    # Ajout des champs qui sont des listes
    for field, default_value in array_fields.items():
        if field not in result_df.columns:
            # Créer une liste vide pour chaque ligne
            result_df[field] = [default_value.copy() for _ in range(len(result_df))]
            errors.append({
                "type": "field_added",
                "severity": "info",
                "field": field,
                "default_value": "liste vide",
                "message": f"Champ '{field}' ajouté avec une liste vide pour chaque ligne"
            })
    
    # Initialisation de stock_import comme liste vide si manquant
    if 'stock_import' not in result_df.columns:
        result_df['stock_import'] = [[] for _ in range(len(result_df))]
        errors.append({
            "type": "field_added",
            "severity": "info",
            "field": "stock_import",
            "default_value": "liste vide",
            "message": "Champ 'stock_import' ajouté avec une liste vide pour chaque ligne"
        })
    elif result_df['stock_import'].isna().any():
        # Si certaines valeurs sont NA, les remplacer par des listes vides
        result_df.loc[result_df['stock_import'].isna(), 'stock_import'] = [[] for _ in range(result_df['stock_import'].isna().sum())]
        errors.append({
            "type": "field_default_applied",
            "severity": "info",
            "field": "stock_import",
            "default_value": "liste vide",
            "message": "Liste vide appliquée aux valeurs NA dans le champ 'stock_import'"
        })
    
    return result_df, errors