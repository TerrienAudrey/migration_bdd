"""
Module pour ajouter les champs manquants au modèle stock_import.
"""

from typing import Dict, List, Tuple, Any

import pandas as pd


def add_missing_fields(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Ajoute les champs manquants requis par le modèle Prisma.
    
    Args:
        df: DataFrame contenant les données stock_import
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les champs manquants ajoutés
        - La liste des informations sur les modifications
    """
    errors = []
    result_df = df.copy()
    
    # Liste des champs à ajouter s'ils sont manquants
    missing_fields = {
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
    
    # Champs qui sont des listes - ils doivent être traités différemment
    array_fields = {
        "si_packaging_method": [],
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
        else:
            # Pour chaque ligne où la valeur est NaN, remplacer par la valeur par défaut
            for idx in result_df.index:
                if pd.isna(result_df.at[idx, field]):
                    result_df.at[idx, field] = default_value
        
        # elif result_df[field].isna().all():
        #     # Si le champ existe mais toutes les valeurs sont NA, définir la valeur par défaut
        #     result_df[field] = default_value
        #     errors.append({
        #         "type": "field_default_applied",
        #         "severity": "info",
        #         "field": field,
        #         "default_value": str(default_value),
        #         "message": f"Valeur par défaut appliquée au champ '{field}' car toutes les valeurs étaient NA"
        #     })
    
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
        elif result_df[field].isna().any():
            # Si certaines valeurs sont NA, les remplacer par des listes vides
            # Attention à la façon de manipuler les valeurs individuellement
            for idx in result_df.index[result_df[field].isna()]:
                result_df.at[idx, field] = default_value.copy()
            
            errors.append({
                "type": "field_default_applied",
                "severity": "info",
                "field": field,
                "default_value": "liste vide",
                "message": f"Liste vide appliquée aux valeurs NA dans le champ '{field}'"
            })
    
    return result_df, errors