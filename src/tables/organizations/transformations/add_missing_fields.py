"""
Module pour ajouter les champs manquants au modèle Organizations.
"""

from typing import Dict, List, Tuple, Any

import pandas as pd


def add_missing_fields(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Ajoute les champs manquants requis par le modèle Prisma.
    
    Args:
        df: DataFrame contenant les données Organizations
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les champs manquants ajoutés
        - La liste des informations sur les modifications
    """
    errors = []
    result_df = df.copy()
    
    # Liste des champs à ajouter s'ils sont manquants
    missing_fields = {
        "or_country": "FRANCE",  # Valeur par défaut pour la France
        "or_desactivation_date": None,  # Valeur par défaut à null
        "fk_us": 0,  # Valeur par défaut pour l'utilisateur
        "fk_ot": 0,  # Valeur par défaut pour organization_types
        "fk_ovs": 0,  # Valeur par défaut pour organization_validation_steps
        "fk_cou": 0,  # Valeur par défaut pour country (France)
        "or_logo": None,
        "fileFi_id": None
    }
    
    # Champs qui sont des listes - ils doivent être traités différemment
    array_fields = {
        "logistic_address": [],
        "organization_categories": [],
        "origin_approvals": [],
        "positioning": [],
        "categories": []
    }
    
    # Ajout des champs manquants (non-listes)
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
    
    # Ajout des champs qui sont des listes (nécessite une approche différente)
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
    
    # Renommage du champ or_additional_address en or_additionnal_information
    if 'or_additional_address' in result_df.columns and 'or_additionnal_information' not in result_df.columns:
        result_df['or_additionnal_information'] = result_df['or_additional_address']
        result_df = result_df.drop(columns=['or_additional_address'])
        errors.append({
            "type": "field_renamed",
            "severity": "info",
            "original": "or_additional_address",
            "renamed": "or_additionnal_information",
            "message": "Champ 'or_additional_address' renommé en 'or_additionnal_information'"
        })
    
    return result_df, errors