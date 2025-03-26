"""
Module de validation des types de données pour les données transports.
Vérifie et corrige les types selon les spécifications.
"""

from typing import Dict, List, Tuple, Any, Optional, Union

import pandas as pd


def validate_data_types(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Valide et corrige les types de données dans le DataFrame.
    
    Vérifications:
    - Conversion des valeurs numériques textuelles en nombres
    - Validation des clés étrangères (entier ou null)
    
    Args:
        df: DataFrame contenant les données transports
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les types de données validés et corrigés
        - La liste des erreurs/modifications détectées
    """
    errors = []
    result_df = df.copy()
    
    # Liste des champs numériques (integer)
    integer_fields = [
        "tra_id",
        "fk_con"
    ]
    
    # Fonction pour convertir en entier
    def convert_to_integer(value: Any) -> Optional[int]:
        if pd.isna(value):
            return None
            
        if isinstance(value, int):
            return value
            
        if isinstance(value, float) and value.is_integer():
            return int(value)
            
        if isinstance(value, str):
            try:
                # Essayer de convertir en float d'abord (pour gérer les notations scientifiques)
                float_val = float(value)
                if float_val.is_integer():
                    return int(float_val)
                return int(float_val)  # Arrondir si nécessaire
            except (ValueError, TypeError):
                pass
        
        # En cas d'échec, retourner None
        return None
    
    # Traitement des champs entiers
    for field in integer_fields:
        if field in result_df.columns:
            # Sauvegarde des valeurs originales
            original_values = result_df[field].copy()
            
            # Conversion
            result_df[field] = result_df[field].apply(convert_to_integer)
            
            # Détecter les modifications
            modified_mask = (original_values != result_df[field]) & (~pd.isna(original_values) | ~pd.isna(result_df[field]))
            modified_indices = result_df.index[modified_mask].tolist()
            
            # Journaliser les modifications
            for idx in modified_indices:
                tra_id = result_df.at[idx, 'tra_id']
                original = original_values.iloc[idx]
                converted = result_df.at[idx, field]
                
                errors.append({
                    "type": "integer_conversion",
                    "severity": "info",
                    "tra_id": tra_id,
                    "index": idx,
                    "field": field,
                    "original": original,
                    "converted": converted
                })
    
    # Vérification des tableaux (stock_import, deliveries, contacts)
    array_fields = ["stock_import", "deliveries", "contacts"]
    
    for field in array_fields:
        if field in result_df.columns:
            # Sauvegarde des valeurs originales
            original_values = result_df[field].copy()
            
            # S'assurer que les valeurs sont des tableaux
            result_df[field] = result_df[field].apply(
                lambda x: x if isinstance(x, list) else ([] if pd.isna(x) else [x])
            )
            
            # Détecter les modifications
            modified_indices = result_df.index[
                result_df[field].apply(str) != original_values.apply(str)
            ].tolist()

# Journaliser les modifications
            for idx in modified_indices:
                tra_id = result_df.at[idx, 'tra_id']
                original = original_values.iloc[idx]
                converted = result_df.at[idx, field]
                
                errors.append({
                    "type": "array_conversion",
                    "severity": "info",
                    "tra_id": tra_id,
                    "index": idx,
                    "field": field,
                    "original": original,
                    "converted": converted
                })
    
    return result_df, errors