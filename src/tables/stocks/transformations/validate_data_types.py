"""
Module de validation des types de données pour les données stocks.
Vérifie et corrige les types selon les spécifications.
"""

from typing import Dict, List, Tuple, Any, Optional, Union

import pandas as pd


def validate_data_types(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Valide et corrige les types de données dans le DataFrame.
    
    Vérifications:
    - Conversion des valeurs booléennes textuelles/numériques en booléens
    - Conversion des valeurs numériques textuelles en nombres
    - Validation des clés étrangères (entier ou null)
    
    Args:
        df: DataFrame contenant les données stocks
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les types de données validés et corrigés
        - La liste des erreurs/modifications détectées
    """
    errors = []
    result_df = df.copy()
    
    # Liste des champs booléens
    boolean_fields = [
        "st_is_freetransport"
    ]
    
    # Liste des champs numériques (integer)
    integer_fields = [
        "fk_sta",
        "fk_co"
    ]
    
    # Liste des champs numériques (float)
    float_fields = [
        "st_commission",
        "st_commission_percent"
    ]
    
    # Fonction pour convertir en booléen
    def convert_to_boolean(value: Any) -> bool:
        if pd.isna(value):
            return False
        
        if isinstance(value, bool):
            return value
            
        if isinstance(value, (int, float)):
            return bool(value)
            
        if isinstance(value, str):
            value_lower = value.lower().strip()
            if value_lower in ['true', 't', 'yes', 'y', '1']:
                return True
            elif value_lower in ['false', 'f', 'no', 'n', '0']:
                return False
        
        # Par défaut, retourner False
        return False
    
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
    
    # Fonction pour convertir en float
    def convert_to_float(value: Any) -> Optional[float]:
        if pd.isna(value):
            return None
            
        if isinstance(value, (int, float)):
            return float(value)
            
        if isinstance(value, str):
            try:
                return float(value)
            except (ValueError, TypeError):
                pass
        
        # En cas d'échec, retourner None
        return None
    
    # Traitement des champs booléens
    for field in boolean_fields:
        if field in result_df.columns:
            # Sauvegarde des valeurs originales
            original_values = result_df[field].copy()
            
            # Conversion
            result_df[field] = result_df[field].apply(convert_to_boolean)
            
            # Détecter les modifications
            modified_indices = result_df.index[original_values != result_df[field]].tolist()
            
            # Journaliser les modifications
            for idx in modified_indices:
                st_id = result_df.at[idx, 'st_id']
                original = original_values.iloc[idx]
                converted = result_df.at[idx, field]
                
                errors.append({
                    "type": "boolean_conversion",
                    "severity": "info",
                    "st_id": st_id,
                    "index": idx,
                    "field": field,
                    "original": original,
                    "converted": converted
                })
    
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
                st_id = result_df.at[idx, 'st_id']
                original = original_values.iloc[idx]
                converted = result_df.at[idx, field]
                
                errors.append({
                    "type": "integer_conversion",
                    "severity": "info",
                    "st_id": st_id,
                    "index": idx,
                    "field": field,
                    "original": original,
                    "converted": converted
                })
    
    # Traitement des champs float
    for field in float_fields:
        if field in result_df.columns:
            # Sauvegarde des valeurs originales
            original_values = result_df[field].copy()
            
            # Conversion
            result_df[field] = result_df[field].apply(convert_to_float)
            
            # Détecter les modifications
            modified_mask = (original_values != result_df[field]) & (~pd.isna(original_values) | ~pd.isna(result_df[field]))
            modified_indices = result_df.index[modified_mask].tolist()
            
            # Journaliser les modifications
            for idx in modified_indices:
                st_id = result_df.at[idx, 'st_id']
                original = original_values.iloc[idx]
                converted = result_df.at[idx, field]
                
                errors.append({
                    "type": "float_conversion",
                    "severity": "info",
                    "st_id": st_id,
                    "index": idx,
                    "field": field,
                    "original": original,
                    "converted": converted
                })
    
    return result_df, errors