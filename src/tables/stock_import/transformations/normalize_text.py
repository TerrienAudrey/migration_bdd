"""
Module de normalisation du texte pour les données stock_import.
Applique les transformations de base: trim, normalisation des espaces, majuscules.
"""

import re
from typing import Dict, List, Tuple, Any

import pandas as pd


def normalize_text(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Normalise le texte des champs textuels du DataFrame.
    
    Opérations effectuées:
    - Suppression des espaces en début et fin de chaîne (trim)
    - Remplacement des séquences d'espaces multiples par un seul espace
    
    Args:
        df: DataFrame contenant les données stock_import
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les champs textuels normalisés
        - La liste des erreurs de normalisation détectées
    """
    errors = []
    result_df = df.copy()
    
    # Liste des champs textuels à normaliser
    text_fields = [
        "si_file",
        "si_filename",
        "si_gpt_file_id",
        "si_gpt_thread_matching_id",
        "si_gpt_thread_category_id"
    ]
    
    # Fonction de normalisation à appliquer sur chaque champ
    def normalize_string(value):
        if pd.isna(value) or not isinstance(value, str):
            return value
        
        # Trim: suppression des espaces en début et fin
        value = value.strip()
        
        # Normalisation des espaces: remplacer les multiples espaces par un seul
        value = re.sub(r'\s+', ' ', value)
        
        return value

    # Appliquer la normalisation sur chaque champ textuel
    for field in text_fields:
        if field in result_df.columns:
            try:
                # Sauvegarde des valeurs originales pour comparaison
                original_values = result_df[field].copy()
                
                # Application de la normalisation
                result_df[field] = result_df[field].apply(normalize_string)
                
                # Détection des lignes modifiées pour logging
                modified_mask = (original_values != result_df[field]) & (~pd.isna(original_values))
                modified_indices = result_df.index[modified_mask].tolist()
                
                # Enregistrement des modifications pour suivi
                for idx in modified_indices:
                    original = original_values.iloc[idx]
                    normalized = result_df[field].iloc[idx]
                    si_id = result_df.at[idx, 'si_id']
                    
                    # Enregistrer la modification comme information, pas comme erreur
                    errors.append({
                        "type": "text_normalization",
                        "severity": "info",
                        "si_id": si_id,
                        "field": field,
                        "original": original,
                        "normalized": normalized,
                        "index": idx
                    })
            except Exception as e:
                # Enregistrer l'erreur si la normalisation échoue
                errors.append({
                    "type": "text_normalization_error",
                    "severity": "error",
                    "field": field,
                    "message": str(e)
                })
    
    return result_df, errors