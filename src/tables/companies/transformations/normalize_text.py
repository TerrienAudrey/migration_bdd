"""
Module de normalisation du texte pour les données Companies.
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
    - Conversion en majuscules
    
    Args:
        df: DataFrame contenant les données Companies
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les champs textuels normalisés
        - La liste des erreurs de normalisation détectées
    """
    errors = []
    result_df = df.copy()
    
    # Liste des champs textuels à normaliser
    text_fields = [
        "co_business_name",
        "co_head_office_address",
        "co_head_office_city",
        "co_legal_form",
        "co_head_office_additional_address"
    ]
    
    # Fonction de normalisation à appliquer sur chaque champ
    def normalize_string(value):
        if pd.isna(value) or not isinstance(value, str):
            return value
        
        # Trim: suppression des espaces en début et fin
        value = value.strip()
        
        # Normalisation des espaces: remplacer les multiples espaces par un seul
        value = re.sub(r'\s+', ' ', value)
        
        # Conversion en majuscules
        value = value.upper()
        
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
                    co_id = result_df.at[idx, 'co_id']
                    
                    # Enregistrer la modification comme information, pas comme erreur
                    errors.append({
                        "type": "text_normalization",
                        "severity": "info",
                        "co_id": co_id,
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
    
    # Création d'un champ normalisé spécifique pour le nom d'entreprise
    # Ce champ sera utilisé pour les recherches et correspondances
    try:
        result_df['co_normalized_business_name'] = result_df['co_business_name'].apply(normalize_string)
    except Exception as e:
        errors.append({
            "type": "field_creation_error",
            "severity": "error",
            "field": "co_normalized_business_name",
            "message": str(e)
        })
    
    return result_df, errors