"""
Module de validation des dénominations de transporteurs.
Vérifie le format et l'unicité des noms de transporteurs.
"""

import re
from typing import Dict, List, Tuple, Any

import pandas as pd


def validate_denomination(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Valide les dénominations de transporteurs et détecte les doublons.
    
    Args:
        df: DataFrame contenant les données transports
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les dénominations validées
        - La liste des erreurs et modifications détectées
    """
    errors = []
    result_df = df.copy()
    
    # Vérifier si la colonne tra_denomination existe
    if 'tra_denomination' not in result_df.columns:
        errors.append({
            "type": "missing_column",
            "severity": "error",
            "message": "La colonne 'tra_denomination' est absente du DataFrame"
        })
        return result_df, errors
    
    # Expression régulière pour valider le format des dénominations
    denomination_pattern = re.compile(r'^[A-Z0-9\s\-\'&.]+$')
    
    # Dictionnaire pour suivre les dénominations et détecter les doublons
    denomination_dict = {}
    
    # Traitement de chaque ligne
    for idx, row in result_df.iterrows():
        tra_id = row['tra_id']
        denomination = row['tra_denomination']
        
        # Vérifier si denomination est nulle ou vide
        if pd.isna(denomination) or denomination == '':
            errors.append({
                "type": "missing_denomination",
                "severity": "error",
                "tra_id": tra_id,
                "index": idx,
                "message": "La dénomination du transporteur est manquante"
            })
            continue
        
        # Vérifier le format
        if not denomination_pattern.match(denomination):
            errors.append({
                "type": "invalid_denomination_format",
                "severity": "warning",
                "tra_id": tra_id,
                "index": idx,
                "value": denomination,
                "reason": "La dénomination doit contenir uniquement des lettres majuscules, chiffres, espaces, tirets, apostrophes, ampersands et points"
            })
        
        # Vérifier l'unicité en ignorant la casse
        normalized_denomination = denomination.upper()
        
        if normalized_denomination in denomination_dict:
            # Un doublon a été détecté
            duplicate_ids = denomination_dict[normalized_denomination]
            
            errors.append({
                "type": "duplicate_denomination",
                "severity": "error",
                "tra_id": tra_id,
                "index": idx,
                "value": denomination,
                "duplicate_ids": duplicate_ids,
                "reason": f"La dénomination '{denomination}' existe déjà pour les transporteurs avec ID: {duplicate_ids}"
            })
        else:
            # Ajouter cette dénomination au dictionnaire
            denomination_dict[normalized_denomination] = [tra_id]
    
    return result_df, errors