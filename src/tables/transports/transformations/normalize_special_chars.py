"""
Module de normalisation des caractères spéciaux pour les données transports.
Remplace les caractères accentués, les caractères spéciaux et standardise les apostrophes.
"""

import re
from typing import Dict, List, Tuple, Any

import pandas as pd
import unidecode


def normalize_special_chars(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Normalise les caractères spéciaux dans les champs textuels.
    
    Opérations effectuées:
    - Suppression/remplacement des accents
    - Standardisation des apostrophes et guillemets
    - Gestion des caractères spéciaux
    
    Args:
        df: DataFrame contenant les données transports
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les caractères spéciaux normalisés
        - La liste des erreurs de normalisation détectées
    """
    errors = []
    result_df = df.copy()
    
    # Liste des champs à normaliser
    text_fields = [
        "tra_denomination"
    ]
    
    # Dictionnaire de substitution pour certains caractères spéciaux
    special_chars_map = {
        # Apostrophes et guillemets
        "'": "'",
        "'": "'",
        "′": "'",
        """: '"',
        """: '"',
        "«": '"',
        "»": '"',
        # Tirets et espaces
        "—": "-",
        "–": "-",
        "‐": "-",
        "‑": "-",
        "­": "",  # Suppression des espaces invisibles
        "\u200b": "",  # Suppression des espaces de largeur nulle
        "\ufeff": "",  # Suppression du BOM UTF-8
        # Autres caractères problématiques
        "�": "",
        # Symboles mathématiques fréquents
        "×": "X",
        "÷": "/",
    }
    
    # Fonction pour normaliser les caractères spéciaux dans une chaîne
    def normalize_chars(value):
        if pd.isna(value) or not isinstance(value, str):
            return value
            
        # Remplacement des caractères spéciaux par leurs équivalents
        for char, replacement in special_chars_map.items():
            if char in value:
                value = value.replace(char, replacement)
        
        # Utilisation d'unidecode pour supprimer les accents
        value = unidecode.unidecode(value)
        
        # Standardisation des espaces (à nouveau car unidecode peut modifier l'espacement)
        value = re.sub(r'\s+', ' ', value).strip()
        
        return value
    
    # Appliquer la normalisation sur chaque champ textuel
    for field in text_fields:
        if field in result_df.columns:
            try:
                # Sauvegarde des valeurs originales pour comparaison
                original_values = result_df[field].copy()
                
                # Application de la normalisation
                result_df[field] = result_df[field].apply(normalize_chars)
                
                # Détection des lignes modifiées pour logging
                modified_mask = (original_values != result_df[field]) & (~pd.isna(original_values))
                modified_indices = result_df.index[modified_mask].tolist()
                
                # Enregistrement des modifications pour suivi
                for idx in modified_indices:
                    original = original_values.iloc[idx]
                    normalized = result_df[field].iloc[idx]
                    tra_id = result_df.at[idx, 'tra_id']
                    
                    # Enregistrer la modification comme information
                    errors.append({
                        "type": "special_chars_normalization",
                        "severity": "info",
                        "tra_id": tra_id,
                        "field": field,
                        "original": original,
                        "normalized": normalized,
                        "index": idx
                    })
            except Exception as e:
                # Enregistrer l'erreur si la normalisation échoue
                errors.append({
                    "type": "special_chars_normalization_error",
                    "severity": "error",
                    "field": field,
                    "message": str(e)
                })
    
    return result_df, errors