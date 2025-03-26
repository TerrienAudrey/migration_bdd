"""
Module de normalisation des caractères spéciaux pour les données Companies.
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
        df: DataFrame contenant les données Companies
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les caractères spéciaux normalisés
        - La liste des erreurs de normalisation détectées
    """
    errors = []
    result_df = df.copy()
    
    # Liste des champs à normaliser
    text_fields = [
        "co_business_name",
        "co_head_office_address",
        "co_head_office_city",
        "co_legal_form",
        "co_head_office_additional_address"
    ]
    
    # Champs à exclure de la normalisation des caractères (comme les numéros d'identification)
    excluded_fields = [
        "co_siren",
        "co_siret",
        "co_vat",
        "co_head_office_postal_code"
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
        # Fractions
        "½": "1/2",
        "¼": "1/4",
        "¾": "3/4"
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
        if field in result_df.columns and field not in excluded_fields:
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
                    co_id = result_df.at[idx, 'co_id']
                    
                    # Enregistrer la modification comme information
                    errors.append({
                        "type": "special_chars_normalization",
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
                    "type": "special_chars_normalization_error",
                    "severity": "error",
                    "field": field,
                    "message": str(e)
                })
    
    # Mise à jour du champ normalisé pour le nom d'entreprise
    if 'co_normalized_business_name' in result_df.columns:
        result_df['co_normalized_business_name'] = result_df['co_normalized_business_name'].apply(normalize_chars)
    
    return result_df, errors