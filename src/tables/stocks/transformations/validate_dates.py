"""
Module de validation des dates pour les données stocks.
Vérifie et convertit les formats de date selon les spécifications.
"""

import re
from typing import Dict, List, Tuple, Any, Optional
from datetime import datetime

import pandas as pd


def validate_dates(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Valide et normalise les formats de date dans le DataFrame.
    
    Opérations effectuées:
    - Validation des formats de date conformes (YYYY-MM-DD)
    - Conversion des formats non standards
    - Correction des dates incohérentes
    
    Args:
        df: DataFrame contenant les données stocks
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les dates validées et normalisées
        - La liste des erreurs/modifications détectées
    """
    errors = []
    result_df = df.copy()
    
    # Liste des champs de date
    date_fields = [
        "st_creation_date"
    ]
    
    # Expression régulière pour identifier les formats de date courants
    iso_date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')  # YYYY-MM-DD
    slash_date_pattern = re.compile(r'^\d{2}/\d{2}/\d{4}$')  # DD/MM/YYYY
    dot_date_pattern = re.compile(r'^\d{2}\.\d{2}\.\d{4}$')  # DD.MM.YYYY
    
    def parse_date(date_str: Optional[str]) -> Optional[str]:
        """
        Parse une chaîne de date et la convertit au format ISO (YYYY-MM-DD).
        
        Args:
            date_str: Chaîne représentant une date
            
        Returns:
            Date formatée en ISO ou None si conversion impossible
        """
        if pd.isna(date_str):
            return None
            
        if not isinstance(date_str, str):
            date_str = str(date_str)
            
        date_str = date_str.strip()
        
        try:
            # Vérifier le format ISO (YYYY-MM-DD)
            if iso_date_pattern.match(date_str):
                # Valider que c'est une date valide
                datetime.strptime(date_str, '%Y-%m-%d')
                return date_str
                
            # Vérifier le format DD/MM/YYYY
            if slash_date_pattern.match(date_str):
                dt = datetime.strptime(date_str, '%d/%m/%Y')
                return dt.strftime('%Y-%m-%d')
                
            # Vérifier le format DD.MM.YYYY
            if dot_date_pattern.match(date_str):
                dt = datetime.strptime(date_str, '%d.%m.%Y')
                return dt.strftime('%Y-%m-%d')
                
            # Tenter une conversion plus générique si aucun format reconnu
            dt = pd.to_datetime(date_str, errors='raise')
            return dt.strftime('%Y-%m-%d')
            
        except (ValueError, TypeError):
            return None
    
    # Traitement de chaque champ de date
    for field in date_fields:
        if field in result_df.columns:
            # Sauvegarde des valeurs originales
            original_values = result_df[field].copy()
            
            # Conversion des dates
            result_df[field] = result_df[field].apply(parse_date)
            
            # Détecter les modifications
            modified_mask = (original_values != result_df[field]) & (~pd.isna(original_values) | ~pd.isna(result_df[field]))
            modified_indices = result_df.index[modified_mask].tolist()
            
            # Journaliser les modifications
            for idx in modified_indices:
                st_id = result_df.at[idx, 'st_id']
                original = original_values.iloc[idx]
                converted = result_df.at[idx, field]
                
                errors.append({
                    "type": "date_conversion",
                    "severity": "info",
                    "st_id": st_id,
                    "index": idx,
                    "field": field,
                    "original": original,
                    "converted": converted
                })
            
            # Détecter les erreurs de conversion (valeurs qui n'ont pas pu être converties)
            error_mask = (pd.notna(original_values) & pd.isna(result_df[field]))
            error_indices = result_df.index[error_mask].tolist()
            
            # Journaliser les erreurs
            for idx in error_indices:
                st_id = result_df.at[idx, 'st_id']
                original = original_values.iloc[idx]
                
                errors.append({
                    "type": "date_conversion_error",
                    "severity": "error",
                    "st_id": st_id,
                    "index": idx,
                    "field": field,
                    "original": original,
                    "message": f"Impossible de convertir '{original}' en date valide"
                })
    
    return result_df, errors