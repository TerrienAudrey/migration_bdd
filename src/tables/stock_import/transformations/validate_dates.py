"""
Module de validation des dates pour les données stock_import.
Vérifie et formate les dates selon les standards ISO.
"""

import re
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional

import pandas as pd


def validate_dates(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Valide et normalise les formats de date dans le DataFrame.
    
    Args:
        df: DataFrame contenant les données stock_import
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les dates validées et normalisées
        - La liste des erreurs et modifications détectées
    """
    errors = []
    result_df = df.copy()
    
    # Liste des champs de date à valider
    date_fields = [
        "si_date_removal",
        "si_date_delivery",
        "si_date_alert_removal",
        "si_date_alert_delivery",
        "si_date_process"
    ]
    
    # Format ISO standard
    iso_format = r"^\d{4}-\d{2}-\d{2}$"
    iso_format_pattern = re.compile(iso_format)
    
    # Formats de date possibles et leur conversion
    date_formats = [
        "%Y-%m-%d",        # 2023-03-01
        "%d/%m/%Y",        # 01/03/2023
        "%d-%m-%Y",        # 01-03-2023
        "%Y/%m/%d",        # 2023/03/01
        "%m/%d/%Y",        # 03/01/2023
        "%d.%m.%Y",        # 01.03.2023
        "%Y.%m.%d"         # 2023.03.01
    ]
    
    # Fonction pour valider et convertir une date
    def validate_and_convert_date(date_value: Any) -> Optional[str]:
        # Si la valeur est null ou vide, retourner None
        if pd.isna(date_value) or date_value == "":
            return None
            
        # Si déjà au format ISO, retourner tel quel
        if isinstance(date_value, str) and iso_format_pattern.match(date_value):
            # Vérifier si la date est valide
            try:
                datetime.strptime(date_value, "%Y-%m-%d")
                return date_value
            except ValueError:
                # Date au format ISO mais invalide
                return None
                
        # Essayer différents formats de date
        if isinstance(date_value, str):
            for date_format in date_formats:
                try:
                    parsed_date = datetime.strptime(date_value, date_format)
                    return parsed_date.strftime("%Y-%m-%d")  # Convertir en format ISO
                except ValueError:
                    continue
                    
        # Si c'est un objet datetime, convertir directement
        if isinstance(date_value, datetime):
            return date_value.strftime("%Y-%m-%d")
            
        # Si aucun format ne correspond, retourner None
        return None
    
    # Traiter chaque champ de date
    for field in date_fields:
        if field in result_df.columns:
            # Sauvegarder les valeurs originales
            original_values = result_df[field].copy()
            
            # Appliquer la validation
            result_df[field] = result_df[field].apply(validate_and_convert_date)
            
            # Détecter les modifications
            for idx, (original, converted) in enumerate(zip(original_values, result_df[field])):
                # Si la valeur a changé et n'est pas None dans l'original
                if original != converted and not pd.isna(original):
                    si_id = result_df.at[idx, 'si_id']
                    
                    errors.append({
                        "type": "date_format_conversion",
                        "severity": "info" if converted is not None else "warning",
                        "si_id": si_id,
                        "field": field,
                        "original": original,
                        "converted": converted,
                        "index": idx,
                        "message": f"Format de date converti" if converted is not None else "Format de date invalide"
                    })
    
    return result_df, errors