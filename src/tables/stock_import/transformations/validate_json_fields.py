"""
Module de validation des champs JSON pour les données stock_import.
Vérifie et formate les champs JSON.
"""

import json
from typing import Dict, List, Tuple, Any, Optional

import pandas as pd


def validate_json_fields(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Valide et normalise les champs JSON dans le DataFrame.
    
    Args:
        df: DataFrame contenant les données stock_import
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les champs JSON validés et normalisés
        - La liste des erreurs et modifications détectées
    """
    errors = []
    result_df = df.copy()
    
    # Liste des champs JSON à valider
    json_fields = [
        "si_gpt_response_matching_json",
        "si_gpt_response_category_json"
    ]
    
    # Fonction pour valider et convertir un champ JSON
    def validate_and_convert_json(json_value: Any) -> str:
        # Si la valeur est null ou vide, retourner un objet JSON vide
        if pd.isna(json_value) or json_value == "":
            return "{}"
            
        # Si c'est déjà un objet Python (dict, list), le convertir en chaîne JSON
        if isinstance(json_value, (dict, list)):
            try:
                return json.dumps(json_value)
            except Exception:
                return "{}"
                
        # Si c'est une chaîne, vérifier que c'est un JSON valide
        if isinstance(json_value, str):
            try:
                # Essayer de parser puis reformater pour standardisation
                parsed = json.loads(json_value)
                return json.dumps(parsed)
            except json.JSONDecodeError:
                # Si pas un JSON valide, retourner objet vide
                return "{}"
                
        # Pour tout autre type, retourner un objet JSON vide
        return "{}"
    
    # Traiter chaque champ JSON
    for field in json_fields:
        if field in result_df.columns:
            # Sauvegarder les valeurs originales
            original_values = result_df[field].copy()
            
            # Appliquer la validation
            result_df[field] = result_df[field].apply(validate_and_convert_json)
            
            # Détecter les modifications
            for idx, (original, converted) in enumerate(zip(original_values, result_df[field])):
                # Si la valeur a changé et n'est pas None dans l'original
                if str(original) != str(converted) and not pd.isna(original):
                    si_id = result_df.at[idx, 'si_id']
                    
                    errors.append({
                        "type": "json_format_conversion",
                        "severity": "info",
                        "si_id": si_id,
                        "field": field,
                        "original": str(original),
                        "converted": converted,
                        "index": idx,
                        "message": "Format JSON normalisé"
                    })
    
    return result_df, errors