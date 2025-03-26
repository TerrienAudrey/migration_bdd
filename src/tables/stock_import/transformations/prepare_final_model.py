"""
Module de préparation du modèle final pour les données stock_import.
Finalise le traitement des données et prépare la structure finale selon le modèle attendu.
"""

from typing import Dict, List, Tuple, Any, Optional
import numpy as np
import pandas as pd

from src.tables.stock_import.output_structure import FIELD_LENGTH_CONSTRAINTS

# def convert_nan_to_none(df: pd.DataFrame) -> pd.DataFrame:
#     """Convertit tous les NaN du DataFrame en None pour la sortie JSON."""
#     result = df.copy()
    
#     # Parcourir toutes les colonnes
#     for col in result.columns:
#         # Pour chaque colonne, remplacer les NaN par None
#         for idx in result.index:
#             if pd.isna(result.at[idx, col]):
#                 result.at[idx, col] = None
    
#     return result

# def convert_nan_to_none(df: pd.DataFrame) -> pd.DataFrame:
#     """Convertit tous les NaN du DataFrame en None pour la sortie JSON."""
#     result = df.copy()
    
#     # Parcourir toutes les colonnes
#     for col in result.columns:
#         # Pour chaque colonne, remplacer les NaN par None
#         for idx in result.index:
#             value = result.at[idx, col]
#             # Vérifier si la valeur est NaN, mais en évitant d'appliquer isna() aux arrays
#             if not isinstance(value, (list, pd.Series, np.ndarray)) and pd.isna(value):
#                 result.at[idx, col] = None
    
#     return result

def convert_nan_to_none(df: pd.DataFrame) -> pd.DataFrame:
    """Convertit tous les NaN du DataFrame en None pour la sortie JSON."""
    result = df.copy()
    
    # Parcourir toutes les colonnes
    for col in result.columns:
        # Pour chaque colonne, remplacer les NaN par None
        for idx in result.index:
            value = result.at[idx, col]
            # Vérifier si c'est un array/Series ou une valeur scalaire avant d'utiliser pd.isna
            if not isinstance(value, (list, pd.Series, np.ndarray)) and pd.isna(value):
                result.at[idx, col] = None
    
    return result


def prepare_final_model(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Prépare la structure finale des données selon le modèle de sortie attendu.
    
    Ce module:
    - Vérifie la présence des champs obligatoires
    - Tronque les champs selon les contraintes de longueur
    - Crée le statut de validation pour chaque enregistrement
    
    Args:
        df: DataFrame contenant les données stock_import traitées
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec la structure finale
        - La liste des erreurs/informations sur la préparation du modèle
    """
    errors = []
    result_df = df.copy()
    
    # Vérifier la présence des champs obligatoires
    required_fields = ["si_id", "si_total_price"]
    missing_fields = [field for field in required_fields if field not in result_df.columns]
    
    if missing_fields:
        for field in missing_fields:
            errors.append({
                "type": "missing_required_field",
                "severity": "error",
                "field": field,
                "message": f"Le champ obligatoire '{field}' est absent du DataFrame"
            })
        # On continue malgré les erreurs pour traiter au maximum les données
    
    # Vérifier et appliquer les contraintes de longueur
    for field, max_length in FIELD_LENGTH_CONSTRAINTS.items():
        if field in result_df.columns:
            # Repérer les champs qui dépassent la limite
            for idx in result_df.index:
                value = result_df.at[idx, field]
                if isinstance(value, str) and len(value) > max_length:
                    # Sauvegarder la valeur originale
                    original_value = value
                    
                    # Tronquer la valeur
                    result_df.at[idx, field] = value[:max_length]
                    
                    # Journaliser la modification
                    errors.append({
                        "type": "field_truncated",
                        "severity": "warning",
                        "si_id": result_df.at[idx, 'si_id'],
                        "field": field,
                        "original_length": len(original_value),
                        "truncated_to": max_length,
                        "message": f"Champ '{field}' tronqué de {len(original_value)} à {max_length} caractères"
                    })
    
    # Créer le champ de statut de validation pour chaque enregistrement
    try:
        # Initialisation du champ validation_status
        validation_status = []
        
        for idx, row in result_df.iterrows():
            si_id = row['si_id']
            
            # Structure du statut de validation
            status = {
                "is_valid": True,
                "field_status": {},
                "error_details": []
            }
            
            # Vérification des champs obligatoires
            for field in required_fields:
                if field in result_df.columns:
                    value = row[field]
                    is_valid = not (pd.isna(value) or value == "")
                    status["field_status"][field] = is_valid
                    
                    if not is_valid:
                        status["is_valid"] = False
                        status["error_details"].append({
                            "field": field,
                            "error": "Champ obligatoire manquant ou vide"
                        })
            
            validation_status.append(status)
        
        result_df['si_validation_status'] = validation_status
        
    except Exception as e:
        errors.append({
            "type": "validation_status_creation_error",
            "severity": "error",
            "message": f"Erreur lors de la création des statuts de validation: {str(e)}"
        })
    # Conversion finale de tous les NaN en None
    result_df = convert_nan_to_none(result_df)

    return result_df, errors