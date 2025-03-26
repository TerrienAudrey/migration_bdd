"""
Module de préparation du modèle final pour les données stocks.
Finalise le traitement des données et prépare la structure finale selon le modèle attendu.
"""

from typing import Dict, List, Tuple, Any, Optional

import pandas as pd
import numpy as np
from src.tables.stocks.output_structure import STEP_PLANNING_VALUES

def convert_nan_to_none(df: pd.DataFrame) -> pd.DataFrame:
    """Convertit tous les NaN du DataFrame en None pour la sortie JSON."""
    result = df.copy()
    
    # Parcourir toutes les colonnes
    for col in result.columns:
        # Pour chaque colonne, remplacer les NaN par None
        for idx in result.index:
            value = result.at[idx, col]
            # Vérifier si la valeur est NaN, mais en évitant d'appliquer isna() aux arrays
            if not isinstance(value, (list, pd.Series, np.ndarray)) and pd.isna(value):
                result.at[idx, col] = None
    
    return result

def prepare_final_model(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Prépare la structure finale des données selon le modèle de sortie attendu.
    
    Ce module:
    - Vérifie la présence des champs obligatoires
    - Valide les valeurs des champs énumérés
    - Crée le statut de validation pour chaque enregistrement
    
    Args:
        df: DataFrame contenant les données stocks traitées
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec la structure finale
        - La liste des erreurs/informations sur la préparation du modèle
    """
    errors = []
    result_df = df.copy()
    
    # Vérifier la présence des champs obligatoires
    required_fields = ["st_id", "st_io"]
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
    
    # Validation de st_step_planning (doit être une valeur de l'enum)
    if 'st_step_planning' in result_df.columns:
        invalid_values = result_df[~result_df['st_step_planning'].isin(STEP_PLANNING_VALUES) & ~result_df['st_step_planning'].isna()]
        
        if not invalid_values.empty:
            for idx, row in invalid_values.iterrows():
                invalid_value = row['st_step_planning']
                # Corriger avec la valeur par défaut
                result_df.at[idx, 'st_step_planning'] = "NOTSTARTED"
                
                errors.append({
                    "type": "invalid_enum_value",
                    "severity": "warning",
                    "st_id": row['st_id'],
                    "field": "st_step_planning",
                    "invalid_value": invalid_value,
                    "corrected_value": "NOTSTARTED",
                    "message": f"Valeur invalide pour st_step_planning, corrigé à 'NOTSTARTED'"
                })
    
    # Créer le champ de statut de validation pour chaque enregistrement
    try:
        # Initialisation du champ validation_status
        validation_status = []
        
        for idx, row in result_df.iterrows():
            st_id = row['st_id']
            
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
                    is_valid = not pd.isna(value) and value != ""
                    status["field_status"][field] = is_valid
                    
                    if not is_valid:
                        status["is_valid"] = False
                        status["error_details"].append({
                            "field": field,
                            "error": "Champ obligatoire manquant ou vide"
                        })
            
            # Vérification spécifique du champ st_io (format)
            if 'st_io' in result_df.columns and not pd.isna(row['st_io']) and row['st_io'] != "":
                st_io = row['st_io']
                # Format attendu: YYYYMMDD-XXX-YYY ou YYYYMMDD-XXX-YYY-Z
                is_valid_format = any([
                    len(st_io) == 16 and st_io[8:9] == "-" and st_io[12:13] == "-",  # YYYYMMDD-XXX-YYY
                    len(st_io) > 17 and st_io[8:9] == "-" and st_io[12:13] == "-" and st_io[16:17] == "-"  # YYYYMMDD-XXX-YYY-Z
                ])
                
                status["field_status"]["st_io_format"] = is_valid_format
                
                if not is_valid_format:
                    status["is_valid"] = False
                    status["error_details"].append({
                        "field": "st_io",
                        "error": "Format d'identifiant st_io invalide"
                    })
            
            # Vérification de la présence d'au moins fk_co
            has_fk_co = not pd.isna(row.get('fk_co')) and row.get('fk_co') != 0
            
            if not has_fk_co:
                status["is_valid"] = False
                status["error_details"].append({
                    "field": "fk_co",
                    "error": "Aucune référence à une entreprise (fk_co) n'est définie"
                })
            
            validation_status.append(status)
        
        result_df['st_validation_status'] = validation_status
        
    except Exception as e:
        errors.append({
            "type": "validation_status_creation_error",
            "severity": "error",
            "message": f"Erreur lors de la création des statuts de validation: {str(e)}"
        })
    
    # Conversion finale de tous les NaN en None
    result_df = convert_nan_to_none(result_df)
    
    return result_df, errors