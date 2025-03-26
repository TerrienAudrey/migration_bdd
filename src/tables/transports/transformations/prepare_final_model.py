"""
Module de préparation du modèle final pour les données transports.
Finalise le traitement des données et prépare la structure finale selon le modèle attendu.
"""

from typing import Dict, List, Tuple, Any, Optional

import pandas as pd

from src.tables.transports.output_structure import FIELD_LENGTH_CONSTRAINTS


def prepare_final_model(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Prépare la structure finale des données selon le modèle de sortie attendu.
    
    Ce module:
    - Vérifie la présence des champs obligatoires
    - Tronque les champs selon les contraintes de longueur
    - Crée le statut de validation pour chaque enregistrement
    
    Args:
        df: DataFrame contenant les données transports traitées
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec la structure finale
        - La liste des erreurs/informations sur la préparation du modèle
    """
    errors = []
    result_df = df.copy()
    
    # Vérifier la présence des champs obligatoires
    required_fields = ["tra_id", "tra_denomination"]
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
            if result_df[field].apply(lambda x: isinstance(x, str) and len(x) > max_length).any():
                # Sauvegarder les valeurs originales
                original_values = result_df[field].copy()
                
                # Tronquer les valeurs
                result_df[field] = result_df[field].apply(
                    lambda x: x[:max_length] if isinstance(x, str) and len(x) > max_length else x
                )
                
                # Journaliser les modifications
                for idx, value in enumerate(original_values):
                    if isinstance(value, str) and len(value) > max_length:
                        errors.append({
                            "type": "field_truncated",
                            "severity": "warning",
                            "tra_id": result_df.at[idx, 'tra_id'],
                            "field": field,
                            "original_length": len(value),
                            "truncated_to": max_length,
                            "message": f"Champ '{field}' tronqué de {len(value)} à {max_length} caractères"
                        })
    
    # Créer le champ de statut de validation pour chaque enregistrement
    try:
        # Initialisation du champ validation_status
        validation_status = []
        
        for idx, row in result_df.iterrows():
            tra_id = row['tra_id']
            
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
            
            validation_status.append(status)
        
        result_df['tra_validation_status'] = validation_status
        
    except Exception as e:
        errors.append({
            "type": "validation_status_creation_error",
            "severity": "error",
            "message": f"Erreur lors de la création des statuts de validation: {str(e)}"
        })
    
    return result_df, errors