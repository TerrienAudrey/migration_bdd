"""
Module de préparation du modèle final pour les données Organizations.
Finalise le traitement des données et prépare la structure finale selon le modèle attendu.
"""

from typing import Dict, List, Tuple, Any, Optional

import pandas as pd

from src.tables.organizations.output_structure import FIELD_LENGTH_CONSTRAINTS


def prepare_final_model(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Prépare la structure finale des données selon le modèle de sortie attendu.
    
    Args:
        df: DataFrame contenant les données Organizations traitées
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec la structure finale
        - La liste des erreurs/informations sur la préparation du modèle
    """
    errors = []
    result_df = df.copy()
    
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
                            "or_id": result_df.at[idx, 'or_id'],
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
            or_id = row['or_id']
            
            # Structure du statut de validation
            status = {
                "is_valid": True,
                "field_status": {},
                "error_details": []
            }
            
            # Vérification des champs obligatoires
            required_fields = ["or_id", "or_denomination", "or_id_address"]
            for field in required_fields:
                value = row.get(field)
                is_valid = not pd.isna(value) and value != ""
                status["field_status"][field] = is_valid
                
                if not is_valid:
                    status["is_valid"] = False
                    status["error_details"].append({
                        "field": field,
                        "error": "Champ obligatoire manquant ou vide"
                    })
            
            # Vérification du RNA si présent
            if not pd.isna(row.get('or_rna')) and row.get('or_rna') != "":
                rna = row['or_rna']
                is_valid_rna = len(str(rna)) == 10 and str(rna).startswith('W')
                status["field_status"]["or_rna"] = is_valid_rna
                
                if not is_valid_rna:
                    status["is_valid"] = False
                    status["error_details"].append({
                        "field": "or_rna",
                        "error": "Format RNA invalide"
                    })
            
            validation_status.append(status)
        
        result_df['or_validation_status'] = validation_status
        
    except Exception as e:
        errors.append({
            "type": "validation_status_creation_error",
            "severity": "error",
            "message": f"Erreur lors de la création des statuts de validation: {str(e)}"
        })
    
    return result_df, errors