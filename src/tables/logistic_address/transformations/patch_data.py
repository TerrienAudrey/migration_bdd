"""
Module d'application des correctifs (patches) pour les données logistic_address.
Permet d'appliquer des corrections manuelles spécifiques à partir de fichiers JSON.
"""

import json
import os
from typing import Dict, List, Tuple, Any, Optional

import pandas as pd


def apply_patches(df: pd.DataFrame, patches_file_path: str) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Applique des correctifs spécifiques aux données logistic_address à partir d'un fichier JSON.
    
    Args:
        df: DataFrame contenant les données logistic_address
        patches_file_path: Chemin vers le fichier de correctifs
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les correctifs appliqués
        - La liste des informations sur les modifications
    """
    errors = []
    result_df = df.copy()
    
    # Vérifier si le fichier de correctifs existe
    if not os.path.exists(patches_file_path):
        errors.append({
            "type": "patches_file_missing",
            "severity": "warning",
            "message": f"Le fichier de correctifs n'existe pas: {patches_file_path}"
        })
        return result_df, errors
    
    try:
        # Chargement du fichier de correctifs
        with open(patches_file_path, 'r', encoding='utf-8') as file:
            patches = json.load(file)
        
        # Vérification de la structure du fichier de correctifs
        if not isinstance(patches, list):
            errors.append({
                "type": "invalid_patches_format",
                "severity": "error",
                "message": "Le fichier de correctifs doit contenir une liste d'objets"
            })
            return result_df, errors
        
        # Application des correctifs
        for patch in patches:
            # Vérification de la structure de chaque correctif
            if not isinstance(patch, dict) or 'la_id' not in patch or 'patches' not in patch:
                errors.append({
                    "type": "invalid_patch_structure",
                    "severity": "error",
                    "patch": patch,
                    "message": "Chaque correctif doit contenir un 'la_id' et un objet 'patches'"
                })
                continue
            
            la_id = patch['la_id']
            field_patches = patch['patches']
            
            # Trouver l'index correspondant à la_id
            try:
                idx = result_df[result_df['la_id'] == la_id].index
                if len(idx) == 0:
                    errors.append({
                        "type": "patch_target_not_found",
                        "severity": "warning",
                        "la_id": la_id,
                        "message": f"Aucune adresse logistique trouvée avec la_id={la_id}"
                    })
                    continue
                    
                if len(idx) > 1:
                    errors.append({
                        "type": "multiple_patch_targets",
                        "severity": "warning",
                        "la_id": la_id,
                        "message": f"Plusieurs adresses logistiques trouvées avec la_id={la_id}, utilisation du premier"
                    })
                
                idx = idx[0]  # Prendre le premier index si plusieurs correspondances
                
                # Appliquer les correctifs champ par champ
                for field, new_value in field_patches.items():
                    # Vérifier si le champ existe
                    if field not in result_df.columns:
                        errors.append({
                            "type": "patch_field_not_found",
                            "severity": "warning",
                            "la_id": la_id,
                            "field": field,
                            "message": f"Le champ '{field}' n'existe pas dans le DataFrame"
                        })
                        continue
                    
                    # Sauvegarde de l'ancienne valeur pour logging
                    old_value = result_df.at[idx, field]
                    
                    # Application du correctif
                    result_df.at[idx, field] = new_value
                    
                    # Enregistrement de l'information du correctif appliqué
                    errors.append({
                        "type": "patch_applied",
                        "severity": "info",
                        "la_id": la_id,
                        "index": idx,
                        "field": field,
                        "old_value": old_value,
                        "new_value": new_value
                    })
            
            except Exception as e:
                errors.append({
                    "type": "patch_application_error",
                    "severity": "error",
                    "la_id": la_id,
                    "message": f"Erreur lors de l'application du correctif: {str(e)}"
                })
    
    except json.JSONDecodeError as e:
        errors.append({
            "type": "patches_file_parse_error",
            "severity": "error",
            "message": f"Erreur de parsing du fichier de correctifs: {str(e)}"
        })
    
    except Exception as e:
        errors.append({
            "type": "patches_application_error",
            "severity": "error",
            "message": f"Erreur lors de l'application des correctifs: {str(e)}"
        })
    
    return result_df, errors