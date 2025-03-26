"""
Module de gestion du champ st_commission_% pour les données stocks.
Ce module est responsable de détecter et renommer ce champ spécifique.
"""

import re
from typing import Dict, List, Tuple, Any

import pandas as pd

from src.tables.stocks.input_structure import FIELDS_TO_RENAME


# def handle_commission_percent(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
#     """
#     Détecte et renomme le champ st_commission_% en st_commission_percent.
    
#     Args:
#         df: DataFrame contenant les données stocks
        
#     Returns:
#         Tuple contenant:
#         - Le DataFrame avec le champ renommé
#         - La liste des modifications effectuées
#     """
#     errors = []
#     result_df = df.copy()
    
#     # Identifier si le champ problématique est présent
#     problematic_field = None
#     for col in result_df.columns:
#         if col == 'st_commission_%' or col == 'st_commission_\\%' or 'commission' in col and '%' in col:
#             problematic_field = col
#             break
    
#     # Si le champ problématique est présent, le renommer
#     if problematic_field:
#         # Vérifier si le champ cible existe déjà
#         target_field = FIELDS_TO_RENAME.get(problematic_field, "st_commission_percent")
        
#         if target_field in result_df.columns:
#             # Le champ cible existe déjà, fusionner les valeurs
#             errors.append({
#                 "type": "field_merge",
#                 "severity": "warning",
#                 "field": problematic_field,
#                 "target_field": target_field,
#                 "message": f"Le champ '{target_field}' existe déjà. Fusion des valeurs."
#             })
            
#             # Copier les valeurs non-null du champ problématique vers le champ cible
#             mask = result_df[problematic_field].notna()
#             result_df.loc[mask, target_field] = result_df.loc[mask, problematic_field]
            
#             # Supprimer le champ problématique
#             result_df = result_df.drop(columns=[problematic_field])
#         else:
#             # Renommer simplement le champ
#             result_df = result_df.rename(columns={problematic_field: target_field})
            
#             errors.append({
#                 "type": "field_renamed",
#                 "severity": "info",
#                 "field": problematic_field,
#                 "target_field": target_field,
#                 "message": f"Le champ '{problematic_field}' a été renommé en '{target_field}'."
#             })
        
#         # Validation de la plage des valeurs (entre 0 et 1)
#         if target_field in result_df.columns:
#             # Convertir en numérique en ignorant les erreurs
#             result_df[target_field] = pd.to_numeric(result_df[target_field], errors='coerce')
            
#             # Identifier les valeurs hors plage
#             out_of_range = result_df[(result_df[target_field] > 1) | (result_df[target_field] < 0)].index
            
#             if len(out_of_range) > 0:
#                 for idx in out_of_range:
#                     # Si valeur > 1, diviser par 100 (probablement exprimée en pourcentage)
#                     if result_df.at[idx, target_field] > 1:
#                         original_value = result_df.at[idx, target_field]
#                         result_df.at[idx, target_field] = original_value / 100
#                         errors.append({
#                             "type": "value_conversion",
#                             "severity": "info",
#                             "st_id": result_df.at[idx, 'st_id'],
#                             "field": target_field,
#                             "original_value": original_value,
#                             "new_value": result_df.at[idx, target_field],
#                             "message": f"Valeur de {target_field} > 1 divisée par 100 pour obtenir un ratio."
#                         })
#     else:
#         # Si le champ st_commission_percent est requis mais absent, l'ajouter
#         if 'st_commission_percent' not in result_df.columns:
#             # Vérifier si on peut dériver la valeur à partir de st_commission
#             if 'st_commission' in result_df.columns:
#                 errors.append({
#                     "type": "field_added",
#                     "severity": "info",
#                     "field": "st_commission_percent",
#                     "message": "Le champ 'st_commission_percent' a été ajouté (valeurs nulles)"
#                 })
#                 result_df['st_commission_percent'] = None
#             else:
#                 errors.append({
#                     "type": "field_added",
#                     "severity": "info",
#                     "field": "st_commission_percent",
#                     "message": "Le champ 'st_commission_percent' a été ajouté (valeurs nulles)"
#                 })
#                 result_df['st_commission_percent'] = None
                
#     return result_df, errors

def handle_commission_percent(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Détecte et renomme le champ st_commission_% en st_commission_percent.
    
    Args:
        df: DataFrame contenant les données stocks
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec le champ renommé
        - La liste des modifications effectuées
    """
    errors = []
    result_df = df.copy()
    
    # Identifier si le champ problématique est présent
    problematic_field = None
    for col in result_df.columns:
        if col == 'st_commission_%' or col == 'st_commission_\\%' or 'commission' in col and '%' in col:
            problematic_field = col
            break
    
    # Si le champ problématique est présent, le renommer
    if problematic_field:
        # Vérifier si le champ cible existe déjà
        target_field = FIELDS_TO_RENAME.get(problematic_field, "st_commission_percent")
        
        if target_field in result_df.columns:
            # Le champ cible existe déjà, fusionner les valeurs
            errors.append({
                "type": "field_merge",
                "severity": "warning",
                "field": problematic_field,
                "target_field": target_field,
                "message": f"Le champ '{target_field}' existe déjà. Fusion des valeurs."
            })
            
            # Copier les valeurs non-null du champ problématique vers le champ cible
            mask = result_df[problematic_field].notna()
            result_df.loc[mask, target_field] = result_df.loc[mask, problematic_field]
            
            # Supprimer le champ problématique
            result_df = result_df.drop(columns=[problematic_field])
        else:
            # Renommer simplement le champ
            result_df = result_df.rename(columns={problematic_field: target_field})
            
            errors.append({
                "type": "field_renamed",
                "severity": "info",
                "field": problematic_field,
                "target_field": target_field,
                "message": f"Le champ '{problematic_field}' a été renommé en '{target_field}'."
            })
        
        # Validation de la plage des valeurs (entre 0 et 1)
        if target_field in result_df.columns:
            # Convertir en numérique en ignorant les erreurs
            result_df[target_field] = pd.to_numeric(result_df[target_field], errors='coerce')
            
            # Remplacer les NaN par None (pour la conversion en JSON ultérieure)
            result_df[target_field] = result_df[target_field].apply(
                lambda x: None if pd.isna(x) else x
            )
            
            # Identifier les valeurs hors plage
            out_of_range = []
            for idx, row in result_df.iterrows():
                val = row[target_field]
                if val is not None and (val > 1 or val < 0):
                    out_of_range.append(idx)
            
            # Traiter les valeurs hors plage
            for idx in out_of_range:
                original_value = result_df.at[idx, target_field]
                
                # Si valeur > 1, diviser par 100 (probablement exprimée en pourcentage)
                if original_value > 1:
                    result_df.at[idx, target_field] = original_value / 100
                    errors.append({
                        "type": "value_conversion",
                        "severity": "info",
                        "st_id": result_df.at[idx, 'st_id'],
                        "field": target_field,
                        "original_value": original_value,
                        "new_value": result_df.at[idx, target_field],
                        "message": f"Valeur de {target_field} > 1 divisée par 100 pour obtenir un ratio."
                    })
    else:
        # Si le champ st_commission_percent est requis mais absent, l'ajouter
        if 'st_commission_percent' not in result_df.columns:
            # Vérifier si on peut dériver la valeur à partir de st_commission
            if 'st_commission' in result_df.columns:
                errors.append({
                    "type": "field_added",
                    "severity": "info",
                    "field": "st_commission_percent",
                    "message": "Le champ 'st_commission_percent' a été ajouté (valeurs nulles)"
                })
                result_df['st_commission_percent'] = None
            else:
                errors.append({
                    "type": "field_added",
                    "severity": "info",
                    "field": "st_commission_percent",
                    "message": "Le champ 'st_commission_percent' a été ajouté (valeurs nulles)"
                })
                result_df['st_commission_percent'] = None
                
    return result_df, errors