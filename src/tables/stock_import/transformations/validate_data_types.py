# """
# Module de validation des types de données pour les données stock_import.
# Vérifie et corrige les types selon les spécifications.
# """

# from typing import Dict, List, Tuple, Any, Optional, Union

# import pandas as pd


# def validate_data_types(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
#     """
#     Valide et corrige les types de données dans le DataFrame.
    
#     Vérifications:
#     - Conversion des valeurs booléennes textuelles/numériques en booléens
#     - Conversion des valeurs numériques textuelles en nombres
#     - Validation des clés étrangères (entier ou null)
    
#     Args:
#         df: DataFrame contenant les données stock_import
        
#     Returns:
#         Tuple contenant:
#         - Le DataFrame avec les types de données validés et corrigés
#         - La liste des erreurs/modifications détectées
#     """
#     errors = []
#     result_df = df.copy()
    
#     # Liste des champs booléens
#     boolean_fields = [
#         "si_is_pallet",
#         "si_is_ready",
#         "si_is_dangerous"
#     ]
    
#     # Liste des champs numériques (integer)
#     integer_fields = [
#         "si_id",
#         "fk_st",
#         "fk_co",
#         "fk_la",
#         "fk_tra",
#         "si_quantity",
#         "si_quantity_stackable"
#     ]
    
#     # Liste des champs numériques (float)
#     float_fields = [
#         "si_total_price"
#     ]
    
#     # Fonction pour convertir en booléen
#     def convert_to_boolean(value: Any) -> bool:
#         if pd.isna(value):
#             return False
        
#         if isinstance(value, bool):
#             return value
            
#         if isinstance(value, (int, float)):
#             return bool(value)
            
#         if isinstance(value, str):
#             value_lower = value.lower().strip()
#             if value_lower in ['true', 't', 'yes', 'y', '1']:
#                 return True
#             elif value_lower in ['false', 'f', 'no', 'n', '0']:
#                 return False
        
#         # Par défaut, retourner False
#         return False
    
#     # # Fonction pour convertir en entier
#     # def convert_to_integer(value: Any) -> Optional[int]:
#     #     if pd.isna(value):
#     #         return None
            
#     #     if isinstance(value, int):
#     #         return value
            
#     #     if isinstance(value, float) and value.is_integer():
#     #         return int(value)
            
#     #     if isinstance(value, str):
#     #         try:
#     #             # Essayer de convertir en float d'abord (pour gérer les notations scientifiques)
#     #             float_val = float(value)
#     #             if float_val.is_integer():
#     #                 return int(float_val)
#     #             return int(float_val)  # Arrondir si nécessaire
#     #         except (ValueError, TypeError):
#     #             pass
        
#     #     # En cas d'échec, retourner None
#     #     return None
#     def convert_to_integer(value: Any) -> Optional[int]:
#         """Convertit en entier tout en préservant les valeurs nulles."""
#         if pd.isna(value):
#             return None  # Préserver les valeurs nulles
                
#         if isinstance(value, int):
#             return value  # Déjà un entier, retourner tel quel
                
#         if isinstance(value, float) and value.is_integer():
#             return int(value)  # Convertir float entier en int
        
#         # Pour les autres cas, essayer de convertir mais ne jamais retourner 0 par défaut
#         try:
#             if isinstance(value, str) and value.strip():
#                 return int(float(value))
#         except (ValueError, TypeError):
#             pass
            
#         # En cas d'échec, retourner la valeur d'origine (ou None si problème)
#         return value if isinstance(value, int) else None
    
#     # Fonction pour convertir en float
#     def convert_to_float(value: Any) -> Optional[float]:
#         if pd.isna(value):
#             return None
            
#         if isinstance(value, (int, float)):
#             return float(value)
            
#         if isinstance(value, str):
#             try:
#                 return float(value)
#             except (ValueError, TypeError):
#                 pass
        
#         # En cas d'échec, retourner None
#         return None
    
#     # Traitement des champs booléens
#     for field in boolean_fields:
#         if field in result_df.columns:
#             # Sauvegarde des valeurs originales
#             original_values = result_df[field].copy()
            
#             # Conversion en appliquant la fonction individuellement à chaque élément
#             for idx in result_df.index:
#                 result_df.at[idx, field] = convert_to_boolean(result_df.at[idx, field])
            
#             # Détecter les modifications
#             modified_indices = []
#             for idx in result_df.index:
#                 if original_values.iloc[idx] != result_df.at[idx, field] and not pd.isna(original_values.iloc[idx]):
#                     modified_indices.append(idx)
            
#             # Journaliser les modifications
#             for idx in modified_indices:
#                 si_id = result_df.at[idx, 'si_id']
#                 original = original_values.iloc[idx]
#                 converted = result_df.at[idx, field]
                
#                 errors.append({
#                     "type": "boolean_conversion",
#                     "severity": "info",
#                     "si_id": si_id,
#                     "index": idx,
#                     "field": field,
#                     "original": original,
#                     "converted": converted
#                 })
    
#     # Traitement des champs entiers
#     for field in integer_fields:
#         if field in result_df.columns:
#             # Sauvegarde des valeurs originales
#             original_values = result_df[field].copy()
            
#             # Conversion en appliquant la fonction individuellement à chaque élément
#             for idx in result_df.index:
#                 result_df.at[idx, field] = convert_to_integer(result_df.at[idx, field])
            
#             # Détecter les modifications
#             modified_indices = []
#             for idx in result_df.index:
#                 orig_val = original_values.iloc[idx]
#                 conv_val = result_df.at[idx, field]
#                 # Compare en tenant compte des valeurs NA
#                 if ((pd.isna(orig_val) and not pd.isna(conv_val)) or 
#                     (not pd.isna(orig_val) and pd.isna(conv_val)) or
#                     (not pd.isna(orig_val) and not pd.isna(conv_val) and orig_val != conv_val)):
#                     modified_indices.append(idx)
            
#             # Journaliser les modifications
#             for idx in modified_indices:
#                 si_id = result_df.at[idx, 'si_id']
#                 original = original_values.iloc[idx]
#                 converted = result_df.at[idx, field]
                
#                 errors.append({
#                     "type": "integer_conversion",
#                     "severity": "info",
#                     "si_id": si_id,
#                     "index": idx,
#                     "field": field,
#                     "original": original,
#                     "converted": converted
#                 })
    
#     # Traitement des champs float
#     for field in float_fields:
#         if field in result_df.columns:
#             # Sauvegarde des valeurs originales
#             original_values = result_df[field].copy()
            
#             # Conversion en appliquant la fonction individuellement à chaque élément
#             for idx in result_df.index:
#                 result_df.at[idx, field] = convert_to_float(result_df.at[idx, field])
            
#             # Détecter les modifications
#             modified_indices = []
#             for idx in result_df.index:
#                 orig_val = original_values.iloc[idx]
#                 conv_val = result_df.at[idx, field]
#                 # Compare en tenant compte des valeurs NA
#                 if ((pd.isna(orig_val) and not pd.isna(conv_val)) or 
#                     (not pd.isna(orig_val) and pd.isna(conv_val)) or
#                     (not pd.isna(orig_val) and not pd.isna(conv_val) and orig_val != conv_val)):
#                     modified_indices.append(idx)
            
#             # Journaliser les modifications
#             for idx in modified_indices:
#                 si_id = result_df.at[idx, 'si_id']
#                 original = original_values.iloc[idx]
#                 converted = result_df.at[idx, field]
                
#                 errors.append({
#                     "type": "float_conversion",
#                     "severity": "info",
#                     "si_id": si_id,
#                     "index": idx,
#                     "field": field,
#                     "original": original,
#                     "converted": converted
#                 })
    
#     # Vérification des tableaux
#     array_fields = ["si_packaging_method", "positioning"]
    
#     for field in array_fields:
#         if field in result_df.columns:
#             # S'assurer que les valeurs sont des tableaux
#             for idx in result_df.index:
#                 value = result_df.at[idx, field]
#                 if not isinstance(value, list):
#                     result_df.at[idx, field] = [] if pd.isna(value) else [value]
#                     si_id = result_df.at[idx, 'si_id']
                    
#                     errors.append({
#                         "type": "array_conversion",
#                         "severity": "info",
#                         "si_id": si_id,
#                         "index": idx,
#                         "field": field,
#                         "original": value,
#                         "converted": result_df.at[idx, field]
#                     })
    
#     return result_df, errors


"""
Module de validation des types de données pour les données stock_import.
Vérifie et corrige les types selon les spécifications.
"""

from typing import Dict, List, Tuple, Any, Optional, Union

import pandas as pd


def validate_data_types(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Valide et corrige les types de données dans le DataFrame.
    
    Vérifications:
    - Conversion des valeurs booléennes textuelles/numériques en booléens
    - Conversion des valeurs numériques textuelles en nombres
    - Validation des clés étrangères (entier ou null)
    
    Args:
        df: DataFrame contenant les données stock_import
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les types de données validés et corrigés
        - La liste des erreurs/modifications détectées
    """
    errors = []
    result_df = df.copy()
    
    # Liste des champs booléens
    boolean_fields = [
        "si_is_pallet",
        "si_is_ready",
        "si_is_dangerous"
    ]
    
    # Liste des champs numériques (integer) à NE PAS MODIFIER mais seulement vérifier
    id_fields = [
        "si_id",
        "fk_st",
        "fk_co",
        "fk_la",
        "fk_tra"
    ]
    
    # Liste des champs numériques (integer) à convertir si nécessaire
    integer_fields = [
        "si_quantity",
        "si_quantity_stackable"
    ]
    
    # Liste des champs numériques (float)
    float_fields = [
        "si_total_price"
    ]
    
    # Fonction pour convertir en booléen
    def convert_to_boolean(value: Any) -> bool:
        if pd.isna(value):
            return False
        
        if isinstance(value, bool):
            return value
            
        if isinstance(value, (int, float)):
            return bool(value)
            
        if isinstance(value, str):
            value_lower = value.lower().strip()
            if value_lower in ['true', 't', 'yes', 'y', '1']:
                return True
            elif value_lower in ['false', 'f', 'no', 'n', '0']:
                return False
        
        # Par défaut, retourner False
        return False
    
    # Fonction pour convertir en entier
    def convert_to_integer(value: Any) -> Optional[int]:
        if pd.isna(value):
            return None
            
        if isinstance(value, int):
            return value
            
        if isinstance(value, float) and value.is_integer():
            return int(value)
            
        if isinstance(value, str):
            try:
                float_val = float(value)
                if float_val.is_integer():
                    return int(float_val)
                return None  # Si ce n'est pas un entier, on retourne None
            except (ValueError, TypeError):
                pass
        
        # En cas d'échec, retourner None
        return None
    
    # Fonction pour convertir en float
    def convert_to_float(value: Any) -> Optional[float]:
        if pd.isna(value):
            return None
            
        if isinstance(value, (int, float)):
            return float(value)
            
        if isinstance(value, str):
            try:
                return float(value)
            except (ValueError, TypeError):
                pass
        
        # En cas d'échec, retourner None
        return None
    
    # Vérification des champs d'id sans modification
    for field in id_fields:
        if field in result_df.columns:
            for idx in result_df.index:
                value = result_df.at[idx, field]
                
                # Vérification sans modification
                if not (pd.isna(value) or isinstance(value, int)):
                    si_id = result_df.at[idx, 'si_id'] if idx < len(result_df) and 'si_id' in result_df.columns else "inconnu"
                    errors.append({
                        "type": "invalid_id_type",
                        "severity": "warning",
                        "si_id": si_id,
                        "index": idx,
                        "field": field,
                        "value": value,
                        "message": f"Le champ {field} devrait être un entier mais a la valeur {value} de type {type(value).__name__}"
                    })
    
    # Traitement des champs booléens
    for field in boolean_fields:
        if field in result_df.columns:
            # Sauvegarde des valeurs originales
            original_values = result_df[field].copy()
            
            # Conversion en appliquant la fonction individuellement à chaque élément
            for idx in result_df.index:
                result_df.at[idx, field] = convert_to_boolean(result_df.at[idx, field])
            
            # Détecter les modifications
            modified_indices = []
            for idx in result_df.index:
                if original_values.iloc[idx] != result_df.at[idx, field] and not pd.isna(original_values.iloc[idx]):
                    modified_indices.append(idx)
            
            # Journaliser les modifications
            for idx in modified_indices:
                si_id = result_df.at[idx, 'si_id'] if 'si_id' in result_df.columns else "inconnu"
                original = original_values.iloc[idx]
                converted = result_df.at[idx, field]
                
                errors.append({
                    "type": "boolean_conversion",
                    "severity": "info",
                    "si_id": si_id,
                    "index": idx,
                    "field": field,
                    "original": original,
                    "converted": converted
                })
    
    # Traitement des champs entiers (non-id)
    for field in integer_fields:
        if field in result_df.columns:
            # Sauvegarde des valeurs originales
            original_values = result_df[field].copy()
            
            # Conversion en appliquant la fonction individuellement à chaque élément
            for idx in result_df.index:
                result_df.at[idx, field] = convert_to_integer(result_df.at[idx, field])
            
            # Détecter les modifications
            modified_indices = []
            for idx in result_df.index:
                orig_val = original_values.iloc[idx]
                conv_val = result_df.at[idx, field]
                # Compare en tenant compte des valeurs NA
                if ((pd.isna(orig_val) and not pd.isna(conv_val)) or 
                    (not pd.isna(orig_val) and pd.isna(conv_val)) or
                    (not pd.isna(orig_val) and not pd.isna(conv_val) and orig_val != conv_val)):
                    modified_indices.append(idx)
            
            # Journaliser les modifications
            for idx in modified_indices:
                si_id = result_df.at[idx, 'si_id'] if 'si_id' in result_df.columns else "inconnu"
                original = original_values.iloc[idx]
                converted = result_df.at[idx, field]
                
                errors.append({
                    "type": "integer_conversion",
                    "severity": "info",
                    "si_id": si_id,
                    "index": idx,
                    "field": field,
                    "original": original,
                    "converted": converted
                })
    
    # Traitement des champs float
    for field in float_fields:
        if field in result_df.columns:
            # Sauvegarde des valeurs originales
            original_values = result_df[field].copy()
            
            # Conversion en appliquant la fonction individuellement à chaque élément
            for idx in result_df.index:
                result_df.at[idx, field] = convert_to_float(result_df.at[idx, field])
            
            # Détecter les modifications
            modified_indices = []
            for idx in result_df.index:
                orig_val = original_values.iloc[idx]
                conv_val = result_df.at[idx, field]
                # Compare en tenant compte des valeurs NA
                if ((pd.isna(orig_val) and not pd.isna(conv_val)) or 
                    (not pd.isna(orig_val) and pd.isna(conv_val)) or
                    (not pd.isna(orig_val) and not pd.isna(conv_val) and orig_val != conv_val)):
                    modified_indices.append(idx)
            
            # Journaliser les modifications
            for idx in modified_indices:
                si_id = result_df.at[idx, 'si_id'] if 'si_id' in result_df.columns else "inconnu"
                original = original_values.iloc[idx]
                converted = result_df.at[idx, field]
                
                errors.append({
                    "type": "float_conversion",
                    "severity": "info",
                    "si_id": si_id,
                    "index": idx,
                    "field": field,
                    "original": original,
                    "converted": converted
                })
    
    # Vérification des tableaux
    array_fields = ["si_packaging_method", "positioning"]
    
    for field in array_fields:
        if field in result_df.columns:
            # S'assurer que les valeurs sont des tableaux
            for idx in result_df.index:
                value = result_df.at[idx, field]
                if not isinstance(value, list):
                    result_df.at[idx, field] = [] if pd.isna(value) else [value]
                    si_id = result_df.at[idx, 'si_id'] if 'si_id' in result_df.columns else "inconnu"
                    
                    errors.append({
                        "type": "array_conversion",
                        "severity": "info",
                        "si_id": si_id,
                        "index": idx,
                        "field": field,
                        "original": value,
                        "converted": result_df.at[idx, field]
                    })
    
    return result_df, errors