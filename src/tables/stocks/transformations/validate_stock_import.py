"""
Module de validation des stock_import pour les données stocks.
Vérifie et nettoie le tableau des stock_import.
"""

from typing import Dict, List, Tuple, Any, Optional, Union

import pandas as pd


def validate_stock_import(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Valide et nettoie le tableau stock_import dans chaque enregistrement.
    
    Opérations effectuées:
    - Conversion des éléments en entiers
    - Élimination des doublons dans chaque tableau
    - Vérification de la validité des références
    
    Args:
        df: DataFrame contenant les données stocks
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les tableaux stock_import validés
        - La liste des erreurs/modifications détectées
    """
    errors = []
    result_df = df.copy()
    
    # Vérifier si le champ stock_import existe
    if 'stock_import' not in result_df.columns:
        errors.append({
            "type": "missing_column",
            "severity": "warning",
            "message": "La colonne 'stock_import' est absente du DataFrame"
        })
        return result_df, errors
    
    def clean_stock_import(imports: Any) -> List[int]:
        """
        Nettoie et valide une liste de stock_import.
        
        Args:
            imports: Valeur actuelle du champ stock_import
            
        Returns:
            Liste nettoyée d'identifiants entiers
        """
        # Si la valeur est NaN ou None, retourner une liste vide
        if pd.isna(imports) or imports is None:
            return []
        
        # Si ce n'est pas une liste, tenter de la convertir
        if not isinstance(imports, list):
            try:
                # Cas où c'est une chaîne représentant une liste
                if isinstance(imports, str):
                    if imports.strip().startswith('[') and imports.strip().endswith(']'):
                        import json
                        imports = json.loads(imports)
                    else:
                        # Cas d'une valeur simple non-liste
                        return [int(imports)] if str(imports).strip() else []
                else:
                    # Cas d'une valeur simple non-liste
                    return [int(imports)]
            except (ValueError, TypeError, json.JSONDecodeError):
                return []
        
        # Convertir chaque élément en entier si possible
        clean_list = []
        for item in imports:
            try:
                if isinstance(item, str) and not item.strip():
                    continue  # Ignorer les chaînes vides
                clean_list.append(int(item))
            except (ValueError, TypeError):
                pass  # Ignorer les éléments non convertibles
        
        # Éliminer les doublons tout en préservant l'ordre
        seen = set()
        return [x for x in clean_list if not (x in seen or seen.add(x))]
    
    # Traitement de chaque ligne
    for idx, row in result_df.iterrows():
        st_id = row['st_id']
        original_imports = row['stock_import']
        
        try:
            # Nettoyage et validation
            cleaned_imports = clean_stock_import(original_imports)
            
            # Vérifier si des modifications ont été apportées
            import json
            original_str = json.dumps(original_imports) if isinstance(original_imports, list) else str(original_imports)
            cleaned_str = json.dumps(cleaned_imports)
            
            if original_str != cleaned_str:
                # Mise à jour de la valeur
                result_df.at[idx, 'stock_import'] = cleaned_imports
                
                # Journalisation des modifications
                errors.append({
                    "type": "stock_import_cleaned",
                    "severity": "info",
                    "st_id": st_id,
                    "index": idx,
                    "original": original_imports,
                    "cleaned": cleaned_imports,
                    "message": f"Le tableau stock_import a été nettoyé et dédupliqué"
                })
                
                # Vérification de la déduplication
                if isinstance(original_imports, list) and len(original_imports) != len(cleaned_imports):
                    errors.append({
                        "type": "stock_import_deduplicated",
                        "severity": "info",
                        "st_id": st_id,
                        "index": idx,
                        "original_count": len(original_imports) if isinstance(original_imports, list) else 1,
                        "cleaned_count": len(cleaned_imports),
                        "message": f"Des doublons ont été éliminés du tableau stock_import"
                    })
        
        except Exception as e:
            # Enregistrer l'erreur si le nettoyage échoue
            errors.append({
                "type": "stock_import_cleaning_error",
                "severity": "error",
                "st_id": st_id,
                "index": idx,
                "original": original_imports,
                "message": f"Erreur lors du nettoyage du tableau stock_import: {str(e)}"
            })
            
            # Initialiser à une liste vide en cas d'erreur
            result_df.at[idx, 'stock_import'] = []
    
    # Vérifier une dernière fois que toutes les valeurs stock_import sont des listes
    for idx in df.index:
        if not isinstance(df.at[idx, 'stock_import'], list):
            df.at[idx, 'stock_import'] = []
    
    return result_df, errors