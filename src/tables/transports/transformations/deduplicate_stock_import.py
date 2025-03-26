"""
Module de déduplication des identifiants stock_import pour les données transports.
Élimine les doublons dans les tableaux stock_import.
"""

from typing import Dict, List, Tuple, Any

import pandas as pd


def deduplicate_stock_import(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Déduplique les identifiants dans les tableaux stock_import.
    
    Args:
        df: DataFrame contenant les données transports
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les tableaux stock_import dédupliqués
        - La liste des modifications effectuées
    """
    errors = []
    result_df = df.copy()
    
    # Vérifier si la colonne stock_import existe
    if 'stock_import' not in result_df.columns:
        errors.append({
            "type": "missing_column",
            "severity": "error",
            "message": "La colonne 'stock_import' est absente du DataFrame"
        })
        return result_df, errors
            
    # Traitement de chaque ligne
    for idx, row in result_df.iterrows():
        tra_id = row['tra_id']
        
        # Récupérer la valeur de stock_import pour cette ligne spécifique
        stock_import = row.get('stock_import')
        
        # Vérifier si stock_import est null ou vide ou pas une liste
        if stock_import is None or not isinstance(stock_import, list):
            # Initialiser comme liste vide
            result_df.at[idx, 'stock_import'] = []
            continue
        
        stock_import = row['stock_import']
        
        # Convertir tous les éléments en entiers si possible
        try:
            # Enregistrer les éléments originaux pour comparaison
            original_stock_import = stock_import.copy()
            
            # Convertir en entiers (si chaînes numériques)
            stock_import = [int(item) if isinstance(item, str) and item.isdigit() else item for item in stock_import]
            
            # Dédupliquer tout en préservant l'ordre
            seen = set()
            deduplicated = []
            
            for item in stock_import:
                if item not in seen:
                    seen.add(item)
                    deduplicated.append(item)
            
            # Si des modifications ont été effectuées, les enregistrer
            if deduplicated != original_stock_import:
                result_df.at[idx, 'stock_import'] = deduplicated
                
                # Identifier les doublons supprimés
                duplicates = [item for item in original_stock_import if original_stock_import.count(item) > 1]
                duplicates_set = set(duplicates)
                
                if duplicates_set:
                    errors.append({
                        "type": "stock_import_deduplication",
                        "severity": "info",
                        "tra_id": tra_id,
                        "index": idx,
                        "original_count": len(original_stock_import),
                        "deduplicated_count": len(deduplicated),
                        "duplicates_removed": list(duplicates_set),
                        "message": f"Doublons supprimés dans stock_import: {list(duplicates_set)}"
                    })
                else:
                    # Si seuls des cas de conversion de type ont été effectués
                    errors.append({
                        "type": "stock_import_type_conversion",
                        "severity": "info",
                        "tra_id": tra_id,
                        "index": idx,
                        "message": "Types de données normalisés dans stock_import"
                    })
        except Exception as e:
            # En cas d'erreur lors de la conversion
            errors.append({
                "type": "stock_import_processing_error",
                "severity": "error",
                "tra_id": tra_id,
                "index": idx,
                "error": str(e),
                "message": f"Erreur lors du traitement de stock_import: {str(e)}"
            })
    
    return result_df, errors