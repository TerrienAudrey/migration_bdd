"""
Module de nettoyage des commentaires pour les données stocks.
Remplace les valeurs "0" par des chaînes vides.
"""

from typing import Dict, List, Tuple, Any
import pandas as pd


def clean_commentary(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Remplace les valeurs "0" dans le champ st_commentary par des chaînes vides.
    
    Args:
        df: DataFrame contenant les données stocks
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les commentaires nettoyés
        - La liste des modifications effectuées
    """
    errors = []
    result_df = df.copy()
    
    # Vérifier si la colonne existe
    if 'st_commentary' not in result_df.columns:
        errors.append({
            "type": "missing_column",
            "severity": "warning",
            "message": "La colonne 'st_commentary' est absente du DataFrame"
        })
        return result_df, errors
    
    # Compter les occurrences initiales de "0"
    zero_mask = result_df['st_commentary'] == "0"
    zero_count = zero_mask.sum()
    
    if zero_count > 0:
        # Collecter les IDs des enregistrements avant modification
        zero_ids = result_df.loc[zero_mask, 'st_id'].tolist()
        
        # Remplacer les "0" par des chaînes vides
        result_df.loc[zero_mask, 'st_commentary'] = ""
        
        # Ajouter une information sur le nettoyage effectué
        errors.append({
            "type": "commentary_cleaned",
            "severity": "info",
            "count": zero_count,
            "affected_ids": zero_ids,
            "message": f"{zero_count} commentaires avec valeur '0' ont été remplacés par des chaînes vides"
        })
    
    return result_df, errors