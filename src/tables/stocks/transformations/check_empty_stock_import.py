"""
Module de vérification des stock_import vides pour les données stocks.
Identifie les enregistrements ayant une liste stock_import vide.
"""

from typing import Dict, List, Tuple, Any
import pandas as pd


def check_empty_stock_import(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Identifie les enregistrements avec une liste stock_import vide.
    
    Args:
        df: DataFrame contenant les données stocks
        
    Returns:
        Tuple contenant:
        - Le DataFrame inchangé
        - La liste des erreurs concernant les stock_import vides
    """
    errors = []
    
    # Vérifier si la colonne existe
    if 'stock_import' not in df.columns:
        errors.append({
            "type": "missing_column",
            "severity": "warning",
            "message": "La colonne 'stock_import' est absente du DataFrame"
        })
        return df, errors
    
    # Identifier les entrées avec stock_import vide
    empty_imports = df[df['stock_import'].apply(lambda x: isinstance(x, list) and len(x) == 0)]
    
    if len(empty_imports) > 0:
        # Collecter les IDs et autres informations pertinentes
        empty_records = []
        for idx, row in empty_imports.iterrows():
            empty_records.append({
                "st_id": row['st_id'],
                "st_io": row.get('st_io', 'N/A'),
                "index": idx
            })
        
        # Ajouter l'erreur pour les stock_import vides
        errors.append({
            "type": "empty_stock_import",
            "severity": "warning",
            "count": len(empty_imports),
            "affected_records": empty_records,
            "message": f"Détection de {len(empty_imports)} enregistrements avec stock_import vide"
        })
    
    return df, errors