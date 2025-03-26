"""
Module de validation des contraintes d'unicité pour les données stocks.
Vérifie l'unicité des identifiants et autres champs soumis à des contraintes.
"""

from typing import Dict, List, Tuple, Any

import pandas as pd


def validate_uniqueness(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Valide les contraintes d'unicité dans le DataFrame.
    
    Vérifications:
    - Unicité du champ st_io dans tout le jeu de données
    
    Args:
        df: DataFrame contenant les données stocks
        
    Returns:
        Tuple contenant:
        - Le DataFrame (inchangé)
        - La liste des erreurs d'unicité détectées
    """
    errors = []
    result_df = df.copy()
    
    # Vérification de l'unicité du champ st_io
    if 'st_io' in result_df.columns:
        # Identifier les valeurs qui apparaissent plus d'une fois
        duplicates = result_df['st_io'].value_counts()
        duplicate_values = duplicates[duplicates > 1].index.tolist()
        
        if duplicate_values:
            # Pour chaque valeur dupliquée, trouver tous les enregistrements concernés
            for value in duplicate_values:
                duplicate_rows = result_df[result_df['st_io'] == value]
                
                # Ajouter une erreur pour chaque instance dupliquée, sauf la première
                first_instance = True
                for idx, row in duplicate_rows.iterrows():
                    if first_instance:
                        first_instance = False
                        continue
                    
                    errors.append({
                        "type": "duplicate_st_io",
                        "severity": "error",
                        "st_id": row['st_id'],
                        "index": idx,
                        "st_io": value,
                        "message": f"La valeur st_io='{value}' est en conflit avec un autre enregistrement"
                    })
    
    # Vérification de st_id (clé primaire du modèle)
    if 'st_id' in result_df.columns:
        # Identifier les valeurs qui apparaissent plus d'une fois
        id_duplicates = result_df['st_id'].value_counts()
        id_duplicate_values = id_duplicates[id_duplicates > 1].index.tolist()
        
        if id_duplicate_values:
            # Pour chaque valeur dupliquée, trouver tous les enregistrements concernés
            for value in id_duplicate_values:
                duplicate_rows = result_df[result_df['st_id'] == value]
                
                # Ajouter une erreur pour chaque instance dupliquée, sauf la première
                first_instance = True
                for idx, row in duplicate_rows.iterrows():
                    if first_instance:
                        first_instance = False
                        continue
                    
                    errors.append({
                        "type": "duplicate_st_id",
                        "severity": "error",
                        "st_id": value,
                        "index": idx,
                        "message": f"La clé primaire st_id={value} est dupliquée"
                    })
    
    return result_df, errors