"""
Module de validation spécifique pour les identifiants si_id.
Vérifie que les si_id sont des entiers valides et uniques.
"""

from typing import Dict, List, Tuple, Any
import pandas as pd


def validate_si_id(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Valide que les si_id sont des entiers valides et uniques.
    
    Vérifications:
    - Présence du champ si_id
    - Type entier entre 0 et n
    - Unicité des valeurs
    
    Args:
        df: DataFrame contenant les données stock_import
        
    Returns:
        Tuple contenant:
        - Le DataFrame original (non modifié)
        - La liste des erreurs détectées
    """
    errors = []
    
    # Vérification 1: Présence du champ si_id
    if 'si_id' not in df.columns:
        errors.append({
            "type": "missing_si_id",
            "severity": "error",
            "message": "Le champ obligatoire 'si_id' est absent du DataFrame"
        })
        return df, errors
    
    # Vérification 2: Type entier de si_id
    for idx, value in enumerate(df['si_id']):
        # Vérifier si la valeur est None ou NaN
        if pd.isna(value):
            errors.append({
                "type": "invalid_si_id",
                "severity": "error",
                "index": idx,
                "value": None,
                "message": "si_id ne peut pas être null"
            })
            continue
            
        # Vérifier le type entier
        if not isinstance(value, int):
            errors.append({
                "type": "invalid_si_id_type",
                "severity": "error",
                "index": idx,
                "value": value,
                "message": f"si_id doit être un entier, trouvé: {type(value).__name__}"
            })
            continue
            
        # Vérifier que la valeur est positive ou nulle
        if value < 0:
            errors.append({
                "type": "negative_si_id",
                "severity": "error",
                "index": idx,
                "value": value,
                "message": "si_id ne peut pas être négatif"
            })
    
    # Vérification 3: Unicité des si_id
    duplicate_ids = df[df.duplicated(subset=['si_id'])]['si_id'].tolist()
    if duplicate_ids:
        for dup_id in duplicate_ids:
            indices = df.index[df['si_id'] == dup_id].tolist()
            errors.append({
                "type": "duplicate_si_id",
                "severity": "error",
                "value": dup_id,
                "indices": indices,
                "message": f"si_id {dup_id} est dupliqué aux indices: {indices}"
            })
    
    return df, errors