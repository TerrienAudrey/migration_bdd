from typing import Dict, List, Tuple, Any
import pandas as pd 

def validate_commission_fields(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Valide les champs de commission pour s'assurer qu'ils ne sont pas null.
    
    Args:
        df: DataFrame contenant les données stocks
        
    Returns:
        Tuple contenant:
        - Le DataFrame (inchangé)
        - La liste des erreurs détectées
    """
    errors = []
    
    # Déterminer quel champ de commission est présent
    commission_percent_field = None
    for field in ['st_commission_percent', 'st_commission_%']:
        if field in df.columns:
            commission_percent_field = field
            break
    
    # Vérifier les valeurs nulles pour st_commission
    if 'st_commission' in df.columns:
        null_commission = df[df['st_commission'].isna()]
        for idx, row in null_commission.iterrows():
            errors.append({
                "type": "missing_commission",
                "severity": "warning",
                "st_id": row['st_id'],
                "st_io": row.get('st_io', 'unknown'),
                "index": idx,
                "field": "st_commission",
                "message": "Le montant de commission (st_commission) est manquant"
            })
    
    # Vérifier les valeurs nulles pour le champ de pourcentage de commission
    if commission_percent_field:
        null_commission_percent = df[df[commission_percent_field].isna()]
        for idx, row in null_commission_percent.iterrows():
            errors.append({
                "type": "missing_commission_percent",
                "severity": "warning",
                "st_id": row['st_id'],
                "st_io": row.get('st_io', 'unknown'),
                "index": idx,
                "field": commission_percent_field,
                "message": f"Le pourcentage de commission ({commission_percent_field}) est manquant"
            })
    
    # Vérifier la cohérence entre les deux champs si les deux sont présents
    if 'st_commission' in df.columns and commission_percent_field:
        for idx, row in df.iterrows():
            commission = row['st_commission']
            percent = row[commission_percent_field]
            
            # Si l'un est null et l'autre non
            if pd.notna(commission) and pd.isna(percent):
                errors.append({
                    "type": "inconsistent_commission",
                    "severity": "warning",
                    "st_id": row['st_id'],
                    "st_io": row.get('st_io', 'unknown'),
                    "index": idx,
                    "message": "Montant de commission présent mais pourcentage manquant"
                })
            elif pd.isna(commission) and pd.notna(percent):
                errors.append({
                    "type": "inconsistent_commission",
                    "severity": "warning",
                    "st_id": row['st_id'],
                    "st_io": row.get('st_io', 'unknown'),
                    "index": idx,
                    "message": "Pourcentage de commission présent mais montant manquant"
                })
    
    return df, errors