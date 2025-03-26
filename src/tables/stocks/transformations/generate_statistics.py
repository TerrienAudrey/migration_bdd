"""
Module de génération de statistiques pour les données stocks.
Analyse les champs st_step_planning et st_transportby pour le rapport.
"""

from typing import Dict, List, Tuple, Any
import pandas as pd


def generate_statistics(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Génère des statistiques sur les valeurs de st_step_planning et st_transportby.
    
    Args:
        df: DataFrame contenant les données stocks
        
    Returns:
        Tuple contenant:
        - Le DataFrame inchangé
        - La liste des statistiques générées
    """
    stats = []
    
    # Statistiques sur st_step_planning
    if 'st_step_planning' in df.columns:
        # Compter les valeurs
        planning_counts = df['st_step_planning'].value_counts(dropna=False).to_dict()
        
        # Calculer les pourcentages
        total_records = len(df)
        planning_percentages = {
            value: f"{(count/total_records)*100:.2f}%" 
            for value, count in planning_counts.items()
        }
        
        # Ajouter aux statistiques
        stats.append({
            "type": "step_planning_stats",
            "severity": "info",
            "field": "st_step_planning",
            "counts": planning_counts,
            "percentages": planning_percentages,
            "total_records": total_records,
            "message": "Statistiques sur les valeurs de st_step_planning"
        })
    
    # Statistiques sur st_transportby
    if 'st_transportby' in df.columns:
        # Compter les valeurs
        transport_counts = df['st_transportby'].value_counts(dropna=False).to_dict()
        
        # Calculer les pourcentages
        total_records = len(df)
        transport_percentages = {
            str(value): f"{(count/total_records)*100:.2f}%" 
            for value, count in transport_counts.items()
        }
        
        # Vérifier les valeurs null ou vides
        null_count = df['st_transportby'].isna().sum()
        empty_count = 0
        if null_count > 0 or empty_count > 0:
            stats.append({
                "type": "missing_transportby",
                "severity": "warning",
                "field": "st_transportby",
                "null_count": null_count,
                "empty_count": empty_count,
                "message": f"Valeurs manquantes détectées dans st_transportby: {null_count} null, {empty_count} vides"
            })
        
        # Ajouter aux statistiques
        stats.append({
            "type": "transportby_stats",
            "severity": "info",
            "field": "st_transportby",
            "counts": transport_counts,
            "percentages": transport_percentages,
            "total_records": total_records,
            "message": "Statistiques sur les valeurs de st_transportby"
        })
    
    return df, stats