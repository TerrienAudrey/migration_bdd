"""
Module de validation des noms de ville pour les données logistic_address.
Nettoie et valide les noms de ville selon les règles françaises.
"""

import re
from typing import Dict, List, Tuple, Any, Optional

import pandas as pd


def validate_city_names(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Valide et nettoie les noms de ville.
    
    Opérations effectuées:
    - Suppression des mentions d'arrondissement (ex: "PARIS 14E", "LYON 8EME ARRONDISSEMENT")
    - Validation des caractères autorisés (lettres, espaces, tirets, apostrophes)
    - Standardisation en majuscules
    
    Args:
        df: DataFrame contenant les données logistic_address
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les noms de ville validés et nettoyés
        - La liste des modifications et erreurs détectées
    """
    errors = []
    result_df = df.copy()
    
    # Vérifier si la colonne de ville existe
    if 'la_city' not in result_df.columns:
        errors.append({
            "type": "missing_column",
            "severity": "error",
            "message": "La colonne 'la_city' est absente du DataFrame"
        })
        return result_df, errors
    
    # Expression régulière pour valider les noms de ville
    city_name_pattern = re.compile(r'^[A-Z\s\-\']+$')
    
    # Patterns pour nettoyer les noms de ville avec arrondissements
    arrondissement_patterns = [
        (r'\s+\d+E\s+ARRONDISSEMENT$', ''),  # ex: "LYON 8E ARRONDISSEMENT" -> "LYON"
        (r'\s+\d+EME\s+ARRONDISSEMENT$', ''),  # ex: "LYON 8EME ARRONDISSEMENT" -> "LYON"
        (r'\s+\d+E$', ''),  # ex: "PARIS 14E" -> "PARIS"
        (r'\s+\d+ER$', ''),  # ex: "PARIS 1ER" -> "PARIS"
        (r'\s+\d+EME$', ''),  # ex: "PARIS 14EME" -> "PARIS"
        (r'\s+CEDEX.*$', '')  # ex: "RENNES CEDEX 9" -> "RENNES"
    ]
    
    # Liste des préfixes courants à préserver
    prefix_list = ["SAINT", "SAINTE", "MONT", "VAL"]
    
    # Fonction pour nettoyer et normaliser un nom de ville
    def clean_city_name(city_name: Optional[str]) -> Optional[str]:
        if pd.isna(city_name) or city_name == '':
            return city_name
        
        if not isinstance(city_name, str):
            city_name = str(city_name)
        
        # Convertir en majuscules
        city_name = city_name.upper().strip()
        
        # Normaliser les espaces
        city_name = re.sub(r'\s+', ' ', city_name)
        
        # Supprimer les mentions d'arrondissement
        original_name = city_name
        for pattern, replacement in arrondissement_patterns:
            city_name = re.sub(pattern, replacement, city_name)
        
        # Vérifier si un préfixe doit être préservé avec son tiret
        for prefix in prefix_list:
            # Convertir "SAINT PIERRE" en "SAINT-PIERRE"
            pattern = f"{prefix}\\s+([A-Z])"
            city_name = re.sub(pattern, f"{prefix}-\\1", city_name)
        
        return city_name.strip()
    
    # Traitement de chaque ligne
    for idx, row in result_df.iterrows():
        city_name = row['la_city']
        la_id = row['la_id']
        
        # Ignorer les valeurs nulles/vides
        if pd.isna(city_name) or city_name == '':
            continue
        
        # Nettoyage du nom de ville
        original_city = city_name
        cleaned_city = clean_city_name(city_name)
        
        # Si le nettoyage a modifié la valeur, enregistrer la modification
        if cleaned_city != original_city:
            result_df.at[idx, 'la_city'] = cleaned_city
            errors.append({
                "type": "city_name_cleaning",
                "severity": "info",
                "la_id": la_id,
                "index": idx,
                "original": original_city,
                "cleaned": cleaned_city
            })
            
            # Mettre à jour la valeur pour la validation suivante
            city_name = cleaned_city
        
        # Validation du format
        if not city_name_pattern.match(city_name):
            errors.append({
                "type": "invalid_city_name_format",
                "severity": "warning",
                "la_id": la_id,
                "index": idx,
                "value": city_name,
                "reason": "Le nom de ville doit contenir uniquement des lettres majuscules, espaces, tirets et apostrophes"
            })
        
        # Vérification de la longueur minimale
        if len(city_name) < 2:
            errors.append({
                "type": "city_name_too_short",
                "severity": "warning",
                "la_id": la_id,
                "index": idx,
                "value": city_name,
                "reason": "Le nom de ville est trop court"
            })
    
    return result_df, errors