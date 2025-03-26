"""
Module de nettoyage de la ponctuation pour les données logistic_address.
Supprime ou standardise les signes de ponctuation selon les règles définies.
"""

import re
from typing import Dict, List, Tuple, Any

import pandas as pd


def clean_punctuation(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Nettoie les signes de ponctuation dans les champs textuels.
    
    Opérations effectuées:
    - Suppression des points, virgules, points-virgules non significatifs
    - Conservation des apostrophes et tirets
    - Gestion des exceptions métier
    
    Args:
        df: DataFrame contenant les données logistic_address
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec la ponctuation nettoyée
        - La liste des erreurs ou modifications détectées
    """
    errors = []
    result_df = df.copy()
    
    # Liste des champs textuels à traiter
    text_fields = [
        "la_house_number",
        "la_street",
        "la_additional_address",
        "la_city"
    ]
    
    # Définition des règles de nettoyage par champ
    cleaning_rules = {
        "la_house_number": {
            "punctuation_to_remove": r'[.,;:!?"()]',
            "punctuation_to_keep": r'[-\']',  # Conserver les tirets et apostrophes
            "exceptions": []
        },
        "la_street": {
            "punctuation_to_remove": r'[;:!?"()]',
            "punctuation_to_keep": r'[-\',.]',  # Conserver les tirets, apostrophes, virgules et points
            "exceptions": ["ST.", "STE.", "AV.", "BD.", "ALL."]
        },
        "la_additional_address": {
            "punctuation_to_remove": r'[;:!?"()]',
            "punctuation_to_keep": r'[-\',.]',
            "exceptions": []
        },
        "la_city": {
            "punctuation_to_remove": r'[.,;:!?"()]',
            "punctuation_to_keep": r'[-\']',
            "exceptions": ["ST.", "STE."]
        }
    }
    
    # Fonction pour nettoyer la ponctuation dans une chaîne
    def clean_punctuation_string(value, field):
        if pd.isna(value) or not isinstance(value, str):
            return value
            
        # Récupérer les règles spécifiques au champ
        rules = cleaning_rules.get(field, {
            "punctuation_to_remove": r'[.,;:!?"()]',
            "punctuation_to_keep": r'[-\']',
            "exceptions": []
        })
        
        # Vérifier si la valeur contient des exceptions à préserver
        for exception in rules["exceptions"]:
            if exception in value:
                # Remplacer temporairement l'exception par un marqueur unique
                placeholder = f"__EXCEPTION_{hash(exception)}__"
                value = value.replace(exception, placeholder)
        
        # Appliquer la règle de suppression de ponctuation
        value = re.sub(rules["punctuation_to_remove"], '', value)
        
        # Restaurer les exceptions
        for exception in rules["exceptions"]:
            placeholder = f"__EXCEPTION_{hash(exception)}__"
            value = value.replace(placeholder, exception)
        
        # Normaliser les espaces (à nouveau, car la suppression de ponctuation peut créer des espaces multiples)
        value = re.sub(r'\s+', ' ', value).strip()
        
        return value
    
    # Appliquer le nettoyage sur chaque champ textuel
    for field in text_fields:
        if field in result_df.columns:
            try:
                # Sauvegarde des valeurs originales pour comparaison
                original_values = result_df[field].copy()
                
                # Application du nettoyage avec les règles spécifiques au champ
                result_df[field] = result_df[field].apply(
                    lambda x: clean_punctuation_string(x, field)
                )
                
                # Détection des lignes modifiées pour logging
                modified_mask = (original_values != result_df[field]) & (~pd.isna(original_values))
                modified_indices = result_df.index[modified_mask].tolist()
                
                # Enregistrement des modifications pour suivi
                for idx in modified_indices:
                    original = original_values.iloc[idx]
                    cleaned = result_df[field].iloc[idx]
                    la_id = result_df.at[idx, 'la_id']
                    
                    # Enregistrer la modification comme information
                    errors.append({
                        "type": "punctuation_cleaning",
                        "severity": "info",
                        "la_id": la_id,
                        "field": field,
                        "original": original,
                        "cleaned": cleaned,
                        "index": idx
                    })
            except Exception as e:
                # Enregistrer l'erreur si le nettoyage échoue
                errors.append({
                    "type": "punctuation_cleaning_error",
                    "severity": "error",
                    "field": field,
                    "message": str(e)
                })
    
    return result_df, errors