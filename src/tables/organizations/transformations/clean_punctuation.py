"""
Module de nettoyage de la ponctuation pour les données Organizations.
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
        df: DataFrame contenant les données Organizations
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec la ponctuation nettoyée
        - La liste des erreurs ou modifications détectées
    """
    errors = []
    result_df = df.copy()
    
    # Liste des champs textuels à traiter
    text_fields = [
        "or_denomination",
        "or_house_number",
        "or_street",
        "or_city",
        "or_state",
        "or_additional_address"
    ]
    
    # Définition des règles de nettoyage par champ
    cleaning_rules = {
        "or_denomination": {
            "punctuation_to_remove": r'[.,;:!?"()]',
            "punctuation_to_keep": r'[-\']',  # Conserver les tirets et apostrophes
            "exceptions": ["ASSOC.", "ASSO.", "ASSOCIATION", "FEDER.", "FEDERATION"]
        },
        "or_street": {
            "punctuation_to_remove": r'[;:!?"()]',
            "punctuation_to_keep": r'[-\',.]',  # Conserver les tirets, apostrophes, virgules et points
            "exceptions": []
        },
        "or_city": {
            "punctuation_to_remove": r'[.,;:!?"()]',
            "punctuation_to_keep": r'[-\']',
            "exceptions": ["ST.", "STE."]
        },
        "or_additional_address": {
            "punctuation_to_remove": r'[;:!?"()]',
            "punctuation_to_keep": r'[-\',.]',
            "exceptions": []
        }
    }
    
    # Règles par défaut pour les champs sans règles spécifiques
    default_rules = {
        "punctuation_to_remove": r'[.,;:!?"()]',
        "punctuation_to_keep": r'[-\']',
        "exceptions": []
    }
    
    # Fonction pour nettoyer la ponctuation dans une chaîne
    def clean_punctuation_string(value, field):
        if pd.isna(value) or not isinstance(value, str):
            return value
            
        # Récupérer les règles spécifiques au champ ou utiliser les règles par défaut
        rules = cleaning_rules.get(field, default_rules)
        
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
                    or_id = result_df.at[idx, 'or_id']
                    
                    # Enregistrer la modification comme information
                    errors.append({
                        "type": "punctuation_cleaning",
                        "severity": "info",
                        "or_id": or_id,
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