"""
Module de nettoyage de la ponctuation pour les données Companies.
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
        df: DataFrame contenant les données Companies
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec la ponctuation nettoyée
        - La liste des erreurs ou modifications détectées
    """
    errors = []
    result_df = df.copy()
    
    # Liste des champs textuels à traiter
    text_fields = [
        "co_business_name",
        "co_head_office_address",
        "co_head_office_city",
        "co_legal_form",
        "co_head_office_additional_address"
    ]
    
    # Définition des règles de nettoyage par champ
    cleaning_rules = {
        "co_business_name": {
            "punctuation_to_remove": r'[.,;:!?"()]',
            "punctuation_to_keep": r'[-\']',  # Conserver les tirets et apostrophes
            "exceptions": ["S.A.", "S.A.R.L.", "S.A.S.", "S.C.I.", "E.U.R.L."]
        },
        "co_head_office_address": {
            "punctuation_to_remove": r'[;:!?"()]',
            "punctuation_to_keep": r'[-\',.]',  # Conserver les tirets, apostrophes, virgules et points
            "exceptions": []
        },
        "co_head_office_city": {
            "punctuation_to_remove": r'[.,;:!?"()]',
            "punctuation_to_keep": r'[-\']',
            "exceptions": ["ST.", "STE."]
        },
        "co_legal_form": {
            "punctuation_to_remove": r'[,;:!?"()]',
            "punctuation_to_keep": r'[-\'.]',  # Conserver les points pour les abréviations
            "exceptions": ["S.A.", "S.A.R.L.", "S.A.S.", "S.C.I.", "E.U.R.L."]
        },
        "co_head_office_additional_address": {
            "punctuation_to_remove": r'[;:!?"()]',
            "punctuation_to_keep": r'[-\',.]',
            "exceptions": []
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
                    co_id = result_df.at[idx, 'co_id']
                    
                    # Enregistrer la modification comme information
                    errors.append({
                        "type": "punctuation_cleaning",
                        "severity": "info",
                        "co_id": co_id,
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
    
    # Mise à jour du champ normalisé pour le nom d'entreprise
    if 'co_normalized_business_name' in result_df.columns:
        result_df['co_normalized_business_name'] = result_df['co_normalized_business_name'].apply(
            lambda x: clean_punctuation_string(x, "co_business_name")
        )
    
    return result_df, errors