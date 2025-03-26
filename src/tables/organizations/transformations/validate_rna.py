"""
Module de validation des numéros RNA pour les données Organizations.
Vérifie la validité des numéros RNA selon les règles françaises.
"""

import re
from typing import Dict, List, Tuple, Any, Optional

import pandas as pd


def validate_rna(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Valide et normalise les numéros RNA des organisations.
    
    Le RNA est un identifiant débutant par W suivi de caractères selon un format spécifique:
    - W suivi de 1 ou 2 chiffres (code département)
    - Puis un chiffre ou une lettre (pour les DOM-TOM ou la Corse)
    - Puis 6 ou 7 chiffres pour compléter les 9 caractères après le W
    
    Args:
        df: DataFrame contenant les données Organizations
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les RNA validés et normalisés
        - La liste des erreurs de validation détectées
    """
    errors = []
    result_df = df.copy()
    
    # Vérifier si la colonne RNA existe
    if 'or_rna' not in result_df.columns:
        errors.append({
            "type": "missing_column",
            "severity": "error",
            "message": "La colonne 'or_rna' est absente du DataFrame"
        })
        return result_df, errors
    
    # Expression régulière pour le format officiel du RNA
    rna_pattern = re.compile(r'^W[0-9]{1,2}[0-9A-Z][0-9]{6,7}$')
    
    def clean_rna(rna):
        """Nettoie un numéro RNA en supprimant les caractères non conformes."""
        if pd.isna(rna) or not isinstance(rna, str):
            return rna
        
        # Suppression des espaces
        rna = rna.strip().upper()
        
        # Conserver uniquement W, les chiffres et les lettres majuscules
        rna = re.sub(r'[^WA-Z0-9]', '', rna)
        
        # Suppression des espaces
        rna = rna.replace(' ', '')
        
        return rna
    
    # Traitement de chaque ligne
    for idx, row in result_df.iterrows():
        rna = row['or_rna']
        or_id = row['or_id']
        
        # Ignorer les valeurs nulles/vides
        if pd.isna(rna) or rna == '':
            continue
        
        # Nettoyage du RNA
        original_rna = rna
        cleaned_rna = clean_rna(rna)
        
        # Si le nettoyage a modifié la valeur, enregistrer la modification
        if cleaned_rna != original_rna:
            result_df.at[idx, 'or_rna'] = cleaned_rna
            errors.append({
                "type": "rna_cleaning",
                "severity": "info",
                "or_id": or_id,
                "index": idx,
                "original": original_rna,
                "cleaned": cleaned_rna
            })
            
            # Mettre à jour la valeur pour la validation suivante
            rna = cleaned_rna
        
        # Validation du format
        if not rna_pattern.match(rna):
            errors.append({
                "type": "invalid_rna_format",
                "severity": "warning",  # Changé de "error" à "warning" pour être moins strict
                "or_id": or_id,
                "index": idx,
                "value": rna,
                "reason": "Le RNA doit être au format W + code département + lettre/chiffre + chiffres (total de 10 caractères)"
            })
    
    return result_df, errors