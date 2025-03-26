"""
Module de validation des numéros SIREN pour les données Companies.
Vérifie la validité des numéros SIREN selon les règles françaises.
"""

import re
from typing import Dict, List, Tuple, Any, Optional

import pandas as pd


def validate_siren(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Valide et normalise les numéros SIREN des entreprises.
    
    Le SIREN est un identifiant de 9 chiffres attribué aux entreprises françaises.
    La validation comprend:
    - Format: exactement 9 chiffres
    - Algorithme de Luhn pour vérifier la clé de contrôle
    
    Args:
        df: DataFrame contenant les données Companies
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les SIREN validés et normalisés
        - La liste des erreurs de validation détectées
    """
    errors = []
    result_df = df.copy()
    
    # Vérifier si la colonne SIREN existe
    if 'co_siren' not in result_df.columns:
        errors.append({
            "type": "missing_column",
            "severity": "error",
            "message": "La colonne 'co_siren' est absente du DataFrame"
        })
        return result_df, errors
    
    # Expression régulière pour valider le format SIREN
    siren_pattern = re.compile(r'^\d{9}$')
    
    def clean_siren(siren):
        if pd.isna(siren) or not isinstance(siren, str):
            return siren
        
        # Suppression des caractères non numériques
        cleaned = re.sub(r'\D', '', siren)
        
        # Si le résultat est une chaîne vide, retourner None
        if not cleaned:
            return None
        
        # Si le SIREN a 8 chiffres, ajouter un 0 au début
        if len(cleaned) == 8:
            cleaned = '0' + cleaned
                
        return cleaned
    
    def is_valid_luhn(digits: str) -> bool:
        """
        Vérifie si une chaîne de chiffres est valide selon l'algorithme de Luhn.
        
        L'algorithme de Luhn, également connu sous le nom de formule mod 10, permet de 
        valider une variété de numéros d'identification, notamment les SIREN.
        """
        # Conversion en liste d'entiers et inversion
        digits = [int(d) for d in digits]
        digits.reverse()
        
        # Application de l'algorithme
        total = 0
        for i, digit in enumerate(digits):
            if i % 2 == 1:  # Positions impaires (en comptant depuis la fin)
                digit *= 2
                if digit > 9:
                    digit -= 9
            total += digit
        
        # Vérification que le total est divisible par 10
        return total % 10 == 0
    
    # Traitement de chaque ligne
    for idx, row in result_df.iterrows():
        siren = row['co_siren']
        co_id = row['co_id']
        
        # Ignorer les valeurs nulles/vides
        if pd.isna(siren) or siren == '':
            continue
        
        # Nettoyage du SIREN
        original_siren = siren
        cleaned_siren = clean_siren(siren)
        
        # Si le nettoyage a modifié la valeur, enregistrer la modification
        if cleaned_siren != original_siren:
            result_df.at[idx, 'co_siren'] = cleaned_siren
            
            # Vérifier si la correction concerne spécifiquement l'ajout d'un zéro au début (SIREN à 8 chiffres)
            if len(re.sub(r'\D', '', original_siren)) == 8 and cleaned_siren == '0' + re.sub(r'\D', '', original_siren):
                errors.append({
                    "type": "siren_missing_leading_zero",
                    "severity": "warning",  # ou "error" selon votre politique
                    "co_id": co_id,
                    "index": idx,
                    "original": original_siren,
                    "cleaned": cleaned_siren,
                    "reason": "SIREN de 8 chiffres corrigé automatiquement avec un zéro en début"
                })
            else:
                # Autres types de corrections
                errors.append({
                    "type": "siren_cleaning",
                    "severity": "info",
                    "co_id": co_id,
                    "index": idx,
                    "original": original_siren,
                    "cleaned": cleaned_siren
                })
            
            # Mettre à jour la valeur pour la validation suivante
            siren = cleaned_siren
        
        # Si le SIREN est None après nettoyage, passer à la ligne suivante
        if siren is None:
            errors.append({
                "type": "invalid_siren",
                "severity": "warning",
                "co_id": co_id,
                "index": idx,
                "original": original_siren,
                "reason": "SIREN vide après nettoyage"
            })
            continue
            
        # Validation du format
        if not siren_pattern.match(siren):
            errors.append({
                "type": "invalid_siren_format",
                "severity": "error",
                "co_id": co_id,
                "index": idx,
                "value": siren,
                "reason": f"Le SIREN doit contenir exactement 9 chiffres (trouvé: {len(siren)})"
            })
            continue
        
        # Validation avec l'algorithme de Luhn
        if not is_valid_luhn(siren):
            errors.append({
                "type": "invalid_siren_checksum",
                "severity": "error",
                "co_id": co_id,
                "index": idx,
                "value": siren,
                "reason": "Le SIREN ne respecte pas l'algorithme de Luhn"
            })
    
    return result_df, errors