"""
Module de validation des numéros SIRET pour les données Companies.
Vérifie la validité des numéros SIRET selon les règles françaises.
"""

import re
from typing import Dict, List, Tuple, Any, Optional

import pandas as pd


def validate_siret(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Valide et normalise les numéros SIRET des entreprises.
    
    Le SIRET est un identifiant de 14 chiffres composé du SIREN (9 chiffres) 
    suivi du NIC (5 chiffres) qui identifie un établissement spécifique.
    
    La validation comprend:
    - Format: exactement 14 chiffres
    - Cohérence avec le SIREN correspondant
    - Algorithme de Luhn pour vérifier la clé de contrôle
    
    Args:
        df: DataFrame contenant les données Companies
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les SIRET validés et normalisés
        - La liste des erreurs de validation détectées
    """
    errors = []
    result_df = df.copy()
    
    # Vérifier si les colonnes nécessaires existent
    required_columns = ['co_siret', 'co_siren']
    for column in required_columns:
        if column not in result_df.columns:
            errors.append({
                "type": "missing_column",
                "severity": "error",
                "message": f"La colonne '{column}' est absente du DataFrame"
            })
            if column == 'co_siret':  # Sans SIRET, impossible de continuer
                return result_df, errors
    
    # Expression régulière pour valider le format SIRET
    siret_pattern = re.compile(r'^\d{14}$')
    
    
    def clean_siret(siret):
        if pd.isna(siret) or not isinstance(siret, str):
            return siret
        
        # Suppression des caractères non numériques
        cleaned = re.sub(r'\D', '', siret)
        
        # Si le résultat est une chaîne vide, retourner None
        if not cleaned:
            return None
        
        # Si le SIRET a 13 chiffres, ajouter un 0 au début pour en avoir 14
        if len(cleaned) == 13:
            cleaned = '0' + cleaned
                
        return cleaned
    
    def is_valid_luhn(digits: str) -> bool:
        """
        Vérifie si une chaîne de chiffres est valide selon l'algorithme de Luhn.
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
        siret = row['co_siret']
        siren = row['co_siren']
        co_id = row['co_id']
        
        # Ignorer les valeurs nulles/vides
        if pd.isna(siret) or siret == '':
            continue
        
        # Nettoyage du SIRET
        original_siret = siret
        cleaned_siret = clean_siret(siret)
        
        # Si le nettoyage a modifié la valeur, enregistrer la modification
        if cleaned_siret != original_siret:
            result_df.at[idx, 'co_siret'] = cleaned_siret
            errors.append({
                "type": "siret_cleaning",
                "severity": "info",
                "co_id": co_id,
                "index": idx,
                "original": original_siret,
                "cleaned": cleaned_siret
            })
            
            # Mettre à jour la valeur pour la validation suivante
            siret = cleaned_siret
        
        # Si le SIRET est None après nettoyage, passer à la ligne suivante
        if siret is None:
            errors.append({
                "type": "invalid_siret",
                "severity": "warning",
                "co_id": co_id,
                "index": idx,
                "original": original_siret,
                "reason": "SIRET vide après nettoyage"
            })
            continue
            
        # Validation du format
        if not siret_pattern.match(siret):
            errors.append({
                "type": "invalid_siret_format",
                "severity": "error",
                "co_id": co_id,
                "index": idx,
                "value": siret,
                "reason": f"Le SIRET doit contenir exactement 14 chiffres (trouvé: {len(siret)})"
            })
            continue
        
        # Validation de la cohérence avec le SIREN
        if not pd.isna(siren) and siren and siret[:9] != siren:
            errors.append({
                "type": "siret_siren_mismatch",
                "severity": "error",
                "co_id": co_id,
                "index": idx,
                "siret": siret,
                "siren": siren,
                "reason": "Les 9 premiers chiffres du SIRET doivent correspondre au SIREN"
            })
        
        # Validation avec l'algorithme de Luhn
        if not is_valid_luhn(siret):
            errors.append({
                "type": "invalid_siret_checksum",
                "severity": "error",
                "co_id": co_id,
                "index": idx,
                "value": siret,
                "reason": "Le SIRET ne respecte pas l'algorithme de Luhn"
            })
    
    return result_df, errors