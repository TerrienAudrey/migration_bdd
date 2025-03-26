"""
Module de validation des codes postaux pour les données Companies.
Vérifie la validité des codes postaux selon les règles françaises et internationales.
"""

import re
from typing import Dict, List, Tuple, Any, Optional, Set

import pandas as pd


# Liste des départements français valides (métropole et DOM-TOM)
VALID_FRENCH_DEPARTMENTS: Set[str] = {
    # Métropole
    "01", "02", "03", "04", "05", "06", "07", "08", "09",
    "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", 
    "20", "21", "22", "23", "24", "25", "26", "27", "28", "29",
    "30", "31", "32", "33", "34", "35", "36", "37", "38", "39",
    "40", "41", "42", "43", "44", "45", "46", "47", "48", "49",
    "50", "51", "52", "53", "54", "55", "56", "57", "58", "59",
    "60", "61", "62", "63", "64", "65", "66", "67", "68", "69",
    "70", "71", "72", "73", "74", "75", "76", "77", "78", "79",
    "80", "81", "82", "83", "84", "85", "86", "87", "88", "89",
    "90", "91", "92", "93", "94", "95",
    # Corse
    "2A", "2B",
    # DOM-TOM
    "971", "972", "973", "974", "975", "976", "977", "978", "984", "986", "987", "988", "989"
}


def validate_postal_code(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Valide et normalise les codes postaux des adresses d'entreprises.
    
    Pour la France:
    - Format: 5 chiffres
    - Les 2 premiers chiffres doivent correspondre à un département valide
    
    Pour les adresses internationales:
    - Détection et validation selon les formats spécifiques à chaque pays
    
    Args:
        df: DataFrame contenant les données Companies
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les codes postaux validés et normalisés
        - La liste des erreurs de validation détectées
    """
    errors = []
    result_df = df.copy()
    
    # Vérifier si la colonne du code postal existe
    if 'co_head_office_postal_code' not in result_df.columns:
        errors.append({
            "type": "missing_column",
            "severity": "error",
            "message": "La colonne 'co_head_office_postal_code' est absente du DataFrame"
        })
        return result_df, errors
    
    # Expression régulière pour valider le format du code postal français
    french_postal_code_pattern = re.compile(r'^\d{5}$')
    
    def clean_postal_code(postal_code: Optional[str]) -> Optional[str]:
        """Nettoie un code postal en supprimant les caractères non conformes."""
        if pd.isna(postal_code) or not isinstance(postal_code, str):
            return postal_code
        
        # Suppression des espaces
        postal_code = postal_code.strip()
        
        # Pour les codes postaux français, extraction des chiffres
        if re.match(r'^(F-|FR-|\+33-)?(\d{1,5})$', postal_code):
            digits = re.match(r'^(F-|FR-|\+33-)?(\d{1,5})$', postal_code).group(2)
            # Si le code a 4 chiffres, ajouter un 0 au début
            if len(digits) == 4:
                return '0' + digits
            return digits.zfill(5)  # Garantit 5 chiffres en ajoutant des zéros à gauche si nécessaire
        
        # Pour les codes avec tirets ou espaces, nettoyage simple
        postal_code = re.sub(r'[^\d\w]', '', postal_code)
        
        # Si après nettoyage on a 4 chiffres, ajouter un 0 au début
        if postal_code.isdigit() and len(postal_code) == 4:
            postal_code = '0' + postal_code
        
        return postal_code
    
    def is_likely_french_address(row) -> bool:
        """Détermine si une adresse est probablement française basée sur d'autres champs."""
        # Vérifier le numéro de TVA
        if not pd.isna(row.get('co_vat')) and isinstance(row['co_vat'], str) and row['co_vat'].startswith('FR'):
            return True
            
        # Vérifier si un SIREN ou SIRET est présent (identifiants français)
        if not pd.isna(row.get('co_siren')) or not pd.isna(row.get('co_siret')):
            return True
            
        # Vérifier si la ville contient des termes français courants
        if not pd.isna(row.get('co_head_office_city')) and isinstance(row['co_head_office_city'], str):
            city = row['co_head_office_city'].upper()
            french_terms = ["CEDEX", "SAINT", "SUR", "LES", "LA ", "LE ", "VILLE"]
            if any(term in city for term in french_terms):
                return True
                
        return False
    
    # Traitement de chaque ligne
    for idx, row in result_df.iterrows():
        postal_code = row['co_head_office_postal_code']
        co_id = row['co_id']
        
        # Ignorer les valeurs nulles/vides
        if pd.isna(postal_code) or postal_code == '':
            continue
        
        # Nettoyage du code postal
        original_postal_code = postal_code
        cleaned_postal_code = clean_postal_code(postal_code)
        
        # Si le nettoyage a modifié la valeur, enregistrer la modification
        if cleaned_postal_code != original_postal_code:
            result_df.at[idx, 'co_head_office_postal_code'] = cleaned_postal_code
            errors.append({
                "type": "postal_code_cleaning",
                "severity": "info",
                "co_id": co_id,
                "index": idx,
                "original": original_postal_code,
                "cleaned": cleaned_postal_code
            })
            
            # Mettre à jour la valeur pour la validation suivante
            postal_code = cleaned_postal_code
        
        # Si l'adresse est probablement française, appliquer les règles de validation françaises
        if is_likely_french_address(row):
            # Validation du format
            if not french_postal_code_pattern.match(postal_code):
                errors.append({
                    "type": "invalid_french_postal_code_format",
                    "severity": "error",
                    "co_id": co_id,
                    "index": idx,
                    "value": postal_code,
                    "reason": "Le code postal français doit contenir exactement 5 chiffres"
                })
                continue
            
            # Validation du département
            department = postal_code[:2]
            if department not in VALID_FRENCH_DEPARTMENTS and postal_code[:3] not in VALID_FRENCH_DEPARTMENTS:
                errors.append({
                    "type": "invalid_french_department",
                    "severity": "warning",
                    "co_id": co_id,
                    "index": idx,
                    "value": postal_code,
                    "department": department,
                    "reason": f"Le département '{department}' n'existe pas en France"
                })
        
        # Pour les adresses non françaises, vérifications spécifiques par pays pourraient être ajoutées ici
        # ...
    
    return result_df, errors