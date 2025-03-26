"""
Module de validation des codes postaux pour les données logistic_address.
Vérifie la validité des codes postaux selon les règles françaises.
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
    Valide et normalise les codes postaux des adresses.
    
    Pour la France:
    - Format: 5 chiffres
    - Les 2 premiers chiffres doivent correspondre à un département valide
    
    Args:
        df: DataFrame contenant les données logistic_address
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les codes postaux validés et normalisés
        - La liste des erreurs de validation détectées
    """
    errors = []
    result_df = df.copy()
    
    # Vérifier si la colonne du code postal existe
    if 'la_postal_code' not in result_df.columns:
        errors.append({
            "type": "missing_column",
            "severity": "error",
            "message": "La colonne 'la_postal_code' est absente du DataFrame"
        })
        return result_df, errors
    
    # Expression régulière pour valider le format du code postal français
    french_postal_code_pattern = re.compile(r'^\d{5}$')
    
    def clean_postal_code(postal_code: Optional[str]) -> Optional[str]:
        """Nettoie un code postal en supprimant les caractères non conformes."""
        if pd.isna(postal_code) or postal_code == '':
            return postal_code
        
        if not isinstance(postal_code, str):
            postal_code = str(postal_code)
        
        # Suppression des espaces
        postal_code = postal_code.strip()
        
        # Suppression des caractères non numériques
        postal_code = re.sub(r'[^0-9]', '', postal_code)
        
        # Si après nettoyage la chaîne est vide, retourner None
        if not postal_code:
            return None
        
        # Pour les codes postaux français, extraction des chiffres
        if len(postal_code) == 4:
            # Si le code a 4 chiffres, ajouter un 0 au début
            return '0' + postal_code
        
        # Si le code n'a pas 5 chiffres après nettoyage, c'est probablement invalide
        if len(postal_code) != 5:
            return postal_code  # On retourne tel quel pour la journalisation
        
        return postal_code
    
    # Traitement de chaque ligne
    for idx, row in result_df.iterrows():
        postal_code = row['la_postal_code']
        la_id = row['la_id']
        
        # Ignorer les valeurs nulles/vides
        if pd.isna(postal_code) or postal_code == '':
            continue
        
        # Nettoyage du code postal
        original_postal_code = postal_code
        cleaned_postal_code = clean_postal_code(postal_code)
        
        # Si le nettoyage a modifié la valeur, enregistrer la modification
        if cleaned_postal_code != original_postal_code:
            result_df.at[idx, 'la_postal_code'] = cleaned_postal_code
            errors.append({
                "type": "postal_code_cleaning",
                "severity": "info",
                "la_id": la_id,
                "index": idx,
                "original": original_postal_code,
                "cleaned": cleaned_postal_code
            })
            
            # Mettre à jour la valeur pour la validation suivante
            postal_code = cleaned_postal_code
        
        # Si le code postal est None après nettoyage, passer à la ligne suivante
        if postal_code is None:
            continue
        
        # Validation du format
        if not french_postal_code_pattern.match(postal_code):
            errors.append({
                "type": "invalid_postal_code_format",
                "severity": "error",
                "la_id": la_id,
                "index": idx,
                "value": postal_code,
                "reason": "Le code postal doit contenir exactement 5 chiffres"
            })
            continue
        
        # Validation du département
        department = postal_code[:2]
        if department not in VALID_FRENCH_DEPARTMENTS and postal_code[:3] not in VALID_FRENCH_DEPARTMENTS:
            errors.append({
                "type": "invalid_department",
                "severity": "warning",
                "la_id": la_id,
                "index": idx,
                "value": postal_code,
                "department": department,
                "reason": f"Le département '{department}' n'existe pas en France"
            })
    
    return result_df, errors