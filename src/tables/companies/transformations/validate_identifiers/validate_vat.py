"""
Module de validation des numéros de TVA pour les données Companies.
Vérifie la validité des numéros de TVA selon les règles françaises.
"""

import re
from typing import Dict, List, Tuple, Any, Optional

import pandas as pd


def validate_vat(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Valide et normalise les numéros de TVA des entreprises.
    
    Pour la France, le numéro de TVA intracommunautaire est composé de:
    - Le code pays 'FR'
    - Une clé de contrôle à 2 chiffres
    - Le numéro SIREN à 9 chiffres
    Format: FRKKXXXXXXXXX (13 caractères)
    
    Args:
        df: DataFrame contenant les données Companies
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les numéros de TVA validés et normalisés
        - La liste des erreurs de validation détectées
    """
    errors = []
    result_df = df.copy()
    
    # Vérifier si les colonnes nécessaires existent
    required_columns = ['co_vat', 'co_siren']
    for column in required_columns:
        if column not in result_df.columns:
            errors.append({
                "type": "missing_column",
                "severity": "error",
                "message": f"La colonne '{column}' est absente du DataFrame"
            })
            if column == 'co_vat':  # Sans TVA, impossible de continuer
                return result_df, errors
    
    # Expression régulière pour valider le format de TVA française
    vat_pattern = re.compile(r'^FR\d{11}$')
    
    def clean_vat(vat: Optional[str]) -> Optional[str]:
        """Nettoie un numéro de TVA en supprimant les caractères non conformes."""
        if pd.isna(vat) or not isinstance(vat, str):
            return vat
        
        # Suppression des espaces et autres caractères non alphanumériques sauf les lettres du code pays
        vat = vat.upper().strip()
        
        # Extraction du code pays (généralement 2 lettres) et des chiffres
        match = re.match(r'^([A-Z]{2})?(\d+)$', vat.replace(' ', ''))
        if match:
            country_code, numbers = match.groups()
            
            # Si pas de code pays détecté mais que les chiffres correspondent à un numéro français
            if not country_code and len(numbers) == 11:
                return f"FR{numbers}"
                
            # Si le code pays est FR, formater correctement
            if country_code == "FR":
                return f"FR{numbers}"
        
        # Si aucun motif reconnu, retourner tel quel
        return vat
    
    def validate_fr_vat_checksum(vat: str, siren: str) -> bool:
        """
        Valide la clé de contrôle d'un numéro de TVA français.
        
        La clé est calculée ainsi: (12 + 3 * (SIREN % 97)) % 97
        """
        if not vat.startswith("FR") or len(vat) != 13:
            return False
            
        # Extraction de la clé et du SIREN depuis le numéro de TVA
        key = int(vat[2:4])
        vat_siren = vat[4:]
        
        # Vérification de la cohérence avec le SIREN fourni
        if siren and vat_siren != siren:
            return False
            
        # Calcul de la clé attendue
        try:
            siren_value = int(vat_siren)
            expected_key = (12 + 3 * (siren_value % 97)) % 97
            return key == expected_key
        except (ValueError, TypeError):
            return False
    
    # Traitement de chaque ligne
    for idx, row in result_df.iterrows():
        vat = row['co_vat']
        siren = row['co_siren']
        co_id = row['co_id']
        
        # Ignorer les valeurs nulles/vides
        if pd.isna(vat) or vat == '':
            continue
        
        # Nettoyage du numéro de TVA
        original_vat = vat
        cleaned_vat = clean_vat(vat)
        
        # Si le nettoyage a modifié la valeur, enregistrer la modification
        if cleaned_vat != original_vat:
            result_df.at[idx, 'co_vat'] = cleaned_vat
            errors.append({
                "type": "vat_cleaning",
                "severity": "info",
                "co_id": co_id,
                "index": idx,
                "original": original_vat,
                "cleaned": cleaned_vat
            })
            
            # Mettre à jour la valeur pour la validation suivante
            vat = cleaned_vat
        
        # Si le numéro de TVA ne commence pas par FR, c'est peut-être une entreprise étrangère
        if not vat.startswith("FR"):
            errors.append({
                "type": "non_french_vat",
                "severity": "info",
                "co_id": co_id,
                "index": idx,
                "value": vat,
                "reason": "Numéro de TVA non français"
            })
            continue
            
        # Validation du format pour les TVA françaises
        if not vat_pattern.match(vat):
            errors.append({
                "type": "invalid_vat_format",
                "severity": "error",
                "co_id": co_id,
                "index": idx,
                "value": vat,
                "reason": "Le numéro de TVA français doit être au format FRKKXXXXXXXXX"
            })
            continue
        
        # Validation de la clé de contrôle et de la cohérence avec le SIREN
        if not pd.isna(siren) and siren:
            if not validate_fr_vat_checksum(vat, siren):
                errors.append({
                    "type": "invalid_vat_checksum",
                    "severity": "error",
                    "co_id": co_id,
                    "index": idx,
                    "vat": vat,
                    "siren": siren,
                    "reason": "La clé de contrôle du numéro de TVA est invalide ou ne correspond pas au SIREN"
                })
    
    return result_df, errors