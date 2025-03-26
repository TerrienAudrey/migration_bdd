"""
Module de validation des relations entre les différents identifiants d'entreprise.
Vérifie la cohérence entre SIREN, SIRET et numéro de TVA.
"""

from typing import Dict, List, Tuple, Any

import pandas as pd


def validate_id_relationships(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Vérifie la cohérence entre les différents identifiants d'entreprise.
    
    Vérifications effectuées:
    - Le SIRET doit commencer par le SIREN (SIRET = SIREN + 5 chiffres)
    - Le numéro de TVA français doit contenir le SIREN
    - Gestion des exceptions pour les entreprises étrangères
    
    Args:
        df: DataFrame contenant les données Companies
        
    Returns:
        Tuple contenant:
        - Le DataFrame original (pas de modifications dans cette fonction)
        - La liste des erreurs de cohérence détectées
    """
    errors = []
    
    # Vérifier si les colonnes nécessaires existent
    required_columns = ['co_siren', 'co_siret', 'co_vat']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        for col in missing_columns:
            errors.append({
                "type": "missing_column",
                "severity": "error",
                "message": f"La colonne '{col}' est absente du DataFrame"
            })
        return df, errors
    
    # Traitement de chaque ligne pour vérifier la cohérence des identifiants
    for idx, row in df.iterrows():
        co_id = row['co_id']
        siren = row['co_siren']
        siret = row['co_siret']
        vat = row['co_vat']
        
        # Ignorer les lignes sans identifiants
        if pd.isna(siren) and pd.isna(siret) and pd.isna(vat):
            continue
        
        # 1. Vérification de la relation SIREN-SIRET
        if not pd.isna(siren) and not pd.isna(siret):
            if len(siret) == 14 and len(siren) == 9:
                if siret[:9] != siren:
                    errors.append({
                        "type": "siren_siret_mismatch",
                        "severity": "error",
                        "co_id": co_id,
                        "index": idx,
                        "siren": siren,
                        "siret": siret,
                        "reason": "Le SIRET doit commencer par le SIREN"
                    })
        
        # 2. Vérification de la relation SIREN-TVA pour les entreprises françaises
        if not pd.isna(siren) and not pd.isna(vat) and vat.startswith("FR"):
            if len(vat) == 13 and len(siren) == 9:
                vat_siren = vat[4:]  # Extraction du SIREN depuis le numéro de TVA
                if vat_siren != siren:
                    errors.append({
                        "type": "siren_vat_mismatch",
                        "severity": "error",
                        "co_id": co_id,
                        "index": idx,
                        "siren": siren,
                        "vat": vat,
                        "vat_siren": vat_siren,
                        "reason": "Le SIREN intégré dans le numéro de TVA ne correspond pas au SIREN déclaré"
                    })
        
        # 3. Vérification de la présence d'identifiants partiels
        # Si un SIRET est présent mais pas de SIREN
        if not pd.isna(siret) and pd.isna(siren):
            if len(siret) == 14:
                extracted_siren = siret[:9]
                errors.append({
                    "type": "missing_siren_with_siret",
                    "severity": "warning",
                    "co_id": co_id,
                    "index": idx,
                    "siret": siret,
                    "extracted_siren": extracted_siren,
                    "reason": "SIREN manquant alors que le SIRET est présent"
                })
        
        # 4. Vérification pour les entreprises avec TVA française mais sans SIREN
        if not pd.isna(vat) and pd.isna(siren) and vat.startswith("FR"):
            if len(vat) == 13:
                extracted_siren = vat[4:]
                errors.append({
                    "type": "missing_siren_with_fr_vat",
                    "severity": "warning",
                    "co_id": co_id,
                    "index": idx,
                    "vat": vat,
                    "extracted_siren": extracted_siren,
                    "reason": "SIREN manquant alors que le numéro de TVA français est présent"
                })
        
        # 5. Détection d'incohérences potentielles pour les entreprises étrangères
        has_foreign_vat = not pd.isna(vat) and not vat.startswith("FR")
        has_french_ids = (not pd.isna(siren) or not pd.isna(siret))
        
        if has_foreign_vat and has_french_ids:
            errors.append({
                "type": "inconsistent_nationality",
                "severity": "warning",
                "co_id": co_id,
                "index": idx,
                "vat": vat,
                "siren": siren,
                "siret": siret,
                "reason": "Numéro de TVA étranger mais identifiants français (SIREN/SIRET) présents"
            })
    
    return df, errors