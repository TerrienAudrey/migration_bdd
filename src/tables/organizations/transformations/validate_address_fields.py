"""
Module de validation des champs d'adresse pour les données Organizations.
Vérifie la validité des composants d'adresse.
"""

import re
from typing import Dict, List, Tuple, Any, Optional

import pandas as pd


def validate_address_fields(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Valide et normalise les champs d'adresse des organisations.
    
    Args:
        df: DataFrame contenant les données Organizations
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les champs d'adresse validés
        - La liste des erreurs de validation détectées
    """
    errors = []
    result_df = df.copy()
    
    # Liste des champs d'adresse à vérifier
    address_fields = [
        "or_house_number", 
        "or_street", 
        "or_postal_code", 
        "or_city", 
        "or_state", 
        "or_additional_address"
    ]
    
    # Vérifier que les colonnes existent
    missing_columns = [field for field in address_fields if field not in result_df.columns]
    if missing_columns:
        for field in missing_columns:
            errors.append({
                "type": "missing_column",
                "severity": "error",
                "message": f"La colonne '{field}' est absente du DataFrame"
            })
    
    # Expression régulière pour valider le format du code postal français
    postal_code_pattern = re.compile(r'^\d{5}$')
    
    # Expression régulière pour valider les villes et pays (lettres, tirets, apostrophes et espaces simples)
    name_pattern = re.compile(r"^[A-Za-zÀ-ÿ]+(-[A-Za-zÀ-ÿ]+)*([ ][A-Za-zÀ-ÿ]+(-[A-Za-zÀ-ÿ]+)*)*([''][A-Za-zÀ-ÿ]+)*$")
    
    # Patterns pour nettoyer les noms de ville avec arrondissements
    arrondissement_patterns = [
        (r'\s+\d+E\s+ARRONDISSEMENT$', ''),  # ex: "LYON 8E ARRONDISSEMENT" -> "LYON"
        (r'\s+\d+EME\s+ARRONDISSEMENT$', ''),  # ex: "LYON 8EME ARRONDISSEMENT" -> "LYON"
        (r'\s+\d+E$', ''),  # ex: "PARIS 14E" -> "PARIS"
        (r'\s+\d+ER$', ''),  # ex: "PARIS 1ER" -> "PARIS"
        (r'\s+\d+EME$', ''),  # ex: "PARIS 14EME" -> "PARIS"
        (r'\s+CEDEX.*$', '')  # ex: "RENNES CEDEX 9" -> "RENNES"
    ]
    
    # Traitement de chaque ligne
    for idx, row in result_df.iterrows():
        or_id = row['or_id']
        
        # Vérification du code postal
        if 'or_postal_code' in result_df.columns:
            postal_code = row['or_postal_code']
            if not pd.isna(postal_code) and postal_code != '':
                # Nettoyage du code postal
                postal_code = str(postal_code).strip()
                
                # Si le code postal a 4 chiffres, ajouter un 0 au début
                if postal_code.isdigit() and len(postal_code) == 4:
                    result_df.at[idx, 'or_postal_code'] = '0' + postal_code
                    errors.append({
                        "type": "postal_code_fixed",
                        "severity": "info",
                        "or_id": or_id,
                        "index": idx,
                        "original": postal_code,
                        "fixed": '0' + postal_code
                    })
                    postal_code = '0' + postal_code
                
                # Validation du format
                if not postal_code_pattern.match(postal_code):
                    errors.append({
                        "type": "invalid_postal_code",
                        "severity": "error",
                        "or_id": or_id,
                        "index": idx,
                        "value": postal_code,
                        "reason": "Le code postal doit contenir exactement 5 chiffres"
                    })
        
        # Vérification de or_house_number
        if 'or_house_number' in result_df.columns:
            house_number = row['or_house_number']
            if not pd.isna(house_number) and house_number != '':
                # Nettoyage du numéro
                cleaned_number = str(house_number).strip()
                if cleaned_number != house_number:
                    result_df.at[idx, 'or_house_number'] = cleaned_number
                    errors.append({
                        "type": "house_number_cleaned",
                        "severity": "info",
                        "or_id": or_id,
                        "index": idx,
                        "original": house_number,
                        "cleaned": cleaned_number
                    })
        
        # Vérification de or_street
        if 'or_street' in result_df.columns:
            street = row['or_street']
            if not pd.isna(street) and street != '':
                # Suppression des espaces en début et fin
                cleaned_street = str(street).strip()
                # Normalisation des espaces multiples en un seul espace
                cleaned_street = re.sub(r'\s+', ' ', cleaned_street)
                if cleaned_street != street:
                    result_df.at[idx, 'or_street'] = cleaned_street
                    errors.append({
                        "type": "street_cleaned",
                        "severity": "info",
                        "or_id": or_id,
                        "index": idx,
                        "original": street,
                        "cleaned": cleaned_street
                    })
        
        # Vérification de or_city (validation et nettoyage)
        if 'or_city' in result_df.columns:
            city = row['or_city']
            if not pd.isna(city) and city != '':
                # Suppression des espaces en début et fin
                cleaned_city = str(city).strip().upper()
                # Normalisation des espaces multiples en un seul espace
                cleaned_city = re.sub(r'\s+', ' ', cleaned_city)
                
                # Suppression des informations d'arrondissement
                original_cleaned = cleaned_city
                for pattern, replacement in arrondissement_patterns:
                    cleaned_city = re.sub(pattern, replacement, cleaned_city)
                
                # Si changement dû à la suppression d'arrondissement, enregistrer
                if original_cleaned != cleaned_city:
                    errors.append({
                        "type": "city_arrondissement_removed",
                        "severity": "info",
                        "or_id": or_id,
                        "index": idx,
                        "original": original_cleaned,
                        "cleaned": cleaned_city
                    })
                
                if cleaned_city != city:
                    result_df.at[idx, 'or_city'] = cleaned_city
                    errors.append({
                        "type": "city_cleaned",
                        "severity": "info",
                        "or_id": or_id,
                        "index": idx,
                        "original": city,
                        "cleaned": cleaned_city
                    })
        
        # Vérification de or_state (validation lettres et format)
        if 'or_state' in result_df.columns:
            state = row['or_state']
            if not pd.isna(state) and state != '':
                # Suppression des espaces en début et fin
                cleaned_state = str(state).strip().upper()
                # Normalisation des espaces multiples en un seul espace
                cleaned_state = re.sub(r'\s+', ' ', cleaned_state)
                
                if cleaned_state != state:
                    result_df.at[idx, 'or_state'] = cleaned_state
                    errors.append({
                        "type": "state_cleaned",
                        "severity": "info",
                        "or_id": or_id,
                        "index": idx,
                        "original": state,
                        "cleaned": cleaned_state
                    })
    
    return result_df, errors