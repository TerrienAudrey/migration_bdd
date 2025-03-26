"""
Module de validation des champs d'adresse pour les données logistic_address.
Vérifie la validité des composants d'adresse.
"""

import re
from typing import Dict, List, Tuple, Any, Optional

import pandas as pd


def validate_address_fields(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Valide et normalise les champs d'adresse.
    
    Vérifications:
    - Formats des numéros de rue
    - Formats des noms de rue
    - Informations redondantes entre les champs
    
    Args:
        df: DataFrame contenant les données logistic_address
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les champs d'adresse validés
        - La liste des erreurs de validation détectées
    """
    errors = []
    result_df = df.copy()
    
    # Liste des champs d'adresse à vérifier
    address_fields = [
        "la_house_number", 
        "la_street", 
        "la_additional_address"
    ]
    
    # Patterns de validation
    house_number_pattern = re.compile(r'^[\d]+[A-Za-z]?$')
    street_name_pattern = re.compile(r'^[A-Za-zÀ-ÿ\s\-\'\.]+$')
    
    # Vérifier que les colonnes existent
    missing_columns = [field for field in address_fields if field not in result_df.columns]
    if missing_columns:
        for field in missing_columns:
            errors.append({
                "type": "missing_column",
                "severity": "error",
                "message": f"La colonne '{field}' est absente du DataFrame"
            })
            
    # Traitement de chaque ligne
    for idx, row in result_df.iterrows():
        la_id = row['la_id']
        
        # Validation du numéro de rue
        if 'la_house_number' in row and not pd.isna(row['la_house_number']) and row['la_house_number'] != '':
            house_number = str(row['la_house_number']).strip()
            
            # Suppression des espaces et caractères non alphanumériques
            cleaned_number = re.sub(r'[^A-Za-z0-9]', '', house_number)
            
            if cleaned_number != house_number:
                result_df.at[idx, 'la_house_number'] = cleaned_number
                errors.append({
                    "type": "house_number_cleaned",
                    "severity": "info",
                    "la_id": la_id,
                    "index": idx,
                    "original": house_number,
                    "cleaned": cleaned_number
                })
                house_number = cleaned_number
            
            # Vérification du format
            if not house_number_pattern.match(house_number):
                errors.append({
                    "type": "invalid_house_number_format",
                    "severity": "warning",
                    "la_id": la_id,
                    "index": idx,
                    "value": house_number,
                    "reason": "Le numéro de rue doit contenir uniquement des chiffres suivis éventuellement d'une lettre"
                })
        
        # Validation du nom de rue
        if 'la_street' in row and not pd.isna(row['la_street']) and row['la_street'] != '':
            street_name = str(row['la_street']).strip()
            
            # Vérification du format
            if not street_name_pattern.match(street_name):
                errors.append({
                    "type": "invalid_street_name_format",
                    "severity": "warning",
                    "la_id": la_id,
                    "index": idx,
                    "value": street_name,
                    "reason": "Le nom de rue doit contenir uniquement des lettres, espaces, tirets, apostrophes et points"
                })
            
            # Vérification que le nom de rue ne commence pas par un numéro (indicateur d'erreur de format)
            if re.match(r'^\d+\s', street_name):
                errors.append({
                    "type": "street_starts_with_number",
                    "severity": "warning",
                    "la_id": la_id,
                    "index": idx,
                    "value": street_name,
                    "reason": "Le nom de rue commence par un numéro, cela peut indiquer que le numéro n'a pas été correctement extrait"
                })
        
        # Validation du complément d'adresse (ne devrait pas contenir d'informations d'adresse principale)
        if 'la_additional_address' in row and not pd.isna(row['la_additional_address']) and row['la_additional_address'] != '':
            additional = str(row['la_additional_address']).strip()
            
            # Vérifier si le complément semble contenir une adresse complète
            contains_postal_code = re.search(r'\b\d{5}\b', additional) is not None
            
            if contains_postal_code:
                errors.append({
                    "type": "postal_code_in_additional_address",
                    "severity": "warning",
                    "la_id": la_id,
                    "index": idx,
                    "value": additional,
                    "reason": "Le complément d'adresse contient un code postal, ce qui suggère une information d'adresse principale"
                })
            
            # Vérifier les mots-clés d'adresse principale dans le complément
            address_keywords = ["rue", "avenue", "boulevard", "allée", "place", "cours", "chemin", "impasse"]
            if any(keyword in additional.lower() for keyword in address_keywords):
                # Présence d'un mot-clé d'adresse principale
                # Vérifier si le même mot-clé apparaît dans le champ la_street pour éviter les faux positifs
                street_content = row.get('la_street', '')
                if not pd.isna(street_content):
                    # Si aucun mot-clé commun, c'est probablement une adresse mal placée
                    if not any(keyword in street_content.lower() for keyword in address_keywords):
                        errors.append({
                            "type": "address_keywords_in_additional",
                            "severity": "warning",
                            "la_id": la_id,
                            "index": idx,
                            "value": additional,
                            "reason": "Le complément d'adresse contient des mots-clés d'adresse principale"
                        })
    
    return result_df, errors