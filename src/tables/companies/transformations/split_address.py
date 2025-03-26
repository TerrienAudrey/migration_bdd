"""
Module de traitement et décomposition des adresses pour les données Companies.
Extrait le numéro et le nom de rue à partir de l'adresse complète.
"""

import re
from typing import Dict, List, Tuple, Any, Optional

import pandas as pd


def split_address(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Analyse et décompose les adresses pour en extraire le numéro et le nom de rue.
    
    Le traitement inclut:
    - Extraction du numéro de rue
    - Extraction du nom de rue
    - Gestion des formats d'adresse variés
    - Prise en compte des cas particuliers (BP, CEDEX, etc.)
    
    Args:
        df: DataFrame contenant les données Companies
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les nouveaux champs d'adresse décomposée
        - La liste des erreurs/informations de traitement
    """
    errors = []
    result_df = df.copy()
    
    # Vérifier si la colonne d'adresse existe
    if 'co_head_office_address' not in result_df.columns:
        errors.append({
            "type": "missing_column",
            "severity": "error",
            "message": "La colonne 'co_head_office_address' est absente du DataFrame"
        })
        return result_df, errors
    
    # Ajouter les colonnes pour le numéro et le nom de rue s'ils n'existent pas
    if 'co_head_office_number' not in result_df.columns:
        result_df['co_head_office_number'] = ""  # Chaîne vide au lieu de None
    if 'co_head_office_street' not in result_df.columns:
        result_df['co_head_office_street'] = None
    
    # Patterns pour l'extraction d'adresses
    # Pattern pour les adresses standards avec numéro au début
    standard_address_pattern = re.compile(
        r'^(\d+[\s,.-]*)?(BIS|TER|QUATER|A|B|C|D)?[\s,.-]*(.+)$',
        re.IGNORECASE
    )
    
    # Pattern pour les BP, CS, CEDEX, etc.
    special_address_pattern = re.compile(
        r'^(BP|CS|TSA|CEDEX)[\s,.-]*(\d+)[\s,.-]*(.*)$',
        re.IGNORECASE
    )
    
    def extract_address_components(address: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extrait le numéro et le nom de rue d'une adresse.
        
        Règles spécifiques:
        - Si une lettre suit directement un nombre (ex: "11A"), elle fait partie du numéro
        - Si une lettre est séparée du nombre par un espace, mais suivie d'un autre espace (ex: "11 A "), 
        elle fait partie du numéro
        - Si une lettre est séparée du nombre par un espace et suivie directement d'un texte (ex: "11 Arue"), 
        elle fait partie du nom de rue
        
        Args:
            address: L'adresse complète
            
        Returns:
            Tuple contenant le numéro de rue et le nom de rue
        """
        if not address:
            return None, None
            
        # Vérifier si c'est une adresse spéciale (BP, CEDEX, etc.)
        special_match = special_address_pattern.match(address)
        if special_match:
            prefix, number, rest = special_match.groups()
            street_number = f"{prefix} {number}".strip()
            street_name = rest.strip() if rest else None
            return street_number, street_name
        
        # Pattern révisé pour adresses standards - capture les formats comme:
        # "11A" (numéro+lettre directement accolée)
        # "11 A" (numéro+espace+lettre+espace)
        # "11 Arue" (numéro+espace+lettre suivie de texte sans espace)
        revised_pattern = re.compile(
            r'^(\d+)(?:([A-Za-z])(?=\s|$)|\s+([A-Za-z])\s+|\s+)(.*)$'
        )
        
        match = revised_pattern.match(address)
        if match:
            number, letter_attached, letter_with_spaces, street = match.groups()
            
            # Construction du numéro de rue
            if letter_attached:  # Cas "11A"
                street_number = f"{number}{letter_attached}"
            elif letter_with_spaces:  # Cas "11 A "
                street_number = f"{number}{letter_with_spaces}"
            else:  # Cas "11" ou "11 Arue"
                street_number = number
            
            # Construction du nom de rue
            if street:
                # Si on a eu le cas "11 A ", la lettre est déjà prise en compte dans le numéro
                if letter_with_spaces:
                    street_name = street.strip()
                # Si pas de lettre accolée ni avec espace, utiliser street directement
                elif not letter_attached:
                    street_name = street.strip()
                # Si une lettre accolée (11A), utiliser street directement
                else:
                    street_name = street.strip()
            else:
                street_name = None
            
            return street_number.strip(), street_name
        
        # Si le format ne correspond à aucun pattern, tout est considéré comme nom de rue
        return None, address.strip()
    
    # Traitement de chaque ligne
    for idx, row in result_df.iterrows():
        address = row['co_head_office_address']
        co_id = row['co_id']
        
        # Ignorer les valeurs nulles/vides
        if pd.isna(address) or address == '':
            continue
        
        try:
            # Extraction des composants de l'adresse SANS considérer l'adresse complémentaire
            number, street = extract_address_components(address)
            
            # Mise à jour des colonnes - ne pas toucher à co_head_office_additional_address
            result_df.at[idx, 'co_head_office_number'] = number or ""
            result_df.at[idx, 'co_head_office_street'] = street
            
            # Enregistrement de l'information de décomposition
            errors.append({
                "type": "address_splitting",
                "severity": "info",
                "co_id": co_id,
                "index": idx,
                "original_address": address,
                "number": number,
                "street": street
            })
            
        except Exception as e:
            # Enregistrement de l'erreur en cas d'échec de décomposition
            errors.append({
                "type": "address_splitting_error",
                "severity": "error",
                "co_id": co_id,
                "index": idx,
                "original_address": address,
                "error_message": str(e)
            })

    # Avant de retourner le DataFrame, garantir que le numéro est toujours une chaîne
    result_df['co_head_office_number'] = result_df['co_head_office_number'].fillna("")

    return result_df, errors

def fix_address_split_issues(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Corrige les problèmes spécifiques liés à la décomposition d'adresse.
    Cette fonction s'assure que l'adresse complémentaire n'est pas mélangée
    avec l'adresse principale dans le champ co_head_office_street.
    """
    errors = []
    result_df = df.copy()
    
    # Identifier les entrées où co_head_office_street contient l'adresse complémentaire
    for idx, row in result_df.iterrows():
        street = row.get('co_head_office_street', '')
        additional = row.get('co_head_office_additional_address', '')
        
        if not pd.isna(street) and not pd.isna(additional) and additional != "" and additional in street:
            # L'adresse complémentaire est incluse dans le nom de rue
            # Extraire uniquement la partie qui correspond vraiment au nom de rue
            corrected_street = street.replace(additional + ", ", "").replace(", " + additional, "")
            corrected_street = corrected_street.replace(additional, "") if corrected_street == additional else corrected_street
            
            # Appliquer la correction
            result_df.at[idx, 'co_head_office_street'] = corrected_street
            
            errors.append({
                "type": "address_split_fixed",
                "severity": "info",
                "co_id": row.get('co_id'),
                "index": idx,
                "original_street": street,
                "corrected_street": corrected_street,
                "additional_address": additional
            })
    
    return result_df, errors