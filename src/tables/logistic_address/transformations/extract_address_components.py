"""
Module d'extraction des composants d'adresse pour les données logistic_address.
Décompose les adresses complètes en leurs composants (numéro, rue, code postal, ville).

Ce module gère de nombreux cas complexes d'adresses:
- Adresses standard (87 RUE DE LA COMMANDERIE, 59500 DOUAI)
- Zones d'activités (ZAC DES MURONS 466 RUE JACQUELINE AURIOL)
- Entreprises avec adresse (LOGISTICS OPERATIONS, 41 RUE MERCIER)
- Codes postaux espacés (RUE DE LA LEAVDE 38 170 SEYSSINET)
- Zones industrielles sans numéro (ZI-DES VAUGUILLETTES RUE DES CHAMPS PLUVIERS)
"""

import re
from typing import Dict, List, Tuple, Any, Optional, Union

import pandas as pd


def extract_address_components(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Analyse et décompose les adresses pour en extraire les composants.
    
    Le traitement inclut:
    - Détection des adresses complètes dans un seul champ
    - Extraction du numéro, de la rue, du code postal et de la ville
    - Traitement des différents formats d'adresse
    - Normalisation des champs (remplacement des NULL par chaînes vides)
    - Suppression des valeurs "0", ".", "/" dans les champs textuels
    
    Args:
        df: DataFrame contenant les données logistic_address
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les composants d'adresse extraits et normalisés
        - La liste des erreurs/informations de traitement
    """
    errors = []
    result_df = df.copy()
    
    # Liste des champs textuels d'adresse à normaliser
    address_fields = ["la_house_number", "la_street", "la_additional_address", "la_postal_code", "la_city"]
    
    # Patterns pour extraction des composants d'adresse
    # Code postal standard ou avec espace (ex: "59500" ou "38 170")
    postal_code_pattern = re.compile(r'(\d{2,3})\s*(\d{2,3})')
    
    # Pattern pour extraire numéro de rue et rue
    street_number_pattern = re.compile(r'^(\d+[\s]?[a-zA-Z]?)\s+(.+)$')
    
    # Pattern pour reconnaissance des types de voies courantes
    street_type_pattern = re.compile(
        r'\b(rue|avenue|boulevard|all[ée]e|place|cours|chemin|impasse|quai|square|route|sentier|passage)\b',
        re.IGNORECASE
    )
    
    # Pattern pour les zones d'activité et zones industrielles
    business_zone_pattern = re.compile(
        r'^(ZA|ZAC|ZI|ZONE\s+[A-Za-z\'\s-]+|PARC\s+[A-Za-z\'\s-]+)[\s-]*(.*?)(?:(\d+[\s]?[a-zA-Z]?)\s+)?(.+)$',
        re.IGNORECASE
    )
    
    # Pattern pour les entreprises suivies d'une adresse
    company_pattern = re.compile(
        r'^([^,]+),\s*(.+)$'
    )
    
    # Fonction de nettoyage des valeurs à remplacer par des chaînes vides
    def clean_empty_values(value: Any) -> str:
        """
        Remplace les valeurs NULL, "0", ".", "/" par des chaînes vides.
        
        Args:
            value: Valeur à nettoyer
            
        Returns:
            Valeur nettoyée ou chaîne vide
        """
        if pd.isna(value):
            return ""
        
        if not isinstance(value, str):
            value = str(value)
            
        if value in ["0", ".", "/", "-"]:
            return ""
            
        return value
    
    def normalize_postal_code(postal_code: str) -> str:
        """
        Normalise un code postal qui peut être espacé.
        
        Args:
            postal_code: Code postal à normaliser (ex: "38 170")
            
        Returns:
            Code postal normalisé (ex: "38170")
        """
        if not postal_code or pd.isna(postal_code):
            return ""
            
        # Vérifier si c'est un code postal espacé
        match = postal_code_pattern.search(postal_code)
        if match:
            # Reconstituer le code postal sans espaces
            return f"{match.group(1)}{match.group(2)}"
            
        # Retirer tous les espaces du code postal
        return re.sub(r'\s+', '', postal_code)
    
    def parse_street_with_number(street_value: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extrait le numéro de rue et le nom de rue d'une chaîne.
        
        Args:
            street_value: Texte pouvant contenir un numéro suivi d'un nom de rue
            
        Returns:
            Tuple (numéro, rue) ou (None, None) si non détecté
        """
        if pd.isna(street_value) or not isinstance(street_value, str):
            return None, None
            
        street_value = street_value.strip()
        match = street_number_pattern.match(street_value)
        
        if match:
            number, street = match.groups()
            return number.strip(), street.strip()
        
        # Tenter d'extraire un numéro suivi d'un type de voie connu
        number_match = re.search(r'^(\d+[a-zA-Z]?)', street_value)
        if number_match:
            number = number_match.group(1)
            street_type_match = street_type_pattern.search(street_value)
            if street_type_match:
                # Numéro trouvé et type de voie détecté
                street = street_value[len(number):].strip()
                return number.strip(), street
                
        return None, street_value
    
    def extract_components_from_address(address: str) -> Dict[str, Optional[str]]:
        """
        Extrait tous les composants possibles d'une adresse complète.
        
        Args:
            address: Adresse complète à décomposer
            
        Returns:
            Dictionnaire des composants (house_number, street, postal_code, city, additional_address)
        """
        components = {
            "house_number": None,
            "street": None, 
            "postal_code": None,
            "city": None,
            "additional_address": None
        }
        
        if pd.isna(address) or not isinstance(address, str):
            return components
            
        address = address.strip()
        
        # Cas 1: Entreprise suivie d'une adresse (LOGISTICS OPERATIONS, 41 RUE MERCIER)
        company_match = company_pattern.match(address)
        if company_match:
            company_name, address_part = company_match.groups()
            
            # Vérifier que la partie entreprise ne ressemble pas à une adresse
            if not re.search(r'\b\d+\b', company_name) and not street_type_pattern.search(company_name.lower()):
                components["additional_address"] = company_name.strip()
                
                # Traiter le reste de l'adresse récursivement
                remaining_components = extract_components_from_address(address_part)
                for key, value in remaining_components.items():
                    if key != "additional_address" or not components["additional_address"]:
                        components[key] = value
                
                return components
                
        # Cas 2: Zone d'activité suivie d'une adresse (ZAC DES MURONS 466 RUE JACQUELINE AURIOL)
        business_zone_match = business_zone_pattern.match(address)
        if business_zone_match:
            zone_prefix, zone_details, house_number, street = business_zone_match.groups()
            
            # Nettoyer les composants
            zone_prefix = zone_prefix.strip()
            zone_details = zone_details.strip() if zone_details else ""
            house_number = house_number.strip() if house_number else None
            street = street.strip()
            
            # Former l'information additionnelle depuis la zone d'activité
            additional_info = zone_prefix
            if zone_details and zone_details != '-':
                additional_info = f"{zone_prefix} {zone_details}".strip()
            
            components["house_number"] = house_number
            components["street"] = street
            components["additional_address"] = additional_info
            
            # Extraire éventuellement ville et code postal de la partie rue
            postal_match = postal_code_pattern.search(street)
            if postal_match:
                parts = re.split(postal_code_pattern, street)
                if len(parts) >= 3:
                    street = parts[0].strip()
                    postal_code = f"{postal_match.group(1)}{postal_match.group(2)}"
                    city = parts[-1].strip()
                    
                    components["street"] = street
                    components["postal_code"] = postal_code
                    components["city"] = city
            
            return components
        
        # Cas 3: Extraire code postal et ville s'ils sont présents
        postal_match = postal_code_pattern.search(address)
        if postal_match:
            postal_code = f"{postal_match.group(1)}{postal_match.group(2)}"
            
            # Séparer l'adresse en parties avant/après le code postal
            parts = re.split(postal_code_pattern, address)
            if len(parts) >= 3:
                before_postal = parts[0].strip()
                after_postal = parts[-1].strip()
                
                # Nettoyer les séparateurs
                before_postal = re.sub(r'[,;.\s]+$', '', before_postal).strip()
                after_postal = re.sub(r'^[,;.\s]+', '', after_postal).strip()
                
                # La partie après est probablement la ville
                components["postal_code"] = postal_code
                components["city"] = after_postal if after_postal else None
                
                # Analyser la partie avant pour extraire numéro et rue
                house_number, street = parse_street_with_number(before_postal)
                components["house_number"] = house_number
                components["street"] = street
                
                return components
        
        # Cas 4: Si on arrive ici, c'est une adresse simple sans code postal/ville
        # Tentons d'extraire au moins le numéro et la rue
        house_number, street = parse_street_with_number(address)
        components["house_number"] = house_number
        components["street"] = street
        
        return components
    
    def clean_redundant_address_info(house_number: str, additional_address: str) -> str:
        """
        Nettoie les informations redondantes dans le champ additional_address.
        
        Args:
            house_number: Le numéro de rue déjà identifié
            additional_address: Le complément d'adresse à nettoyer
            
        Returns:
            Le complément d'adresse nettoyé
        """
        if pd.isna(additional_address) or not isinstance(additional_address, str) or not house_number:
            return additional_address
            
        # Chercher des mentions du numéro dans le complément d'adresse
        if house_number in additional_address:
            # Patterns courants à supprimer
            patterns = [
                f"{house_number}[/-]\\d+",  # Ex: "199-201"
                f"{house_number}/\\d+",      # Ex: "199/201"
                f"{house_number} ?[-/] ?\\d+",  # Ex: "199 - 201"
                f"{house_number}"           # Le numéro lui-même
            ]
            
            cleaned_address = additional_address
            for pattern in patterns:
                cleaned_address = re.sub(pattern, "", cleaned_address)
            
            # Nettoyer les espaces multiples résultants
            cleaned_address = re.sub(r'\s+', ' ', cleaned_address).strip()
            
            if cleaned_address != additional_address:
                return cleaned_address
        
        # Rechercher des motifs de type "numéro + type de voie"
        street_type_match = street_type_pattern.search(additional_address.lower())
        if street_type_match:
            # Trouver si un numéro précède le type de voie
            street_type_pos = street_type_match.start()
            before_type = additional_address[:street_type_pos].strip()
            
            number_match = re.search(r'(\d+[a-zA-Z]?)\s*$', before_type)
            if number_match:
                # Trouver la fin de ce segment (jusqu'au prochain point, virgule ou fin de chaîne)
                segment_start = additional_address.find(number_match.group(1))
                
                end_markers = [',', '.', ';']
                segment_end = len(additional_address)
                
                for marker in end_markers:
                    marker_pos = additional_address.find(marker, street_type_pos)
                    if marker_pos > 0 and marker_pos < segment_end:
                        segment_end = marker_pos
                
                # Segment à supprimer (numéro + type de voie + suite)
                if segment_end > segment_start:
                    segment_to_remove = additional_address[segment_start:segment_end]
                    cleaned_address = additional_address.replace(segment_to_remove, '')
                    cleaned_address = re.sub(r'\s+', ' ', cleaned_address).strip()
                    cleaned_address = re.sub(r'^[,;.\s]+|[,;.\s]+$', '', cleaned_address).strip()
                    
                    if cleaned_address != additional_address and cleaned_address != '':
                        return cleaned_address
                
        return additional_address
    
    # Traitement principal: parcourir chaque ligne du DataFrame
    for idx, row in result_df.iterrows():
        la_id = row['la_id']
        
        # Étape 1: Vérifier si certains champs d'adresse sont vides
        has_house_number = not (pd.isna(row.get('la_house_number', '')) or row.get('la_house_number', '') == '')
        has_populated_street = not (pd.isna(row.get('la_street', '')) or row.get('la_street', '') == '')
        has_postal_code = not (pd.isna(row.get('la_postal_code', '')) or row.get('la_postal_code', '') == '')
        has_city = not (pd.isna(row.get('la_city', '')) or row.get('la_city', '') == '')
        
        # Étape 2: Extraire les informations d'adresse de la_street si elle existe
        if has_populated_street:
            street_value = row['la_street']
            
            # Vérifier si la_street contient un code postal (adresse complète potentielle)
            contains_postal_code = postal_code_pattern.search(street_value) is not None
            
            # Vérifier d'abord si c'est une entreprise avec adresse (ex: LOGISTICS OPERATIONS, ...)
            company_match = company_pattern.match(street_value)
            if company_match:
                company_name, address_part = company_match.groups()
                
                # Vérifier que la partie entreprise ne ressemble pas à une adresse
                if not re.search(r'\b\d+\b', company_name) and not street_type_pattern.search(company_name.lower()):
                    # Extraire les composants de la partie adresse
                    components = extract_components_from_address(address_part)
                    
                    # Mettre à jour la rue et le numéro
                    if components["house_number"] and not has_house_number:
                        result_df.at[idx, 'la_house_number'] = components["house_number"]
                    
                    if components["street"]:
                        result_df.at[idx, 'la_street'] = components["street"]
                    
                    if components["postal_code"] and not has_postal_code:
                        result_df.at[idx, 'la_postal_code'] = components["postal_code"]
                    
                    if components["city"] and not has_city:
                        result_df.at[idx, 'la_city'] = components["city"]
                    
                    # Ajouter le nom de l'entreprise au complément d'adresse
                    existing_additional = row.get('la_additional_address', '')
                    if pd.isna(existing_additional) or existing_additional == '':
                        result_df.at[idx, 'la_additional_address'] = company_name.strip()
                    else:
                        # Si un complément existe déjà, concaténer
                        result_df.at[idx, 'la_additional_address'] = f"{company_name.strip()} - {existing_additional}"
                    
                    errors.append({
                        "type": "company_name_extracted",
                        "severity": "info",
                        "la_id": la_id,
                        "index": idx,
                        "original_street": street_value,
                        "company_name": company_name.strip(),
                        "address_part": address_part
                    })
                    
                    continue  # Passer à l'entrée suivante
            
            # Vérifier si c'est une zone d'activité avec adresse (ZAC, ZA, ZI)
            business_zone_match = business_zone_pattern.match(street_value)
            if business_zone_match:
                components = extract_components_from_address(street_value)
                
                # Mise à jour de la rue et du numéro
                if components["house_number"] and not has_house_number:
                    result_df.at[idx, 'la_house_number'] = components["house_number"]
                
                if components["street"]:
                    result_df.at[idx, 'la_street'] = components["street"]
                
                if components["postal_code"] and not has_postal_code:
                    result_df.at[idx, 'la_postal_code'] = components["postal_code"]
                
                if components["city"] and not has_city:
                    result_df.at[idx, 'la_city'] = components["city"]
                
                # Ajouter ou concaténer le complément d'adresse
                if components["additional_address"]:
                    existing_additional = row.get('la_additional_address', '')
                    if pd.isna(existing_additional) or existing_additional == '':
                        result_df.at[idx, 'la_additional_address'] = components["additional_address"]
                    else:
                        # Si un complément existe déjà, concaténer
                        result_df.at[idx, 'la_additional_address'] = f"{components['additional_address']} - {existing_additional}"
                
                errors.append({
                    "type": "business_zone_extracted",
                    "severity": "info",
                    "la_id": la_id,
                    "index": idx,
                    "original_street": street_value,
                    "extracted_components": components
                })
                
                continue  # Passer à l'entrée suivante
            
            # Si tous les champs sont déjà remplis mais la rue contient un code postal,
            # nous devons extraire uniquement la partie rue
            if has_house_number and has_postal_code and has_city and contains_postal_code:
                # Extraire la partie rue de l'adresse complète sans modifier les autres champs
                components = extract_components_from_address(street_value)
                if components["street"]:
                    result_df.at[idx, 'la_street'] = components["street"]
                    errors.append({
                        "type": "street_part_extracted",
                        "severity": "info",
                        "la_id": la_id,
                        "index": idx,
                        "original_street": street_value,
                        "extracted_street": components["street"]
                    })
            
            # Si la rue contient potentiellement une adresse complète et que certains 
            # champs sont vides, extraire tous les composants
            elif contains_postal_code and (not has_postal_code or not has_city):
                components = extract_components_from_address(street_value)
                
                # Mise à jour conditionnelle: n'écraser que les champs vides
                if components["house_number"] and not has_house_number:
                    result_df.at[idx, 'la_house_number'] = components["house_number"]
                
                if components["street"]:
                    result_df.at[idx, 'la_street'] = components["street"]
                
                if components["postal_code"] and not has_postal_code:
                    result_df.at[idx, 'la_postal_code'] = components["postal_code"]
                
                if components["city"] and not has_city:
                    result_df.at[idx, 'la_city'] = components["city"]
                
                # Si un complément d'adresse a été extrait, le traiter
                if components["additional_address"]:
                    existing_additional = row.get('la_additional_address', '')
                    if pd.isna(existing_additional) or existing_additional == '':
                        result_df.at[idx, 'la_additional_address'] = components["additional_address"]
                
                errors.append({
                    "type": "address_components_extracted",
                    "severity": "info",
                    "la_id": la_id,
                    "index": idx,
                    "original_street": street_value,
                    "extracted_components": components
                })
            
            # Si la rue ne contient pas de code postal mais pourrait contenir un numéro
            # et que le champ numéro est vide, extraire le numéro
            elif not has_house_number:
                house_number, street = parse_street_with_number(street_value)
                
                if house_number:
                    result_df.at[idx, 'la_house_number'] = house_number
                    result_df.at[idx, 'la_street'] = street
                    
                    errors.append({
                        "type": "house_number_extracted",
                        "severity": "info",
                        "la_id": la_id,
                        "index": idx,
                        "original_street": street_value,
                        "extracted_number": house_number,
                        "remaining_street": street
                    })
        
        # Étape 3: Nettoyer les informations redondantes dans le complément d'adresse
        if has_house_number and not pd.isna(row.get('la_additional_address', '')) and row.get('la_additional_address', '') != '':
            house_number = row['la_house_number']
            additional_address = row['la_additional_address']
            
            cleaned_additional = clean_redundant_address_info(house_number, additional_address)
            
            if cleaned_additional != additional_address:
                result_df.at[idx, 'la_additional_address'] = cleaned_additional
                
                errors.append({
                    "type": "redundant_info_removed",
                    "severity": "info",
                    "la_id": la_id,
                    "index": idx,
                    "original_additional": additional_address,
                    "cleaned_additional": cleaned_additional
                })
        
        # Étape 4: Normaliser les codes postaux (gérer les formats espacés comme "38 170")
        if 'la_postal_code' in row and not pd.isna(row['la_postal_code']) and row['la_postal_code'] != '':
            postal_code = row['la_postal_code']
            normalized_postal = normalize_postal_code(postal_code)
            
            if normalized_postal != postal_code:
                result_df.at[idx, 'la_postal_code'] = normalized_postal
                errors.append({
                    "type": "postal_code_normalized",
                    "severity": "info",
                    "la_id": la_id,
                    "index": idx,
                    "original_postal": postal_code,
                    "normalized_postal": normalized_postal
                })
    
    # Étape finale: Normaliser tous les champs d'adresse (NULL, "0", ".", "/" -> "")
    for field in address_fields:
        if field in result_df.columns:
            result_df[field] = result_df[field].apply(clean_empty_values)
            
    return result_df, errors