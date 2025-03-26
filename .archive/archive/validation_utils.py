"""
Module d'utilitaires de validation pour le projet de transformation de données.
Fournit des fonctions de validation communes à toutes les tables.
"""

import re
from typing import Dict, List, Tuple, Any, Optional, Set, Union


def check_duplicates(data: List[Dict[str, Any]], key_field: str) -> List[Dict[str, Any]]:
    """
    Détecte les valeurs en double pour un champ spécifique.
    
    Args:
        data: Liste de dictionnaires à vérifier
        key_field: Nom du champ à vérifier pour les doublons
        
    Returns:
        Liste des erreurs de doublons détectées
    """
    errors = []
    seen_values = {}
    
    for index, item in enumerate(data):
        # Ignorer les enregistrements sans la clé ou avec une valeur vide
        if key_field not in item or item[key_field] is None or item[key_field] == "":
            continue
            
        value = str(item[key_field])
        
        if value in seen_values:
            # Doublon trouvé
            errors.append({
                "type": f"duplicate_{key_field}",
                "severity": "error",
                "field": key_field,
                "value": value,
                "first_index": seen_values[value],
                "duplicate_index": index,
                "message": f"Valeur en double pour {key_field}: '{value}' aux indices {seen_values[value]} et {index}"
            })
        else:
            seen_values[value] = index
    
    return errors


def validate_french_postal_code(postal_code: str) -> Tuple[bool, Optional[str]]:
    """
    Valide un code postal français.
    
    Args:
        postal_code: Code postal à valider
        
    Returns:
        Tuple contenant:
        - Un booléen indiquant si le code postal est valide
        - Un message d'erreur si le code postal est invalide, sinon None
    """
    # Nettoyage des espaces et autres caractères
    postal_code = re.sub(r'\s+', '', postal_code)
    
    # Validation du format (5 chiffres)
    if not re.match(r'^\d{5}$', postal_code):
        return False, "Le code postal doit contenir exactement 5 chiffres"
    
    # Validation du département (les deux premiers chiffres)
    department = postal_code[:2]
    valid_departments = set([f"{i:02d}" for i in range(1, 96)] + ["2A", "2B"] + [str(i) for i in range(971, 990)])
    
    if department not in valid_departments:
        return False, f"Le département '{department}' n'existe pas en France"
    
    return True, None


def validate_french_siret(siret: str) -> Tuple[bool, Optional[str]]:
    """
    Valide un numéro SIRET français.
    
    Args:
        siret: Numéro SIRET à valider
        
    Returns:
        Tuple contenant:
        - Un booléen indiquant si le SIRET est valide
        - Un message d'erreur si le SIRET est invalide, sinon None
    """
    # Nettoyage des espaces et autres caractères
    siret = re.sub(r'\D', '', siret)
    
    # Validation du format (14 chiffres)
    if len(siret) != 14:
        return False, f"Le SIRET doit contenir exactement 14 chiffres (trouvé: {len(siret)})"
    
    # Validation avec l'algorithme de Luhn
    if not is_valid_luhn(siret):
        return False, "Le SIRET ne respecte pas l'algorithme de Luhn"
    
    return True, None


def validate_french_siren(siren: str) -> Tuple[bool, Optional[str]]:
    """
    Valide un numéro SIREN français.
    
    Args:
        siren: Numéro SIREN à valider
        
    Returns:
        Tuple contenant:
        - Un booléen indiquant si le SIREN est valide
        - Un message d'erreur si le SIREN est invalide, sinon None
    """
    # Nettoyage des espaces et autres caractères
    siren = re.sub(r'\D', '', siren)
    
    # Validation du format (9 chiffres)
    if len(siren) != 9:
        return False, f"Le SIREN doit contenir exactement 9 chiffres (trouvé: {len(siren)})"
    
    # Validation avec l'algorithme de Luhn
    if not is_valid_luhn(siren):
        return False, "Le SIREN ne respecte pas l'algorithme de Luhn"
    
    return True, None


def validate_french_vat(vat: str, siren: Optional[str] = None) -> Tuple[bool, Optional[str]]:
    """
    Valide un numéro de TVA français.
    
    Args:
        vat: Numéro de TVA à valider
        siren: Numéro SIREN optionnel pour la validation croisée
        
    Returns:
        Tuple contenant:
        - Un booléen indiquant si le numéro de TVA est valide
        - Un message d'erreur si le numéro de TVA est invalide, sinon None
    """
    # Nettoyage et standardisation
    vat = vat.upper().replace(' ', '')
    
    # Validation du format (FR + 11 chiffres)
    if not re.match(r'^FR\d{11}$', vat):
        return False, "Le numéro de TVA français doit être au format FR + 11 chiffres"
    
    # Extraction de la clé et du SIREN depuis le numéro de TVA
    key = int(vat[2:4])
    vat_siren = vat[4:]
    
    # Vérification de la cohérence avec le SIREN fourni
    if siren and vat_siren != siren:
        return False, f"Le SIREN inclus dans le numéro de TVA ({vat_siren}) ne correspond pas au SIREN fourni ({siren})"
    
    # Calcul de la clé attendue
    try:
        siren_value = int(vat_siren)
        expected_key = (12 + 3 * (siren_value % 97)) % 97
        
        if key != expected_key:
            return False, f"La clé de contrôle du numéro de TVA est invalide (attendu: {expected_key}, trouvé: {key})"
    except (ValueError, TypeError):
        return False, "Erreur lors du calcul de la clé de contrôle"
    
    return True, None


def is_valid_luhn(digits: str) -> bool:
    """
    Vérifie si une chaîne de chiffres est valide selon l'algorithme de Luhn.
    
    L'algorithme de Luhn, également connu sous le nom de formule mod 10, permet de 
    valider une variété de numéros d'identification, notamment les SIREN et SIRET.
    
    Args:
        digits: Chaîne de chiffres à valider
        
    Returns:
        Booléen indiquant si la chaîne respecte l'algorithme de Luhn
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


def validate_rna(rna: str) -> Tuple[bool, Optional[str]]:
    """
    Valide un numéro RNA (Répertoire National des Associations).
    
    Args:
        rna: Numéro RNA à valider
        
    Returns:
        Tuple contenant:
        - Un booléen indiquant si le RNA est valide
        - Un message d'erreur si le RNA est invalide, sinon None
    """
    # Nettoyage et standardisation
    rna = rna.upper().strip()
    
    # Validation du format (W + 9 chiffres)
    if not re.match(r'^W\d{9}$', rna):
        return False, "Le numéro RNA doit être au format W + 9 chiffres"
    
    return True, None


def validate_id_relationships(siren: Optional[str], siret: Optional[str], vat: Optional[str]) -> List[Dict[str, Any]]:
    """
    Vérifie la cohérence entre les différents identifiants d'entreprise.
    
    Args:
        siren: Numéro SIREN
        siret: Numéro SIRET
        vat: Numéro de TVA
        
    Returns:
        Liste des erreurs de cohérence détectées
    """
    errors = []
    
    # Vérification SIREN-SIRET
    if siren and siret and len(siret) == 14 and len(siren) == 9:
        if siret[:9] != siren:
            errors.append({
                "type": "siren_siret_mismatch",
                "severity": "error",
                "siren": siren,
                "siret": siret,
                "reason": "Le SIRET doit commencer par le SIREN"
            })
    
    # Vérification SIREN-TVA pour les entreprises françaises
    if siren and vat and vat.startswith("FR") and len(vat) == 13 and len(siren) == 9:
        vat_siren = vat[4:]
        if vat_siren != siren:
            errors.append({
                "type": "siren_vat_mismatch",
                "severity": "error",
                "siren": siren,
                "v