"""
Module principal pour le traitement des données companies.
Responsable de l'orchestration complète du processus de transformation.
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any, Union

import pandas as pd

from src.tables.companies.transformations.validate_input_structure import validate_input_structure
from src.tables.companies.transformations.normalize_text import normalize_text
from src.tables.companies.transformations.normalize_special_chars import normalize_special_chars
from src.tables.companies.transformations.clean_punctuation import clean_punctuation
from src.tables.companies.transformations.validate_identifiers import validate_siren, validate_siret, validate_vat
from src.tables.companies.transformations.validate_id_relationships import validate_id_relationships
from src.tables.companies.transformations.validate_postal_code import validate_postal_code
from src.tables.companies.transformations.split_address import split_address, fix_address_split_issues
from src.tables.companies.transformations.patch_data import apply_patches_siret_manquant, apply_patches_address
from src.tables.companies.transformations.prepare_final_model import prepare_final_model
from src.tables.companies.error_reporting.generate_error_report import generate_error_report
from src.utils.logging_manager import setup_logger


def clean_companies_data(
    input_file_path: str, 
    output_file_path: str,
    patches_dir: str = "data/patches",
    error_report_dir: str = "data/error_report",
    log_dir: str = "logs"
) -> Tuple[bool, Optional[str]]:
    """
    Fonction principale pour nettoyer et transformer les données companies.
    
    Args:
        input_file_path: Chemin vers le fichier JSON d'entrée
        output_file_path: Chemin vers le fichier JSON de sortie
        patches_dir: Répertoire contenant les fichiers de correctifs
        error_report_dir: Répertoire pour les rapports d'erreurs
        log_dir: Répertoire pour les fichiers de log
        
    Returns:
        Tuple[bool, Optional[str]]: (Succès, Chemin du rapport d'erreurs si généré)
    """
    # Configuration du logger
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"companies_transformation_{timestamp}.log")
    logger = setup_logger("companies_transformation", log_file)
    
    logger.info(f"Démarrage du traitement des données companies: {input_file_path}")
    
    # Dictionnaire pour collecter les erreurs
    errors = {
        "structure": [],
        "siren": [],
        "siret": [],
        "vat": [],
        "id_relationships": [],
        "postal_code": [],
        "address": [],
        "general": []
    }
    
    # Lecture du fichier d'entrée
    try:
        with open(input_file_path, 'r', encoding='utf-8') as file:
            input_data = json.load(file)
        logger.info(f"Fichier chargé avec succès: {len(input_data)} entrées")
    except Exception as e:
        logger.error(f"Erreur lors de la lecture du fichier d'entrée: {str(e)}")
        errors["general"].append({"error": f"Erreur de lecture du fichier: {str(e)}"})
        return False, None
    
    # Étape 1: Validation de la structure d'entrée
    logger.info("Étape 1: Validation de la structure d'entrée")
    input_data, structure_errors = validate_input_structure(input_data)
    if structure_errors:
        errors["structure"].extend(structure_errors)
        logger.warning(f"Détection de {len(structure_errors)} erreurs de structure")
    
    # Conversion en DataFrame pour faciliter le traitement
    df = pd.DataFrame(input_data)
    original_count = len(df)
    logger.info(f"Conversion en DataFrame: {original_count} lignes")
    
    # Étape 2: Normalisation du texte
    logger.info("Étape 2: Normalisation du texte")
    df, text_errors = normalize_text(df)
    if text_errors:
        errors["general"].extend(text_errors)
    
    # Étape 3: Normalisation des caractères spéciaux
    logger.info("Étape 3: Normalisation des caractères spéciaux")
    df, special_chars_errors = normalize_special_chars(df)
    if special_chars_errors:
        errors["general"].extend(special_chars_errors)
    
    # Étape 4: Nettoyage de la ponctuation
    logger.info("Étape 4: Nettoyage de la ponctuation")
    df, punctuation_errors = clean_punctuation(df)
    if punctuation_errors:
        errors["general"].extend(punctuation_errors)
    
    # Étape 5: Validation des identifiants
    logger.info("Étape 5: Validation des identifiants")
    
    # 5.1: Validation SIREN
    df, siren_errors = validate_siren(df)
    if siren_errors:
        errors["siren"].extend(siren_errors)
        logger.warning(f"Détection de {len(siren_errors)} erreurs de SIREN")
    
    # 5.2: Validation SIRET
    df, siret_errors = validate_siret(df)
    if siret_errors:
        errors["siret"].extend(siret_errors)
        logger.warning(f"Détection de {len(siret_errors)} erreurs de SIRET")
    
    # 5.3: Validation VAT
    df, vat_errors = validate_vat(df)
    if vat_errors:
        errors["vat"].extend(vat_errors)
        logger.warning(f"Détection de {len(vat_errors)} erreurs de VAT")
    
    # Après la validation SIREN et avant la validation SIRET
    logger.info("Application des correctifs spécifiques (SIRET, VAT, forme juridique)")
    siret_patches_file = "data/patches/companies_siret_manquant.json"
    if os.path.exists(siret_patches_file):
        df, patch_specific_errors = apply_patches_siret_manquant(df, siret_patches_file)
        if patch_specific_errors:
            errors["siret"].extend(patch_specific_errors)
    else:
        logger.info(f"Aucun fichier de correctifs spécifiques trouvé: {siret_patches_file}")

    # Étape: Application des correctifs d'adresse
    logger.info("Application des correctifs d'adresse")
    address_patches_file = os.path.join(patches_dir, "companies_address_mal_formate.json")
    if os.path.exists(address_patches_file):
        df, address_patch_errors = apply_patches_address(df, address_patches_file)
        if address_patch_errors:
            errors["address"].extend(address_patch_errors)
    else:
        logger.info(f"Aucun fichier de correctifs d'adresse trouvé: {address_patches_file}")
        
    # Étape 6: Validation des relations entre identifiants
    logger.info("Étape 6: Validation des relations entre identifiants")
    df, id_rel_errors = validate_id_relationships(df)
    if id_rel_errors:
        errors["id_relationships"].extend(id_rel_errors)
        logger.warning(f"Détection de {len(id_rel_errors)} erreurs de relations entre identifiants")
    
    # Étape 7: Validation des codes postaux
    logger.info("Étape 7: Validation des codes postaux")
    df, postal_errors = validate_postal_code(df)
    if postal_errors:
        errors["postal_code"].extend(postal_errors)
        logger.warning(f"Détection de {len(postal_errors)} erreurs de code postal")
        
    # Étape 8: Traitement des adresses
    logger.info("Étape 8: Traitement des adresses")
    df, address_errors = split_address(df)
    if address_errors:
        errors["address"].extend(address_errors)
        logger.warning(f"Détection de {len(address_errors)} erreurs d'adresse")

    # Étape 8bis: Correction des problèmes de décomposition d'adresse
    logger.info("Étape 8bis: Correction des problèmes de décomposition d'adresse")
    df, address_split_fix_errors = fix_address_split_issues(df)
    if address_split_fix_errors:
        errors["address"].extend(address_split_fix_errors)
        logger.info(f"Correction de {len(address_split_fix_errors)} problèmes de décomposition d'adresse")
    
    # Étape 10: Préparation du modèle final
    logger.info("Étape 10: Préparation du modèle final")
    df, final_errors = prepare_final_model(df)
    if final_errors:
        errors["general"].extend(final_errors)
    
    # Vérification de la préservation des données
    final_count = len(df)
    if final_count != original_count:
        message = f"ALERTE: Différence de nombre d'entrées - Original: {original_count}, Final: {final_count}"
        logger.error(message)
        errors["general"].append({"error": message})
    
    # Conversion du DataFrame en liste de dictionnaires
    output_data = df.to_dict(orient='records')

    # Suppression du champ co_validation_status pour chaque enregistrement
    for record in output_data:
        if 'co_validation_status' in record:
            del record['co_validation_status']

    # Sauvegarde du fichier de sortie
    try:
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
        with open(output_file_path, 'w', encoding='utf-8') as file:
            json.dump(output_data, file, ensure_ascii=False, indent=2)
        logger.info(f"Fichier de sortie sauvegardé avec succès: {output_file_path}")
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde du fichier de sortie: {str(e)}")
        errors["general"].append({"error": f"Erreur de sauvegarde: {str(e)}"})
        return False, None

    # Génération du rapport d'erreurs si nécessaire
    has_errors = any(error_list for error_list in errors.values())
    error_report_path = None
    
    if has_errors:
        os.makedirs(error_report_dir, exist_ok=True)
        error_report_path = os.path.join(
            error_report_dir, 
            f"companies_errors_{timestamp}.xlsx"
        )
        generate_error_report(errors, error_report_path, input_data)
        logger.info(f"Rapport d'erreurs généré: {error_report_path}")
    else:
        logger.info("Aucune erreur détectée, pas de rapport généré")
    
    logger.info("Traitement des données companies terminé")
    
    return True, error_report_path


if __name__ == "__main__":
    
    # Exemple d'utilisation
    success, report_path = clean_companies_data(
        input_file_path="data/raw/companies.json",
        output_file_path="data/clean/companies.json"
    )
    
    if success:
        print("Transformation réussie!")
        if report_path:
            print(f"Un rapport d'erreurs a été généré: {report_path}")
    else:
        print("Erreur lors de la transformation")
        if report_path:
            print(f"Consultez le rapport d'erreurs: {report_path}")