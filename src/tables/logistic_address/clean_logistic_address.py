"""
Module principal pour le traitement des données logistic_address.
Responsable de l'orchestration complète du processus de transformation.
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any, Union
import numpy as np
import pandas as pd

from src.tables.logistic_address.transformations.validate_input_structure import validate_input_structure
from src.tables.logistic_address.transformations.normalize_text import normalize_text
from src.tables.logistic_address.transformations.normalize_special_chars import normalize_special_chars
from src.tables.logistic_address.transformations.clean_punctuation import clean_punctuation
from src.tables.logistic_address.transformations.extract_address_components import extract_address_components
from src.tables.logistic_address.transformations.validate_data_types import validate_data_types
from src.tables.logistic_address.transformations.validate_address_fields import validate_address_fields
from src.tables.logistic_address.transformations.validate_postal_code import validate_postal_code
from src.tables.logistic_address.transformations.validate_city_names import validate_city_names
from src.tables.logistic_address.transformations.add_missing_fields import add_missing_fields
from src.tables.logistic_address.transformations.patch_data import apply_patches
from src.tables.logistic_address.transformations.prepare_final_model import prepare_final_model
from src.tables.logistic_address.error_reporting.generate_error_report import generate_error_report
from src.utils.logging_manager import setup_logger


def clean_logistic_address_data(
    input_file_path: str, 
    output_file_path: str,
    patches_dir: str = "data/patches",
    error_report_dir: str = "data/error_report",
    log_dir: str = "logs"
) -> Tuple[bool, Optional[str]]:
    """
    Fonction principale pour nettoyer et transformer les données logistic_address.
    
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
    log_file = os.path.join(log_dir, f"logistic_address_transformation_{timestamp}.log")
    logger = setup_logger("logistic_address_transformation", log_file)
    
    logger.info(f"Démarrage du traitement des données logistic_address: {input_file_path}")
    
    # Dictionnaire pour collecter les erreurs
    errors = {
        "structure": [],
        "address": [],
        "data_types": [],
        "postal_code": [],
        "city": [],
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
    
    # Étape 5: Extraction des composants d'adresse
    logger.info("Étape 5: Extraction des composants d'adresse")
    df, address_extraction_errors = extract_address_components(df)
    if address_extraction_errors:
        errors["address"].extend(address_extraction_errors)
        logger.warning(f"Détection de {len(address_extraction_errors)} erreurs/modifications d'extraction d'adresse")
    
    # Étape 6: Validation des types de données
    logger.info("Étape 6: Validation des types de données")
    df, data_type_errors = validate_data_types(df)
    if data_type_errors:
        errors["data_types"].extend(data_type_errors)
        logger.warning(f"Détection de {len(data_type_errors)} erreurs de types de données")
    
    # Étape 7: Validation des champs d'adresse
    logger.info("Étape 7: Validation des champs d'adresse")
    df, address_field_errors = validate_address_fields(df)
    if address_field_errors:
        errors["address"].extend(address_field_errors)
        logger.warning(f"Détection de {len(address_field_errors)} erreurs de champs d'adresse")
    
    # Étape 8: Validation des codes postaux
    logger.info("Étape 8: Validation des codes postaux")
    df, postal_code_errors = validate_postal_code(df)
    if postal_code_errors:
        errors["postal_code"].extend(postal_code_errors)
        logger.warning(f"Détection de {len(postal_code_errors)} erreurs de code postal")
    
    # Étape 9: Validation des noms de ville
    logger.info("Étape 9: Validation des noms de ville")
    df, city_name_errors = validate_city_names(df)
    if city_name_errors:
        errors["city"].extend(city_name_errors)
        logger.warning(f"Détection de {len(city_name_errors)} erreurs de nom de ville")
    
    # Étape 10: Ajout des champs manquants
    logger.info("Étape 10: Ajout des champs manquants")
    df, missing_fields_errors = add_missing_fields(df)
    if missing_fields_errors:
        errors["general"].extend(missing_fields_errors)
        logger.info(f"{len(missing_fields_errors)} champs ajoutés ou modifiés")
    
    # Étape 11: Application des correctifs spécifiques
    logger.info("Étape 11: Application des correctifs spécifiques")
    patches_file = os.path.join(patches_dir, "logistic_address_patches.json")
    if os.path.exists(patches_file):
        df, patch_errors = apply_patches(df, patches_file)
        if patch_errors:
            errors["general"].extend(patch_errors)
            logger.info(f"{len(patch_errors)} correctifs appliqués")
    else:
        logger.info(f"Aucun fichier de correctifs trouvé: {patches_file}")
    
    # Étape 12: Préparation du modèle final
    logger.info("Étape 12: Préparation du modèle final")
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

    # Convertir les NaN en None (qui deviendra null en JSON)
    for record in output_data:
        for key, value in record.items():
            # Vérifiez si c'est un array/Series ou une valeur scalaire avant d'utiliser pd.isna
            if isinstance(value, (list, pd.Series, np.ndarray)):
                # Pour les listes et arrays, ne rien faire ou traiter différemment si nécessaire
                continue
            elif pd.isna(value):
                record[key] = None
                
    # Suppression du champ la_validation_status pour chaque enregistrement
    for record in output_data:
        if 'la_validation_status' in record:
            del record['la_validation_status']
    
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
            f"logistic_address_errors_{timestamp}.xlsx"
        )
        generate_error_report(errors, error_report_path, input_data)
        logger.info(f"Rapport d'erreurs généré: {error_report_path}")
    else:
        logger.info("Aucune erreur détectée, pas de rapport généré")
    
    logger.info("Traitement des données logistic_address terminé")
    
    return True, error_report_path


if __name__ == "__main__":
    # Exemple d'utilisation
    success, report_path = clean_logistic_address_data(
        input_file_path="data/raw/logistic_address.json",
        output_file_path="data/clean/logistic_address.json"
    )
    
    if success:
        print("Transformation réussie!")
        if report_path:
            print(f"Un rapport d'erreurs a été généré: {report_path}")
    else:
        print("Erreur lors de la transformation")
        if report_path:
            print(f"Consultez le rapport d'erreurs: {report_path}")