"""
Module principal pour le traitement des données transports.
Responsable de l'orchestration complète du processus de transformation.
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any, Union

import pandas as pd

from src.tables.transports.transformations.validate_input_structure import validate_input_structure
from src.tables.transports.transformations.normalize_text import normalize_text
from src.tables.transports.transformations.normalize_special_chars import normalize_special_chars
from src.tables.transports.transformations.deduplicate_stock_import import deduplicate_stock_import
from src.tables.transports.transformations.validate_denomination import validate_denomination
from src.tables.transports.transformations.validate_data_types import validate_data_types
from src.tables.transports.transformations.add_missing_fields import add_missing_fields
from src.tables.transports.transformations.patch_data import apply_patches
from src.tables.transports.transformations.prepare_final_model import prepare_final_model
from src.tables.transports.error_reporting.generate_error_report import generate_error_report
from src.utils.logging_manager import setup_logger


def clean_transports_data(
    input_file_path: str, 
    output_file_path: str,
    patches_dir: str = "data/patches",
    error_report_dir: str = "data/error_report",
    log_dir: str = "logs"
) -> Tuple[bool, Optional[str]]:
    """
    Fonction principale pour nettoyer et transformer les données transports.
    
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
    log_file = os.path.join(log_dir, f"transports_transformation_{timestamp}.log")
    logger = setup_logger("transports_transformation", log_file)
    
    logger.info(f"Démarrage du traitement des données transports: {input_file_path}")
    
    # Dictionnaire pour collecter les erreurs
    errors = {
        "structure": [],
        "denomination": [],
        "data_types": [],
        "stock_import": [],
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
    
    # Étape 4: Déduplication des identifiants stock_import
    logger.info("Étape 4: Déduplication des identifiants stock_import")
    df, stock_import_errors = deduplicate_stock_import(df)
    if stock_import_errors:
        errors["stock_import"].extend(stock_import_errors)
        logger.warning(f"Détection de {len(stock_import_errors)} erreurs/modifications de stock_import")
    
    # Étape 5: Validation des dénominations
    logger.info("Étape 5: Validation des dénominations")
    df, denomination_errors = validate_denomination(df)
    if denomination_errors:
        errors["denomination"].extend(denomination_errors)
        logger.warning(f"Détection de {len(denomination_errors)} erreurs/modifications de dénomination")
    
    # Étape 6: Validation des types de données
    logger.info("Étape 6: Validation des types de données")
    df, data_type_errors = validate_data_types(df)
    if data_type_errors:
        errors["data_types"].extend(data_type_errors)
        logger.warning(f"Détection de {len(data_type_errors)} erreurs de types de données")
    
    # Étape 7: Ajout des champs manquants
    logger.info("Étape 7: Ajout des champs manquants")
    df, missing_fields_errors = add_missing_fields(df)
    if missing_fields_errors:
        errors["general"].extend(missing_fields_errors)
        logger.info(f"{len(missing_fields_errors)} champs ajoutés ou modifiés")
    
    # Étape 8: Application des correctifs spécifiques
    logger.info("Étape 8: Application des correctifs spécifiques")
    patches_file = os.path.join(patches_dir, "transports_patches.json")
    if os.path.exists(patches_file):
        df, patch_errors = apply_patches(df, patches_file)
        if patch_errors:
            errors["general"].extend(patch_errors)
            logger.info(f"{len(patch_errors)} correctifs appliqués")
    else:
        logger.info(f"Aucun fichier de correctifs trouvé: {patches_file}")
    
    # Étape 9: Préparation du modèle final
    logger.info("Étape 9: Préparation du modèle final")
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
    
    # Suppression du champ tra_validation_status pour chaque enregistrement
    for record in output_data:
        if 'tra_validation_status' in record:
            del record['tra_validation_status']
    
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
            f"transports_errors_{timestamp}.xlsx"
        )
        generate_error_report(errors, error_report_path, input_data)
        logger.info(f"Rapport d'erreurs généré: {error_report_path}")
    else:
        logger.info("Aucune erreur détectée, pas de rapport généré")
    
    logger.info("Traitement des données transports terminé")
    
    return True, error_report_path


if __name__ == "__main__":
    # Exemple d'utilisation
    success, report_path = clean_transports_data(
        input_file_path="data/raw/transports.json",
        output_file_path="data/clean/transports.json"
    )
    
    if success:
        print("Transformation réussie!")
        if report_path:
            print(f"Un rapport d'erreurs a été généré: {report_path}")
    else:
        print("Erreur lors de la transformation")
        if report_path:
            print(f"Consultez le rapport d'erreurs: {report_path}")                                      