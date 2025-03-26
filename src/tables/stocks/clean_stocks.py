# """
# Module principal pour le traitement des données stocks.
# Responsable de l'orchestration complète du processus de transformation.
# """

# import json
# import logging
# import os
# from datetime import datetime
# from typing import Dict, List, Optional, Tuple, Any, Union

# import pandas as pd
# import numpy as np 

# from src.tables.stocks.transformations.validate_input_structure import validate_input_structure
# from src.tables.stocks.transformations.normalize_text import normalize_text
# from src.tables.stocks.transformations.normalize_special_chars import normalize_special_chars
# from src.tables.stocks.transformations.handle_commission_percent import handle_commission_percent
# from src.tables.stocks.transformations.validate_data_types import validate_data_types
# from src.tables.stocks.transformations.validate_dates import validate_dates
# from src.tables.stocks.transformations.validate_uniqueness import validate_uniqueness
# from src.tables.stocks.transformations.validate_stock_import import validate_stock_import
# from src.tables.stocks.transformations.add_missing_fields import add_missing_fields
# from src.tables.stocks.transformations.patch_data import apply_patches
# from src.tables.stocks.transformations.prepare_final_model import prepare_final_model
# from src.tables.stocks.error_reporting.generate_error_report import generate_error_report
# from src.utils.logging_manager import setup_logger
# from src.tables.stocks.transformations.validate_commission_fields import validate_commission_fields

# def clean_stocks_data(
#     input_file_path: str, 
#     output_file_path: str,
#     patches_dir: str = "data/patches",
#     error_report_dir: str = "data/error_report",
#     log_dir: str = "logs"
# ) -> Tuple[bool, Optional[str]]:
#     """
#     Fonction principale pour nettoyer et transformer les données stocks.
    
#     Args:
#         input_file_path: Chemin vers le fichier JSON d'entrée
#         output_file_path: Chemin vers le fichier JSON de sortie
#         patches_dir: Répertoire contenant les fichiers de correctifs
#         error_report_dir: Répertoire pour les rapports d'erreurs
#         log_dir: Répertoire pour les fichiers de log
        
#     Returns:
#         Tuple[bool, Optional[str]]: (Succès, Chemin du rapport d'erreurs si généré)
#     """
#     # Configuration du logger
#     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#     log_file = os.path.join(log_dir, f"stocks_transformation_{timestamp}.log")
#     logger = setup_logger("stocks_transformation", log_file)
    
#     logger.info(f"Démarrage du traitement des données stocks: {input_file_path}")
    
#     # Dictionnaire pour collecter les erreurs
#     errors = {
#         "structure": [],
#         "text": [],
#         "data_types": [],
#         "dates": [],
#         "uniqueness": [],
#         "stock_import": [],
#         "general": []
#     }
    
#     # Lecture du fichier d'entrée
#     try:
#         with open(input_file_path, 'r', encoding='utf-8') as file:
#             input_data = json.load(file)
#         logger.info(f"Fichier chargé avec succès: {len(input_data)} entrées")
#     except Exception as e:
#         logger.error(f"Erreur lors de la lecture du fichier d'entrée: {str(e)}")
#         errors["general"].append({"error": f"Erreur de lecture du fichier: {str(e)}"})
#         return False, None
    
#     # Étape 1: Validation de la structure d'entrée
#     logger.info("Étape 1: Validation de la structure d'entrée")
#     input_data, structure_errors = validate_input_structure(input_data)
#     if structure_errors:
#         errors["structure"].extend(structure_errors)
#         logger.warning(f"Détection de {len(structure_errors)} erreurs de structure")
    
#     # Conversion en DataFrame pour faciliter le traitement
#     df = pd.DataFrame(input_data)
#     original_count = len(df)
#     logger.info(f"Conversion en DataFrame: {original_count} lignes")
    
#     # Étape 2: Gestion du champ st_commission_%
#     logger.info("Étape 2: Gestion du champ st_commission_%")
#     df, commission_percent_errors = handle_commission_percent(df)
#     if commission_percent_errors:
#         errors["general"].extend(commission_percent_errors)
#         logger.info(f"Détection de {len(commission_percent_errors)} modifications du champ commission")
    
    
#     # Étape 2.5: Validation des champs de commission
#     logger.info("Étape 2bis: Validation des champs de commission")
#     df, commission_errors = validate_commission_fields(df)
#     if commission_errors:
#         # Créer une nouvelle catégorie d'erreurs spécifique aux commissions
#         errors["commission"] = commission_errors
#         logger.warning(f"Détection de {len(commission_errors)} problèmes de commission")
    
#     # Étape 3: Normalisation du texte
#     logger.info("Étape 3: Normalisation du texte")
#     df, text_errors = normalize_text(df)
#     if text_errors:
#         errors["text"].extend(text_errors)
#         logger.info(f"Détection de {len(text_errors)} modifications de texte")
    
#     # Étape 4: Normalisation des caractères spéciaux
#     logger.info("Étape 4: Normalisation des caractères spéciaux")
#     df, special_chars_errors = normalize_special_chars(df)
#     if special_chars_errors:
#         errors["text"].extend(special_chars_errors)
#         logger.info(f"Détection de {len(special_chars_errors)} modifications de caractères spéciaux")
    
#     # Étape 5: Validation des types de données
#     logger.info("Étape 5: Validation des types de données")
#     df, data_type_errors = validate_data_types(df)
#     if data_type_errors:
#         errors["data_types"].extend(data_type_errors)
#         logger.warning(f"Détection de {len(data_type_errors)} erreurs de types de données")
    
#     # Étape 6: Validation des dates
#     logger.info("Étape 6: Validation des dates")
#     df, date_errors = validate_dates(df)
#     if date_errors:
#         errors["dates"].extend(date_errors)
#         logger.warning(f"Détection de {len(date_errors)} erreurs de dates")
    
#     # Étape 7: Validation des contraintes d'unicité
#     logger.info("Étape 7: Validation des contraintes d'unicité")
#     df, uniqueness_errors = validate_uniqueness(df)
#     if uniqueness_errors:
#         errors["uniqueness"].extend(uniqueness_errors)
#         logger.warning(f"Détection de {len(uniqueness_errors)} erreurs d'unicité")
    
#     # Étape 8: Validation des stock_import
#     logger.info("Étape 8: Validation des stock_import")
#     df, stock_import_errors = validate_stock_import(df)
#     if stock_import_errors:
#         errors["stock_import"].extend(stock_import_errors)
#         logger.warning(f"Détection de {len(stock_import_errors)} erreurs de stock_import")
    
#     # Étape 9: Ajout des champs manquants
#     logger.info("Étape 9: Ajout des champs manquants")
#     df, missing_fields_errors = add_missing_fields(df)
#     if missing_fields_errors:
#         errors["general"].extend(missing_fields_errors)
#         logger.info(f"{len(missing_fields_errors)} champs ajoutés ou modifiés")
    
#     # Étape 10: Application des correctifs spécifiques
#     logger.info("Étape 10: Application des correctifs spécifiques")
#     patches_file = os.path.join(patches_dir, "stocks_patches.json")
#     if os.path.exists(patches_file):
#         df, patch_errors = apply_patches(df, patches_file)
#         if patch_errors:
#             errors["general"].extend(patch_errors)
#             logger.info(f"{len(patch_errors)} correctifs appliqués")
#     else:
#         logger.info(f"Aucun fichier de correctifs trouvé: {patches_file}")
    
#     # Étape 11: Préparation du modèle final
#     logger.info("Étape 11: Préparation du modèle final")
#     df, final_errors = prepare_final_model(df)
#     if final_errors:
#         errors["general"].extend(final_errors)
    
#     # Vérification de la préservation des données
#     final_count = len(df)
#     if final_count != original_count:
#         message = f"ALERTE: Différence de nombre d'entrées - Original: {original_count}, Final: {final_count}"
#         logger.error(message)
#         errors["general"].append({"error": message})
    
#     # Conversion du DataFrame en liste de dictionnaires
#     output_data = df.to_dict(orient='records')
    
#     # Convertir les NaN en None (qui deviendra null en JSON)
#     for record in output_data:
#         for key, value in record.items():
#             # Vérifiez si c'est un array/Series ou une valeur scalaire avant d'utiliser pd.isna
#             if isinstance(value, (list, pd.Series, np.ndarray)):
#                 # Pour les listes et arrays, ne rien faire ou traiter différemment si nécessaire
#                 continue
#             elif pd.isna(value):
#                 record[key] = None
    
#     # Suppression du champ st_validation_status pour chaque enregistrement
#     for record in output_data:
#         if 'st_validation_status' in record:
#             del record['st_validation_status']
    
#     # Sauvegarde du fichier de sortie
#     try:
#         os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
#         with open(output_file_path, 'w', encoding='utf-8') as file:
#             json.dump(output_data, file, ensure_ascii=False, indent=2)
#         logger.info(f"Fichier de sortie sauvegardé avec succès: {output_file_path}")
#     except Exception as e:    
#         logger.error(f"Erreur lors de la sauvegarde du fichier de sortie: {str(e)}")
#         errors["general"].append({"error": f"Erreur de sauvegarde: {str(e)}"})
#         return False, None
    
#     # Génération du rapport d'erreurs si nécessaire
#     has_errors = any(error_list for error_list in errors.values())
#     error_report_path = None
    
#     if has_errors:
#         os.makedirs(error_report_dir, exist_ok=True)
#         error_report_path = os.path.join(
#             error_report_dir, 
#             f"stocks_errors_{timestamp}.xlsx"
#         )
#         generate_error_report(errors, error_report_path, input_data)
#         logger.info(f"Rapport d'erreurs généré: {error_report_path}")
#     else:
#         logger.info("Aucune erreur détectée, pas de rapport généré")
    
#     logger.info("Traitement des données stocks terminé")
    
#     return True, error_report_path


# if __name__ == "__main__":
#     # Exemple d'utilisation
#     success, report_path = clean_stocks_data(
#         input_file_path="data/raw/stocks.json",
#         output_file_path="data/clean/stocks.json"
#     )
    
#     if success:
#         print("Transformation réussie!")
#         if report_path:
#             print(f"Un rapport d'erreurs a été généré: {report_path}")
#     else:
#         print("Erreur lors de la transformation")
#         if report_path:
#             print(f"Consultez le rapport d'erreurs: {report_path}")


"""
Module principal pour le traitement des données stocks.
Responsable de l'orchestration complète du processus de transformation.
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any, Union

import pandas as pd
import numpy as np 

from src.tables.stocks.transformations.validate_input_structure import validate_input_structure
from src.tables.stocks.transformations.normalize_text import normalize_text
from src.tables.stocks.transformations.normalize_special_chars import normalize_special_chars
from src.tables.stocks.transformations.handle_commission_percent import handle_commission_percent
from src.tables.stocks.transformations.validate_data_types import validate_data_types
from src.tables.stocks.transformations.validate_dates import validate_dates
from src.tables.stocks.transformations.validate_uniqueness import validate_uniqueness
from src.tables.stocks.transformations.validate_stock_import import validate_stock_import
from src.tables.stocks.transformations.add_missing_fields import add_missing_fields
from src.tables.stocks.transformations.patch_data import apply_patches
from src.tables.stocks.transformations.prepare_final_model import prepare_final_model
from src.tables.stocks.error_reporting.generate_error_report import generate_error_report
from src.utils.logging_manager import setup_logger
from src.tables.stocks.transformations.validate_commission_fields import validate_commission_fields
from src.tables.stocks.transformations.clean_commentary import clean_commentary
from src.tables.stocks.transformations.generate_statistics import generate_statistics
from src.tables.stocks.transformations.check_empty_stock_import import check_empty_stock_import


def clean_stocks_data(
    input_file_path: str, 
    output_file_path: str,
    patches_dir: str = "data/patches",
    error_report_dir: str = "data/error_report",
    log_dir: str = "logs"
) -> Tuple[bool, Optional[str]]:
    """
    Fonction principale pour nettoyer et transformer les données stocks.
    
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
    log_file = os.path.join(log_dir, f"stocks_transformation_{timestamp}.log")
    logger = setup_logger("stocks_transformation", log_file)
    
    logger.info(f"Démarrage du traitement des données stocks: {input_file_path}")
    
    # Dictionnaire pour collecter les erreurs
    errors = {
        "structure": [],
        "text": [],
        "data_types": [],
        "dates": [],
        "uniqueness": [],
        "stock_import": [],
        "commission": [],
        "statistics": [],
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
    
    # Étape 2: Gestion du champ st_commission_%
    logger.info("Étape 2: Gestion du champ st_commission_%")
    df, commission_percent_errors = handle_commission_percent(df)
    if commission_percent_errors:
        errors["general"].extend(commission_percent_errors)
        logger.info(f"Détection de {len(commission_percent_errors)} modifications du champ commission")
    
    # Étape 2.5: Validation des champs de commission
    logger.info("Étape 2bis: Validation des champs de commission")
    df, commission_errors = validate_commission_fields(df)
    if commission_errors:
        errors["commission"].extend(commission_errors)
        logger.warning(f"Détection de {len(commission_errors)} problèmes de commission")
    
    # Étape 2.6: Nettoyage des valeurs "0" dans st_commentary
    logger.info("Étape 2ter: Nettoyage des commentaires avec valeur '0'")
    df, commentary_errors = clean_commentary(df)
    if commentary_errors:
        errors["general"].extend(commentary_errors)
        logger.info(f"Détection de {len(commentary_errors)} modifications de commentaires")
    
    # Étape 2.7: Vérification des stock_import vides
    logger.info("Étape 2quater: Vérification des stock_import vides")
    df, empty_stock_import_errors = check_empty_stock_import(df)
    if empty_stock_import_errors:
        errors["stock_import"].extend(empty_stock_import_errors)
        logger.warning(f"Détection de {len(empty_stock_import_errors)} problèmes de stock_import vide")
    
    # Étape 3: Normalisation du texte
    logger.info("Étape 3: Normalisation du texte")
    df, text_errors = normalize_text(df)
    if text_errors:
        errors["text"].extend(text_errors)
        logger.info(f"Détection de {len(text_errors)} modifications de texte")
        
    # Vérification de l'état des stock_import après la normalisation
    df, empty_stock_import_first_check = check_empty_stock_import(df)
    if empty_stock_import_first_check:
        # Ne pas ajouter aux erreurs maintenant, simplement pour le débogage
        logger.info(f"Premier contrôle: {len(empty_stock_import_first_check)} stock_import vides détectés")
    
    # Étape 4: Normalisation des caractères spéciaux
    logger.info("Étape 4: Normalisation des caractères spéciaux")
    df, special_chars_errors = normalize_special_chars(df)
    if special_chars_errors:
        errors["text"].extend(special_chars_errors)
        logger.info(f"Détection de {len(special_chars_errors)} modifications de caractères spéciaux")
    
    # Étape 5: Validation des types de données
    logger.info("Étape 5: Validation des types de données")
    df, data_type_errors = validate_data_types(df)
    if data_type_errors:
        errors["data_types"].extend(data_type_errors)
        logger.warning(f"Détection de {len(data_type_errors)} erreurs de types de données")
    
    # Étape 6: Validation des dates
    logger.info("Étape 6: Validation des dates")
    df, date_errors = validate_dates(df)
    if date_errors:
        errors["dates"].extend(date_errors)
        logger.warning(f"Détection de {len(date_errors)} erreurs de dates")
    
    # Étape 7: Validation des contraintes d'unicité
    logger.info("Étape 7: Validation des contraintes d'unicité")
    df, uniqueness_errors = validate_uniqueness(df)
    if uniqueness_errors:
        errors["uniqueness"].extend(uniqueness_errors)
        logger.warning(f"Détection de {len(uniqueness_errors)} erreurs d'unicité")
    
    # Étape 8: Validation des stock_import
    logger.info("Étape 8: Validation des stock_import")
    df, stock_import_errors = validate_stock_import(df)
    if stock_import_errors:
        errors["stock_import"].extend(stock_import_errors)
        logger.warning(f"Détection de {len(stock_import_errors)} erreurs de stock_import")
    
    # Étape 9: Ajout des champs manquants
    logger.info("Étape 9: Ajout des champs manquants")
    df, missing_fields_errors = add_missing_fields(df)
    if missing_fields_errors:
        errors["general"].extend(missing_fields_errors)
        logger.info(f"{len(missing_fields_errors)} champs ajoutés ou modifiés")
    
    # Étape 10: Application des correctifs spécifiques
    logger.info("Étape 10: Application des correctifs spécifiques")
    patches_file = os.path.join(patches_dir, "stocks_patches.json")
    if os.path.exists(patches_file):
        df, patch_errors = apply_patches(df, patches_file)
        if patch_errors:
            errors["general"].extend(patch_errors)
            logger.info(f"{len(patch_errors)} correctifs appliqués")
    else:
        logger.info(f"Aucun fichier de correctifs trouvé: {patches_file}")
    
    # Étape 11: Préparation du modèle final
    logger.info("Étape 11: Préparation du modèle final")
    df, final_errors = prepare_final_model(df)
    if final_errors:
        errors["general"].extend(final_errors)
    
    # Étape 12: Génération des statistiques (n'affecte pas les données)
    logger.info("Étape 12: Génération des statistiques")
    df, statistics = generate_statistics(df)
    if statistics:
        errors["statistics"].extend(statistics)
        logger.info(f"Génération de {len(statistics)} éléments statistiques")
    
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
            # Vérifier si c'est un array/Series ou une valeur scalaire avant d'utiliser pd.isna
            if isinstance(value, (list, pd.Series, np.ndarray)):
                # Pour les listes et arrays, ne rien faire ou traiter différemment si nécessaire
                continue
            elif pd.isna(value):
                record[key] = None
    
    # Suppression du champ st_validation_status pour chaque enregistrement
    for record in output_data:
        if 'st_validation_status' in record:
            del record['st_validation_status']
    
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
            f"stocks_errors_{timestamp}.xlsx"
        )
        generate_error_report(errors, error_report_path, input_data)
        logger.info(f"Rapport d'erreurs généré: {error_report_path}")
    else:
        logger.info("Aucune erreur détectée, pas de rapport généré")
    
    logger.info("Traitement des données stocks terminé")
    
    return True, error_report_path


if __name__ == "__main__":
    # Exemple d'utilisation
    success, report_path = clean_stocks_data(
        input_file_path="data/raw/stocks.json",
        output_file_path="data/clean/stocks.json"
    )
    
    if success:
        print("Transformation réussie!")
        if report_path:
            print(f"Un rapport d'erreurs a été généré: {report_path}")
    else:
        print("Erreur lors de la transformation")
        if report_path:
            print(f"Consultez le rapport d'erreurs: {report_path}")