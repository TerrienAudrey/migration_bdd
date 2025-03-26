# """
# Module principal pour le traitement des données stock_import.
# Responsable de l'orchestration complète du processus de transformation.
# """

# import json
# import logging
# import os
# from datetime import datetime
# from typing import Dict, List, Optional, Tuple, Any, Union
# import numpy as np
# import pandas as pd

# from src.tables.stock_import.transformations.validate_input_structure import validate_input_structure
# from src.tables.stock_import.transformations.normalize_text import normalize_text
# from src.tables.stock_import.transformations.validate_dates import validate_dates
# from src.tables.stock_import.transformations.validate_data_types import validate_data_types
# from src.tables.stock_import.transformations.validate_json_fields import validate_json_fields
# from src.tables.stock_import.transformations.add_missing_fields import add_missing_fields
# # from src.tables.stock_import.transformations.patch_data import apply_patches
# from src.tables.stock_import.transformations.prepare_final_model import prepare_final_model
# from src.tables.stock_import.error_reporting.generate_error_report import generate_error_report
# from src.utils.logging_manager import setup_logger


# def clean_stock_import_data(
#     input_file_path: str, 
#     output_file_path: str,
#     patches_dir: str = "data/patches",
#     error_report_dir: str = "data/error_report",
#     log_dir: str = "logs"
# ) -> Tuple[bool, Optional[str]]:
#     """
#     Fonction principale pour nettoyer et transformer les données stock_import.
    
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
#     log_file = os.path.join(log_dir, f"stock_import_transformation_{timestamp}.log")
#     logger = setup_logger("stock_import_transformation", log_file)
    
#     logger.info(f"Démarrage du traitement des données stock_import: {input_file_path}")
    
#     # Dictionnaire pour collecter les erreurs
#     errors = {
#         "structure": [],
#         "dates": [],
#         "data_types": [],
#         "json_fields": [],
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
    
#     # Étape 2: Normalisation du texte
#     logger.info("Étape 2: Normalisation du texte")
#     df, text_errors = normalize_text(df)
#     if text_errors:
#         errors["general"].extend(text_errors)
    
#     # Étape 3: Validation des dates
#     logger.info("Étape 3: Validation des dates")
#     df, date_errors = validate_dates(df)
#     if date_errors:
#         errors["dates"].extend(date_errors)
#         logger.warning(f"Détection de {len(date_errors)} erreurs/modifications de dates")
    
#     # Étape 4: Validation des types de données
#     logger.info("Étape 4: Validation des types de données")
#     df, data_type_errors = validate_data_types(df)
#     if data_type_errors:
#         errors["data_types"].extend(data_type_errors)
#         logger.warning(f"Détection de {len(data_type_errors)} erreurs de types de données")
    
#     # Étape 5: Validation des champs JSON
#     logger.info("Étape 5: Validation des champs JSON")
#     df, json_field_errors = validate_json_fields(df)
#     if json_field_errors:
#         errors["json_fields"].extend(json_field_errors)
#         logger.warning(f"Détection de {len(json_field_errors)} erreurs/modifications de champs JSON")
    
#     # Étape 6: Ajout des champs manquants
#     logger.info("Étape 6: Ajout des champs manquants")
#     df, missing_fields_errors = add_missing_fields(df)
#     if missing_fields_errors:
#         errors["general"].extend(missing_fields_errors)
#         logger.info(f"{len(missing_fields_errors)} champs ajoutés ou modifiés")
    
#     # # Étape 7: Application des correctifs spécifiques
#     # logger.info("Étape 7: Application des correctifs spécifiques")
#     # patches_file = os.path.join(patches_dir, "stock_import_patches.json")
#     # if os.path.exists(patches_file):
#     #     df, patch_errors = apply_patches(df, patches_file)
#     #     if patch_errors:
#     #         errors["general"].extend(patch_errors)
#     #         logger.info(f"{len(patch_errors)} correctifs appliqués")
#     # else:
#     #     logger.info(f"Aucun fichier de correctifs trouvé: {patches_file}")
    
#     # Étape 8: Préparation du modèle final
#     logger.info("Étape 8: Préparation du modèle final")
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
    
#     # # Suppression du champ si_validation_status pour chaque enregistrement
#     # for record in output_data:
#     #     if 'si_validation_status' in record:
#     #         del record['si_validation_status']
    
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
#             f"stock_import_errors_{timestamp}.xlsx"
#         )
#         generate_error_report(errors, error_report_path, input_data)
#         logger.info(f"Rapport d'erreurs généré: {error_report_path}")
#     else:
#         logger.info("Aucune erreur détectée, pas de rapport généré")
    
#     logger.info("Traitement des données stock_import terminé")
    
#     return True, error_report_path


# if __name__ == "__main__":
#     # Exemple d'utilisation
#     success, report_path = clean_stock_import_data(
#         input_file_path="data/raw/stock_import.json",
#         output_file_path="data/clean/stock_import.json"
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
Module principal pour le traitement des données stock_import.
Responsable de l'orchestration complète du processus de transformation.
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any, Union
import numpy as np
import pandas as pd

from src.tables.stock_import.transformations.validate_input_structure import validate_input_structure
from src.tables.stock_import.transformations.normalize_text import normalize_text
from src.tables.stock_import.transformations.validate_dates import validate_dates
from src.tables.stock_import.transformations.validate_data_types import validate_data_types
from src.tables.stock_import.transformations.validate_json_fields import validate_json_fields
from src.tables.stock_import.transformations.add_missing_fields import add_missing_fields
# from src.tables.stock_import.transformations.patch_data import apply_patches
from src.tables.stock_import.transformations.prepare_final_model import prepare_final_model
from src.tables.stock_import.error_reporting.generate_error_report import generate_error_report
from src.utils.logging_manager import setup_logger
from src.tables.stock_import.transformations.validate_si_id import validate_si_id

def clean_stock_import_data(
    input_file_path: str, 
    output_file_path: str,
    patches_dir: str = "data/patches",
    error_report_dir: str = "data/error_report",
    log_dir: str = "logs"
) -> Tuple[bool, Optional[str]]:
    """
    Fonction principale pour nettoyer et transformer les données stock_import.
    
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
    log_file = os.path.join(log_dir, f"stock_import_transformation_{timestamp}.log")
    logger = setup_logger("stock_import_transformation", log_file)
    
    logger.info(f"Démarrage du traitement des données stock_import: {input_file_path}")
    
    # Dictionnaire pour collecter les erreurs
    errors = {
        "structure": [],
        "dates": [],
        "data_types": [],
        "json_fields": [],
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

    # Nouvelle étape: Validation spécifique des si_id
    logger.info("Étape 1bis: Validation des identifiants si_id")
    df, si_id_errors = validate_si_id(df)
    if si_id_errors:
        errors["structure"].extend(si_id_errors)
        logger.warning(f"Détection de {len(si_id_errors)} erreurs d'identifiants si_id")
    
    # Étape 2: Normalisation du texte
    logger.info("Étape 2: Normalisation du texte")
    df, text_errors = normalize_text(df)
    if text_errors:
        errors["general"].extend(text_errors)
    
    # Étape 3: Validation des dates
    logger.info("Étape 3: Validation des dates")
    df, date_errors = validate_dates(df)
    if date_errors:
        errors["dates"].extend(date_errors)
        logger.warning(f"Détection de {len(date_errors)} erreurs/modifications de dates")
    
    # Étape 4: Validation des types de données
    logger.info("Étape 4: Validation des types de données")
    df, data_type_errors = validate_data_types(df)
    if data_type_errors:
        errors["data_types"].extend(data_type_errors)
        logger.warning(f"Détection de {len(data_type_errors)} erreurs de types de données")
    
    # Étape 5: Validation des champs JSON
    logger.info("Étape 5: Validation des champs JSON")
    df, json_field_errors = validate_json_fields(df)
    if json_field_errors:
        errors["json_fields"].extend(json_field_errors)
        logger.warning(f"Détection de {len(json_field_errors)} erreurs/modifications de champs JSON")
    
    # Étape 6: Ajout des champs manquants
    logger.info("Étape 6: Ajout des champs manquants")
    df, missing_fields_errors = add_missing_fields(df)
    if missing_fields_errors:
        errors["general"].extend(missing_fields_errors)
        logger.info(f"{len(missing_fields_errors)} champs ajoutés ou modifiés")
    
    # # Étape 7: Application des correctifs spécifiques
    # logger.info("Étape 7: Application des correctifs spécifiques")
    # patches_file = os.path.join(patches_dir, "stock_import_patches.json")
    # if os.path.exists(patches_file):
    #     df, patch_errors = apply_patches(df, patches_file)
    #     if patch_errors:
    #         errors["general"].extend(patch_errors)
    #         logger.info(f"{len(patch_errors)} correctifs appliqués")
    # else:
    #     logger.info(f"Aucun fichier de correctifs trouvé: {patches_file}")
    
    # Étape 8: Préparation du modèle final
    logger.info("Étape 8: Préparation du modèle final")
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
    
    # Suppression du champ si_validation_status pour chaque enregistrement
    for record in output_data:
        if 'si_validation_status' in record:
            del record['si_validation_status']
    
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
            f"stock_import_errors_{timestamp}.xlsx"
        )
        generate_error_report(errors, error_report_path, input_data)
        logger.info(f"Rapport d'erreurs généré: {error_report_path}")
    else:
        logger.info("Aucune erreur détectée, pas de rapport généré")
    
    logger.info("Traitement des données stock_import terminé")
    
    return True, error_report_path


if __name__ == "__main__":
    # Exemple d'utilisation
    success, report_path = clean_stock_import_data(
        input_file_path="data/raw/stock_import.json",
        output_file_path="data/clean/stock_import.json"
    )
    
    if success:
        print("Transformation réussie!")
        if report_path:
            print(f"Un rapport d'erreurs a été généré: {report_path}")
    else:
        print("Erreur lors de la transformation")
        if report_path:
            print(f"Consultez le rapport d'erreurs: {report_path}")