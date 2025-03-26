# # """
# # Module principal pour le traitement des données organizations.
# # Responsable de l'orchestration complète du processus de transformation.
# # """

# # import json
# # import logging
# # import os
# # from datetime import datetime
# # from typing import Dict, List, Optional, Tuple, Any, Union

# # import pandas as pd
# # from src.tables.organizations.transformations.add_missing_fields import add_missing_fields
# # from src.tables.organizations.transformations.validate_input_structure import validate_input_structure
# # from src.tables.organizations.transformations.normalize_text import normalize_text
# # from src.tables.organizations.transformations.normalize_special_chars import normalize_special_chars
# # from src.tables.organizations.transformations.clean_punctuation import clean_punctuation
# # from src.tables.organizations.transformations.validate_rna import validate_rna
# # from src.tables.organizations.transformations.validate_address_fields import validate_address_fields
# # from src.tables.organizations.transformations.patch_data import apply_patches
# # from src.tables.organizations.transformations.prepare_final_model import prepare_final_model
# # from src.tables.organizations.error_reporting.generate_error_report import generate_error_report
# # from src.utils.logging_manager import setup_logger


# # def clean_organizations_data(
# #     input_file_path: str, 
# #     output_file_path: str,
# #     patches_dir: str = "data/patches",
# #     error_report_dir: str = "data/error_report",
# #     log_dir: str = "logs"
# # ) -> Tuple[bool, Optional[str]]:
# #     """
# #     Fonction principale pour nettoyer et transformer les données organizations.
    
# #     Args:
# #         input_file_path: Chemin vers le fichier JSON d'entrée
# #         output_file_path: Chemin vers le fichier JSON de sortie
# #         patches_dir: Répertoire contenant les fichiers de correctifs
# #         error_report_dir: Répertoire pour les rapports d'erreurs
# #         log_dir: Répertoire pour les fichiers de log
        
# #     Returns:
# #         Tuple[bool, Optional[str]]: (Succès, Chemin du rapport d'erreurs si généré)
# #     """
# #     # Configuration du logger
# #     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
# #     log_file = os.path.join(log_dir, f"organizations_transformation_{timestamp}.log")
# #     logger = setup_logger("organizations_transformation", log_file)
    
# #     logger.info(f"Démarrage du traitement des données organizations: {input_file_path}")
    
# #     # Dictionnaire pour collecter les erreurs
# #     errors = {
# #         "structure": [],
# #         "rna": [],
# #         "address": [],
# #         "general": []
# #     }
    
# #     # Lecture du fichier d'entrée
# #     try:
# #         with open(input_file_path, 'r', encoding='utf-8') as file:
# #             input_data = json.load(file)
# #         logger.info(f"Fichier chargé avec succès: {len(input_data)} entrées")
# #     except Exception as e:
# #         logger.error(f"Erreur lors de la lecture du fichier d'entrée: {str(e)}")
# #         errors["general"].append({"error": f"Erreur de lecture du fichier: {str(e)}"})
# #         return False, None
    
# #     # Étape 1: Validation de la structure d'entrée
# #     logger.info("Étape 1: Validation de la structure d'entrée")
# #     input_data, structure_errors = validate_input_structure(input_data)
# #     if structure_errors:
# #         errors["structure"].extend(structure_errors)
# #         logger.warning(f"Détection de {len(structure_errors)} erreurs de structure")
    
# #     # Conversion en DataFrame pour faciliter le traitement
# #     df = pd.DataFrame(input_data)
# #     original_count = len(df)
# #     logger.info(f"Conversion en DataFrame: {original_count} lignes")
    
# #     # Étape 2: Normalisation du texte
# #     logger.info("Étape 2: Normalisation du texte")
# #     df, text_errors = normalize_text(df)
# #     if text_errors:
# #         errors["general"].extend(text_errors)
    
# #     # Étape 3: Normalisation des caractères spéciaux
# #     logger.info("Étape 3: Normalisation des caractères spéciaux")
# #     df, special_chars_errors = normalize_special_chars(df)
# #     if special_chars_errors:
# #         errors["general"].extend(special_chars_errors)
    
# #     # Étape 4: Nettoyage de la ponctuation
# #     logger.info("Étape 4: Nettoyage de la ponctuation")
# #     df, punctuation_errors = clean_punctuation(df)
# #     if punctuation_errors:
# #         errors["general"].extend(punctuation_errors)
    
# #     # Étape 5: Validation du RNA
# #     logger.info("Étape 5: Validation du RNA")
# #     df, rna_errors = validate_rna(df)
# #     if rna_errors:
# #         errors["rna"].extend(rna_errors)
# #         logger.warning(f"Détection de {len(rna_errors)} erreurs de RNA")
    
# #     # Étape 6: Validation des champs d'adresse
# #     logger.info("Étape 6: Validation des champs d'adresse")
# #     df, address_errors = validate_address_fields(df)
# #     if address_errors:
# #         errors["address"].extend(address_errors)
# #         logger.warning(f"Détection de {len(address_errors)} erreurs d'adresse")
    
# #     # Étape 7: Ajout des champs manquants
# #     logger.info("Étape 7: Ajout des champs manquants")
# #     df, missing_fields_errors = add_missing_fields(df)
# #     if missing_fields_errors:
# #         errors["general"].extend(missing_fields_errors)

# #     # Étape 8: Application des correctifs spécifiques (renuméroter cette étape)
# #     logger.info("Étape 8: Application des correctifs spécifiques")
# #     patches_file = os.path.join(patches_dir, "organizations_patches.json")
# #     if os.path.exists(patches_file):
# #         df, patch_errors = apply_patches(df, patches_file)
# #         if patch_errors:
# #             errors["general"].extend(patch_errors)
# #     else:
# #         logger.info(f"Aucun fichier de correctifs trouvé: {patches_file}")

# #     # Étape 9: Préparation du modèle final (renuméroter cette étape)
# #     logger.info("Étape 9: Préparation du modèle final")
# #     df, final_errors = prepare_final_model(df)
# #     if final_errors:
# #         errors["general"].extend(final_errors)
    
# #     # Vérification de la préservation des données
# #     final_count = len(df)
# #     if final_count != original_count:
# #         message = f"ALERTE: Différence de nombre d'entrées - Original: {original_count}, Final: {final_count}"
# #         logger.error(message)
# #         errors["general"].append({"error": message})


# #     # Conversion du DataFrame en liste de dictionnaires
# #     output_data = df.to_dict(orient='records')

# #     # Suppression du champ or_validation_status pour chaque enregistrement
# #     for record in output_data:
# #         if 'or_validation_status' in record:
# #             del record['or_validation_status']

# #     # Sauvegarde du fichier de sortie
# #     try:
# #         os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
# #         with open(output_file_path, 'w', encoding='utf-8') as file:
# #             json.dump(output_data, file, ensure_ascii=False, indent=2)
# #         logger.info(f"Fichier de sortie sauvegardé avec succès: {output_file_path}")
# #     except Exception as e:
# #         logger.error(f"Erreur lors de la sauvegarde du fichier de sortie: {str(e)}")
# #         errors["general"].append({"error": f"Erreur de sauvegarde: {str(e)}"})
# #         return False, None

    
# #     # Génération du rapport d'erreurs si nécessaire
# #     has_errors = any(error_list for error_list in errors.values())
# #     error_report_path = None
    
# #     if has_errors:
# #         os.makedirs(error_report_dir, exist_ok=True)
# #         error_report_path = os.path.join(
# #             error_report_dir, 
# #             f"organizations_errors_{timestamp}.xlsx"
# #         )
# #         generate_error_report(errors, error_report_path, input_data)
# #         logger.info(f"Rapport d'erreurs généré: {error_report_path}")
# #     else:
# #         logger.info("Aucune erreur détectée, pas de rapport généré")
    
# #     logger.info("Traitement des données organizations terminé")
    
# #     return True, error_report_path

# """
# Module principal pour le traitement des données organizations.
# Responsable de l'orchestration complète du processus de transformation.
# """

# import json
# import logging
# import os
# from datetime import datetime
# from typing import Dict, List, Optional, Tuple, Any, Union

# import pandas as pd
# from src.tables.organizations.transformations.add_missing_fields import add_missing_fields
# from src.tables.organizations.transformations.validate_input_structure import validate_input_structure
# from src.tables.organizations.transformations.normalize_text import normalize_text
# from src.tables.organizations.transformations.normalize_special_chars import normalize_special_chars
# from src.tables.organizations.transformations.clean_punctuation import clean_punctuation
# from src.tables.organizations.transformations.validate_rna import validate_rna
# from src.tables.organizations.transformations.validate_address_fields import validate_address_fields
# from src.tables.organizations.transformations.patch_data import apply_patches
# from src.tables.organizations.transformations.prepare_final_model import prepare_final_model
# from src.tables.organizations.error_reporting.generate_error_report import generate_error_report
# from src.utils.logging_manager import setup_logger


# def clean_organizations_data(
#     input_file_path: str, 
#     output_file_path: str,
#     patches_dir: str = "data/patches",
#     error_report_dir: str = "data/error_report",
#     log_dir: str = "logs"
# ) -> Tuple[bool, Optional[str]]:
#     """
#     Fonction principale pour nettoyer et transformer les données organizations.
    
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
#     log_file = os.path.join(log_dir, f"organizations_transformation_{timestamp}.log")
#     logger = setup_logger("organizations_transformation", log_file)
    
#     logger.info(f"Démarrage du traitement des données organizations: {input_file_path}")
    
#     # Dictionnaire pour collecter les erreurs
#     errors = {
#         "structure": [],
#         "rna": [],
#         "address": [],
#         "general": [],
#         "duplicates": []  # Nouvelle catégorie d'erreurs pour les doublons
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
    
#     # Étape 3: Normalisation des caractères spéciaux
#     logger.info("Étape 3: Normalisation des caractères spéciaux")
#     df, special_chars_errors = normalize_special_chars(df)
#     if special_chars_errors:
#         errors["general"].extend(special_chars_errors)
    
#     # Étape 4: Nettoyage de la ponctuation
#     logger.info("Étape 4: Nettoyage de la ponctuation")
#     df, punctuation_errors = clean_punctuation(df)
#     if punctuation_errors:
#         errors["general"].extend(punctuation_errors)
    
#     # Étape 5: Validation du RNA
#     logger.info("Étape 5: Validation du RNA")
#     df, rna_errors = validate_rna(df)
#     if rna_errors:
#         errors["rna"].extend(rna_errors)
#         logger.warning(f"Détection de {len(rna_errors)} erreurs de RNA")
    
#     # Étape 6: Validation des champs d'adresse
#     logger.info("Étape 6: Validation des champs d'adresse")
#     df, address_errors = validate_address_fields(df)
#     if address_errors:
#         errors["address"].extend(address_errors)
#         logger.warning(f"Détection de {len(address_errors)} erreurs d'adresse")
    
#     # Étape 7: Ajout des champs manquants
#     logger.info("Étape 7: Ajout des champs manquants")
#     df, missing_fields_errors = add_missing_fields(df)
#     if missing_fields_errors:
#         errors["general"].extend(missing_fields_errors)

#     # Étape 8: Application des correctifs spécifiques
#     logger.info("Étape 8: Application des correctifs spécifiques")
#     patches_file = os.path.join(patches_dir, "organizations_patches.json")
#     if os.path.exists(patches_file):
#         df, patch_errors = apply_patches(df, patches_file)
#         if patch_errors:
#             errors["general"].extend(patch_errors)
#     else:
#         logger.info(f"Aucun fichier de correctifs trouvé: {patches_file}")

#     # Étape 9: Préparation du modèle final
#     logger.info("Étape 9: Préparation du modèle final")
#     df, final_errors = prepare_final_model(df)
#     if final_errors:
#         errors["general"].extend(final_errors)
    
#     # Étape 10: Vérification des doublons sur les champs clés
#     logger.info("Étape 10: Vérification des doublons")
#     duplicates_errors = check_duplicates(df)
#     if duplicates_errors:
#         errors["duplicates"].extend(duplicates_errors)
#         logger.warning(f"Détection de {len(duplicates_errors)} erreurs de doublons")
    
#     # Étape 11: Remplacement des valeurs null par des chaînes vides dans or_house_number
#     logger.info("Étape 11: Remplacement des valeurs null par des chaînes vides dans or_house_number")
#     df, null_replacement_errors = replace_null_with_empty_string(df, 'or_house_number')
#     if null_replacement_errors:
#         errors["general"].extend(null_replacement_errors)
    
#     # Vérification de la préservation des données
#     final_count = len(df)
#     if final_count != original_count:
#         message = f"ALERTE: Différence de nombre d'entrées - Original: {original_count}, Final: {final_count}"
#         logger.error(message)
#         errors["general"].append({"error": message})

#     # Conversion du DataFrame en liste de dictionnaires
#     output_data = df.to_dict(orient='records')

#     # Suppression du champ or_validation_status pour chaque enregistrement
#     for record in output_data:
#         if 'or_validation_status' in record:
#             del record['or_validation_status']

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

#     # Générer le rapport d'erreurs
#     # Note: Nous générons désormais un rapport même si seules des erreurs de doublons sont détectées
#     has_errors = any(error_list for error_list in errors.values())
#     error_report_path = None
    
#     if has_errors:
#         os.makedirs(error_report_dir, exist_ok=True)
#         error_report_path = os.path.join(
#             error_report_dir, 
#             f"organizations_errors_{timestamp}.xlsx"
#         )
#         generate_error_report(errors, error_report_path, input_data)
#         logger.info(f"Rapport d'erreurs généré: {error_report_path}")
#     else:
#         logger.info("Aucune erreur détectée, pas de rapport généré")
    
#     logger.info("Traitement des données organizations terminé")
    
#     return True, error_report_path


# def check_duplicates(df: pd.DataFrame) -> List[Dict[str, Any]]:
#     """
#     Vérifie la présence de doublons dans les champs clés du DataFrame.
    
#     Args:
#         df: DataFrame contenant les données Organizations
        
#     Returns:
#         List[Dict[str, Any]]: Liste des erreurs de doublons
#     """
#     errors = []
    
#     # Liste des champs à vérifier pour les doublons
#     key_fields = ['or_id', 'or_denomination']
    
#     # Vérifier aussi or_rna s'il est présent et non vide
#     if 'or_rna' in df.columns:
#         # Filtrer pour exclure les valeurs nulles ou vides
#         rna_df = df[~df['or_rna'].isna() & (df['or_rna'] != '')].copy()
#         if not rna_df.empty:
#             duplicates = rna_df[rna_df.duplicated('or_rna', keep=False)]
#             if not duplicates.empty:
#                 for _, row in duplicates.iterrows():
#                     errors.append({
#                         "type": "duplicate_rna",
#                         "severity": "error",
#                         "or_id": row['or_id'],
#                         "or_rna": row['or_rna'],
#                         "message": f"RNA '{row['or_rna']}' en doublon"
#                     })
    
#     # Vérifier les champs clés standards
#     for field in key_fields:
#         if field in df.columns:
#             duplicates = df[df.duplicated(field, keep=False)]
#             if not duplicates.empty:
#                 for _, row in duplicates.iterrows():
#                     errors.append({
#                         "type": f"duplicate_{field}",
#                         "severity": "error",
#                         "or_id": row['or_id'],
#                         "field": field,
#                         "value": row[field],
#                         "message": f"Valeur '{row[field]}' en doublon pour le champ '{field}'"
#                     })
    
#     return errors


# def replace_null_with_empty_string(
#     df: pd.DataFrame, 
#     field: str
# ) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
#     """
#     Remplace les valeurs null par des chaînes vides dans un champ spécifique.
    
#     Args:
#         df: DataFrame contenant les données
#         field: Nom du champ à traiter
        
#     Returns:
#         Tuple contenant:
#         - Le DataFrame avec les valeurs null remplacées
#         - La liste des modifications effectuées
#     """
#     errors = []
#     result_df = df.copy()
    
#     if field not in result_df.columns:
#         errors.append({
#             "type": "field_not_found",
#             "severity": "error",
#             "field": field,
#             "message": f"Le champ '{field}' n'existe pas dans le DataFrame"
#         })
#         return result_df, errors
    
#     # Compter les valeurs nulles avant remplacement
#     null_count = result_df[field].isna().sum()
    
#     # Identifier les indices des lignes avec des valeurs nulles
#     null_indices = result_df[result_df[field].isna()].index.tolist()
    
#     # Remplacer les valeurs null par des chaînes vides
#     result_df[field] = result_df[field].fillna("")
    
#     # Enregistrer l'information de modification
#     if null_count > 0:
#         errors.append({
#             "type": "null_to_empty_replacement",
#             "severity": "info",
#             "field": field,
#             "count": null_count,
#             "message": f"{null_count} valeurs null remplacées par des chaînes vides dans le champ '{field}'"
#         })
        
#         # Enregistrer des informations détaillées pour chaque ligne modifiée
#         for idx in null_indices:
#             or_id = result_df.at[idx, 'or_id']
#             errors.append({
#                 "type": "null_replacement_detail",
#                 "severity": "info",
#                 "or_id": or_id,
#                 "index": idx,
#                 "field": field,
#                 "message": f"Valeur null remplacée par chaîne vide pour or_id={or_id}"
#             })
    
#     return result_df, errors

"""
Module principal pour le traitement des données organizations.
Responsable de l'orchestration complète du processus de transformation.
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any, Union

import pandas as pd
from src.tables.organizations.transformations.add_missing_fields import add_missing_fields
from src.tables.organizations.transformations.validate_input_structure import validate_input_structure
from src.tables.organizations.transformations.normalize_text import normalize_text
from src.tables.organizations.transformations.normalize_special_chars import normalize_special_chars
from src.tables.organizations.transformations.clean_punctuation import clean_punctuation
from src.tables.organizations.transformations.validate_rna import validate_rna
from src.tables.organizations.transformations.validate_address_fields import validate_address_fields
from src.tables.organizations.transformations.patch_data import apply_patches
from src.tables.organizations.transformations.prepare_final_model import prepare_final_model
from src.tables.organizations.error_reporting.generate_error_report import generate_error_report
from src.utils.logging_manager import setup_logger


def clean_organizations_data(
    input_file_path: str, 
    output_file_path: str,
    patches_dir: str = "data/patches",
    error_report_dir: str = "data/error_report",
    log_dir: str = "logs"
) -> Tuple[bool, Optional[str]]:
    """
    Fonction principale pour nettoyer et transformer les données organizations.
    
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
    log_file = os.path.join(log_dir, f"organizations_transformation_{timestamp}.log")
    logger = setup_logger("organizations_transformation", log_file)
    
    logger.info(f"Démarrage du traitement des données organizations: {input_file_path}")
    
    # Dictionnaire pour collecter les erreurs
    errors = {
        "structure": [],
        "rna": [],
        "address": [],
        "general": [],
        "duplicates": []  # Nouvelle catégorie d'erreurs pour les doublons
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
    
    # Étape 5: Validation du RNA
    logger.info("Étape 5: Validation du RNA")
    df, rna_errors = validate_rna(df)
    if rna_errors:
        errors["rna"].extend(rna_errors)
        logger.warning(f"Détection de {len(rna_errors)} erreurs de RNA")
    
    # Étape 6: Validation des champs d'adresse
    logger.info("Étape 6: Validation des champs d'adresse")
    df, address_errors = validate_address_fields(df)
    if address_errors:
        errors["address"].extend(address_errors)
        logger.warning(f"Détection de {len(address_errors)} erreurs d'adresse")
    
    # Étape 7: Ajout des champs manquants
    logger.info("Étape 7: Ajout des champs manquants")
    df, missing_fields_errors = add_missing_fields(df)
    if missing_fields_errors:
        errors["general"].extend(missing_fields_errors)

    # Étape 8: Application des correctifs spécifiques
    logger.info("Étape 8: Application des correctifs spécifiques")
    patches_file = os.path.join(patches_dir, "organizations_patches.json")
    if os.path.exists(patches_file):
        df, patch_errors = apply_patches(df, patches_file)
        if patch_errors:
            errors["general"].extend(patch_errors)
    else:
        logger.info(f"Aucun fichier de correctifs trouvé: {patches_file}")

    # Étape 9: Préparation du modèle final
    logger.info("Étape 9: Préparation du modèle final")
    df, final_errors = prepare_final_model(df)
    if final_errors:
        errors["general"].extend(final_errors)
    
    # Étape 10: Vérification des doublons sur les champs clés
    logger.info("Étape 10: Vérification des doublons")
    duplicates_errors = check_duplicates(df)
    if duplicates_errors:
        errors["duplicates"].extend(duplicates_errors)
        logger.warning(f"Détection de {len(duplicates_errors)} erreurs de doublons")
        
    # Vérification des RNA manquants - pour s'assurer que les erreurs sont correctement identifiées
    missing_rna_count = df['or_rna'].isna().sum() + (df['or_rna'] == '').sum()
    if missing_rna_count > 0:
        logger.warning(f"Détection de {missing_rna_count} RNA manquants")
    
    # Étape 11: Remplacement des valeurs null par des chaînes vides dans or_house_number
    logger.info("Étape 11: Remplacement des valeurs null par des chaînes vides dans or_house_number")
    df, null_replacement_errors = replace_null_with_empty_string(df, 'or_house_number')
    if null_replacement_errors:
        errors["general"].extend(null_replacement_errors)
    
    # Vérification de la préservation des données
    final_count = len(df)
    if final_count != original_count:
        message = f"ALERTE: Différence de nombre d'entrées - Original: {original_count}, Final: {final_count}"
        logger.error(message)
        errors["general"].append({"error": message})

    # Conversion du DataFrame en liste de dictionnaires
    output_data = df.to_dict(orient='records')

    # Suppression du champ or_validation_status pour chaque enregistrement
    for record in output_data:
        if 'or_validation_status' in record:
            del record['or_validation_status']

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

    # Générer le rapport d'erreurs
    # Note: Nous générons désormais un rapport même si seules des erreurs de doublons sont détectées
    has_errors = any(error_list for error_list in errors.values())
    error_report_path = None
    
    if has_errors:
        os.makedirs(error_report_dir, exist_ok=True)
        error_report_path = os.path.join(
            error_report_dir, 
            f"organizations_errors_{timestamp}.xlsx"
        )
        generate_error_report(errors, error_report_path, input_data)
        logger.info(f"Rapport d'erreurs généré: {error_report_path}")
    else:
        logger.info("Aucune erreur détectée, pas de rapport généré")
    
    logger.info("Traitement des données organizations terminé")
    
    return True, error_report_path


def check_duplicates(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Vérifie la présence de doublons dans les champs clés du DataFrame.
    
    Args:
        df: DataFrame contenant les données Organizations
        
    Returns:
        List[Dict[str, Any]]: Liste des erreurs de doublons
    """
    errors = []
    
    # Liste des champs à vérifier pour les doublons
    key_fields = ['or_id', 'or_denomination']
    
    # Vérifier aussi or_rna s'il est présent et non vide
    if 'or_rna' in df.columns:
        # Filtrer pour exclure les valeurs nulles ou vides
        rna_df = df[~df['or_rna'].isna() & (df['or_rna'] != '')].copy()
        if not rna_df.empty:
            duplicates = rna_df[rna_df.duplicated('or_rna', keep=False)]
            if not duplicates.empty:
                for _, row in duplicates.iterrows():
                    errors.append({
                        "type": "duplicate_rna",
                        "severity": "error",
                        "or_id": row['or_id'],
                        "or_rna": row['or_rna'],
                        "message": f"RNA '{row['or_rna']}' en doublon"
                    })
    
    # Vérifier les champs clés standards
    for field in key_fields:
        if field in df.columns:
            duplicates = df[df.duplicated(field, keep=False)]
            if not duplicates.empty:
                for _, row in duplicates.iterrows():
                    errors.append({
                        "type": f"duplicate_{field}",
                        "severity": "error",
                        "or_id": row['or_id'],
                        "field": field,
                        "value": row[field],
                        "message": f"Valeur '{row[field]}' en doublon pour le champ '{field}'"
                    })
    
    return errors


def replace_null_with_empty_string(
    df: pd.DataFrame, 
    field: str
) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Remplace les valeurs null par des chaînes vides dans un champ spécifique.
    
    Args:
        df: DataFrame contenant les données
        field: Nom du champ à traiter
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les valeurs null remplacées
        - La liste des modifications effectuées
    """
    errors = []
    result_df = df.copy()
    
    if field not in result_df.columns:
        errors.append({
            "type": "field_not_found",
            "severity": "error",
            "field": field,
            "message": f"Le champ '{field}' n'existe pas dans le DataFrame"
        })
        return result_df, errors
    
    # Compter les valeurs nulles avant remplacement
    null_count = result_df[field].isna().sum()
    
    # Identifier les indices des lignes avec des valeurs nulles
    null_indices = result_df[result_df[field].isna()].index.tolist()
    
    # Remplacer les valeurs null par des chaînes vides
    result_df[field] = result_df[field].fillna("")
    
    # Enregistrer l'information de modification
    if null_count > 0:
        errors.append({
            "type": "null_to_empty_replacement",
            "severity": "info",
            "field": field,
            "count": null_count,
            "message": f"{null_count} valeurs null remplacées par des chaînes vides dans le champ '{field}'"
        })
        
        # Enregistrer des informations détaillées pour chaque ligne modifiée
        for idx in null_indices:
            or_id = result_df.at[idx, 'or_id']
            errors.append({
                "type": "null_replacement_detail",
                "severity": "info",
                "or_id": or_id,
                "index": idx,
                "field": field,
                "message": f"Valeur null remplacée par chaîne vide pour or_id={or_id}"
            })
    
    return result_df, errors