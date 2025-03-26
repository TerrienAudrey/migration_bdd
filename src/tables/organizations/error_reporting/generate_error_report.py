# """
# Module de génération de rapport d'erreurs pour les données Organizations.
# Crée un fichier Excel détaillant les erreurs et modifications effectuées.
# """

# import os
# from typing import Dict, List, Any

# import pandas as pd


# def generate_error_report(
#     errors: Dict[str, List[Dict[str, Any]]],
#     output_path: str,
#     original_data: List[Dict[str, Any]]
# ) -> None:
#     """
#     Génère un rapport d'erreurs au format Excel.
    
#     Le rapport contient plusieurs onglets:
#     - Résumé global des erreurs
#     - Onglets détaillés par type d'erreur
#     - Informations sur les modifications effectuées
    
#     Args:
#         errors: Dictionnaire des erreurs par catégorie
#         output_path: Chemin de sortie pour le fichier Excel
#         original_data: Données originales pour référence
        
#     Returns:
#         None
#     """
#     # Créer le dossier de sortie si nécessaire
#     os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
#     # Créer un writer Excel avec pandas
#     with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
#         # Onglet de résumé
#         summary_data = []
#         total_errors = 0
        
#         for category, error_list in errors.items():
#             # Filtrer pour ne compter que les erreurs réelles (pas les infos)
#             real_errors = [e for e in error_list if e.get("severity", "") in ["error", "warning"]]
#             count = len(real_errors)
#             total_errors += count
            
#             if count > 0:
#                 summary_data.append({
#                     "Catégorie": category,
#                     "Nombre d'erreurs": count,
#                     "Pourcentage": f"{(count / len(original_data) * 100):.2f}%" if original_data else "N/A"
#                 })
        
#         # Ajouter une ligne de total
#         summary_data.append({
#             "Catégorie": "TOTAL",
#             "Nombre d'erreurs": total_errors,
#             "Pourcentage": f"{(total_errors / len(original_data) * 100):.2f}%" if original_data else "N/A"
#         })
        
#         # Création du DataFrame de résumé et écriture dans Excel
#         if summary_data:
#             summary_df = pd.DataFrame(summary_data)
#             summary_df.to_excel(writer, sheet_name="Résumé", index=False)
            
#             # Formatage de la feuille de résumé
#             workbook = writer.book
#             summary_sheet = writer.sheets["Résumé"]
            
#             # Format pour les titres et totaux
#             header_format = workbook.add_format({
#                 'bold': True,
#                 'bg_color': '#D8E4BC',
#                 'border': 1
#             })
            
#             total_format = workbook.add_format({
#                 'bold': True,
#                 'bg_color': '#E6E6E6',
#                 'border': 1
#             })
            
#             # Appliquer les formats
#             for col_num, _ in enumerate(summary_df.columns):
#                 summary_sheet.write(0, col_num, summary_df.columns[col_num], header_format)
#                 summary_sheet.write(len(summary_df), col_num, summary_df.iloc[-1, col_num], total_format)
            
#             # Ajuster la largeur des colonnes
#             summary_sheet.set_column(0, 0, 25)
#             summary_sheet.set_column(1, 1, 20)
#             summary_sheet.set_column(2, 2, 15)
        
#         # Création des onglets détaillés par catégorie d'erreur
#         for category, error_list in errors.items():
#             if not error_list:
#                 continue
                
#             # Convertir la liste d'erreurs en DataFrame
#             # Normaliser les structures qui peuvent varier
#             normalized_errors = []
            
#             for error in error_list:
#                 # Créer une copie pour ne pas modifier l'original
#                 error_copy = error.copy()
                
#                 # Extraire les valeurs de premier niveau
#                 normalized_error = {
#                     "Type": error_copy.pop("type", "unknown"),
#                     "Sévérité": error_copy.pop("severity", "info"),
#                     "ID": error_copy.pop("or_id", ""),
#                     "Index": error_copy.pop("index", ""),
#                     "Message": error_copy.pop("message", error_copy.pop("reason", "")),
#                 }
                
#                 # Ajouter les autres champs spécifiques
#                 for key, value in error_copy.items():
#                     if key not in normalized_error:
#                         normalized_error[key] = value
                
#                 normalized_errors.append(normalized_error)
            
#             if normalized_errors:
#                 # Créer le DataFrame et l'écrire dans Excel
#                 error_df = pd.DataFrame(normalized_errors)
                
#                 # Limiter la longueur du nom de l'onglet à 31 caractères (limite Excel)
#                 sheet_name = category[:30] if len(category) > 30 else category
                
#                 error_df.to_excel(writer, sheet_name=sheet_name, index=False)
                
#                 # Formatage de la feuille d'erreurs
#                 error_sheet = writer.sheets[sheet_name]
                
#                 # Format pour les entêtes
#                 header_format = workbook.add_format({
#                     'bold': True,
#                     'bg_color': '#D8E4BC',
#                     'border': 1
#                 })
                
#                 # Format pour les erreurs et avertissements
#                 error_format = workbook.add_format({'bg_color': '#FFC7CE'})
#                 warning_format = workbook.add_format({'bg_color': '#FFEB9C'})
#                 info_format = workbook.add_format({'bg_color': '#DDEBF7'})
                
#                 # Appliquer les formats d'entête
#                 for col_num, _ in enumerate(error_df.columns):
#                     error_sheet.write(0, col_num, error_df.columns[col_num], header_format)
                
#                 # Appliquer les formats conditionnels
#                 severity_col = error_df.columns.get_loc("Sévérité") if "Sévérité" in error_df.columns else None
                
#                 if severity_col is not None:
#                     # Formatage conditionnel basé sur la sévérité
#                     for row_num, row in enumerate(error_df.values):
#                         severity = row[severity_col]
#                         if severity == "error":
#                             error_sheet.set_row(row_num + 1, None, error_format)
#                         elif severity == "warning":
#                             error_sheet.set_row(row_num + 1, None, warning_format)
#                         elif severity == "info":
#                             error_sheet.set_row(row_num + 1, None, info_format)
                
#                 # Ajuster la largeur des colonnes automatiquement
#                 for i, col in enumerate(error_df.columns):
#                     # Calculer la longueur maximale
#                     max_len = max(
#                         error_df[col].astype(str).map(len).max(),
#                         len(str(col))
#                     ) + 2  # Ajouter une marge
                    
#                     error_sheet.set_column(i, i, min(max_len, 50))  # Limiter à 50 caractères max
        
#         # Création d'un onglet avec les données originales pour référence
#         if original_data:
#             original_df = pd.DataFrame(original_data)
#             original_df.to_excel(writer, sheet_name="Données originales", index=False)
            
#             # Formatage de base
#             original_sheet = writer.sheets["Données originales"]
#             header_format = workbook.add_format({
#                 'bold': True,
#                 'bg_color': '#D8E4BC',
#                 'border': 1
#             })
            
#             for col_num, _ in enumerate(original_df.columns):
#                 original_sheet.write(0, col_num, original_df.columns[col_num], header_format)
            

#             # Ajuster la largeur des colonnes automatiquement
#             for i, col in enumerate(original_df.columns):
#                 max_len = max(
#                     original_df[col].astype(str).map(len).max(),
#                     len(str(col))
#                 ) + 2
#                 original_sheet.set_column(i, i, min(max_len, 30))
    
#     # Calcul de statistiques sur les erreurs
#     if total_errors > 0:
#         stats_data = []
        
#         # Compter les erreurs par type
#         error_types = {}
#         for category, error_list in errors.items():
#             for error in error_list:
#                 error_type = error.get("type", "unknown")
#                 severity = error.get("severity", "info")
                
#                 if severity in ["error", "warning"]:
#                     if error_type not in error_types:
#                         error_types[error_type] = 0
#                     error_types[error_type] += 1
        
#         # Trier par fréquence
#         sorted_types = sorted(error_types.items(), key=lambda x: x[1], reverse=True)
        
#         for error_type, count in sorted_types:
#             stats_data.append({
#                 "Type d'erreur": error_type,
#                 "Nombre": count,
#                 "Pourcentage": f"{(count / total_errors * 100):.2f}%"
#             })
        
#         # Création du DataFrame de statistiques et écriture dans Excel
#         if stats_data:
#             stats_df = pd.DataFrame(stats_data)
#             stats_df.to_excel(writer, sheet_name="Statistiques", index=False)
            
#             # Formatage
#             stats_sheet = writer.sheets["Statistiques"]
            
#             # Format pour les entêtes
#             header_format = workbook.add_format({
#                 'bold': True,
#                 'bg_color': '#D8E4BC',
#                 'border': 1
#             })
            
#             # Appliquer les formats
#             for col_num, _ in enumerate(stats_df.columns):
#                 stats_sheet.write(0, col_num, stats_df.columns[col_num], header_format)
            
#             # Ajuster la largeur des colonnes
#             stats_sheet.set_column(0, 0, 30)
#             stats_sheet.set_column(1, 1, 15)
#             stats_sheet.set_column(2, 2, 15)

"""
Module de génération de rapport d'erreurs pour les données Organizations.
Crée un fichier Excel détaillant les erreurs et modifications effectuées,
avec une mise en évidence des erreurs critiques comme les doublons et les valeurs manquantes.
"""

import os
from typing import Dict, List, Any

import pandas as pd


def generate_error_report(
    errors: Dict[str, List[Dict[str, Any]]],
    output_path: str,
    original_data: List[Dict[str, Any]]
) -> None:
    """
    Génère un rapport d'erreurs au format Excel.
    
    Le rapport contient plusieurs onglets:
    - Résumé global des erreurs
    - Onglets détaillés par type d'erreur
    - Résumé spécifique des champs manquants et doublons
    - Informations sur les modifications effectuées
    
    Args:
        errors: Dictionnaire des erreurs par catégorie
        output_path: Chemin de sortie pour le fichier Excel
        original_data: Données originales pour référence
        
    Returns:
        None
    """
    # Créer le dossier de sortie si nécessaire
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Créer un writer Excel avec pandas
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        # Onglet de résumé
        summary_data = []
        total_errors = 0
        total_warnings = 0
        total_infos = 0
        
        for category, error_list in errors.items():
            # Compter les différentes sévérités d'erreurs
            error_count = len([e for e in error_list if e.get("severity", "") == "error"])
            warning_count = len([e for e in error_list if e.get("severity", "") == "warning"])
            info_count = len([e for e in error_list if e.get("severity", "") == "info"])
            
            total_errors += error_count
            total_warnings += warning_count
            total_infos += info_count
            
            if error_count > 0 or warning_count > 0:
                summary_data.append({
                    "Catégorie": category,
                    "Erreurs critiques": error_count,
                    "Avertissements": warning_count,
                    "Informations": info_count,
                    "Total": error_count + warning_count + info_count,
                    "Pourcentage": f"{((error_count + warning_count) / len(original_data) * 100):.2f}%" if original_data else "N/A"
                })
        
        # Ajouter une ligne de total
        summary_data.append({
            "Catégorie": "TOTAL",
            "Erreurs critiques": total_errors,
            "Avertissements": total_warnings,
            "Informations": total_infos,
            "Total": total_errors + total_warnings + total_infos,
            "Pourcentage": f"{((total_errors + total_warnings) / len(original_data) * 100):.2f}%" if original_data else "N/A"
        })
        
        # Création du DataFrame de résumé et écriture dans Excel
        if summary_data:
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name="Résumé", index=False)
            
            # Formatage de la feuille de résumé
            workbook = writer.book
            summary_sheet = writer.sheets["Résumé"]
            
            # Format pour les titres et totaux
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#D8E4BC',
                'border': 1
            })
            
            total_format = workbook.add_format({
                'bold': True,
                'bg_color': '#E6E6E6',
                'border': 1
            })
            
            # Appliquer les formats
            for col_num, _ in enumerate(summary_df.columns):
                summary_sheet.write(0, col_num, summary_df.columns[col_num], header_format)
                summary_sheet.write(len(summary_df), col_num, summary_df.iloc[-1, col_num], total_format)
            
            # Ajuster la largeur des colonnes
            summary_sheet.set_column(0, 0, 25)
            summary_sheet.set_column(1, 5, 15)
        
        # AMÉLIORATION: Créer un onglet spécifique pour les champs manquants et les doublons
        missing_data = []
        for category, error_list in errors.items():
            for error in error_list:
                if error.get("type") in ["missing_rna", "missing_column", "duplicate_or_id", "duplicate_rna", "duplicate_or_denomination"]:
                    missing_data.append({
                        "Catégorie": category,
                        "Type": error.get("type", ""),
                        "Sévérité": error.get("severity", ""),
                        "ID": error.get("or_id", ""),
                        "Champ": error.get("field", ""),
                        "Valeur": error.get("value", ""),
                        "Message": error.get("message", "")
                    })
        
        if missing_data:
            missing_df = pd.DataFrame(missing_data)
            missing_df.to_excel(writer, sheet_name="Champs critiques", index=False)
            
            # Formatage de l'onglet des champs critiques
            critical_sheet = writer.sheets["Champs critiques"]
            
            # Format pour les entêtes
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#D8E4BC',
                'border': 1
            })
            
            # Appliquer le format d'entête
            for col_num, _ in enumerate(missing_df.columns):
                critical_sheet.write(0, col_num, missing_df.columns[col_num], header_format)
            
            # Ajuster la largeur des colonnes
            for i, col in enumerate(missing_df.columns):
                # Calculer la longueur maximale
                max_len = max(
                    missing_df[col].astype(str).map(len).max(),
                    len(str(col))
                ) + 2  # Ajouter une marge
                
                critical_sheet.set_column(i, i, min(max_len, 50))  # Limiter à 50 caractères max
        
        # Création des onglets détaillés par catégorie d'erreur
        for category, error_list in errors.items():
            if not error_list:
                continue
                
            # Convertir la liste d'erreurs en DataFrame
            # Normaliser les structures qui peuvent varier
            normalized_errors = []
            
            for error in error_list:
                # Créer une copie pour ne pas modifier l'original
                error_copy = error.copy()
                
                # Récupérer l'ID de l'organisation
                or_id = error_copy.pop("or_id", "")
                
                # Extraire les valeurs de premier niveau
                normalized_error = {
                    "Type": error_copy.pop("type", "unknown"),
                    "Sévérité": error_copy.pop("severity", "info"),
                    "ID": or_id,
                    "Index": error_copy.pop("index", ""),
                    "Message": error_copy.pop("message", error_copy.pop("reason", "")),
                }
                
                # Ajouter les informations d'identification clés si l'identifiant existe
                if or_id and original_data:
                    # Rechercher l'enregistrement original correspondant
                    org_record = next((item for item in original_data if item.get("or_id") == or_id), None)
                    if org_record:
                        # Ajouter les informations clés qui aideront à identifier l'enregistrement
                        normalized_error["Dénomination"] = org_record.get("or_denomination", "")
                        normalized_error["RNA"] = org_record.get("or_rna", "")
                        # Si les champs SIREN/SIRET existent dans les données originales, les ajouter
                        if "or_siret" in org_record:
                            normalized_error["SIRET"] = org_record.get("or_siret", "")
                        if "or_siren" in org_record:
                            normalized_error["SIREN"] = org_record.get("or_siren", "")
                
                # Ajouter les autres champs spécifiques
                for key, value in error_copy.items():
                    if key not in normalized_error:
                        normalized_error[key] = value
                
                normalized_errors.append(normalized_error)
            
            if normalized_errors:
                # Créer le DataFrame et l'écrire dans Excel
                error_df = pd.DataFrame(normalized_errors)
                
                # Limiter la longueur du nom de l'onglet à 31 caractères (limite Excel)
                sheet_name = category[:30] if len(category) > 30 else category
                
                error_df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                # Formatage de la feuille d'erreurs
                error_sheet = writer.sheets[sheet_name]
                
                # Format pour les entêtes
                header_format = workbook.add_format({
                    'bold': True,
                    'bg_color': '#D8E4BC',
                    'border': 1
                })
                
                # Format pour les erreurs et avertissements
                error_format = workbook.add_format({'bg_color': '#FFC7CE'})
                warning_format = workbook.add_format({'bg_color': '#FFEB9C'})
                info_format = workbook.add_format({'bg_color': '#DDEBF7'})
                
                # Appliquer les formats d'entête
                for col_num, _ in enumerate(error_df.columns):
                    error_sheet.write(0, col_num, error_df.columns[col_num], header_format)
                
                # Appliquer les formats conditionnels
                severity_col = error_df.columns.get_loc("Sévérité") if "Sévérité" in error_df.columns else None
                
                if severity_col is not None:
                    # Formatage conditionnel basé sur la sévérité
                    for row_num, row in enumerate(error_df.values):
                        severity = row[severity_col]
                        if severity == "error":
                            error_sheet.set_row(row_num + 1, None, error_format)
                        elif severity == "warning":
                            error_sheet.set_row(row_num + 1, None, warning_format)
                        elif severity == "info":
                            error_sheet.set_row(row_num + 1, None, info_format)
                
                # Ajuster la largeur des colonnes automatiquement
                for i, col in enumerate(error_df.columns):
                    # Calculer la longueur maximale
                    max_len = max(
                        error_df[col].astype(str).map(len).max(),
                        len(str(col))
                    ) + 2  # Ajouter une marge
                    
                    error_sheet.set_column(i, i, min(max_len, 50))  # Limiter à 50 caractères max
        
        # Création d'un onglet avec les données originales pour référence
        if original_data:
            original_df = pd.DataFrame(original_data)
            original_df.to_excel(writer, sheet_name="Données originales", index=False)
            
            # Formatage de base
            original_sheet = writer.sheets["Données originales"]
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#D8E4BC',
                'border': 1
            })
            
            for col_num, _ in enumerate(original_df.columns):
                original_sheet.write(0, col_num, original_df.columns[col_num], header_format)
            
            # Ajuster la largeur des colonnes automatiquement
            for i, col in enumerate(original_df.columns):
                max_len = max(
                    original_df[col].astype(str).map(len).max(),
                    len(str(col))
                ) + 2
                original_sheet.set_column(i, i, min(max_len, 30))
    
    # Calcul de statistiques sur les erreurs
    if total_errors > 0 or total_warnings > 0:
        stats_data = []
        
        # Compter les erreurs par type
        error_types = {}
        for category, error_list in errors.items():
            for error in error_list:
                error_type = error.get("type", "unknown")
                severity = error.get("severity", "info")
                
                if severity in ["error", "warning"]:
                    if error_type not in error_types:
                        error_types[error_type] = 0
                    error_types[error_type] += 1
        
        # Trier par fréquence
        sorted_types = sorted(error_types.items(), key=lambda x: x[1], reverse=True)
        
        for error_type, count in sorted_types:
            stats_data.append({
                "Type d'erreur": error_type,
                "Nombre": count,
                "Pourcentage": f"{(count / (total_errors + total_warnings) * 100):.2f}%"
            })
        
        # Création du DataFrame de statistiques et écriture dans Excel
        if stats_data:
            stats_df = pd.DataFrame(stats_data)
            stats_df.to_excel(writer, sheet_name="Statistiques", index=False)
            
            # Formatage
            stats_sheet = writer.sheets["Statistiques"]
            
            # Format pour les entêtes
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#D8E4BC',
                'border': 1
            })
            
            # Appliquer les formats
            for col_num, _ in enumerate(stats_df.columns):
                stats_sheet.write(0, col_num, stats_df.columns[col_num], header_format)
            
            # Ajuster la largeur des colonnes
            stats_sheet.set_column(0, 0, 30)
            stats_sheet.set_column(1, 1, 15)
            stats_sheet.set_column(2, 2, 15)
