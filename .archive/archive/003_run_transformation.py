#!/usr/bin/env python
"""
Script unifié de transformation de données pour toutes les tables.
Permet de lancer une transformation spécifique ou toutes les transformations.
"""

import os
import sys
import json
import shutil
import argparse
from datetime import datetime
import logging
from typing import List, Dict, Any, Optional, Tuple, Union

# Ajouter le répertoire racine au chemin Python
sys.path.append(os.path.abspath('.'))

# Importer les modules de transformation
from src.tables.companies.clean_companies import clean_companies_data
from src.tables.organizations.clean_organizations import clean_organizations_data
from src.tables.logistic_address.clean_logistic_address import clean_logistic_address_data
from src.tables.stock_import.clean_stock_import import clean_stock_import_data
from src.tables.stocks.clean_stocks import clean_stocks_data
from src.tables.transports.clean_transports import clean_transports_data
from src.utils.logging_manager import setup_logger

# Dictionnaire des noms de tables et leurs fonctions de transformation associées
TRANSFORMATION_FUNCTIONS = {
    "companies": clean_companies_data,
    "organizations": clean_organizations_data,
    "logistic_address": clean_logistic_address_data,
    "stock_import": clean_stock_import_data,
    "stocks": clean_stocks_data,
    "transports": clean_transports_data
}

# Configuration des chemins
DATA_DIR = "data"
RAW_DIR = os.path.join(DATA_DIR, "raw")
CLEAN_DIR = os.path.join(DATA_DIR, "clean")
ARCHIVE_DIR = os.path.join(DATA_DIR, "archive")
PATCHES_DIR = os.path.join(DATA_DIR, "patches")
ERROR_REPORT_DIR = os.path.join(DATA_DIR, "error_report")
LOGS_DIR = "logs"

# Données d'exemples pour chaque table (utilisées uniquement si les données réelles sont absentes)
SAMPLE_DATA = {
    "companies": [
        {
            "co_id": 175,
            "co_business_name": "LUCAS FRANCE",
            "co_siren": "513703389",
            "co_siret": "51370338900017",
            "co_vat": "FR78513703389",
            "co_code_ent": "821747",
            "co_head_office_address": "ZONE ARTISANALE 2 GUILLEME",
            "co_head_office_city": "BAZAS",
            "co_head_office_postal_code": "33430",
            "co_legal_form": "SAS, société par actions simplifiée",
            "fk_us": 0,
            "co_head_office_additional_address": ""
        },
        {
            "co_id": 176,
            "co_business_name": "Établissements Martín et Fils",
            "co_siren": "542107651",
            "co_siret": "54210765100023",
            "co_vat": "FR71542107651",
            "co_code_ent": "752369",
            "co_head_office_address": "16, rue des Lilas",
            "co_head_office_city": "PARIS",
            "co_head_office_postal_code": "75008",
            "co_legal_form": "SARL",
            "fk_us": 12,
            "co_head_office_additional_address": "Bâtiment C, 3ème étage"
        }
    ],
    "organizations": [
        {
            "or_id": 7,
            "or_denomination": "ASSOCIATION LAIQUE POUR L'EDUCATION ET LA FORMATION PROFESSIONNELLE DES ADOLESCENTS (A.L.E.F.P.A)",
            "or_rna": "W595026866",
            "or_house_number": "199",
            "or_street": "COLBERT",
            "or_additional_address": "CENTRE VAUBAN 199-201 199/201 RUE COLBERT",
            "or_postal_code": "59800",
            "or_city": "LILLE",
            "or_id_address": 46446,
            "or_state": "Hauts-de-France"
        },
        {
            "or_id": 8,
            "or_denomination": "Fédération Française de Tennis",
            "or_rna": "W751204086",
            "or_house_number": "89",
            "or_street": "JEAN DE BEAUVAIS",
            "or_additional_address": "2ème étage",
            "or_postal_code": "75005",
            "or_city": "PARIS",
            "or_id_address": 46447,
            "or_state": "Ile-de-France"
        }
    ],
    "logistic_address": [
        {
            "la_id": 9,
            "la_house_number": "",
            "la_street": "87 RUE DE LA COMMANDERIE, 59500 DOUAI",
            "la_additional_address": "",
            "la_postal_code": "",
            "la_city": None,
            "la_truck_access": False,
            "la_loading_dock": False,
            "la_forklift": False,
            "la_pallet": False,
            "la_fenwick": False,
            "la_palet_capacity": 0,
            "la_longitude": 0,
            "la_latitude": 0,
            "la_isactive": False,
            "fk_co": 251,
            "fk_or": None,
            "stock_import": ["20230408--001-006-1"]
        },
        {
            "la_id": 306,
            "la_house_number": "199",
            "la_street": " COLBERT",
            "la_additional_address": "CENTRE VAUBAN 199-201  199/201 rue colbert ",
            "la_postal_code": "59800",
            "la_city": "Lille",
            "la_truck_access": False,
            "la_loading_dock": False,
            "la_forklift": False,
            "la_pallet": False,
            "la_fenwick": False,
            "la_palet_capacity": 0,
            "la_longitude": 0,
            "la_latitude": 0,
            "la_isactive": False,
            "fk_co": None,
            "fk_or": 7,
            "stock_import": ["20230310--005-002-1", "20231018-033-039-1", "20230527-003-010-1"]
        }
    ],
    "stock_import": [
        {
            "id_ope": "20230221--006-001",
            "si_id": 0,
            "si_io": "20230221--006-001-1",
            "si_date_removal": None,
            "si_date_delivery": "2023-03-01",
            "si_total_price": 56178.71,
            "fk_st": 0,
            "fk_co": 104
        },
        {
            "id_ope": "20230221--006-001",
            "si_id": 1,
            "si_io": "20230221--006-001-2",
            "si_date_removal": None,
            "si_date_delivery": "2023-03-01",
            "si_total_price": 56178.71,
            "fk_st": 0,
            "fk_co": 104
        },
        {
            "id_ope": "20230201--010-003",
            "si_id": 2,
            "si_io": "20230201--010-003-1",
            "si_date_removal": "2023-02-15",
            "si_date_delivery": "2023-02-25",
            "si_total_price": 12450.32,
            "fk_st": 1,
            "fk_co": 78
        },
        {
            "id_ope": "20230305--008-002",
            "si_id": 3,
            "si_io": "20230305--008-002-1",
            "si_date_removal": None,
            "si_date_delivery": "03/20/2023",
            "si_total_price": "8946.84",
            "fk_st": 5,
            "fk_co": 125
        }
    ],
    "stocks": [
        {
            "st_id": 0,
            "st_io": "20230221--006-001",
            "co_code_ent": "052282",
            "st_commission_%": 0.05,
            "st_commission": 2808.9355,
            "st_creation_date": "2023-02-21",
            "st_transportby": "OFFERT",
            "st_commentary": "REMISE EXCEPTIONNELLE POUR LA PREMIÈRE OPÉRATION (5% DE COMMISSION SUR LA VALEUR DU STOCK).",
            "st_is_freetransport": True,
            "fk_sta": 8,
            "fk_co": 104,
            "stock_import": [0, 1, 2, 3]
        },
        {
            "st_id": 1,
            "st_io": "20240311-153-103",
            "co_code_ent": "483367",
            "st_commission_%": 0.2,
            "st_commission": 6536.3919999999998,
            "st_creation_date": "2024-03-11",
            "st_transportby": "CLIENT",
            "st_commentary": "0",
            "st_is_freetransport": False,
            "fk_sta": 8,
            "fk_co": 86,
            "stock_import": [4]
        }
    ],
    "transports": [
        {
            "tra_id": 0,
            "tra_denomination": "CHRONOPOST",
            "stock_import": [176, 445, 454, 485, 506, 507, 530]
        },
        {
            "tra_id": 1,
            "tra_denomination": "TMF OPERATING",
            "stock_import": [162, 280, 216, 251, 72, 253, 180, 230, 125, 320, 299, 285, 326, 277, 273, 335, 221, 363]
        },
        {
            "tra_id": 2,
            "tra_denomination": "Geodis",
            "stock_import": [190, 190, 225, 350]
        },
        {
            "tra_id": 3,
            "tra_denomination": "GÉODIS",
            "stock_import": [410, 510]
        }
    ]
}


def create_directory_structure() -> None:
    """Crée la structure de dossiers nécessaire si elle n'existe pas déjà."""
    directories = [
        RAW_DIR,
        CLEAN_DIR,
        ARCHIVE_DIR,
        PATCHES_DIR,
        ERROR_REPORT_DIR,
        LOGS_DIR
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"Dossier créé ou vérifié : {directory}")


def check_for_duplicates(data: List[Dict[Any, Any]], table_name: str) -> List[Dict[str, Any]]:
    """
    Vérifie les doublons dans les données selon la table.
    
    Args:
        data: Liste des données à vérifier
        table_name: Nom de la table pour déterminer les champs à vérifier
        
    Returns:
        Liste des erreurs de doublons détectées
    """
    errors = []
    
    # Définie les champs uniques par table
    unique_fields = {
        "companies": ["co_siret", "co_business_name"],
        "organizations": ["or_rna", "or_denomination"],
        "stock_import": ["si_io"],
        "stocks": ["st_io"],
        "transports": ["tra_denomination"],
        "logistic_address": []  # Pas de contrainte d'unicité spécifique
    }
    
    # Si la table n'a pas de contraintes d'unicité, sortir
    if table_name not in unique_fields or not unique_fields[table_name]:
        return errors
    
    # Vérifier chaque champ unique
    for field in unique_fields[table_name]:
        seen_values = {}
        
        for index, item in enumerate(data):
            if field not in item or item[field] is None or item[field] == "":
                continue
                
            value = str(item[field])
            
            if value in seen_values:
                # Doublon trouvé
                errors.append({
                    "type": f"duplicate_{field}",
                    "severity": "error",
                    "field": field,
                    "value": value,
                    "first_index": seen_values[value],
                    "duplicate_index": index,
                    "message": f"Valeur en double pour {field}: '{value}' aux indices {seen_values[value]} et {index}"
                })
            else:
                seen_values[value] = index
    
    return errors


def ensure_input_file(table_name: str, input_file_path: Optional[str] = None) -> str:
    """
    S'assure qu'un fichier d'entrée existe pour la table spécifiée.
    Si aucun fichier n'est spécifié ou n'existe, utilise les données d'exemple.
    
    Args:
        table_name: Nom de la table
        input_file_path: Chemin optionnel vers un fichier d'entrée spécifique
        
    Returns:
        Chemin vers le fichier d'entrée à utiliser
    """
    # Si un fichier d'entrée est spécifié et existe, l'utiliser
    if input_file_path and os.path.exists(input_file_path):
        return input_file_path
    
    # Chemin par défaut pour le fichier d'entrée
    default_path = os.path.join(RAW_DIR, f"{table_name}.json")
    
    # Si le fichier par défaut existe, l'utiliser
    if os.path.exists(default_path):
        return default_path


def transform_table(table_name: str, input_file: Optional[str] = None, output_file: Optional[str] = None, check_dupes: bool = True) -> Tuple[bool, Optional[str]]:
    """
    Transforme une table spécifique.
    
    Args:
        table_name: Nom de la table à transformer
        input_file: Chemin optionnel vers le fichier d'entrée
        output_file: Chemin optionnel vers le fichier de sortie
        check_dupes: Indique si la vérification des doublons doit être effectuée
        
    Returns:
        Tuple contenant:
        - Un booléen indiquant si la transformation a réussi
        - Le chemin vers le rapport d'erreurs s'il a été généré, sinon None
    """
    # Vérifier que la table existe
    if table_name not in TRANSFORMATION_FUNCTIONS:
        print(f"❌ Table inconnue: {table_name}")
        return False, None


def transform_all_tables(check_dupes: bool = True) -> Dict[str, Tuple[bool, Optional[str]]]:
    """
    Transforme toutes les tables disponibles.
    
    Args:
        check_dupes: Indique si la vérification des doublons doit être effectuée
        
    Returns:
        Dictionnaire associant chaque table à son statut de transformation et rapport d'erreurs
    """
    results = {}
    success_count = 0
    failure_count = 0
    
    print("=" * 80)
    print(f"TRANSFORMATION DE TOUTES LES TABLES")
    print("=" * 80)
    
    # Création de la structure de dossiers
    create_directory_structure()
    
    # Transformation de chaque table
    for table_name in TRANSFORMATION_FUNCTIONS.keys():
        print("\n" + "-" * 40)
        print(f"TABLE: {table_name}")
        print("-" * 40)
        
        success, error_report = transform_table(table_name, check_dupes=check_dupes)
        results[table_name] = (success, error_report)
        
        if success:
            success_count += 1
        else:
            failure_count += 1
    
    # Résumé des résultats
    print("\n" + "=" * 80)
    print(f"RÉSUMÉ DES TRANSFORMATIONS")
    print("=" * 80)
    print(f"Tables traitées: {len(results)}")
    print(f"Succès: {success_count}")
    print(f"Échecs: {failure_count}")
    
    for table_name, (success, error_report) in results.items():
        status = "✅" if success else "❌"
        report_info = f" (Rapport: {error_report})" if error_report else ""
        print(f"{status} {table_name}{report_info}")
    
    return results


def list_available_tables() -> None:
    """Affiche la liste des tables disponibles pour transformation."""
    print("Tables disponibles pour transformation:")
    for table in sorted(TRANSFORMATION_FUNCTIONS.keys()):
        print(f"  - {table}")


def parse_arguments():
    """Parse les arguments de ligne de commande."""
    parser = argparse.ArgumentParser(description="Outil de transformation de données.")
    
    # Options générales
    parser.add_argument('--list', action='store_true', help="Liste toutes les tables disponibles")
    parser.add_argument('--all', action='store_true', help="Transforme toutes les tables")
    parser.add_argument('--no-dupes-check', action='store_true', help="Désactive la vérification des doublons")
    
    # Options spécifiques à une table
    parser.add_argument('--table', type=str, help="Nom de la table à transformer")
    parser.add_argument('--input', type=str, help="Chemin vers le fichier d'entrée (optionnel)")
    parser.add_argument('--output', type=str, help="Chemin vers le fichier de sortie (optionnel)")
    
    return parser.parse_args()


def main():
    """Fonction principale d'exécution."""
    args = parse_arguments()
    
    # Configuration des dossiers
    create_directory_structure()
    
    # Afficher la liste des tables
    if args.list:
        list_available_tables()
        return

    # Vérification des doublons
    check_dupes = not args.no_dupes_check
    
    # Transformer toutes les tables
    if args.all:
        transform_all_tables(check_dupes=check_dupes)
        return
    
    # Transformer une table spécifique
    if args.table:
        if args.table not in TRANSFORMATION_FUNCTIONS:
            print(f"❌ Table inconnue: {args.table}")
            print("Utilisez --list pour voir les tables disponibles.")
            return
        
        success, error_report = transform_table(
            table_name=args.table,
            input_file=args.input,
            output_file=args.output,
            check_dupes=check_dupes
        )
        
        if not success:
            print(f"❌ Transformation échouée pour la table {args.table}")
            exit(1)
        return
    
    # Si aucune option n'est spécifiée, afficher l'aide
    if not (args.list or args.all or args.table):
        print("Aucune action spécifiée. Utilisez --help pour voir les options disponibles.")
        print("\nExemples d'utilisation:")
        print("  python run_transformation.py --list")
        print("  python run_transformation.py --all")
        print("  python run_transformation.py --table companies")
        print("  python run_transformation.py --table organizations --input data/raw/custom_orgs.json")


if __name__ == "__main__":
    main()
    
    # Obtenir le fichier d'entrée
    input_file_path = ensure_input_file(table_name, input_file)
    
    # Définir le fichier de sortie
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if output_file:
        output_file_path = output_file
    else:
        output_file_path = os.path.join(CLEAN_DIR, f"{table_name}_{timestamp}.json")
    
    # Charger les données pour vérifier les doublons si demandé
    if check_dupes:
        try:
            with open(input_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Vérifier les doublons
            duplicate_errors = check_for_duplicates(data, table_name)
            if duplicate_errors:
                print(f"⚠️  Doublons détectés dans {table_name}:")
                for error in duplicate_errors:
                    print(f"  - {error['message']}")
                print("Traitement poursuivi malgré les doublons...")
        except Exception as e:
            print(f"⚠️  Erreur lors de la vérification des doublons: {str(e)}")
            print("Traitement poursuivi sans vérification des doublons...")
    
    # Exécuter la transformation
    print(f"🔄 Transformation de {table_name} en cours...")
    print(f"  Fichier d'entrée : {input_file_path}")
    print(f"  Fichier de sortie : {output_file_path}")
    
    try:
        transform_function = TRANSFORMATION_FUNCTIONS[table_name]
        success, error_report = transform_function(
            input_file_path=input_file_path,
            output_file_path=output_file_path,
            patches_dir=PATCHES_DIR,
            error_report_dir=ERROR_REPORT_DIR,
            log_dir=LOGS_DIR
        )
        
        if success:
            print(f"✅ Transformation de {table_name} réussie.")
            
            # Archiver les anciens fichiers
            archive_old_files(table_name)
            
            if error_report:
                print(f"⚠️  Des erreurs ou avertissements ont été détectés.")
                print(f"   Rapport d'erreurs : {error_report}")
            else:
                print(f"✅ Aucune erreur détectée pour {table_name}.")
        else:
            print(f"❌ Échec de la transformation de {table_name}.")
            if error_report:
                print(f"   Consultez le rapport d'erreurs : {error_report}")
        
        return success, error_report
        
    except Exception as e:
        print(f"❌ Erreur lors de la transformation de {table_name}: {str(e)}")
        return False, None
    
    # Sinon, créer un fichier d'exemple
    print(f"Aucun fichier d'entrée trouvé pour {table_name}, création d'un exemple...")
    
    # Vérifier si des données d'exemple existent pour cette table
    if table_name not in SAMPLE_DATA:
        raise ValueError(f"Aucune donnée d'exemple disponible pour la table '{table_name}'")
    
    # Créer le fichier d'exemple
    os.makedirs(os.path.dirname(default_path), exist_ok=True)
    with open(default_path, 'w', encoding='utf-8') as f:
        json.dump(SAMPLE_DATA[table_name], f, ensure_ascii=False, indent=2)
    
    print(f"Fichier d'exemple créé : {default_path}")
    return default_path


def archive_old_files(table_name: str) -> None:
    """
    Archive les anciennes versions des fichiers nettoyés.
    Garde uniquement la version la plus récente dans le dossier clean avec un nom simplifié.
    
    Args:
        table_name: Nom de la table dont les fichiers doivent être archivés
    """
    # Pattern pour les fichiers à rechercher
    base_name = f"{table_name}.json"
    clean_files = []
    
    # Recherche tous les fichiers correspondant à la table dans le dossier clean
    for file in os.listdir(CLEAN_DIR):
        if file.startswith(f"{table_name}_") and file.endswith(".json"):
            clean_files.append(file)
    
    # S'il y a plus d'un fichier, archiver tous sauf le plus récent
    if len(clean_files) > 0:
        # Trier par timestamp (qui est dans le nom du fichier)
        clean_files.sort(reverse=True)
        
        # Renommer le plus récent avec le nom de base
        newest_file = clean_files[0]
        newest_path = os.path.join(CLEAN_DIR, newest_file)
        base_path = os.path.join(CLEAN_DIR, base_name)
        
        # Si un fichier de base existe déjà, l'archiver d'abord
        if os.path.exists(base_path):
            archive_path = os.path.join(ARCHIVE_DIR, f"{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            shutil.move(base_path, archive_path)
            print(f"Fichier existant archivé: {base_path} → {archive_path}")
        
        # Renommer le plus récent
        shutil.copy(newest_path, base_path)
        print(f"Fichier le plus récent copié en tant que version principale: {newest_path} → {base_path}")
        
        # Archiver tous les autres fichiers (sauf le plus récent)
        for old_file in clean_files[1:]:
            old_path = os.path.join(CLEAN_DIR, old_file)
            archive_path = os.path.join(ARCHIVE_DIR, old_file)
            shutil.move(old_path, archive_path)
            print(f"Ancien fichier archivé: {old_path} → {archive_path}")