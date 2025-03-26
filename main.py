#!/usr/bin/env python
"""
Script principal de transformation des données.
Permet de lancer le processus complet de transformation et nettoyage des données
pour toutes les tables ou une table spécifique, avec archivage automatique des fichiers.
"""

import os
import sys
import argparse
import shutil
from datetime import datetime
from typing import Optional, Tuple, List, Dict
import glob
import re

# Ajouter le répertoire racine au chemin Python
sys.path.append(os.path.abspath('.'))

from src.tables.companies.clean_companies import clean_companies_data
from src.tables.organizations.clean_organizations import clean_organizations_data
from src.tables.logistic_address.clean_logistic_address import clean_logistic_address_data
from src.tables.transports.clean_transports import clean_transports_data
from src.tables.stock_import.clean_stock_import import clean_stock_import_data
from src.tables.stocks.clean_stocks import clean_stocks_data
from src.utils.logging_manager import setup_logger


def create_directory_structure():
    """Crée la structure de dossiers nécessaire si elle n'existe pas déjà."""
    directories = [
        "data/raw",
        "data/clean",
        "data/archive",
        "data/patches",
        "data/error_report",
        "logs"
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"Dossier créé ou vérifié : {directory}")


def get_available_files(data_type: str) -> List[str]:
    """
    Obtient la liste des fichiers disponibles pour un type de données.
    
    Args:
        data_type: Type de données (nom de la table)
        
    Returns:
        Liste des fichiers JSON disponibles
    """
    raw_dir = "data/raw"
    if not os.path.exists(raw_dir):
        return []
        
    return [f for f in os.listdir(raw_dir) if f.endswith(".json") and 
            (data_type.lower() in f.lower() or data_type == "all")]


def archive_previous_files(table_name: str, logger, current_file=None):
    """
    Archive tous les fichiers précédents du répertoire clean vers archive,
    à l'exception du fichier actuel spécifié (s'il est fourni).
    
    Args:
        table_name: Nom de la table pour filtrer les fichiers
        logger: Logger pour journaliser les opérations
        current_file: Chemin du fichier actuel à ne pas archiver (optionnel)
    """
    clean_dir = "data/clean"
    archive_dir = "data/archive"
    
    # S'assurer que le répertoire d'archive existe
    os.makedirs(archive_dir, exist_ok=True)
    
    # Rechercher tous les fichiers pour cette table dans le répertoire clean
    pattern = os.path.join(clean_dir, f"{table_name}_*.json")
    files = glob.glob(pattern)
    
    if not files:
        logger.info(f"Aucun fichier à archiver pour la table {table_name}")
        return
    
    # Conserver le fichier actuel (s'il est spécifié), archiver les autres
    files_to_archive = []
    for file_path in files:
        # Si un fichier actuel est spécifié et que c'est celui-ci, le conserver
        if current_file and os.path.abspath(file_path) == os.path.abspath(current_file):
            logger.info(f"Conservation du fichier actuel: {os.path.basename(file_path)}")
            continue
        files_to_archive.append(file_path)
    
    # Archiver tous les autres fichiers
    for file_path in files_to_archive:
        filename = os.path.basename(file_path)
        archive_path = os.path.join(archive_dir, filename)
        
        # S'assurer que le fichier de destination n'existe pas déjà
        if os.path.exists(archive_path):
            # Ajouter un suffixe pour éviter la collision
            base, ext = os.path.splitext(archive_path)
            archive_path = f"{base}_duplicate_{int(datetime.now().timestamp())}{ext}"
        
        try:
            shutil.move(file_path, archive_path)
            logger.info(f"Fichier archivé: {filename} → {os.path.basename(archive_path)}")
        except Exception as e:
            logger.error(f"Erreur lors de l'archivage de {filename}: {str(e)}")
            
    # Vérifier si nous avons réussi à archiver tous les fichiers
    remaining_files = glob.glob(pattern)
    if len(remaining_files) > 1 or (current_file is None and len(remaining_files) > 0):
        logger.warning(f"Certains fichiers n'ont pas été archivés: {[os.path.basename(f) for f in remaining_files]}")


def process_table(
    table_name: str,
    input_file: str,
    output_dir: str = "data/clean",
    patches_dir: str = "data/patches",
    error_report_dir: str = "data/error_report",
    log_dir: str = "logs",
    archive: bool = True
) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Traite une table spécifique.
    
    Args:
        table_name: Nom de la table à traiter
        input_file: Chemin vers le fichier d'entrée
        output_dir: Répertoire de sortie
        patches_dir: Répertoire des correctifs
        error_report_dir: Répertoire des rapports d'erreurs
        log_dir: Répertoire des logs
        archive: Indique si les fichiers précédents doivent être archivés
        
    Returns:
        Tuple contenant:
        - Succès de l'opération
        - Chemin du rapport d'erreurs (si généré)
        - Chemin du fichier de sortie
    """
    # Configuration du logger
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"{table_name}_{timestamp}.log")
    logger = setup_logger(f"{table_name}_processing", log_file)
    
    # Format du nom de fichier: table_timestamp.json (sans "transformed")
    output_filename = f"{table_name}_{timestamp}.json"
    output_file = os.path.join(output_dir, output_filename)
    
    logger.info(f"Traitement de la table {table_name}")
    logger.info(f"Fichier d'entrée: {input_file}")
    logger.info(f"Fichier de sortie: {output_file}")
    
    print(f"\nTraitement de la table {table_name}")
    print(f"  Fichier d'entrée: {input_file}")
    print(f"  Fichier de sortie: {output_file}")
    
    # Sélection de la fonction de nettoyage appropriée
    success = False
    error_report = None
    
    try:
        if table_name.lower() == "companies":
            success, error_report = clean_companies_data(input_file, output_file, patches_dir, error_report_dir, log_dir)
        elif table_name.lower() == "organizations":
            success, error_report = clean_organizations_data(input_file, output_file, patches_dir, error_report_dir, log_dir)
        elif table_name.lower() == "logistic_address":
            success, error_report = clean_logistic_address_data(input_file, output_file, patches_dir, error_report_dir, log_dir)
        elif table_name.lower() == "transports":
            success, error_report = clean_transports_data(input_file, output_file, patches_dir, error_report_dir, log_dir)
        elif table_name.lower() == "stock_import":
            success, error_report = clean_stock_import_data(input_file, output_file, patches_dir, error_report_dir, log_dir)
        elif table_name.lower() == "stocks":
            success, error_report = clean_stocks_data(input_file, output_file, patches_dir, error_report_dir, log_dir)
        else:
            logger.error(f"Table non reconnue: {table_name}")
            print(f"Table non reconnue: {table_name}")
            return False, None, None
            
        logger.info(f"Traitement {'réussi' if success else 'échoué'}")
        if error_report:
            logger.info(f"Rapport d'erreurs généré: {error_report}")
        
        # Si le traitement a réussi et l'archivage est activé, archiver tous les fichiers précédents
        if success and archive:
            logger.info("Archivage des fichiers précédents...")
            # Archiver tous les fichiers précédents en conservant uniquement le fichier actuel
            archive_previous_files(table_name, logger, current_file=output_file)
            
    except Exception as e:
        logger.error(f"Erreur lors du traitement de {table_name}: {str(e)}")
        print(f"Erreur: {str(e)}")
        success = False
        
    return success, error_report, output_file


def main():
    """Fonction principale d'exécution."""
    # Configuration du parser d'arguments
    parser = argparse.ArgumentParser(description="Transformation des données de tables")
    parser.add_argument("--table", type=str, default="all", 
                        help="Table à transformer (companies, organizations, logistic_address, transports, stock_import, stocks, all)")
    parser.add_argument("--input", type=str, default=None,
                        help="Fichier d'entrée spécifique (chemin complet)")
    parser.add_argument("--no-archive", action="store_true",
                        help="Désactive l'archivage automatique des anciens fichiers")
    args = parser.parse_args()
    
    # Configuration du logger principal
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join("logs", f"transformation_{timestamp}.log")
    logger = setup_logger("transformation_main", log_file)
    
    logger.info("Démarrage du processus de transformation")
    
    # Création de la structure de dossiers
    create_directory_structure()
    
    # Tables disponibles
    available_tables = ["companies", "organizations", "logistic_address", "transports", "stock_import", "stocks"]
    
    # Déterminer les tables à traiter
    tables_to_process = [args.table.lower()] if args.table.lower() != "all" else available_tables
    
    # Vérifier que la table demandée est valide
    if args.table.lower() != "all" and args.table.lower() not in available_tables:
        logger.error(f"Table invalide: {args.table}")
        print(f"Erreur: Table '{args.table}' non reconnue.")
        print(f"Tables disponibles: {', '.join(available_tables)} ou 'all'")
        return
    
    # Résultats globaux
    results = []
    
    # Traitement pour chaque table
    for table in tables_to_process:
        if args.input:
            # Utiliser le fichier spécifié par l'utilisateur
            input_files = [args.input]
        else:
            # Obtenir la liste des fichiers disponibles pour cette table
            input_files_relative = get_available_files(table)
            if not input_files_relative:
                logger.warning(f"Aucun fichier JSON trouvé pour la table {table}")
                print(f"Aucun fichier JSON trouvé pour la table {table}")
                continue
            
            # Transformer les chemins relatifs en chemins absolus
            input_files = [os.path.join("data/raw", f) for f in input_files_relative]
        
        # Traiter chaque fichier d'entrée
        for input_file in input_files:
            if not os.path.exists(input_file):
                logger.warning(f"Fichier non trouvé: {input_file}")
                print(f"Fichier non trouvé: {input_file}")
                continue
            
            # Traiter la table avec ou sans archivage selon l'option
            success, error_report, output_file = process_table(
                table, 
                input_file, 
                archive=not args.no_archive
            )
            
            # Enregistrer le résultat
            results.append({
                "table": table,
                "input_file": input_file,
                "output_file": output_file,
                "success": success,
                "error_report": error_report
            })
    
    # Affichage récapitulatif
    print("\n" + "="*80)
    print("RÉCAPITULATIF DES TRANSFORMATIONS")
    print("="*80)
    
    for result in results:
        status = "✅ Réussie" if result["success"] else "❌ Échec"
        print(f"Table: {result['table']}")
        print(f"  Fichier d'entrée: {os.path.basename(result['input_file'])}")
        if result["output_file"]:
            print(f"  Fichier de sortie: {os.path.basename(result['output_file'])}")
        print(f"  Statut: {status}")
        if result["error_report"]:
            print(f"  Rapport d'erreurs: {os.path.basename(result['error_report'])}")
        print("-"*40)
    
    logger.info("Fin du processus de transformation")


if __name__ == "__main__":
    main()