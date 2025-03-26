#!/usr/bin/env python
"""
Script principal de transformation des données.
Permet de lancer le processus complet de transformation et nettoyage des données
pour toutes les tables ou une table spécifique.
"""

import os
import sys
import argparse
from datetime import datetime
from typing import Optional, Tuple, List

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


def process_table(
    table_name: str,
    input_file: str,
    output_dir: str = "data/clean",
    patches_dir: str = "data/patches",
    error_report_dir: str = "data/error_report",
    log_dir: str = "logs"
) -> Tuple[bool, Optional[str]]:
    """
    Traite une table spécifique.
    
    Args:
        table_name: Nom de la table à traiter
        input_file: Chemin vers le fichier d'entrée
        output_dir: Répertoire de sortie
        patches_dir: Répertoire des correctifs
        error_report_dir: Répertoire des rapports d'erreurs
        log_dir: Répertoire des logs
        
    Returns:
        Tuple indiquant le succès et le chemin du rapport d'erreurs
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_dir, f"{table_name}_transformed_{timestamp}.json")
    
    print(f"\nTraitement de la table {table_name}")
    print(f"  Fichier d'entrée : {input_file}")
    print(f"  Fichier de sortie : {output_file}")
    
    # Sélection de la fonction de nettoyage appropriée
    if table_name.lower() == "companies":
        return clean_companies_data(input_file, output_file, patches_dir, error_report_dir, log_dir)
    elif table_name.lower() == "organizations":
        return clean_organizations_data(input_file, output_file, patches_dir, error_report_dir, log_dir)
    elif table_name.lower() == "logistic_address":
        return clean_logistic_address_data(input_file, output_file, patches_dir, error_report_dir, log_dir)
    elif table_name.lower() == "transports":
        return clean_transports_data(input_file, output_file, patches_dir, error_report_dir, log_dir)
    elif table_name.lower() == "stock_import":
        return clean_stock_import_data(input_file, output_file, patches_dir, error_report_dir, log_dir)
    elif table_name.lower() == "stocks":
        return clean_stocks_data(input_file, output_file, patches_dir, error_report_dir, log_dir)
    else:
        print(f"Table non reconnue: {table_name}")
        return False, None


def main():
    """Fonction principale d'exécution."""
    # Configuration du parser d'arguments
    parser = argparse.ArgumentParser(description="Transformation des données de tables")
    parser.add_argument("--table", type=str, default="all", 
                        help="Table à transformer (companies, organizations, logistic_address, transports, stock_import, stocks, all)")
    parser.add_argument("--input", type=str, default=None,
                        help="Fichier d'entrée spécifique (chemin complet)")
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
                
            success, error_report = process_table(table, input_file)
            
            # Enregistrer le résultat
            results.append({
                "table": table,
                "input_file": input_file,
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
        print(f"  Fichier: {os.path.basename(result['input_file'])}")
        print(f"  Statut: {status}")
        if result["error_report"]:
            print(f"  Rapport d'erreurs: {result['error_report']}")
        print("-"*40)
    
    logger.info("Fin du processus de transformation")


if __name__ == "__main__":
    main()