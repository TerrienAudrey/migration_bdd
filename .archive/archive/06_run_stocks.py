#!/usr/bin/env python
"""
Script d'exécution autonome pour la transformation des données Stocks.
Permet de lancer le processus complet de transformation et nettoyage des données.
"""

import os
import sys
import json
from datetime import datetime

# Ajouter le répertoire racine au chemin Python
sys.path.append(os.path.abspath('.'))

from src.tables.stocks.clean_stocks import clean_stocks_data


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


def check_sample_data():
    """Vérifie si des données d'exemple existent, sinon en crée."""
    sample_file = "data/raw/stocks_sample.json"
    
    if not os.path.exists(sample_file):
        sample_data = [
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
                "stock_import": [
                    0,
                    1,
                    2,
                    3
                ]
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
                "stock_import": [
                    4
                ]
            }
        ]
        
        with open(sample_file, 'w', encoding='utf-8') as f:
            json.dump(sample_data, f, ensure_ascii=False, indent=2)
            
        print(f"Fichier d'exemple créé : {sample_file}")
    else:
        print(f"Fichier d'exemple existant : {sample_file}")


def main():
    """Fonction principale d'exécution."""
    print("=" * 80)
    print("TRANSFORMATION DES DONNÉES STOCKS")
    print("=" * 80)
    
    # Créer la structure de dossiers
    create_directory_structure()
    
    # Vérifier les données d'exemple
    check_sample_data()
    
    # Demander à l'utilisateur le fichier d'entrée
    print("\nFichiers disponibles dans data/raw/:")
    raw_files = [f for f in os.listdir("data/raw") if f.endswith(".json")]
    
    if not raw_files:
        print("Aucun fichier JSON trouvé dans data/raw/")
        return
    
    for i, file in enumerate(raw_files):
        print(f"  {i+1}. {file}")
    
    try:
        choice = int(input("\nChoisissez un fichier (numéro) : "))
        if choice < 1 or choice > len(raw_files):
            print("Choix invalide.")
            return
            
        input_file = os.path.join("data/raw", raw_files[choice-1])
    except ValueError:
        print("Entrée invalide.")
        return
    
    # Générer le nom du fichier de sortie
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join("data/clean", f"stocks_transformed_{timestamp}.json")
    
    print(f"\nTraitement en cours...")
    print(f"  Fichier d'entrée : {input_file}")
    print(f"  Fichier de sortie : {output_file}")
    
    # Exécuter la transformation
    success, error_report = clean_stocks_data(
        input_file_path=input_file,
        output_file_path=output_file
    )
    
    # Afficher le résultat
    print("\nRésultat de la transformation :")
    if success:
        print(f"✅ Transformation réussie.")
        print(f"   Fichier de sortie : {output_file}")
        
        if error_report:
            print(f"⚠️  Des erreurs ou avertissements ont été détectés.")
            print(f"   Rapport d'erreurs : {error_report}")
        else:
            print(f"✅ Aucune erreur détectée.")
    else:
        print(f"❌ Échec de la transformation.")
        if error_report:
            print(f"   Consultez le rapport d'erreurs : {error_report}")
    
    print("\nTraitement terminé.")


if __name__ == "__main__":
    main()