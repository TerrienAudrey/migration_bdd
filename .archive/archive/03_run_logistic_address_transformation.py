#!/usr/bin/env python
"""
Script d'exécution autonome pour la transformation des données Logistic Address.
Permet de lancer le processus complet de transformation et nettoyage des données.
"""

import os
import sys
import json
from datetime import datetime

# Ajouter le répertoire racine au chemin Python
sys.path.append(os.path.abspath('.'))

from src.tables.logistic_address.clean_logistic_address import clean_logistic_address_data


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
    sample_file = "data/raw/logistic_address_sample.json"
    
    if not os.path.exists(sample_file):
        sample_data = [
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
                "stock_import": [
                    "20230408--001-006-1"
                ]
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
                "stock_import": [
                    "20230310--005-002-1",
                    "20231018-033-039-1",
                    "20230527-003-010-1"
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
    print("TRANSFORMATION DES DONNÉES LOGISTIC ADDRESS")
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
    output_file = os.path.join("data/clean", f"logistic_address_transformed_{timestamp}.json")
    
    print(f"\nTraitement en cours...")
    print(f"  Fichier d'entrée : {input_file}")
    print(f"  Fichier de sortie : {output_file}")
    
    # Exécuter la transformation
    success, error_report = clean_logistic_address_data(
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