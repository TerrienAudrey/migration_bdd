#!/usr/bin/env python
"""
Script d'exécution autonome pour la transformation des données Stock Import.
Permet de lancer le processus complet de transformation et nettoyage des données.
"""

import os
import sys
import json
from datetime import datetime

# Ajouter le répertoire racine au chemin Python
sys.path.append(os.path.abspath('.'))

from src.tables.stock_import.clean_stock_import import clean_stock_import_data


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
    sample_file = "data/raw/stock_import_sample.json"
    
    if not os.path.exists(sample_file):
        sample_data = [
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
                "si_date_delivery": "03/20/2023", # Format de date non standard pour tester la conversion
                "si_total_price": "8946.84", # Chaîne au lieu de nombre pour tester la conversion
                "fk_st": 5,
                "fk_co": 125
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
    print("TRANSFORMATION DES DONNÉES STOCK IMPORT")
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
    output_file = os.path.join("data/clean", f"stock_import_transformed_{timestamp}.json")
    
    print(f"\nTraitement en cours...")
    print(f"  Fichier d'entrée : {input_file}")
    print(f"  Fichier de sortie : {output_file}")
    
    # Exécuter la transformation
    success, error_report = clean_stock_import_data(
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