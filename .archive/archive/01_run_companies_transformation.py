#!/usr/bin/env python
"""
Script d'exécution autonome pour la transformation des données Companies.
Permet de lancer le processus complet de transformation et nettoyage des données.
"""

import os
import sys
import json
from datetime import datetime

# Ajouter le répertoire racine au chemin Python
sys.path.append(os.path.abspath('.'))

from src.tables.companies.clean_companies import clean_companies_data


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
    sample_file = "data/raw/companies_sample.json"
    
    if not os.path.exists(sample_file):
        sample_data = [
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
        ]
        
        with open(sample_file, 'w', encoding='utf-8') as f:
            json.dump(sample_data, f, ensure_ascii=False, indent=2)
            
        print(f"Fichier d'exemple créé : {sample_file}")
    else:
        print(f"Fichier d'exemple existant : {sample_file}")


def main():
    """Fonction principale d'exécution."""
    print("=" * 80)
    print("TRANSFORMATION DES DONNÉES COMPANIES")
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
    output_file = os.path.join("data/clean", f"companies_transformed_{timestamp}.json")
    
    print(f"\nTraitement en cours...")
    print(f"  Fichier d'entrée : {input_file}")
    print(f"  Fichier de sortie : {output_file}")
    
    # Exécuter la transformation
    success, error_report = clean_companies_data(
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