"""
Module de préparation du modèle final pour les données Companies.
Finalise le traitement des données et prépare la structure finale selon le modèle attendu.
"""

from typing import Dict, List, Tuple, Any, Optional

import pandas as pd

from src.tables.companies.output_structure import OUTPUT_SCHEMA


def prepare_final_model(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Prépare la structure finale des données selon le modèle de sortie attendu.
    
    Ce module:
    - Vérifie la présence des champs obligatoires
    - Renomme les champs selon le modèle Prisma
    - Déplace les informations entre champs
    - Supprime les champs temporaires
    - Crée le statut de validation pour chaque enregistrement
    
    Args:
        df: DataFrame contenant les données Companies traitées
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec la structure finale
        - La liste des erreurs/informations sur la préparation du modèle
    """
    errors = []
    result_df = df.copy()
    
    # Vérifier la présence des champs obligatoires
    required_fields = ["co_id", "co_business_name", "fk_us"]
    missing_fields = [field for field in required_fields if field not in result_df.columns]
    
    # Dans prepare_final_model.py, après les autres transformations
    # Vérification finale des codes postaux
    if 'co_head_office_postal_code' in result_df.columns:
        result_df['co_head_office_postal_code'] = result_df['co_head_office_postal_code'].apply(
            lambda x: '0' + x if isinstance(x, str) and x.isdigit() and len(x) == 4 else x
        )
        
    if missing_fields:
        for field in missing_fields:
            errors.append({
                "type": "missing_required_field",
                "severity": "error",
                "field": field,
                "message": f"Le champ obligatoire '{field}' est absent du DataFrame"
            })
        # On continue malgré les erreurs pour traiter au maximum les données
    
    # Transformation finale du modèle
    try:
        # 1. Déplacer co_normalized_business_name vers co_business_name
        if 'co_normalized_business_name' in result_df.columns:
            # Sauvegarde de l'ancienne valeur pour référence dans les logs
            original_business_names = result_df['co_business_name'].copy()
            
            # Remplacement
            result_df['co_business_name'] = result_df['co_normalized_business_name']
            
            # Suppression de la colonne temporaire
            result_df = result_df.drop(columns=['co_normalized_business_name'])
            
            errors.append({
                "type": "field_transformed",
                "severity": "info",
                "message": "co_normalized_business_name a été déplacé vers co_business_name puis supprimé"
            })
        
        # 2. Renommer les colonnes d'adresse selon le modèle Prisma
        column_mapping = {
            'co_head_office_street_name': 'co_head_office_street',
            'co_head_office_street_number': 'co_head_office_number'
        }
        
        for old_name, new_name in column_mapping.items():
            if old_name in result_df.columns:
                result_df = result_df.rename(columns={old_name: new_name})
                errors.append({
                    "type": "field_renamed",
                    "severity": "info",
                    "old_name": old_name,
                    "new_name": new_name,
                    "message": f"Colonne '{old_name}' renommée en '{new_name}'"
                })
        
        # 3. Suppression de co_head_office_address après vérification
        if ('co_head_office_street' in result_df.columns and 
            'co_head_office_number' in result_df.columns and
            'co_head_office_address' in result_df.columns):
            
            result_df = result_df.drop(columns=['co_head_office_address'])
            
            errors.append({
                "type": "field_removed",
                "severity": "info",
                "field": "co_head_office_address",
                "message": "co_head_office_address a été supprimé car les informations ont été extraites"
            })
        
        # 4. Ajouter les champs manquants requis par le modèle Prisma
        missing_prisma_fields = {
            "co_head_office_country": "FRANCE",  # Valeur par défaut pour la France
            "co_desactivation_date": None        # Valeur par défaut à null
        }

        for field, default_value in missing_prisma_fields.items():
            if field not in result_df.columns:
                result_df[field] = default_value
                errors.append({
                    "type": "field_added",
                    "severity": "info",
                    "field": field,
                    "default_value": str(default_value),
                    "message": f"Champ '{field}' ajouté avec la valeur par défaut"
        })
                
    except Exception as e:
        errors.append({
            "type": "model_transformation_error",
            "severity": "error",
            "message": f"Erreur lors de la transformation finale du modèle: {str(e)}"
        })
    
    # Créer le champ de statut de validation pour chaque enregistrement
    try:
        # Initialisation du champ validation_status
        validation_status = []
        
        for idx, row in result_df.iterrows():
            co_id = row['co_id']
            
            # Structure du statut de validation
            status = {
                "is_valid": True,
                "field_status": {}
            }
            
            # Vérification des champs obligatoires
            for field in required_fields:
                if field in result_df.columns:
                    value = row[field]
                    is_valid = not pd.isna(value) and value != ""
                    status["field_status"][field] = is_valid
                    
                    if not is_valid:
                        status["is_valid"] = False
            
            # Vérification spécifique des identifiants
            # SIREN
            if 'co_siren' in result_df.columns and not pd.isna(row['co_siren']):
                siren = row['co_siren']
                is_valid_siren = len(str(siren)) == 9 if siren else False
                status["field_status"]["co_siren"] = is_valid_siren
                
                if not is_valid_siren:
                    status["is_valid"] = False
            
            # SIRET
            if 'co_siret' in result_df.columns and not pd.isna(row['co_siret']):
                siret = row['co_siret']
                is_valid_siret = len(str(siret)) == 14 if siret else False
                status["field_status"]["co_siret"] = is_valid_siret
                
                if not is_valid_siret:
                    status["is_valid"] = False
            
            # TVA
            if 'co_vat' in result_df.columns and not pd.isna(row['co_vat']):
                vat = row['co_vat']
                is_valid_vat = vat.startswith("FR") and len(vat) == 13 if vat else False
                status["field_status"]["co_vat"] = is_valid_vat
                
                if not is_valid_vat and vat:
                    status["is_valid"] = False
            
            validation_status.append(status)
        
        # Ajout du statut de validation au DataFrame
        result_df['co_validation_status'] = validation_status
        
    except Exception as e:
        errors.append({
            "type": "validation_status_creation_error",
            "severity": "error",
            "message": f"Erreur lors de la création des statuts de validation: {str(e)}"
        })
    
    # Vérification/création des champs décomposés d'adresse s'ils n'existent pas
    # en utilisant les nouveaux noms conformes au modèle Prisma
    address_fields = ["co_head_office_number", "co_head_office_street"]
    for field in address_fields:
        if field not in result_df.columns:
            result_df[field] = None
            errors.append({
                "type": "missing_field_created",
                "severity": "info",
                "field": field,
                "message": f"Le champ '{field}' a été créé avec des valeurs null"
            })
    
    return result_df, errors