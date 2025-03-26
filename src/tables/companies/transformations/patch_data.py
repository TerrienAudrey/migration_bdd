"""
Module d'application des correctifs (patches) pour les données Companies.
Permet d'appliquer des corrections manuelles spécifiques à partir de fichiers JSON.
"""

import json
import os
from typing import Dict, List, Tuple, Any, Optional, Union

import pandas as pd


def apply_patches_siret_manquant(df: pd.DataFrame, patches_file_path: str) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Applique des correctifs spécifiques aux données Companies à partir de fichiers JSON.
    
    Cette fonction corrige les SIRET manquants ainsi que les VAT et formes juridiques
    en utilisant le fichier de correctifs.
    """
    errors = []
    result_df = df.copy()
    
    # Application des correctifs pour les SIRET manquants
    siret_patches_file = "data/patches/companies_siret_manquant.json"
    if os.path.exists(siret_patches_file):
        try:
            with open(siret_patches_file, 'r', encoding='utf-8') as file:
                siret_patches = json.load(file)
            
            # Création d'un dictionnaire SIREN -> informations complètes
            siren_to_info = {}
            for patch in siret_patches:
                if 'co_siren' in patch:
                    siren_to_info[patch['co_siren']] = patch
            
            # Compteurs
            patched_count = {
                'siret': 0,
                'vat': 0,
                'legal_form': 0
            }
            
            # Application des correctifs SIRET, VAT et forme juridique
            for idx, row in result_df.iterrows():
                siren = row.get('co_siren')
                
                # Si on a un SIREN et un correctif pour ce SIREN
                if not pd.isna(siren) and siren in siren_to_info:
                    patch_info = siren_to_info[siren]
                    changes = {}
                    
                    # SIRET
                    current_siret = row.get('co_siret')
                    if (pd.isna(current_siret) or current_siret == "" or 
                        current_siret == siren) and 'co_siret' in patch_info:
                        result_df.at[idx, 'co_siret'] = patch_info['co_siret']
                        changes['siret'] = {
                            'old': current_siret,
                            'new': patch_info['co_siret']
                        }
                        patched_count['siret'] += 1
                    
                    # VAT
                    current_vat = row.get('co_vat')
                    if (pd.isna(current_vat) or current_vat == "") and 'co_vat' in patch_info:
                        result_df.at[idx, 'co_vat'] = patch_info['co_vat']
                        changes['vat'] = {
                            'old': current_vat,
                            'new': patch_info['co_vat']
                        }
                        patched_count['vat'] += 1
                    
                    # Forme juridique
                    current_legal_form = row.get('co_legal_form')
                    if (pd.isna(current_legal_form) or current_legal_form == "") and 'co_legal_form' in patch_info:
                        result_df.at[idx, 'co_legal_form'] = patch_info['co_legal_form']
                        changes['legal_form'] = {
                            'old': current_legal_form,
                            'new': patch_info['co_legal_form']
                        }
                        patched_count['legal_form'] += 1
                    
                    # Enregistrer l'information des modifications
                    if changes:
                        errors.append({
                            "type": "company_info_patched",
                            "severity": "info",
                            "co_id": row.get('co_id'),
                            "index": idx,
                            "co_business_name": row.get('co_business_name'),
                            "co_siren": siren,
                            "changes": changes
                        })
            
            # Ajouter un résumé
            total_patches = sum(patched_count.values())
            if total_patches > 0:
                errors.append({
                    "type": "patch_summary",
                    "severity": "info",
                    "message": f"Correctifs appliqués: {patched_count['siret']} SIRET, {patched_count['vat']} VAT, {patched_count['legal_form']} formes juridiques"
                })
                
        except Exception as e:
            errors.append({
                "type": "patches_error",
                "severity": "error",
                "message": f"Erreur lors de l'application des correctifs: {str(e)}"
            })
    
    return result_df, errors

def apply_general_patches(df: pd.DataFrame, patches_file_path: str) -> List[Dict[str, Any]]:
    """
    Applique les correctifs généraux depuis le fichier de correctifs standard.
    """
    errors = []
    
    # Vérifier si le fichier de correctifs existe
    if not os.path.exists(patches_file_path):
        errors.append({
            "type": "patches_file_missing",
            "severity": "warning",
            "message": f"Le fichier de correctifs n'existe pas: {patches_file_path}"
        })
        return errors
    
    try:
        # Chargement du fichier de correctifs
        with open(patches_file_path, 'r', encoding='utf-8') as file:
            patches = json.load(file)
        
        # Vérification de la structure du fichier de correctifs
        if not isinstance(patches, list):
            errors.append({
                "type": "invalid_patches_format",
                "severity": "error",
                "message": "Le fichier de correctifs doit contenir une liste d'objets"
            })
            return errors
        
        # Application des correctifs
        for patch in patches:
            # Vérification de la structure de chaque correctif
            if not isinstance(patch, dict) or 'co_id' not in patch or 'patches' not in patch:
                errors.append({
                    "type": "invalid_patch_structure",
                    "severity": "error",
                    "patch": patch,
                    "message": "Chaque correctif doit contenir un 'co_id' et un objet 'patches'"
                })
                continue
            
            co_id = patch['co_id']
            field_patches = patch['patches']
            
            # Trouver l'index correspondant à co_id
            try:
                idx = df[df['co_id'] == co_id].index
                if len(idx) == 0:
                    errors.append({
                        "type": "patch_target_not_found",
                        "severity": "warning",
                        "co_id": co_id,
                        "message": f"Aucune entreprise trouvée avec co_id={co_id}"
                    })
                    continue
                    
                if len(idx) > 1:
                    errors.append({
                        "type": "multiple_patch_targets",
                        "severity": "warning",
                        "co_id": co_id,
                        "message": f"Plusieurs entreprises trouvées avec co_id={co_id}, utilisation du premier"
                    })
                
                idx = idx[0]  # Prendre le premier index si plusieurs correspondances
                
                # Appliquer les correctifs champ par champ
                for field, new_value in field_patches.items():
                    # Vérifier si le champ existe
                    if field not in df.columns:
                        errors.append({
                            "type": "patch_field_not_found",
                            "severity": "warning",
                            "co_id": co_id,
                            "field": field,
                            "message": f"Le champ '{field}' n'existe pas dans le DataFrame"
                        })
                        continue
                    
                    # Sauvegarde de l'ancienne valeur pour logging
                    old_value = df.at[idx, field]
                    
                    # Application du correctif
                    df.at[idx, field] = new_value
                    
                    # Enregistrement de l'information du correctif appliqué
                    errors.append({
                        "type": "patch_applied",
                        "severity": "info",
                        "co_id": co_id,
                        "index": idx,
                        "field": field,
                        "old_value": old_value,
                        "new_value": new_value
                    })
            
            except Exception as e:
                errors.append({
                    "type": "patch_application_error",
                    "severity": "error",
                    "co_id": co_id,
                    "message": f"Erreur lors de l'application du correctif: {str(e)}"
                })
    
    except json.JSONDecodeError as e:
        errors.append({
            "type": "patches_file_parse_error",
            "severity": "error",
            "message": f"Erreur de parsing du fichier de correctifs: {str(e)}"
        })
    
    except Exception as e:
        errors.append({
            "type": "patches_application_error",
            "severity": "error",
            "message": f"Erreur lors de l'application des correctifs: {str(e)}"
        })
    
    return errors



def apply_patches_address(df: pd.DataFrame, patches_file_path: str) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Applique des correctifs pour les adresses mal formatées en utilisant le SIRET comme clé de jointure.
    
    Args:
        df: DataFrame contenant les données Companies
        patches_file_path: Chemin vers le fichier de correctifs d'adresses
        
    Returns:
        Tuple contenant:
        - Le DataFrame avec les adresses corrigées
        - La liste des informations sur les modifications
    """
    errors = []
    result_df = df.copy()
    
    # Normaliser les champs d'adresse
    if 'co_head_office_additional_address' in result_df.columns:
        result_df['co_head_office_additional_address'] = result_df['co_head_office_additional_address'].fillna("")
    else:
        result_df['co_head_office_additional_address'] = ""
        
    # Normaliser les champs d'adresse supplémentaire et numéro pour qu'ils soient toujours des chaînes
    if 'co_head_office_number' in result_df.columns:
        result_df['co_head_office_number'] = result_df['co_head_office_number'].fillna("")
    
    
    # Vérifier si le fichier de correctifs existe
    if not os.path.exists(patches_file_path):
        errors.append({
            "type": "address_patches_file_missing",
            "severity": "warning",
            "message": f"Le fichier de correctifs d'adresses n'existe pas: {patches_file_path}"
        })
        return result_df, errors
    
    try:
        # Chargement du fichier de correctifs
        with open(patches_file_path, 'r', encoding='utf-8') as file:
            address_patches = json.load(file)
        
        # Création d'un dictionnaire SIRET -> informations d'adresse
        siret_to_address = {}
        for patch in address_patches:
            if 'co_siret' in patch and patch['co_siret']:
                siret_to_address[patch['co_siret']] = patch
        
        # Compteurs pour le suivi
        patched_count = {
            'address': 0,
            'additional_address': 0
        }
        
        # Application des correctifs d'adresse
        for idx, row in result_df.iterrows():
            siret = row.get('co_siret')
            
            # Si SIRET valide et présent dans les correctifs
            if not pd.isna(siret) and siret and siret in siret_to_address:
                patch_info = siret_to_address[siret]
                changes = {}
                
                # Correction de l'adresse principale
                if 'co_head_office_address' in patch_info:
                    old_address = row.get('co_head_office_address', '')
                    new_address = patch_info['co_head_office_address']
                    
                    # Appliquer la correction 
                    if old_address != new_address:
                        result_df.at[idx, 'co_head_office_address'] = new_address
                        changes['address'] = {
                            'old': old_address,
                            'new': new_address
                        }
                        patched_count['address'] += 1
                
                # Correction de l'adresse complémentaire (complètement séparée)
                if 'co_head_office_additional_address' in patch_info:
                    old_additional = row.get('co_head_office_additional_address', '')
                    new_additional = patch_info['co_head_office_additional_address'] or ""
                    
                    if old_additional != new_additional:
                        result_df.at[idx, 'co_head_office_additional_address'] = new_additional
                        changes['additional_address'] = {
                            'old': old_additional,
                            'new': new_additional
                        }
                        patched_count['additional_address'] += 1
                
                
                # Enregistrer les informations des modifications
                if changes:
                    errors.append({
                        "type": "address_patched",
                        "severity": "info",
                        "co_id": row.get('co_id'),
                        "co_business_name": row.get('co_business_name'),
                        "index": idx,
                        "co_siret": siret,
                        "changes": changes
                    })
        
        # Ajouter un résumé des opérations
        total_patches = sum(patched_count.values())
        if total_patches > 0:
            errors.append({
                "type": "address_patch_summary",
                "severity": "info",
                "message": f"Correctifs d'adresse appliqués: {patched_count['address']} adresses principales, {patched_count['additional_address']} adresses complémentaires"
            })
        
    except Exception as e:
        errors.append({
            "type": "address_patches_application_error",
            "severity": "error",
            "message": f"Erreur lors de l'application des correctifs d'adresse: {str(e)}"
        })
    
    return result_df, errors



def apply_siret_patches(df: pd.DataFrame, siret_patches_path: str) -> List[Dict[str, Any]]:
    """
    Applique les correctifs pour les SIRET manquants en utilisant le SIREN comme clé de jointure.
    """
    errors = []
    
    try:
        # Chargement du fichier de correctifs SIRET
        with open(siret_patches_path, 'r', encoding='utf-8') as file:
            siret_patches = json.load(file)
        
        # Création d'un dictionnaire de correspondance SIREN -> SIRET
        siren_to_siret = {patch['co_siren']: patch['co_siret'] for patch in siret_patches if 'co_siren' in patch and 'co_siret' in patch}
        
        # Compteurs pour le suivi
        patched_count = 0
        remaining_null_count = 0
        
        # Parcourir chaque entrée du DataFrame
        for idx, row in df.iterrows():
            # Si le SIRET n'est pas nul, on passe
            if not pd.isna(row['co_siret']) and row['co_siret'] != "":
                continue
                
            # Chercher une correspondance par SIREN
            if not pd.isna(row['co_siren']) and row['co_siren'] != "" and row['co_siren'] in siren_to_siret:
                # Récupérer le SIRET correspondant au SIREN
                correct_siret = siren_to_siret[row['co_siren']]
                
                # Sauvegarder l'ancienne valeur (même si nulle)
                old_value = row['co_siret']
                
                # Appliquer le correctif
                df.at[idx, 'co_siret'] = correct_siret
                patched_count += 1
                
                # Enregistrer l'information du correctif appliqué
                errors.append({
                    "type": "siret_patch_applied",
                    "severity": "info",
                    "co_id": row.get('co_id', "unknown"),
                    "co_business_name": row.get('co_business_name', ""),
                    "index": idx,
                    "field": "co_siret",
                    "old_value": old_value,
                    "new_value": correct_siret,
                    "co_siren": row['co_siren']
                })
            else:
                # Si aucun correctif n'est trouvé, enregistrer comme erreur à corriger manuellement
                if pd.isna(row['co_siret']) or row['co_siret'] == "":
                    remaining_null_count += 1
                    errors.append({
                        "type": "siret_still_missing",
                        "severity": "warning",
                        "co_id": row.get('co_id', "unknown"),
                        "index": idx,
                        "co_business_name": row.get('co_business_name', ""),
                        "co_siren": row.get('co_siren', ""),
                        "reason": "SIRET manquant et aucun correctif trouvé pour ce SIREN"
                    })
        
        # Ajouter un résumé des opérations
        errors.append({
            "type": "siret_patch_summary",
            "severity": "info",
            "patched_count": patched_count,
            "remaining_null_count": remaining_null_count,
            "message": f"SIRET: {patched_count} corrigés, {remaining_null_count} toujours manquants"
        })
        
    except Exception as e:
        errors.append({
            "type": "siret_patches_application_error",
            "severity": "error",
            "message": f"Erreur lors de l'application des correctifs SIRET: {str(e)}"
        })
    
    return errors