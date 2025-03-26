"""
Module de validation de la structure d'entrée pour les données Organizations.
Vérifie la conformité des données par rapport au schéma défini.
"""

import json
from typing import Dict, List, Tuple, Any, Optional, Union

import jsonschema
import pandas as pd

from src.tables.organizations.input_structure import (
    INPUT_SCHEMA,
    REQUIRED_FIELDS,
    OPTIONAL_FIELDS,
    FIELD_TYPES,
    COMMON_ERRORS
)


def validate_input_structure(data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Valide la structure des données d'entrée selon le schéma défini.
    
    Args:
        data: Liste de dictionnaires représentant les données d'organisations
        
    Returns:
        Tuple contenant:
        - Les données après validation de base (conversion de types)
        - La liste des erreurs de structure détectées
    """
    structure_errors = []
    validated_data = []
    
    # Vérification que data est bien une liste
    if not isinstance(data, list):
        error = {
            "error_type": "invalid_structure",
            "description": "Les données d'entrée doivent être une liste d'objets",
            "details": f"Type trouvé: {type(data).__name__}"
        }
        structure_errors.append(error)
        return [], structure_errors
    
    # Validation globale avec jsonschema
    try:
        jsonschema.validate(instance=data, schema=INPUT_SCHEMA)
    except jsonschema.exceptions.ValidationError as e:
        error = {
            "error_type": "schema_validation",
            "description": "Les données ne respectent pas le schéma défini",
            "details": str(e),
            "path": list(e.path) if hasattr(e, 'path') else []
        }
        structure_errors.append(error)
        # Nous continuons malgré l'erreur pour collecter toutes les erreurs spécifiques
    
    # Validation détaillée par entrée
    for index, item in enumerate(data):
        item_errors = []
        validated_item = {}
        
        # Vérification des champs obligatoires
        for field in REQUIRED_FIELDS:
            if field not in item:
                item_errors.append({
                    "field": field,
                    "error_type": "missing_required_field",
                    "description": f"Le champ obligatoire '{field}' est manquant"
                })
            elif item[field] is None:
                item_errors.append({
                    "field": field,
                    "error_type": "null_required_field",
                    "description": f"Le champ obligatoire '{field}' ne peut pas être null"
                })
        
        # Validation des types et conversion si nécessaire
        for field, value in item.items():
            # Ignorer les champs inconnus
            if field not in REQUIRED_FIELDS and field not in OPTIONAL_FIELDS:
                # Ne pas signaler comme erreur pour permettre des champs supplémentaires
                validated_item[field] = value
                continue
                
            # Valider et convertir le type si nécessaire
            expected_type = FIELD_TYPES.get(field)
            if expected_type and value is not None:
                try:
                    # Tentative de conversion
                    if expected_type == str and not isinstance(value, str):
                        validated_item[field] = str(value).strip()
                    elif expected_type == int and not isinstance(value, int):
                        validated_item[field] = int(value)
                    else:
                        validated_item[field] = value
                except (ValueError, TypeError):
                    item_errors.append({
                        "field": field,
                        "error_type": "type_conversion_error",
                        "description": f"Impossible de convertir '{field}' en {expected_type.__name__}",
                        "value": value
                    })
                    # En cas d'erreur, conserver la valeur originale
                    validated_item[field] = value
            else:
                # Conserver la valeur telle quelle
                validated_item[field] = value
            
            # Vérification des chaînes vides pour les champs non-nulls
            if expected_type == str and value == "":
                item_errors.append({
                    "field": field,
                    "error_type": "empty_string",
                    "description": f"Le champ '{field}' est une chaîne vide"
                })
        
        # Ajouter les erreurs d'entrée spécifiques à la liste globale d'erreurs
        if item_errors:
            structure_errors.append({
                "index": index,
                "or_id": item.get("or_id", "unknown"),
                "errors": item_errors
            })
            
        # Ajouter l'entrée validée à la liste des données validées
        validated_data.append(validated_item)
    
    return validated_data, structure_errors