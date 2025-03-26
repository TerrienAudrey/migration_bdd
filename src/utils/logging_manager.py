"""
Module de gestion des logs pour le projet de transformation de données.
Configure et fournit les loggers pour les différents modules.
"""

import logging
import os
from typing import Optional


def setup_logger(
    logger_name: str,
    log_file: Optional[str] = None,
    console_level: int = logging.INFO,
    file_level: int = logging.DEBUG
) -> logging.Logger:
    """
    Configure et retourne un logger avec le nom spécifié.
    
    Args:
        logger_name: Nom du logger à configurer
        log_file: Chemin du fichier de log (si None, pas de logging dans un fichier)
        console_level: Niveau de logging pour la console
        file_level: Niveau de logging pour le fichier
        
    Returns:
        Logger configuré
    """
    # Créer le logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)  # Niveau le plus bas pour capture maximale
    
    # Éviter les handlers dupliqués
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # Formatter pour les messages de log
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Handler pour la console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Handler pour le fichier si spécifié
    if log_file:
        # Créer le répertoire si nécessaire
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(file_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Récupère un logger existant ou en crée un nouveau.
    
    Args:
        name: Nom du logger à récupérer
        
    Returns:
        Logger demandé
    """
    return logging.getLogger(name)