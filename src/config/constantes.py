from typing import FrozenSet
import os

# Valeurs spéciales à nettoyer
SPECIAL_VALUES: FrozenSet[str] = frozenset({
    ".", "x", "X", "/", "0", ",", "-", "N/A", "n/a", "none"
})

# Format timestamp pour les fichiers de sortie
TIMESTAMP_FORMAT: str = "%Y%m%d_%H%M%S"

# Répertoires requis
REQUIRED_DIRS: list[str] = ['output_directory', 'output_info_directory']

# Chemins par défaut
DEFAULT_PATHS = {
    'INPUT_DIR': os.path.join('data', 'raw_json'),
    'OUTPUT_DIR': os.path.join('data', 'cleaned_json'),
    'INFO_DIR': os.path.join('data', 'info_cleaned_json')
}

def resolve_path(path: str, is_absolute: bool = False) -> str:
    """
    Résout le chemin en absolu ou relatif selon le besoin
    Args:
        path: Chemin à résoudre
        is_absolute: Si True, renvoie le chemin absolu, sinon garde le chemin relatif
    Returns:
        str: Chemin résolu
    """
    if is_absolute:
        return os.path.abspath(path)
    return path
