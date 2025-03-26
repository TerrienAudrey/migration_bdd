"""
Modules de transformation pour les données stock_import.
"""

from src.tables.stock_import.transformations.validate_input_structure import validate_input_structure
from src.tables.stock_import.transformations.normalize_text import normalize_text
from src.tables.stock_import.transformations.validate_dates import validate_dates
from src.tables.stock_import.transformations.validate_data_types import validate_data_types
from src.tables.stock_import.transformations.validate_json_fields import validate_json_fields
from src.tables.stock_import.transformations.add_missing_fields import add_missing_fields
# from src.tables.stock_import.transformations.patch_data import apply_patches
from src.tables.stock_import.transformations.prepare_final_model import prepare_final_model

__all__ = [
    'validate_input_structure',
    'normalize_text',
    'validate_dates',
    'validate_data_types',
    'validate_json_fields',
    'add_missing_fields',
    'apply_patches',
    'prepare_final_model'
]