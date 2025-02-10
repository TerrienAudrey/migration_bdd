import logging
import sys
import os
import argparse
from typing import Dict, Any
from src.config.constantes import DEFAULT_PATHS, resolve_path
from src.core.processeurs.processeur_transport import ProcesseurTransport
from src.core.processeurs.processeur_stock import ProcesseurStock
from src.core.processeurs.processeur_stock_import import ProcesseurStockImport
from src.core.processeurs.processeur_organization import ProcesseurOrganization
from src.core.processeurs.processeur_logistic_address import ProcesseurLogisticAddress
from src.core.processeurs.processeur_company import ProcesseurCompany

def configure_logging() -> None:
    """Configure les paramètres de logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def parse_arguments() -> Dict[str, str]:
    """Parse les arguments de la ligne de commande"""
    parser = argparse.ArgumentParser(
        description='Traitement des données'
    )

    parser.add_argument(
        '--input-dir',
        help='Chemin du répertoire contenant les fichiers JSON source'
    )
    parser.add_argument(
        '--input-file',
        help='Nom du fichier JSON à traiter'
    )
    parser.add_argument(
        '--file-type',
        choices=['stock', 'transport', 'stock_import', 'organization', 'logistic_address', 'company'],
        required=True,
        help='Type de fichier à traiter (stock ou transport)'
    )
    parser.add_argument(
        '--output-dir',
        help='Chemin du répertoire de sortie pour les fichiers nettoyés'
    )
    parser.add_argument(
        '--info-dir',
        help='Chemin du répertoire pour les fichiers d\'information'
    )
    parser.add_argument(
        '--absolute-paths',
        action='store_true',
        help='Utiliser des chemins absolus plutôt que relatifs'
    )

    args = parser.parse_args()

    input_dir = args.input_dir or DEFAULT_PATHS['INPUT_DIR']
    output_dir = args.output_dir or DEFAULT_PATHS['OUTPUT_DIR']
    info_dir = args.info_dir or DEFAULT_PATHS['INFO_DIR']

    input_dir = resolve_path(input_dir, args.absolute_paths)
    output_dir = resolve_path(output_dir, args.absolute_paths)
    info_dir = resolve_path(info_dir, args.absolute_paths)

    return {
        'input_file': os.path.join(input_dir, args.input_file),
        'output': output_dir,
        'info': info_dir,
        'file_type': args.file_type
    }

def get_processor(file_type: str):
    """Retourne le processeur approprié selon le type de fichier"""
    processors = {
        'stock': ProcesseurStock,
        'transport': ProcesseurTransport,
        'stock_import': ProcesseurStockImport,
        'organization': ProcesseurOrganization,
        'logistic_address': ProcesseurLogisticAddress,
        'company': ProcesseurCompany
    }
    processor_class = processors.get(file_type)
    if not processor_class:
        raise ValueError(f"Type de fichier non supporté : {file_type}")
    return processor_class()

def print_results(results: Dict[str, Any], prefix: str) -> None:
    """Affiche les résultats du traitement"""
    logging.info(f"\nRésumé du traitement {prefix}:")
    for key, value in results.items():
        if isinstance(value, int):
            logging.info(f"- {key}: {value}")

def main() -> None:
    """Fonction principale"""
    try:
        configure_logging()
        logging.info("Démarrage du traitement des données...")

        paths = parse_arguments()

        logging.info("Chemins utilisés :")
        logging.info(f"- Fichier source : {paths['input_file']}")
        logging.info(f"- Répertoire de sortie : {paths['output']}")
        logging.info(f"- Répertoire d'information : {paths['info']}")

        os.makedirs(paths['output'], exist_ok=True)
        os.makedirs(paths['info'], exist_ok=True)

        processor = get_processor(paths['file_type'])
        results = processor.process_data(
            input_file=paths['input_file'],
            output_directory=paths['output'],
            output_info_directory=paths['info'],
            field_rules=processor.get_field_rules(),
            create_key_func=processor.create_key,
            initialize_output_info=processor.initialize_output_info
        )
        print_results(results, paths['file_type'])

        logging.info("Traitement terminé avec succès!")

    except Exception as e:
        logging.error(f"Erreur fatale : {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
