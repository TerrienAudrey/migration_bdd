import json

def check_additional_information(organizations_file='organizations_20250210_173434.json'):
    try:
        # Lecture du fichier des organisations
        with open(organizations_file, 'r', encoding='utf-8') as file:
            organizations = json.load(file)

        # Compteurs
        total_orgs = len(organizations)
        null_count = 0
        not_null_count = 0

        # Liste pour stocker les organisations avec info additionnelle
        orgs_with_info = []

        # Vérification de chaque organisation
        for org in organizations:
            if org.get('or_additionnal_information') is None:
                null_count += 1
            else:
                not_null_count += 1
                orgs_with_info.append({
                    'denomination': org.get('or_denomination', 'N/A'),
                    'additional_info': org.get('or_additionnal_information')
                })

        # Affichage du rapport
        print("\nRapport sur le champ or_additionnal_information :")
        print(f"Nombre total d'organisations : {total_orgs}")
        print(f"Nombre d'organisations avec champ null : {null_count}")
        print(f"Nombre d'organisations avec information : {not_null_count}")

        if not_null_count > 0:
            print("\nOrganisations avec informations additionnelles :")
            for org in orgs_with_info:
                print(f"\nDénomination : {org['denomination']}")
                print(f"Information additionnelle : {org['additional_info']}")

        return {
            'total': total_orgs,
            'null_count': null_count,
            'not_null_count': not_null_count,
            'orgs_with_info': orgs_with_info
        }

    except FileNotFoundError:
        print(f"Erreur : Le fichier {organizations_file} n'a pas été trouvé.")
        return None
    except json.JSONDecodeError as e:
        print(f"Erreur : Format JSON invalide dans le fichier. Détails : {str(e)}")
        return None

if __name__ == "__main__":
    stats = check_additional_information()
