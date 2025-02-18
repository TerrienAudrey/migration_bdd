import json

def remove_fake_rna(organizations_file='organizations.json'):
    try:
        # Lecture du fichier
        with open(organizations_file, 'r', encoding='utf-8') as file:
            organizations = json.load(file)

        # Compteurs pour le rapport
        total_orgs = len(organizations)
        removed_count = 0
        removed_list = []

        # Traitement de chaque organisation
        for org in organizations:
            # Vérifier si c'est un Fake RNA via le commentaire
            if org.get('or_additionnal_information') in ["FAKE RNA", "REUSED FAKE RNA"]:
                # Stocker les infos pour le rapport
                removed_list.append({
                    'denomination': org.get('or_denomination', 'N/A'),
                    'removed_rna': org.get('or_rna', '')
                })

                # Réinitialiser les champs
                org['or_rna'] = ""
                org['or_additionnal_information'] = None
                removed_count += 1

        # Sauvegarde des modifications
        with open(organizations_file, 'w', encoding='utf-8') as file:
            json.dump(organizations, file, ensure_ascii=False, indent=2)

        # Affichage du rapport
        print("\nRapport de suppression des RNA :")
        print(f"Nombre total d'organisations : {total_orgs}")
        print(f"Nombre de RNA supprimés : {removed_count}")

        if removed_list:
            print("\nDétail des suppressions :")
            for removal in removed_list:
                print(f"\nDénomination : {removal['denomination']}")
                print(f"RNA supprimé : {removal['removed_rna']}")

        return {
            'total': total_orgs,
            'removed': removed_count,
            'removals': removed_list
        }

    except FileNotFoundError:
        print(f"Erreur : Le fichier {organizations_file} n'a pas été trouvé.")
        return None
    except json.JSONDecodeError as e:
        print(f"Erreur : Format JSON invalide dans le fichier. Détails : {str(e)}")
        return None

if __name__ == "__main__":
    stats = remove_fake_rna()
