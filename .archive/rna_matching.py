import json
import re

# Définition directe des mises à jour dans le script
updates = [
    {
        "or_denomination": "SOC DE DEFENSE DES ANIMAUX DU NORD (SDA)",
        "or_rna": "W592005487"
    },

]

def normalize_string(s):
    """Normalise une chaîne de caractères pour la comparaison"""
    if not s:
        return ""
    # Remplace les retours à la ligne par des espaces et normalise les espaces multiples
    s = re.sub(r'\s+', ' ', s)
    return s.lower().strip()

def clean_json_content(content):
    """Nettoie le contenu JSON des caractères problématiques"""
    # Supprime les caractères de contrôle sauf les tabs
    content = re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F-\x9F]', '', content)

    # Remplace les retours à la ligne dans les valeurs par des espaces
    # Cherche les valeurs entre guillemets et remplace les retours à la ligne
    content = re.sub(r'": "([^"]*?)\n\s*([^"]*)"', r'": "\1 \2"', content)
    return content

def update_organizations_rna(updates_list, organizations_file='organizations_20250210_173434.json'):
    total_updates = len(updates_list)
    matched = 0
    updated = 0
    not_found = []

    try:
        # Lecture du fichier des organisations
        with open(organizations_file, 'r', encoding='utf-8') as file:
            organizations = json.load(file)

        # Pour chaque mise à jour dans la liste
        for update in updates_list:
            found = False
            denomination = update.get('or_denomination')
            new_rna = update.get('or_rna')

            if not denomination or not new_rna:
                print(f"Erreur : données manquantes pour la mise à jour : {update}")
                continue

            normalized_denomination = normalize_string(denomination)

            # Compter combien d'organisations ont été mises à jour pour cette dénomination
            updates_for_this_denomination = 0

            # Parcourir toutes les organisations pour trouver les correspondances
            for org in organizations:
                if normalize_string(org.get('or_denomination')) == normalized_denomination:
                    found = True
                    if org.get('or_rna') != new_rna:
                        org['or_rna'] = new_rna
                        updates_for_this_denomination += 1

            if found:
                matched += 1
                updated += updates_for_this_denomination
                print(f"\nMise à jour effectuée pour '{denomination}':")
                print(f"- {updates_for_this_denomination} organisation(s) mise(s) à jour avec le RNA '{new_rna}'")
            else:
                not_found.append(denomination)

        # Sauvegarde des modifications
        with open(organizations_file, 'w', encoding='utf-8') as file:
            json.dump(organizations, file, ensure_ascii=False, indent=2)

        # Rapport détaillé
        print("\nRapport détaillé des mises à jour :")
        print(f"Nombre de dénominations dans la liste de mise à jour : {total_updates}")
        print(f"Nombre de dénominations trouvées : {matched}")
        print(f"Nombre total d'organisations mises à jour : {updated}")
        print(f"Nombre de dénominations non trouvées : {len(not_found)}")

        if not_found:
            print("\nDénominations non trouvées :")
            for org in not_found:
                print(f"  * {org}")

    except FileNotFoundError:
        print(f"Erreur : Le fichier {organizations_file} n'a pas été trouvé.")
    except json.JSONDecodeError as e:
        print(f"Erreur : Format JSON invalide dans le fichier. Détails : {str(e)}")

    return {
        'total_updates': total_updates,
        'matched': matched,
        'total_organizations_updated': updated,
        'not_found': len(not_found)
    }

if __name__ == "__main__":
    try:
        stats = update_organizations_rna(updates)
        print("\nStatistiques retournées :", stats)
    except Exception as e:
        print(f"Une erreur inattendue s'est produite : {str(e)}")
