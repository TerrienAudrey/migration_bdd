import json
import random
import re

def extract_five_digit_number(text):
    """Extrait un nombre à 5 chiffres d'une chaîne de texte"""
    if not text:
        return None
    matches = re.findall(r'\b\d{5}\b', text)
    return matches[0] if matches else None

def extract_two_digit_number(text):
    """Extrait un nombre à 2 chiffres d'une chaîne de texte"""
    if not text:
        return None
    matches = re.findall(r'\b\d{2}\b', text)
    return matches[0] if matches else None

def generate_random_suffix():
    """Génère un nombre aléatoire à 4 chiffres en format string"""
    return f"{random.randint(0, 9999):04d}"

def get_five_digits(organization):
    """Obtient les 5 premiers chiffres selon la logique définie"""
    # Étape a: Vérifier or_postal_code pour 5 chiffres
    postal_five = extract_five_digit_number(organization.get('or_postal_code', ''))
    if postal_five:
        return postal_five

    # Étape b: Vérifier or_street pour 5 chiffres
    street_five = extract_five_digit_number(organization.get('or_street', ''))
    if street_five:
        return street_five

    # Étape c: Retour à or_postal_code pour 2 chiffres
    postal_two = extract_two_digit_number(organization.get('or_postal_code', ''))
    if postal_two:
        return f"{postal_two}000"

    # Si rien trouvé, retourner "00000"
    return "00000"

def normalize_denomination(s):
    """Normalise une dénomination pour la comparaison"""
    if not s:
        return ""
    return s.lower().strip()

def update_empty_rna(organizations_file='organizations.json'):
    try:
        # Lecture du fichier
        with open(organizations_file, 'r', encoding='utf-8') as file:
            organizations = json.load(file)

        # Créer un dictionnaire des RNA existants par dénomination
        existing_rnas = {}
        for org in organizations:
            if org.get('or_rna'):  # Si le RNA n'est pas vide
                denomination = normalize_denomination(org.get('or_denomination'))
                if denomination:
                    existing_rnas[denomination] = org.get('or_rna')

        # Compteurs et liste pour le rapport
        total_orgs = len(organizations)
        updated_count = 0
        reused_count = 0
        updated_list = []

        # Traitement de chaque organisation
        for org in organizations:
            # Ne traiter que les RNA vides
            if org.get('or_rna') == "":
                denomination = normalize_denomination(org.get('or_denomination'))

                # Vérifier si cette dénomination existe déjà
                if denomination in existing_rnas:
                    # Réutiliser le RNA existant
                    new_rna = existing_rnas[denomination]
                    org['or_rna'] = new_rna
                    org['or_additionnal_information'] = "REUSED FAKE RNA"
                    reused_count += 1
                else:
                    # Générer un nouveau RNA
                    five_digits = get_five_digits(org)
                    random_suffix = generate_random_suffix()
                    new_rna = f"W{five_digits}{random_suffix}"

                    # Mettre à jour l'organisation
                    org['or_rna'] = new_rna
                    org['or_additionnal_information'] = "FAKE RNA"

                    # Ajouter au dictionnaire des RNA existants
                    existing_rnas[denomination] = new_rna

                updated_count += 1
                updated_list.append({
                    'denomination': org.get('or_denomination', 'N/A'),
                    'new_rna': new_rna,
                    'reused': denomination in existing_rnas
                })

        # Sauvegarde des modifications
        with open(organizations_file, 'w', encoding='utf-8') as file:
            json.dump(organizations, file, ensure_ascii=False, indent=2)

        # Affichage du rapport
        print("\nRapport de génération des RNA :")
        print(f"Nombre total d'organisations : {total_orgs}")
        print(f"Nombre d'organisations mises à jour : {updated_count}")
        print(f"- dont RNA réutilisés : {reused_count}")
        print(f"- dont nouveaux RNA générés : {updated_count - reused_count}")

        if updated_list:
            print("\nDétail des mises à jour :")
            for update in updated_list:
                print(f"\nDénomination : {update['denomination']}")
                print(f"RNA {'réutilisé' if update['reused'] else 'généré'} : {update['new_rna']}")

        return {
            'total': total_orgs,
            'updated': updated_count,
            'reused': reused_count,
            'generated': updated_count - reused_count,
            'updates': updated_list
        }

    except FileNotFoundError:
        print(f"Erreur : Le fichier {organizations_file} n'a pas été trouvé.")
        return None
    except json.JSONDecodeError as e:
        print(f"Erreur : Format JSON invalide dans le fichier. Détails : {str(e)}")
        return None

if __name__ == "__main__":
    stats = update_empty_rna()
