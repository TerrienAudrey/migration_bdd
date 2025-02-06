import json
import psycopg2
from psycopg2.extras import execute_values
from collections import defaultdict

try:
    conn = psycopg2.connect(
        dbname="dealinka_migration",
        user="postgres",
        password="797479",
        host="localhost",
        port="5432"
    )
    print("Connexion réussie !")

    cur = conn.cursor()

    # 1. Création ou réinitialisation des tables
    tables_creation = """
    -- Table principale
    DROP TABLE IF EXISTS companies CASCADE;
    CREATE TABLE companies (
        co_id SERIAL PRIMARY KEY,
        co_business_name VARCHAR(255) NOT NULL,
        co_siren VARCHAR(25) NOT NULL,
        co_siret VARCHAR(25) NOT NULL,
        co_vat VARCHAR(20),
        co_code_ent VARCHAR(50) UNIQUE,
        co_head_office_address VARCHAR(255),
        cp_head_office_city VARCHAR(255),
        cp_head_office_postal_code VARCHAR(255),
        co_legal_form VARCHAR(100),
        fk_us INTEGER DEFAULT 0,
        "user" INTEGER,
        stocks INTEGER[],
        logistic_address INTEGER[],
        contacts INTEGER[]
    );

    -- Table des doublons
    DROP TABLE IF EXISTS companies_duplicates CASCADE;
    CREATE TABLE companies_duplicates (
        id SERIAL PRIMARY KEY,
        co_code_ent VARCHAR(50),
        duplicate_count INTEGER,
        original_data JSONB
    );

    -- Table des problèmes de NULL
    DROP TABLE IF EXISTS companies_null_issues CASCADE;
    CREATE TABLE companies_null_issues (
        id SERIAL PRIMARY KEY,
        co_code_ent VARCHAR(50),
        field_name VARCHAR(100),
        expected_type VARCHAR(50),
        original_value TEXT,
        replaced_value TEXT
    );
    """
    cur.execute(tables_creation)
    print("Tables créées avec succès !")

    # 2. Chargement et traitement des données
    with open('raw_json/companies.json', 'r') as file:
        data = json.load(file)

    # 3. Gestion des doublons
    code_ent_count = defaultdict(list)
    for item in data:
        code_ent = item.get('co_code_ent')
        if code_ent:
            code_ent_count[code_ent].append(item)

    # Préparation des données uniques et doublons
    unique_entries = {}
    duplicates_to_insert = []
    null_issues_to_insert = []

    # 4. Traitement de chaque entrée
    for code_ent, items in code_ent_count.items():
        # Gestion des doublons
        if len(items) > 1:
            for item in items:
                duplicates_to_insert.append((
                    item.get('co_code_ent'),
                    len(items),
                    json.dumps(item)
                ))
        else:
            item = items[0]
            unique_entries[code_ent] = item

            # Vérification des valeurs NULL
            null_checks = [
                ('co_business_name', 'varchar', item.get('co_business_name')),
                ('co_siren', 'varchar', item.get('co_siren')),
                ('co_siret', 'varchar', item.get('co_siret')),
                ('fk_us', 'integer', item.get('fk_us'))
            ]

            for field_name, field_type, value in null_checks:
                if not value and value != 0:
                    replaced_value = {
                        'varchar': '.',
                        'integer': 0,
                        'boolean': False
                    }.get(field_type)

                    null_issues_to_insert.append((
                        code_ent,
                        field_name,
                        field_type,
                        str(value),
                        str(replaced_value)
                    ))

                    # Mise à jour de la valeur dans unique_entries
                    item[field_name] = replaced_value

    # 5. Insertion des données uniques
    unique_values = [
        (
            item.get('co_business_name') or '.',
            item.get('co_siren') or '.',
            item.get('co_siret') or '.',
            item.get('co_vat'),
            code_ent,
            item.get('co_head_office_address'),
            item.get('cp_head_office_city'),
            item.get('cp_head_office_postal_code'),
            item.get('co_legal_form'),
            item.get('fk_us', 0),
            item.get('user'),
            item.get('stocks', []),
            item.get('logistic_address', []),
            item.get('contacts', [])
        )
        for code_ent, item in unique_entries.items()
    ]

    # 6. Insertions dans les tables
    if unique_values:
        execute_values(cur, """
            INSERT INTO companies (
                co_business_name, co_siren, co_siret, co_vat, co_code_ent,
                co_head_office_address, cp_head_office_city, cp_head_office_postal_code,
                co_legal_form, fk_us, "user", stocks, logistic_address, contacts
            ) VALUES %s
        """, unique_values)
        print(f"Nombre d'entrées uniques insérées : {len(unique_values)}")

    if duplicates_to_insert:
        execute_values(cur, """
            INSERT INTO companies_duplicates (
                co_code_ent, duplicate_count, original_data
            ) VALUES %s
        """, duplicates_to_insert)
        print(f"Nombre de doublons enregistrés : {len(duplicates_to_insert)}")

    if null_issues_to_insert:
        execute_values(cur, """
            INSERT INTO companies_null_issues (
                co_code_ent, field_name, expected_type, original_value, replaced_value
            ) VALUES %s
        """, null_issues_to_insert)
        print(f"Nombre de problèmes de NULL enregistrés : {len(null_issues_to_insert)}")

    conn.commit()
    print("Toutes les opérations ont été effectuées avec succès !")

except Exception as e:
    print(f"Une erreur s'est produite : {e}")
    if conn:
        conn.rollback()

finally:
    if cur:
        cur.close()
    if conn:
        conn.close()
    print("Connexion fermée.")
