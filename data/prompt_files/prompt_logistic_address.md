# Prompt pour la création du module de transformation des données logistic_address

## 🔍 Contexte du projet

Nous devons créer un module pour transformer, nettoyer et valider les données d'adresses logistiques (`logistic_address`) à partir d'un fichier JSON d'entrée. Ce module fait partie d'un projet plus large de migration de base de données, où nous avons déjà implémenté des modules similaires pour les tables `companies` et `organizations`.

### Problématiques spécifiques à résoudre

Les données d'entrée présentent plusieurs cas de figure problématiques :

1. **Adresses mal structurées** : Dans certains cas, l'adresse complète est mise dans le champ `la_street` au lieu d'être correctement répartie entre les champs `la_house_number`, `la_street`, `la_postal_code`, `la_city`
   ```json
   {
     "la_street": "87 RUE DE LA COMMANDERIE, 59500 DOUAI",
     "la_postal_code": "",
     "la_city": null
   }
   ```

2. **Données bien structurées** : Dans d'autres cas, les données sont correctement réparties
   ```json
   {
     "la_house_number": "199",
     "la_street": " COLBERT",
     "la_postal_code": "59800",
     "la_city": "Lille"
   }
   ```

3. **Doublons et informations redondantes** : Par exemple, le numéro de rue présent dans `la_house_number` et répété dans `la_additional_address`
   ```json
   {
     "la_house_number": "199",
     "la_additional_address": "CENTRE VAUBAN 199-201  199/201 rue colbert"
   }
   ```

## 📄 Format de données d'entrée et champs à conserver

### Format typique d'entrée

Les données d'entrée sont au format JSON, avec une structure similaire à :

```json
[
  {
    "la_id": 9,
    "la_house_number": "",
    "la_street": "87 RUE DE LA COMMANDERIE, 59500 DOUAI",
    "la_additional_address": "",
    "la_postal_code": "",
    "la_city": null,
    "la_truck_access": false,
    "la_loading_dock": false,
    "la_forklift": false,
    "la_pallet": false,
    "la_fenwick": false,
    "la_palet_capacity": 0,
    "la_longitude": 0,
    "la_latitude": 0,
    "la_isactive": false,
    "fk_co": 251,
    "fk_or": null,
    "stock_import": [
      "20230408--001-006-1"
    ]
  },
  {
    "la_id": 306,
    "la_house_number": "199",
    "la_street": " COLBERT",
    "la_additional_address": "CENTRE VAUBAN 199-201  199/201 rue colbert ",
    "la_postal_code": "59800",
    "la_city": "Lille",
    "la_truck_access": false,
    "la_loading_dock": false,
    "la_forklift": false,
    "la_pallet": false,
    "la_fenwick": false,
    "la_palet_capacity": 0,
    "la_longitude": 0,
    "la_latitude": 0,
    "la_isactive": false,
    "fk_co": null,
    "fk_or": 7,
    "stock_import": [
      "20230310--005-002-1",
      "20231018-033-039-1",
      "20230527-003-010-1"
    ]
  }
]
```

### Champs à conserver sans modification structurelle

Les champs suivants doivent être conservés tels quels (sauf pour la normalisation des valeurs) :

- `la_id` : Identifiant unique de l'adresse (clé primaire)
- `la_truck_access` : Indicateur d'accessibilité pour les camions (booléen)
- `la_loading_dock` : Indicateur de présence d'un quai de chargement (booléen)
- `la_forklift` : Indicateur de disponibilité d'un chariot élévateur (booléen)
- `la_pallet` : Indicateur de disponibilité de palettes (booléen)
- `la_fenwick` : Indicateur de disponibilité d'un Fenwick (booléen)
- `la_palet_capacity` : Capacité de stockage en palettes (entier)
- `la_longitude` : Coordonnée longitudinale (entier)
- `la_latitude` : Coordonnée latitudinale (entier)
- `la_isactive` : Indicateur d'activité de l'adresse (booléen)
- `fk_co` : Clé étrangère vers la table companies (entier ou null)
- `fk_or` : Clé étrangère vers la table organizations (entier ou null)
- `fk_re` : Clé étrangère vers la table recycling (entier ou null)
- `fk_con` : Clé étrangère vers la table contacts (entier ou null)
- `stock_import` : Tableau de chaînes identifiant les imports de stock (conserver exactement tel quel)

### Champs à transformer ou extraire

Les champs suivants doivent être analysés, nettoyés et potentiellement restructurés :

- `la_house_number` : Numéro de rue
- `la_street` : Nom de la rue
- `la_postal_code` : Code postal
- `la_city` : Ville
- `la_additional_address` : Informations complémentaires d'adresse

### Champs à ajouter

Les champs suivants, présents dans le modèle Prisma mais potentiellement absents des données d'entrée, doivent être ajoutés avec des valeurs par défaut :

- `fk_cou` : Clé étrangère vers la table countries (par défaut : 0 pour la France)
- `fk_re` : Clé étrangère vers la table ressource (par défaut : 0)
- `fk_con` : Clé étrangère vers la table contacts (par défaut : 0)
- `opening_hour` : Tableau vide (à initialiser comme `[]` si absent)

### Traitement des relations

- Les champs `stock_import` et `opening_hour` représentent des relations one-to-many dans le modèle Prisma, mais doivent être conservés dans leur format d'origine dans les données de sortie.
- Dans le cas où `opening_hour` n'existe pas dans les données d'entrée, il doit être initialisé comme un tableau vide.
- Aucune transformation spécifique ne doit être appliquée à ces tableaux au-delà de s'assurer qu'ils sont présents dans la sortie.

### Structure du modèle Prisma cible

```prisma
model logistic_address {
  la_id                 Int            @id @default(autoincrement())
  la_house_number       String?        @db.VarChar(255)
  la_street             String?        
  la_postal_code        String         @db.VarChar(255)
  la_city               String?        @db.VarChar(255)
  la_additional_address String?        @db.VarChar(255)
  la_truck_access       Boolean
  la_loading_dock       Boolean
  la_forklift           Boolean
  la_pallet             Boolean
  la_fenwick            Boolean
  la_palet_capacity     Int?
  la_longitude          Int?
  la_latitude           Int?
  la_isactive           Boolean        @default(true)
  fk_cou                Int?           @default(1)
  fk_or                 Int?
  fk_re                 Int?
  fk_co                 Int?
  fk_con                Int?
  companies             companies?     @relation(fields: [fk_co], references: [co_id])
  contacts              contacts?      @relation(fields: [fk_con], references: [con_id], onDelete: NoAction, onUpdate: NoAction)
  countries             countries?     @relation(fields: [fk_cou], references: [cou_id])
  organizations         organizations? @relation(fields: [fk_or], references: [or_id])
  recycling             recycling?     @relation(fields: [fk_re], references: [re_id])
  opening_hour          opening_hour[]
  positioning           positioning[]
  stock_import          stock_import[]
}
```
   

## 📂 Structure du projet à implémenter

```
logistic_address_transformation/
├── src/
│   └── tables/
│       └── logistic_address/
│           ├── clean_logistic_address.py         # Module principal d'orchestration
│           ├── input_structure.py                # Définition du schéma d'entrée
│           ├── output_structure.py               # Définition du schéma de sortie
│           ├── transformations/
│           │   ├── validate_input_structure.py
│           │   ├── normalize_text.py
│           │   ├── normalize_special_chars.py
│           │   ├── clean_punctuation.py
│           │   ├── extract_address_components.py # Nouveau module pour extraire les composants d'adresse
│           │   ├── validate_address_fields.py
│           │   ├── validate_data_types.py        # Nouveau module pour valider les types de données
│           │   ├── validate_postal_code.py       # Module spécifique pour valider les codes postaux
│           │   ├── validate_city_names.py        # Module pour nettoyer et valider les noms de ville
│           │   ├── add_missing_fields.py
│           │   ├── patch_data.py
│           │   └── prepare_final_model.py
│           └── error_reporting/
│               └── generate_error_report.py
```

## 🔄 Flux de traitement attendu

1. **Validation de la structure d'entrée**
   - Vérification de la conformité des données au schéma d'entrée
   - Validation des types de données

2. **Normalisation du texte**
   - Mise en majuscules, suppression des espaces superflus
   - Standardisation des espaces multiples

3. **Normalisation des caractères spéciaux**
   - Suppression des accents
   - Remplacement des caractères non standard

4. **Nettoyage de la ponctuation**
   - Suppression/standardisation des signes de ponctuation

5. **Extraction des composants d'adresse** (nouveau module)
   - Analyse des champs d'adresse pour extraire les composants manquants
   - Traitement des cas où l'adresse complète est dans un seul champ
   - Détection de patterns comme "87 RUE DE LA COMMANDERIE, 59500 DOUAI"
   - Extraction du numéro, de la rue, du code postal et de la ville

6. **Validation des types de données**
   - Vérification que les champs booléens contiennent bien des valeurs booléennes
   - Vérification que les champs entiers contiennent bien des valeurs entières
   - Conversion et correction des types si nécessaire

7. **Validation des champs d'adresse**
   - Vérification du format du code postal
   - Nettoyage des noms de ville (suppression des suffixes d'arrondissement)
   - Validation des formats de rue et numéro

8. **Validation des noms de ville**
   - Suppression des mentions d'arrondissement (ex: "PARIS 14E" → "PARIS")
   - Vérification que les noms ne contiennent que des lettres, espaces, tirets et apostrophes
   - Standardisation en majuscules

9. **Ajout des champs manquants**
   - Ajout de `fk_cou` avec valeur par défaut
   - Initialisation des champs booléens si manquants
   - Ajout de `opening_hour` comme tableau vide si absent
   - Ajout de `positioning` comme tableau vide si absent

10. **Application des correctifs spécifiques**
    - Gestion des cas particuliers via fichier de correctifs

11. **Préparation du modèle final**
    - Conformité au schéma de sortie
    - Contrôle des longueurs de champs

12. **Génération du rapport d'erreurs**
    - Excel avec onglets par type d'erreur
    - Informations sur les transformations effectuées

## 📋 Modules à créer en détail

### 1. input_structure.py
Définit le schéma d'entrée, types attendus, règles de validation.

### 2. output_structure.py
Définit la structure de sortie finale, contraintes de longueur, valeurs par défaut.

### 3. validate_input_structure.py
Vérifie la conformité des données au schéma d'entrée.

### 4. normalize_text.py
Standardisation des chaînes, majuscules, espaces.

### 5. normalize_special_chars.py
Suppression des accents, standardisation des caractères spéciaux.

### 6. clean_punctuation.py
Nettoyage des signes de ponctuation selon règles définies.

### 7. extract_address_components.py
Module crucial pour extraire les composants d'adresse à partir de champs mal structurés. Doit :
- Détecter si l'adresse est complète dans le champ `la_street`
- Extraire le numéro, la rue, le code postal et la ville
- Gérer les différents formats (avec/sans virgule, etc.)
- Détecter et traiter les cas avec code postal inclus

### 8. validate_data_types.py
Nouveau module pour vérifier et corriger les types de données :
- Conversion des valeurs booléennes incorrectes (`"true"`, `"false"`, `0`, `1`, etc.)
- Vérification que les champs numériques contiennent bien des nombres
- Normalisation des types selon le modèle Prisma

### 9. validate_postal_code.py
Module spécifique pour valider les codes postaux :
- Vérification du format (5 chiffres pour les codes postaux français)
- Correction des codes à 4 chiffres (ajout d'un 0 au début)
- Vérification de l'existence du département

### 10. validate_city_names.py
Module pour nettoyer et valider les noms de ville :
- Suppression des mentions d'arrondissement (ex: "PARIS 14E", "LYON 8EME ARRONDISSEMENT")
- Validation des caractères autorisés (lettres, espaces, tirets, apostrophes)
- Standardisation en majuscules

### 11. validate_address_fields.py
Validation des autres champs d'adresse (rue, numéro, complément).

### 12. add_missing_fields.py
Ajout des champs manquants avec valeurs par défaut (fk_cou, opening_hour, etc.).

### 13. patch_data.py
Application des correctifs spécifiques depuis un fichier externe.

### 14. prepare_final_model.py
Préparation de la structure finale selon les contraintes du modèle.

### 15. generate_error_report.py
Génération d'un rapport Excel détaillant les erreurs et modifications.

### 16. clean_logistic_address.py
Module principal orchestrant l'ensemble du processus.

## 🧪 Validation des types de données

### Validation des champs booléens
Pour les champs `la_truck_access`, `la_loading_dock`, `la_forklift`, `la_pallet`, `la_fenwick`, `la_isactive` :
- Valider que ce sont des booléens (true/false)
- Convertir les valeurs textuelles (`"true"`, `"false"`) en booléens
- Convertir les valeurs numériques (`0`, `1`) en booléens
- Définir une valeur par défaut (`false`) en cas de valeur invalide ou absente

### Validation des champs numériques
Pour les champs `la_palet_capacity`, `la_longitude`, `la_latitude` :
- Valider que ce sont des entiers
- Convertir les valeurs textuelles numériques en entiers
- Appliquer des valeurs par défaut (0) en cas de valeur invalide ou absente

### Validation des clés étrangères
Pour les champs `fk_co`, `fk_or`, `fk_re`, `fk_con`, `fk_cou` :
- Valider que ce sont des entiers ou null
- Convertir les valeurs textuelles numériques en entiers
- Laisser null en cas de valeur invalide ou absente

## 📊 Gestion des cas particuliers

### 1. Extraction d'adresse
Analyse regex pour décomposer les adresses complètes :
```
"87 RUE DE LA COMMANDERIE, 59500 DOUAI" → 
{
  "la_house_number": "87",
  "la_street": "RUE DE LA COMMANDERIE",
  "la_postal_code": "59500",
  "la_city": "DOUAI"
}
```

### 2. Déduplication d'informations
Détecter et supprimer les répétitions entre champs :
```
{
  "la_house_number": "199",
  "la_additional_address": "CENTRE VAUBAN 199-201 199/201 rue colbert"
}
```
Transformé en :
```
{
  "la_house_number": "199",
  "la_additional_address": "CENTRE VAUBAN"
}
```

### 3. Conformité géographique
Maintenir la cohérence des données géographiques :
- Valider les codes postaux français
- Standardiser les noms de ville (majuscules, sans arrondissement)
- Prendre en compte les apostrophes dans les noms (ex: "CÔTE D'AZUR")

### 4. Traitement des noms de ville avec arrondissement
Pour les noms de ville comme "PARIS 14E" ou "LYON 8E ARRONDISSEMENT" :
- Extraire uniquement le nom de la ville principale
- Supprimer toute référence à un arrondissement ou numéro
- Exemple : "PARIS 14E" → "PARIS", "LYON 8E ARRONDISSEMENT" → "LYON"

### 5. Champs booléens et numériques
Valider et initialiser les champs booléens et numériques avec des valeurs par défaut appropriées.

## 🛠️ Conventions de code à respecter

1. **Type hints** obligatoires pour toutes les fonctions
2. **Docstrings** détaillés pour chaque module et fonction
3. **Gestion explicite des exceptions** avec messages clairs
4. **Journalisation** détaillée des opérations et erreurs
5. **Structure des données intermédiaires** sous forme de DataFrame pandas
6. **Nommage cohérent** des fonctions et variables
7. **Fichiers limités à 200-250 lignes** maximum
8. **Approche modulaire** avec responsabilités clairement définies