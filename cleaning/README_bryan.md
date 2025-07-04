# Pipeline de Nettoyage des Données - Bryan

## Description
Pipeline automatisé de nettoyage et normalisation des données du marché tech européen avec validation d'entreprises SIRENE.

## Dépendances
```bash
pip install pandas pyarrow fuzzywuzzy python-Levenshtein
```

## Utilisation
```bash
cd cleaning
python 02_clean_modular.py  # Version recommandée
```

## Architecture
```
cleaning/
├── 02_clean.py         # Pipeline principal
├── modules/
│   ├── sirene_validator.py     # Validation SIRENE
│   └── cleaners/
│       ├── base_cleaner.py     # Classe de base
│       ├── job_cleaner.py      # Nettoyeur jobs
│       ├── github_cleaner.py   # Nettoyeur GitHub
│       ├── trends_cleaner.py   # Nettoyeur Trends
│       └── survey_cleaner.py   # Nettoyeur Surveys
└── README_bryan.md             # Documentation

dictionaries/
├── tech_mapping.json           # 41 mappings technos
├── countries.json             # 42 mappings pays
└── regions.json              # 55 mappings régions
```

## Données traitées
| Source | Volume | Type | Traitements spécifiques |
|--------|--------|------|------------------------|
| **Adzuna** | 3,086 | Offres d'emploi | Validation SIRENE (3.7%) |
| **Glassdoor** | 1,000 | Offres d'emploi | Validation SIRENE (16.1%) |
| **GitHub Language Stats** | 15 | Statistiques langages | Normalisation langages |
| **GitHub Trending Repos** | 308 | Repos tendance | Normalisation langages |
| **Google Trends Tech** | 40 | Comparaisons tech | Extraction technologies |
| **Google Trends Country** | 110 | Tendances pays | Normalisation pays |
| **Kaggle Europe** | 907 | Enquête dev Europe | Normalisation salaires |
| **Kaggle Raw** | 26,232 | Enquête dev globale | Normalisation salaires |
| **StackOverflow** | 14,982 | Enquête dev 2022 | Normalisation salaires |


## Traitements appliqués
- **Consolidation** : fusion fichiers multiples par source
- **Normalisation** : technologies (`js` → `JavaScript`), pays (`france` → `FR`), salaires → EUR
- **Validation SIRENE** : 52/1,083 entreprises françaises vérifiées (base 31 entreprises tech)
- **Suppression doublons** : avec gestion sécurisée des colonnes listes
- **Enrichissement** : métadonnées, flags utiles
- **Séparation stricte** : un fichier par type/source de données


## Module : Validation SIRENE 
- **Base référence** : 31 entreprises tech françaises (a améliorer)
- **Méthodes** : exact match, fuzzy matching optimisé, détection patterns suspects
- **Flag** : `is_verified_company` pour filtrer offres douteuses

## Résultats
**10 fichiers Parquet** dans `data/clean/` :

### Offres d'emploi
- `adzuna_jobs_clean.parquet` (3,086 lignes)
- `glassdoor_jobs_clean.parquet` (1,000 lignes) 
- `jobs_consolidated_clean.parquet` (4,086 lignes - fusion des 2 précédents)

### Données GitHub séparées
- `github_language_stats_clean.parquet` (15 lignes - statistiques langages)
- `github_trending_repos_clean.parquet` (308 lignes - repos tendance)

### Données Google Trends séparées
- `tech_comparisons_clean.parquet` (40 lignes - comparaisons technos)
- `country_trends_clean.parquet` (110 lignes - tendances par pays)

### Enquêtes développeurs séparées
- `kaggle_europe_clean.parquet` (907 lignes - survey Europe)
- `kaggle_raw_clean.parquet` (26,232 lignes - survey global)
- `stackoverflow_clean.parquet` (14,982 lignes - survey 2022)
