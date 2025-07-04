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
python 02_clean.py
```

## Architecture
```
cleaning/
├── 02_clean.py                  # Script principal
├── modules/sirene_validator.py  # Validation SIRENE (plus a venir maybe)
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
| **GitHub** | 323 | Repos/langages | Normalisation langages |
| **Google Trends** | 260 | Tendances tech | Extraction technologies |
| **Kaggle** | 27,139 | Enquêtes dev | Normalisation salaires |
| **StackOverflow** | 103,232 | Enquêtes dev | Normalisation salaires |

**Total : 135,040 lignes**

## Traitements appliqués
- **Consolidation** : fusion fichiers multiples par source
- **Normalisation** : technologies (`js` → `JavaScript`), pays (`france` → `FR`), salaires → EUR
- **Validation SIRENE** : 52/1,083 entreprises françaises vérifiées (base 31 entreprises tech)
- **Suppression doublons** : avec exclusion colonnes listes
- **Enrichissement** : métadonnées, flags utiles

## Module : Validation SIRENE 
- **Base référence** : 31 entreprises tech françaises (a améliorer)
- **Méthodes** : exact match, fuzzy matching optimisé, détection patterns suspects
- **Flag** : `is_verified_company` pour filtrer offres douteuses

## Résultats
**6 fichiers Parquet** dans `data/clean/` :
- `adzuna_jobs_clean.parquet` (3,086 lignes)
- `glassdoor_jobs_clean.parquet` (1,000 lignes) 
- `github_data_clean.parquet` (323 lignes)
- `trends_data_clean.parquet` (260 lignes)
- `surveys_data_clean.parquet` (130,371 lignes)
- `jobs_consolidated_clean.parquet` (4,086 lignes)
