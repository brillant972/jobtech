# jobtech# TALENTINSIGHT - COLLECTE DE DONNEES

## OBJECTIF

Collecter des donnees sur l'emploi tech europeen depuis 4 sources pour alimenter un Data Warehouse et une API.

## ARCHITECTURE

```
jobtech/
├── scrapers/
│   ├── 01_scrape_adzuna.py         # API Adzuna Jobs (FONCTIONNE)
│   ├── 02_scrape_stackoverflow.py  # Stack Overflow Survey (FONCTIONNE)  
│   ├── 03_scrape_github.py         # GitHub Trending (FONCTIONNE)
│   ├── 05_scrape_trends.py         # Google Trends (FONCTIONNE)
│   ├── run_all_scrapers.py         # Script principal
│   ├── config.py                   # Configuration
│   └── utils.py                    # Utilitaires
├── raw/                            # Donnees brutes (sortie)
│   ├── adzuna/
│   ├── stackoverflow/
│   ├── github/
│   └── google_trends/
├── logs/                           # Logs execution
├── requirements.txt                # Dependances Python
├── .env                            # Variables environnement
└── README.md                       # Cette documentation
```

## INSTALLATION

### 1. Dependances Python

```bash
pip install requests pandas beautifulsoup4 python-dotenv pytrends lxml openpyxl
```

### 2. Cles API requises

#### Adzuna API (OBLIGATOIRE)
- Site: https://developer.adzuna.com/
- Creer compte gratuit
- Creer nouvelle application
- Noter App ID et API Key
- Limite: 1000 appels/mois

#### GitHub Token (OPTIONNEL mais recommande)
- Site: https://github.com/settings/tokens
- Generate new token (classic)
- Cocher "public_repo"
- Copier le token
- Limite: 5000 appels/heure avec token, 60 sans

## UTILISATION

### Lancement automatique (RECOMMANDE)

```bash
cd scrapers
python run_all_scrapers.py
```

Le script va:
1. Demander vos cles API
2. Creer le fichier .env automatiquement
3. Lancer les 4 scrapers sequentiellement
4. Generer un rapport

### Lancement de tous les scrappers un à la fois

```bash
cd scrapers
python run_all_scrapers.py (ne fonctionne pas pour l'instant) --single
```

### Lancement manuel d'un scraper

```bash
cd scrapers
python 01_scrape_adzuna.py
```
02,03,05 Pas de 04 indeedd ne fonctionne toujours pas
## SOURCES DE DONNEES

### 1. Adzuna Jobs API
- **Type**: API REST officielle
- **Donnees**: Offres emploi, salaires, entreprises, competences
- **Pays**: FR, DE, NL, GB, IT
- **Sortie**: `raw/adzuna/adzuna_fr_2025-06-30.csv`
- **Volume attendu**: 500-1500 emplois par pays

**Champs collectes:**
- id, title, company, location, country
- salary_min, salary_max, currency
- skills, contract_type, posted_date

### 2. Stack Overflow Developer Survey
- **Type**: Dataset CSV public (~90k reponses)
- **Donnees**: Salaires, technologies, experience, education
- **Mise a jour**: Annuelle
- **Sortie**: `raw/stackoverflow/stackoverflow_fr_2025-06-30.csv`
- **Volume attendu**: 2000-3000 reponses europeennes

**Champs collectes:**
- country, salary_yearly, years_experience
- languages_worked, developer_type, education_level

### 3. GitHub Trending Repositories
- **Type**: API REST GitHub
- **Donnees**: Repositories tendance, langages populaires
- **Limite**: 5000 appels/heure avec token
- **Sortie**: `raw/github/github_trending_repos_2025-06-30.csv`
- **Volume attendu**: 100-200 repositories

**Champs collectes:**
- name, language, stars_count, forks_count
- owner_login, topics, created_at

### 4. Google Trends
- **Type**: pytrends (API non-officielle)
- **Donnees**: Popularite technologies par pays
- **Limitation**: Rate limiting Google
- **Sortie**: `raw/google_trends/trends_fr_2025-06-30.csv`
- **Volume attendu**: 50-100 analyses

**Champs collectes:**
- keyword, country, avg_interest, trend_direction
- category, analysis_date

## CONFIGURATION .ENV

Le script `run_all_scrapers.py` cree automatiquement le fichier .env.

Format manuel si necessaire:
```bash
# Cles API
ADZUNA_APP_ID=44534cc0
ADZUNA_API_KEY=*****
GITHUB_TOKEN=*****

# Configuration
TARGET_COUNTRIES=FR,DE,NL,GB,IT
```

## EXECUTION ET RESULTATS

### Duree d'execution
- **Total**: 12-20 minutes
- **Adzuna**: 3-5 min
- **Stack Overflow**: 4-6 min  
- **GitHub**: 2-4 min
- **Google Trends**: 3-5 min

### Fichiers generes

Apres execution complete:
```
raw/
├── adzuna/
│   ├── adzuna_all_countries_2025-06-30.csv     # ~3000 emplois
│   ├── adzuna_fr_2025-06-30.csv               # ~600 emplois
│   └── adzuna_de_2025-06-30.csv               # ~700 emplois
├── stackoverflow/
│   ├── stackoverflow_all_countries_2025-06-30.csv  # ~2500 reponses
│   └── stackoverflow_fr_2025-06-30.csv        # ~400 reponses
├── github/
│   ├── github_trending_repos_2025-06-30.csv   # ~150 repos
│   └── github_language_stats_2025-06-30.csv   # ~25 langages
└── google_trends/
    ├── trends_all_countries_2025-06-30.csv    # ~100 analyses
    └── trends_fr_2025-06-30.csv               # ~20 analyses
```

### Volume de donnees total attendu
- **Emplois**: 3000-5000 offres
- **Reponses enquete**: 2000-3000 developpeurs
- **Repositories**: 100-200 projets
- **Analyses tendances**: 100+ mots-cles

## GESTION ERREURS

### Problemes frequents

**Erreur 400 Adzuna**
- Verifier cles API sur dashboard Adzuna
- Verifier quota (1000 appels/mois)
- Attendre si rate limit depasse

**Rate limit GitHub**
- Utiliser token GitHub pour 5000 req/h
- Sans token: limite 60 req/h

**pytrends indisponible**
```bash
pip install pytrends
```

**Encoding Windows**
- Deja gere dans les scripts
- Force UTF-8 automatiquement

### Logs et debugging

Tous les logs sont dans:
- `logs/collection_report_YYYY-MM-DD.csv`
- Sortie console detaillee

## TRANSMISSION PERSONNE 2

### Fichiers a traiter
Le dossier `raw/` contient toutes les donnees CSV brutes.

### Format standardise
Tous les fichiers incluent:
- **source**: adzuna, stackoverflow, github, google_trends
- **country**: FR, DE, NL, GB, IT  
- **collected_at**: timestamp collecte

### Champs a normaliser prioritairement
1. **Salaires**: Convertir en EUR, gerer devises
2. **Pays**: Codes ISO uniformes
3. **Competences**: Normaliser noms technologies
4. **Dates**: Format YYYY-MM-DD
5. **Experience**: Junior/Mid-level/Senior

### Problemes de qualite identifies
- Salaires manquants sur ~30% offres
- Competences a extraire des descriptions
- Doublons possibles entre sources
- Localisations a geocoder

## SUPPORT TECHNIQUE

### Verifications avant lancement
1. Python 3.10+ installe
2. Dependances installees: `pip install -r requirements.txt`
3. Cles API Adzuna obtenues
4. Structure dossiers creee

### Test rapide
```bash
cd scrapers
python 01_scrape_adzuna.py
```

### Contact
En cas de probleme technique:
1. Verifier logs dans `logs/`
2. Tester scrapers individuellement
3. Verifier quotas API

## PROCHAINES ETAPES

1. **TERMINE**: Collecte donnees (Personne 1)
2. **SUIVANT**: Nettoyage et normalisation (Personne 2)
3. **APRES**: Construction Data Warehouse (Personne 3)  
4. **FINAL**: Developpement API Django (Personne 4)