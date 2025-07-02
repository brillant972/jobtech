# Nettoyage et Normalisation - Bryan

## Objectif
Pipeline de nettoyage et normalisation des données scrappées pour préparer l'analyse comparative de salaires et la validation d'entreprises.

## Structure
```
cleaning/
├── 02_clean.py              # Script principal de nettoyage
├── modules/                 # Modules spécialisés (en cours de dev)
└── README_bryan.md          # Cette documentation

dictionaries/
├── tech_mapping.json        # Normalisation technologies
├── countries.json           # Normalisation pays  
└── regions.json             # Mapping code postal → région
```

## Fonctionnalités implémentées

### Étape 1 - Base 
- Normalisation des technologies (`js` → `JavaScript`)
- Normalisation des pays (`france` → `FR`)
- Extraction régions depuis codes postaux
- Sauvegarde en format Parquet
- Pipeline de test avec données fictives

### Étape 2 - Données réelles
- Traitement 4,565 jobs réels (Adzuna + Glassdoor)
- 12 pays européens représentés
- Consolidation sources multiples
- Normalisation salaires multidevises → EUR

### Étape 3 - Validation SIRENE
- Base SIRENE échantillon (10 entreprises tech FR)
- Validation entreprises françaises (exact + fuzzy matching)
- Flag `is_verified_company` pour détecter fausses offres
- Score de correspondance `sirene_match_score`
- Méthode de validation `sirene_match_method`

### Étape 4 - Optimisations avancées
- **Base SIRENE étendue** : 31 entreprises tech françaises (vs 10 avant)
- **Performance optimisée** : traitement par chunks (50 lignes)
- **Cache intelligent** : évite recalculs similarités (406 entrées uniques)
- **Validation améliorée** : 4.8% de correspondances (vs 3.0% avant)
- **Méthodes multiples** : exact_match, fuzzy_match_optimized, détection patterns suspects
- **Logging détaillé** : progression par chunks, stats de performance

### À venir
- Géolocalisation avancée (lat/long)
- Détection d'anomalies avancée

## Utilisation

### Test du pipeline de base
```bash
cd cleaning
python 02_clean.py
```

### Output attendu
- Fichiers Parquet dans `data/clean/`
- Logs détaillés du processus
- Validation du pipeline

## Dictionnaires

### Technologies
- 40+ mappings de technologies courantes
- Format: `{"js": "JavaScript", "py": "Python"}`

### Pays  
- 25+ mappings de pays européens
- Format: `{"france": "FR", "germany": "DE"}`

### Régions
- Codes postaux français → régions
- Format: `{"75001": "Île-de-France"}`

## Résultats actuels

### Données traitées - TOUTES SOURCES
- **135,040 lignes** au total traitées
- **6 sources** : Adzuna, Glassdoor, GitHub, Google Trends, Kaggle, StackOverflow
- **4,086 offres d'emploi** (Adzuna: 3,086 + Glassdoor: 1,000)
- **130,371 réponses surveys** (Kaggle + StackOverflow)
- **323 données GitHub** (langages, repos trending)
- **260 tendances Google Trends** (par pays/technologie)

### Validation SIRENE optimisée
- **Base SIRENE étendue** : 31 entreprises tech françaises de référence
- **52 entreprises vérifiées** sur 1,083 françaises (4.8%)
- **Adzuna** : 37/990 vérifiées (3.7%)
- **Glassdoor** : 15/93 vérifiées (16.1% - meilleur taux)
- **Méthodes multiples** : correspondance exacte, fuzzy optimisée, détection patterns suspects
- **Performance** : traitement par chunks, cache intelligent
- **Flag qualité** : `is_verified_company` pour filtrer offres douteuses

### Fichiers de sortie
- `adzuna_jobs_clean.parquet` : 3,086 offres Adzuna
- `glassdoor_jobs_clean.parquet` : 1,000 offres Glassdoor
- `github_jobs_clean.parquet` : 323 données GitHub
- `trends_data_clean.parquet` : 260 tendances Google
- `surveys_data_clean.parquet` : 130,371 réponses surveys
- `jobs_consolidated_clean.parquet` : 4,086 offres consolidées

## Prochaines étapes
1. Test pipeline de base ✅ TERMINÉ 
2. Intégration données réelles de Khalil ✅ TERMINÉ
3. Ajout validation SIRENE pour entreprises françaises ✅ TERMINÉ
4. Optimisation performance et base SIRENE étendue ✅ TERMINÉ
5. **Extension TOUTES sources** ✅ **TERMINÉ - 6 sources traitées**
6. Géolocalisation Nominatim (optionnel)
7. Analyses avancées multi-sources (optionnel)
