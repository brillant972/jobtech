#!/usr/bin/env python3
"""
Script de nettoyage et normalisation - Bryan
"""

import pandas as pd
import json
import os
import sys
from pathlib import Path
import logging

sys.path.append(str(Path(__file__).parent))
from modules.sirene_validator import SIRENEValidator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataCleaner:
    """Classe principale pour le nettoyage des données"""
    
    def __init__(self):
        self.project_root = Path(__file__).parent.parent
        self.dictionaries = {}
        self.sirene_validator = SIRENEValidator()
        self.load_dictionaries()
    
    def load_dictionaries(self):
        """Charge tous les dictionnaires de normalisation"""
        dict_path = self.project_root / "dictionaries"
        
        try:
            # Dictionnaire technologies
            with open(dict_path / "tech_mapping.json", 'r', encoding='utf-8') as f:
                self.dictionaries['tech'] = json.load(f)
            
            # Dictionnaire pays
            with open(dict_path / "countries.json", 'r', encoding='utf-8') as f:
                self.dictionaries['countries'] = json.load(f)
            
            # Dictionnaire régions
            with open(dict_path / "regions.json", 'r', encoding='utf-8') as f:
                self.dictionaries['regions'] = json.load(f)
            
            logger.info("[OK] Dictionnaires chargés avec succès")
            logger.info(f"   - Technologies: {len(self.dictionaries['tech'])} entrées")
            logger.info(f"   - Pays: {len(self.dictionaries['countries'])} entrées")  
            logger.info(f"   - Régions: {len(self.dictionaries['regions'])} entrées")
            
        except Exception as e:
            logger.error(f"[NOK] Erreur chargement dictionnaires: {e}")
            raise
    
    def normalize_technology(self, tech_str):
        """Normalise une technologie"""
        if pd.isna(tech_str):
            return tech_str
        
        tech_clean = str(tech_str).lower().strip()
        return self.dictionaries['tech'].get(tech_clean, tech_str)
    
    def normalize_country(self, country_str):
        """Normalise un pays"""
        if pd.isna(country_str):
            return country_str
        
        country_clean = str(country_str).lower().strip()
        return self.dictionaries['countries'].get(country_clean, country_str)
    
    def extract_region_from_postal(self, postal_code):
        """Extrait la région depuis un code postal"""
        if pd.isna(postal_code):
            return None
        
        postal_str = str(postal_code).strip()
        return self.dictionaries['regions'].get(postal_str, None)
    
    def clean_adzuna_data(self):
        """Nettoie et normalise les données Adzuna"""
        logger.info("Nettoyage données Adzuna...")
        
        adzuna_files = list((self.project_root / "data" / "raw" / "adzuna").glob("*.csv"))
        all_adzuna = []
        
        for file in adzuna_files:
            try:
                df = pd.read_csv(file)
                logger.info(f"{file.name}: {len(df)} lignes")
                all_adzuna.append(df)
            except Exception as e:
                logger.error(f"[NOK] Erreur lecture {file.name}: {e}")
        
        if not all_adzuna:
            logger.warning("[ATTENTION] Aucune donnée Adzuna trouvée")
            return None
        
        # Consolidation
        df_adzuna = pd.concat(all_adzuna, ignore_index=True)
        logger.info(f"Total Adzuna: {len(df_adzuna)} lignes")
        
        # Nettoyage et normalisation
        df_clean = df_adzuna.copy()
        
        # Normalisation des compétences (skills)
        if 'skills' in df_clean.columns:
            df_clean['skills_normalized'] = df_clean['skills'].apply(self.normalize_skills_list)
        
        # Normalisation pays
        if 'country' in df_clean.columns:
            df_clean['country_normalized'] = df_clean['country'].apply(self.normalize_country)
        
        # Extraction région depuis location
        df_clean['region'] = df_clean['location'].apply(self.extract_region_from_location)
        
        # Normalisation salaires
        df_clean['salary_eur_min'] = df_clean.apply(lambda row: self.normalize_salary(row.get('salary_min'), row.get('currency')), axis=1)
        df_clean['salary_eur_max'] = df_clean.apply(lambda row: self.normalize_salary(row.get('salary_max'), row.get('currency')), axis=1)
        df_clean['salary_eur_avg'] = (df_clean['salary_eur_min'] + df_clean['salary_eur_max']) / 2
        
        # Enrichissement métadonnées
        df_clean['source_type'] = 'job_board'
        df_clean['processed_at'] = pd.Timestamp.now()
        
        # drop doublons
        df_clean = self.remove_duplicates_safe(df_clean, "Adzuna")
        
        return df_clean
    
    def clean_glassdoor_data(self):
        """Nettoie et normalise les données Glassdoor"""
        logger.info("Nettoyage données Glassdoor...")
        
        glassdoor_file = self.project_root / "data" / "raw" / "glassdoor" / "glassdoor_tech_jobs_europe_2025-07-02_11-14.csv"
        
        if not glassdoor_file.exists():
            logger.warning("[ATTENTION] Fichier Glassdoor non trouvé")
            return None
        
        try:
            df_glassdoor = pd.read_csv(glassdoor_file)
            logger.info(f"Glassdoor: {len(df_glassdoor)} lignes")
        except Exception as e:
            logger.error(f"[NOK] Erreur lecture Glassdoor: {e}")
            return None
        
        # Harmonisation colonnes avec Adzuna
        df_clean = df_glassdoor.copy()
        
        # Rename pour harmonisation
        if 'job_title' in df_clean.columns:
            df_clean = df_clean.rename(columns={'job_title': 'title'})
        if 'country_code' in df_clean.columns:
            df_clean = df_clean.rename(columns={'country_code': 'country'})
        
        # Normalisation
        if 'skills' in df_clean.columns:
            df_clean['skills_normalized'] = df_clean['skills'].apply(self.normalize_skills_list)
        
        if 'country' in df_clean.columns:
            df_clean['country_normalized'] = df_clean['country'].apply(self.normalize_country)
        
        df_clean['region'] = df_clean['location'].apply(self.extract_region_from_location)
        
        # Salaires
        df_clean['salary_eur_min'] = df_clean.apply(lambda row: self.normalize_salary(row.get('salary_min'), row.get('currency')), axis=1)
        df_clean['salary_eur_max'] = df_clean.apply(lambda row: self.normalize_salary(row.get('salary_max'), row.get('currency')), axis=1)
        df_clean['salary_eur_avg'] = (df_clean['salary_eur_min'] + df_clean['salary_eur_max']) / 2
        
        df_clean['source_type'] = 'job_board'
        df_clean['processed_at'] = pd.Timestamp.now()
        
        # drop doublons
        df_clean = self.remove_duplicates_safe(df_clean, "Glassdoor")
        
        return df_clean
    
    def clean_github_data(self):
        """Nettoie et normalise les données GitHub"""
        logger.info("Nettoyage données GitHub...")
        
        github_path = self.project_root / "data" / "raw" / "github"
        if not github_path.exists():
            logger.warning("[ATTENTION] Dossier GitHub non trouvé")
            return None
        
        github_files = list(github_path.glob("*.csv"))
        all_github = []
        
        for file in github_files:
            try:
                df = pd.read_csv(file)
                df['source_file'] = file.name
                logger.info(f"{file.name}: {len(df)} lignes")
                all_github.append(df)
            except Exception as e:
                logger.error(f"[NOK] Erreur lecture {file.name}: {e}")
        
        if not all_github:
            logger.warning("[ATTENTION] Aucune donnée GitHub trouvée")
            return None
        
        # Consolidation GitHub
        df_github = pd.concat(all_github, ignore_index=True)
        logger.info(f"Total GitHub: {len(df_github)} lignes")
        
        # Nettoyage et normalisation
        df_clean = df_github.copy()
        
        # Normalisation des langages si colonne 'language' présente
        if 'language' in df_clean.columns:
            df_clean['language_normalized'] = df_clean['language'].apply(self.normalize_technology)
        
        # Enrichissement métadonnées
        df_clean['source_type'] = 'github_data'
        df_clean['processed_at'] = pd.Timestamp.now()
        
        # drop doublons
        df_clean = self.remove_duplicates_safe(df_clean, "GitHub")
        
        return df_clean
    
    def clean_trends_data(self):
        """Nettoie et normalise les données Google Trends"""
        logger.info("Nettoyage données Google Trends...")
        
        trends_path = self.project_root / "data" / "raw" / "google_trends"
        if not trends_path.exists():
            logger.warning("[ATTENTION] Dossier Google Trends non trouvé")
            return None
        
        trends_files = list(trends_path.glob("*.csv"))
        all_trends = []
        
        for file in trends_files:
            try:
                df = pd.read_csv(file)
                df['source_file'] = file.name
                logger.info(f"{file.name}: {len(df)} lignes")
                all_trends.append(df)
            except Exception as e:
                logger.error(f"[NOK] Erreur lecture {file.name}: {e}")
        
        if not all_trends:
            logger.warning("[ATTENTION] Aucune donnée Trends trouvée")
            return None
        
        # Consolidation Trends
        df_trends = pd.concat(all_trends, ignore_index=True)
        logger.info(f"Total Trends: {len(df_trends)} lignes")
        
        # Nettoyage et normalisation
        df_clean = df_trends.copy()
        
        # Normalisation des pays si colonne 'country' présente
        if 'country' in df_clean.columns:
            df_clean['country_normalized'] = df_clean['country'].apply(self.normalize_country)
        
        # Normalisation des mots-clés/technologies si colonne 'keyword' présente
        if 'keyword' in df_clean.columns:
            df_clean['keyword_normalized'] = df_clean['keyword'].apply(self.extract_technology_from_keyword)
        
        # Enrichissement métadonnées
        df_clean['source_type'] = 'trends_data'
        df_clean['processed_at'] = pd.Timestamp.now()
        
        # drop doublons
        df_clean = self.remove_duplicates_safe(df_clean, "Google Trends")
        
        return df_clean
    
    def clean_survey_data(self):
        """Nettoie et normalise les données de surveys (Kaggle + StackOverflow)"""
        logger.info("Nettoyage données Surveys...")
        
        all_surveys = []
        
        # Kaggle surveys
        kaggle_path = self.project_root / "data" / "raw" / "kaggle"
        if kaggle_path.exists():
            kaggle_files = list(kaggle_path.glob("*.csv"))
            for file in kaggle_files:
                try:
                    df = pd.read_csv(file)
                    df['survey_source'] = 'kaggle'
                    df['source_file'] = file.name
                    logger.info(f"Kaggle {file.name}: {len(df)} lignes")
                    all_surveys.append(df)
                except Exception as e:
                    logger.error(f"[NOK] Erreur lecture Kaggle {file.name}: {e}")
        
        # StackOverflow surveys
        stackoverflow_path = self.project_root / "data" / "raw" / "stackoverflow"
        if stackoverflow_path.exists():
            stackoverflow_files = list(stackoverflow_path.glob("*.csv"))
            for file in stackoverflow_files:
                try:
                    df = pd.read_csv(file)
                    df['survey_source'] = 'stackoverflow'
                    df['source_file'] = file.name
                    logger.info(f"StackOverflow {file.name}: {len(df)} lignes")
                    all_surveys.append(df)
                except Exception as e:
                    logger.error(f"[NOK] Erreur lecture StackOverflow {file.name}: {e}")
        
        if not all_surveys:
            logger.warning("[ATTENTION] Aucune donnée Survey trouvée")
            return None
        
        # Consolidation Surveys
        df_surveys = pd.concat(all_surveys, ignore_index=True)
        logger.info(f"Total Surveys: {len(df_surveys)} lignes")
        
        # Nettoyage et normalisation
        df_clean = df_surveys.copy()
        
        # Harmonisation des colonnes communes
        if 'country_code' in df_clean.columns and 'country' not in df_clean.columns:
            df_clean['country'] = df_clean['country_code']
        
        # Normalisation pays
        if 'country' in df_clean.columns:
            df_clean['country_normalized'] = df_clean['country'].apply(self.normalize_country)
        
        # Normalisation des compétences/langages
        skill_columns = ['skills', 'languages_worked', 'language']
        for col in skill_columns:
            if col in df_clean.columns:
                df_clean[f'{col}_normalized'] = df_clean[col].apply(self.normalize_skills_list)
        
        # Normalisation salaires en EUR
        salary_columns = ['salary_eur', 'salary_yearly', 'salary']
        for col in salary_columns:
            if col in df_clean.columns:
                currency_col = 'currency' if 'currency' in df_clean.columns else None
                df_clean[f'{col}_eur_normalized'] = df_clean.apply(
                    lambda row: self.normalize_salary(row.get(col), row.get(currency_col)),
                    axis=1
                )
        
        # Enrichissement métadonnées
        df_clean['source_type'] = 'survey_data'
        df_clean['processed_at'] = pd.Timestamp.now()
        
        # drop doublons
        df_clean = self.remove_duplicates_safe(df_clean, "Surveys")
        
        return df_clean
    
    def extract_technology_from_keyword(self, keyword_str):
        """Extrait la technologie d'un mot-clé Google Trends"""
        if pd.isna(keyword_str):
            return keyword_str
        
        keyword_clean = str(keyword_str).lower()
        
        # Extraction de la technologie depuis les patterns comme "Python programming"
        tech_patterns = {
            'python': 'Python',
            'javascript': 'JavaScript',
            'java ': 'Java',  # Espace pour éviter javascript
            'typescript': 'TypeScript',
            'react': 'React',
            'angular': 'Angular',
            'vue': 'Vue.js',
            'docker': 'Docker',
            'kubernetes': 'Kubernetes',
            'aws': 'AWS',
            'azure': 'Azure'
        }
        
        for pattern, tech in tech_patterns.items():
            if pattern in keyword_clean:
                return tech
        
        return keyword_str
    
    def normalize_skills_list(self, skills_str):
        """Normalise une liste de compétences"""
        if pd.isna(skills_str):
            return []
        
        try:
            # Si c'est une liste JSON string
            if isinstance(skills_str, str) and skills_str.startswith('['):
                skills_list = eval(skills_str)  # Attention: eval dangereux, à améliorer
            elif isinstance(skills_str, str):
                skills_list = [s.strip() for s in skills_str.split(',')]
            else:
                skills_list = [str(skills_str)]
            
            # Normalisation de chaque skill
            normalized = []
            for skill in skills_list:
                normalized_skill = self.normalize_technology(skill)
                if normalized_skill:
                    normalized.append(normalized_skill)
            
            return normalized
            
        except Exception as e:
            logger.warning(f"Erreur normalisation skills '{skills_str}': {e}")
            return []
    
    def normalize_salary(self, salary_value, currency):
        """Normalise un salaire en EUR"""
        if pd.isna(salary_value) or salary_value == 0:
            return None
        
        # Taux de change approximatifs (à améliorer avec API réelle)
        rates = {
            'EUR': 1.0,
            'USD': 0.92,
            'GBP': 1.17,
            'CHF': 1.05,
            'SEK': 0.085,
            'NOK': 0.09,
            'DKK': 0.13
        }
        
        currency_clean = str(currency).upper() if currency else 'EUR'
        rate = rates.get(currency_clean, 1.0)
        
        try:
            return float(salary_value) * rate
        except:
            return None
    
    def extract_region_from_location(self, location_str):
        """Extrait la région depuis une location string"""
        if pd.isna(location_str):
            return None
        
        location_clean = str(location_str).lower()
        
        # Recherche directe dans le nom de ville
        city_mappings = {
            'paris': 'Île-de-France',
            'lyon': 'Auvergne-Rhône-Alpes',
            'marseille': 'Provence-Alpes-Côte d\'Azur',
            'toulouse': 'Occitanie',
            'nantes': 'Pays de la Loire',
            'strasbourg': 'Grand Est',
            'lille': 'Hauts-de-France',
            'rennes': 'Bretagne',
            'bordeaux': 'Nouvelle-Aquitaine'
        }
        
        for city, region in city_mappings.items():
            if city in location_clean:
                return region
        
        return None
    
    def validate_french_companies(self, df):
        """Valide les entreprises françaises via SIRENE (version optimisée)"""
        logger.info("Validation optimisée des entreprises françaises...")
        
        # Filtrer les données françaises
        df_french = df[df['country_normalized'] == 'FR'].copy()
        df_others = df[df['country_normalized'] != 'FR'].copy()
        
        if len(df_french) == 0:
            logger.info("Aucune entreprise française à valider")
            # Ajouter colonnes par défaut pour les autres pays
            df_others['is_verified_company'] = None
            df_others['sirene_match_score'] = None
            df_others['sirene_match_method'] = None
            return df_others
        
        logger.info(f"Validation optimisée de {len(df_french)} entreprises françaises...")
        
        # Validation SIRENE optimisée avec chunks
        df_french_validated = self.sirene_validator.validate_dataframe_optimized(
            df_french, 
            company_col='company',
            chunk_size=50  # Chunks plus petits pour suivi progress
        )
        
        # Ajouter colonnes par défaut pour les autres pays
        df_others['is_verified_company'] = None
        df_others['sirene_match_score'] = None
        df_others['sirene_match_method'] = None
        
        # Reconstituer le DataFrame complet
        df_complete = pd.concat([df_french_validated, df_others], ignore_index=True)
        
        # Statistiques détaillées
        french_verified = df_french_validated['is_verified_company'].sum()
        french_total = len(df_french_validated)
        
        # Analyse par méthode de validation
        method_stats = df_french_validated['sirene_match_method'].value_counts()
        
        logger.info(f"[OK] Validation SIRENE optimisée terminée:")
        logger.info(f"   - Entreprises françaises vérifiées: {french_verified}/{french_total} ({french_verified/french_total*100:.1f}%)")
        logger.info(f"   - Méthodes utilisées: {dict(method_stats)}")
        logger.info(f"   - Autres pays: {len(df_others)} (non validés)")
        
        return df_complete
    
    def process_real_data(self):
        """Pipeline principal pour traiter les vraies données"""
        logger.info("Traitement des données réelles...")
        
        results = {}
        
        # Nettoyage Adzuna
        adzuna_clean = self.clean_adzuna_data()
        if adzuna_clean is not None:
            # Validation SIRENE pour les entreprises françaises
            adzuna_validated = self.validate_french_companies(adzuna_clean)
            
            output_path = self.project_root / "data" / "clean" / "adzuna_jobs_clean.parquet"
            adzuna_validated.to_parquet(output_path, compression='snappy', index=False)
            logger.info(f"[OK] Adzuna sauvé: {len(adzuna_validated)} lignes → {output_path}")
            results['adzuna'] = len(adzuna_validated)
            adzuna_clean = adzuna_validated
        
        # Nettoyage Glassdoor
        glassdoor_clean = self.clean_glassdoor_data()
        if glassdoor_clean is not None:
            # Validation SIRENE pour les entreprises françaises
            glassdoor_validated = self.validate_french_companies(glassdoor_clean)
            
            output_path = self.project_root / "data" / "clean" / "glassdoor_jobs_clean.parquet"
            glassdoor_validated.to_parquet(output_path, compression='snappy', index=False)
            logger.info(f"[OK] Glassdoor sauvé: {len(glassdoor_validated)} lignes → {output_path}")
            results['glassdoor'] = len(glassdoor_validated)
            glassdoor_clean = glassdoor_validated
        
        # Nettoyage GitHub
        github_clean = self.clean_github_data()
        if github_clean is not None:
            output_path = self.project_root / "data" / "clean" / "github_jobs_clean.parquet"
            github_clean.to_parquet(output_path, compression='snappy', index=False)
            logger.info(f"[OK] GitHub sauvé: {len(github_clean)} lignes → {output_path}")
            results['github'] = len(github_clean)
        
        # Nettoyage Trends
        trends_clean = self.clean_trends_data()
        if trends_clean is not None:
            output_path = self.project_root / "data" / "clean" / "trends_data_clean.parquet"
            trends_clean.to_parquet(output_path, compression='snappy', index=False)
            logger.info(f"[OK] Trends sauvé: {len(trends_clean)} lignes → {output_path}")
            results['trends'] = len(trends_clean)
        
        # Nettoyage Surveys
        surveys_clean = self.clean_survey_data()
        if surveys_clean is not None:
            output_path = self.project_root / "data" / "clean" / "surveys_data_clean.parquet"
            surveys_clean.to_parquet(output_path, compression='snappy', index=False)
            logger.info(f"[OK] Surveys sauvé: {len(surveys_clean)} lignes → {output_path}")
            results['surveys'] = len(surveys_clean)
        
        # Consolidation
        if adzuna_clean is not None and glassdoor_clean is not None:
            # Harmonisation des colonnes communes
            common_cols = ['title', 'company', 'location', 'country_normalized', 'region', 
                          'skills_normalized', 'salary_eur_avg', 'source_type', 'processed_at',
                          'is_verified_company', 'sirene_match_score', 'sirene_match_method']
            
            available_cols = [col for col in common_cols if col in adzuna_clean.columns and col in glassdoor_clean.columns]
            
            df_consolidated = pd.concat([
                adzuna_clean[available_cols],
                glassdoor_clean[available_cols]
            ], ignore_index=True)
            
            output_path = self.project_root / "data" / "clean" / "jobs_consolidated_clean.parquet"
            df_consolidated.to_parquet(output_path, compression='snappy', index=False)
            logger.info(f"[OK] Consolidé sauvé: {len(df_consolidated)} lignes → {output_path}")
            results['consolidated'] = len(df_consolidated)
            
            # Statistiques globales de validation
            french_jobs = df_consolidated[df_consolidated['country_normalized'] == 'FR']
            if len(french_jobs) > 0:
                verified_count = french_jobs['is_verified_company'].sum()
                total_french = len(french_jobs)
                logger.info(f"Validation globale: {verified_count}/{total_french} entreprises françaises vérifiées ({verified_count/total_french*100:.1f}%)")
        
        return results

    def remove_duplicates_safe(self, df, source_name):
        """Supprime les doublons en excluant les colonnes contenant des listes"""
        initial_count = len(df)
        
        # Identifier les colonnes contenant des listes
        list_columns = []
        for col in df.columns:
            try:
                if df[col].apply(lambda x: isinstance(x, list)).any():
                    list_columns.append(col)
            except:
                # En cas d'erreur, passer à la colonne suivante
                continue
        
        # Colonnes à utiliser pour la détection de doublons
        duplicate_check_columns = [col for col in df.columns if col not in list_columns]
        
        if len(duplicate_check_columns) > 0:
            df_clean = df.drop_duplicates(subset=duplicate_check_columns)
        else:
            # Si toutes les colonnes contiennent des listes, utiliser l'index
            df_clean = df.drop_duplicates()
        
        final_count = len(df_clean)
        duplicates_removed = initial_count - final_count
        
        if duplicates_removed > 0:
            logger.info(f"{source_name} - Doublons supprimés: {duplicates_removed} lignes ({duplicates_removed/initial_count*100:.1f}%)")
        else:
            logger.info(f"{source_name} - Aucun doublon détecté")
        
        return df_clean

def main():
    """Fonction principale"""
    logger.info("Démarrage du nettoyage de données - Bryan")
    
    try:
        cleaner = DataCleaner()
        
        # Test dictionnaires d'abord
        logger.info("Test rapide des dictionnaires...")
        test_tech = cleaner.normalize_technology("py")
        test_country = cleaner.normalize_country("france")
        logger.info(f"Test normalisation: py → {test_tech}, france → {test_country}")
        
        # Traitement des vraies données
        results = cleaner.process_real_data()
        
        if results:
            logger.info("[OK] Pipeline terminé avec succès!")
            logger.info(f"Résultats:")
            for source, count in results.items():
                logger.info(f"   - {source}: {count} lignes")
        else:
            logger.error("[NOK] Aucune donnée traitée.")
            
    except Exception as e:
        logger.error(f"[NOK] Erreur générale: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
