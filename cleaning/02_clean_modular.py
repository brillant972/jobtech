#!/usr/bin/env python3
"""
Script principal de nettoyage et normalisation
"""

import pandas as pd
import json
import logging
from pathlib import Path
import sys

# Import des modules de nettoyage
sys.path.append(str(Path(__file__).parent))
from modules.sirene_validator import SIRENEValidator
from modules.cleaners.job_cleaner import JobDataCleaner
from modules.cleaners.github_cleaner import GitHubDataCleaner
from modules.cleaners.trends_cleaner import TrendsDataCleaner
from modules.cleaners.survey_cleaner import SurveyDataCleaner

# Configuration des logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataCleaningPipeline:
    """Pipeline principal de nettoyage des données"""
    
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
    
    def test_dictionaries(self):
        """Test rapide des dictionnaires"""
        logger.info("Test rapide des dictionnaires...")
        
        # Test de normalisation
        test_tech = self.dictionaries['tech'].get('py', 'py')
        test_country = self.dictionaries['countries'].get('france', 'france')
        
        logger.info(f"Test normalisation: py → {test_tech}, france → {test_country}")
    
    def process_job_data(self):
        """Traite les données d'emploi (Adzuna + Glassdoor)"""
        logger.info("=== TRAITEMENT DES DONNÉES D'EMPLOI ===")
        
        job_cleaner = JobDataCleaner(self.project_root, self.dictionaries, self.sirene_validator)
        job_results = job_cleaner.clean_data()
        
        results = {}
        
        # Sauvegarde des résultats
        if 'adzuna' in job_results:
            output_path = self.project_root / "data" / "clean" / "adzuna_jobs_clean.parquet"
            job_results['adzuna'].to_parquet(output_path, compression='snappy', index=False)
            logger.info(f"[OK] Adzuna sauvé: {len(job_results['adzuna'])} lignes → {output_path}")
            results['adzuna'] = len(job_results['adzuna'])
        
        if 'glassdoor' in job_results:
            output_path = self.project_root / "data" / "clean" / "glassdoor_jobs_clean.parquet"
            job_results['glassdoor'].to_parquet(output_path, compression='snappy', index=False)
            logger.info(f"[OK] Glassdoor sauvé: {len(job_results['glassdoor'])} lignes → {output_path}")
            results['glassdoor'] = len(job_results['glassdoor'])
        
        # Consolidation des offres d'emploi
        if 'adzuna' in job_results and 'glassdoor' in job_results:
            consolidated = self._consolidate_job_data(job_results['adzuna'], job_results['glassdoor'])
            if consolidated is not None:
                output_path = self.project_root / "data" / "clean" / "jobs_consolidated_clean.parquet"
                consolidated.to_parquet(output_path, compression='snappy', index=False)
                logger.info(f"[OK] Consolidé sauvé: {len(consolidated)} lignes → {output_path}")
                results['consolidated'] = len(consolidated)
                
                # Statistiques globales de validation
                self._log_validation_stats(consolidated)
        
        return results
    
    def process_github_data(self):
        """Traite les données GitHub"""
        logger.info("=== TRAITEMENT DES DONNÉES GITHUB ===")
        
        github_cleaner = GitHubDataCleaner(self.project_root, self.dictionaries)
        github_results = github_cleaner.clean_data()
        
        results = {}
        
        if github_results:
            # Sauvegarde séparée des language stats et trending repos
            if 'language_stats' in github_results:
                output_path = self.project_root / "data" / "clean" / "github_language_stats_clean.parquet"
                github_results['language_stats'].to_parquet(output_path, compression='snappy', index=False)
                logger.info(f"[OK] GitHub Language Stats sauvé: {len(github_results['language_stats'])} lignes → {output_path}")
                results['github_language_stats'] = len(github_results['language_stats'])
            
            if 'trending_repos' in github_results:
                output_path = self.project_root / "data" / "clean" / "github_trending_repos_clean.parquet"
                github_results['trending_repos'].to_parquet(output_path, compression='snappy', index=False)
                logger.info(f"[OK] GitHub Trending Repos sauvé: {len(github_results['trending_repos'])} lignes → {output_path}")
                results['github_trending_repos'] = len(github_results['trending_repos'])
        
        return results
    
    def process_trends_data(self):
        """Traite les données Google Trends"""
        logger.info("=== TRAITEMENT DES DONNÉES GOOGLE TRENDS ===")
        
        trends_cleaner = TrendsDataCleaner(self.project_root, self.dictionaries)
        trends_results = trends_cleaner.clean_data()
        
        results = {}
        
        if trends_results:
            # Sauvegarde séparée des comparaisons tech et tendances pays
            if 'tech_comparisons' in trends_results:
                output_path = self.project_root / "data" / "clean" / "tech_comparisons_clean.parquet"
                trends_results['tech_comparisons'].to_parquet(output_path, compression='snappy', index=False)
                logger.info(f"[OK] Tech Comparisons sauvé: {len(trends_results['tech_comparisons'])} lignes → {output_path}")
                results['tech_comparisons'] = len(trends_results['tech_comparisons'])
            
            if 'country_trends' in trends_results:
                output_path = self.project_root / "data" / "clean" / "country_trends_clean.parquet"
                trends_results['country_trends'].to_parquet(output_path, compression='snappy', index=False)
                logger.info(f"[OK] Country Trends sauvé: {len(trends_results['country_trends'])} lignes → {output_path}")
                results['country_trends'] = len(trends_results['country_trends'])
        
        return results
    
    def process_survey_data(self):
        """Traite les données de surveys"""
        logger.info("=== TRAITEMENT DES DONNÉES SURVEYS ===")
        
        survey_cleaner = SurveyDataCleaner(self.project_root, self.dictionaries)
        survey_results = survey_cleaner.clean_data()
        
        results = {}
        
        if survey_results:
            # Sauvegarde séparée de chaque survey
            if 'kaggle_europe' in survey_results:
                output_path = self.project_root / "data" / "clean" / "kaggle_europe_clean.parquet"
                survey_results['kaggle_europe'].to_parquet(output_path, compression='snappy', index=False)
                logger.info(f"[OK] Kaggle Europe sauvé: {len(survey_results['kaggle_europe'])} lignes → {output_path}")
                results['kaggle_europe'] = len(survey_results['kaggle_europe'])
            
            if 'kaggle_raw' in survey_results:
                output_path = self.project_root / "data" / "clean" / "kaggle_raw_clean.parquet"
                survey_results['kaggle_raw'].to_parquet(output_path, compression='snappy', index=False)
                logger.info(f"[OK] Kaggle Raw sauvé: {len(survey_results['kaggle_raw'])} lignes → {output_path}")
                results['kaggle_raw'] = len(survey_results['kaggle_raw'])
            
            if 'stackoverflow' in survey_results:
                output_path = self.project_root / "data" / "clean" / "stackoverflow_clean.parquet"
                survey_results['stackoverflow'].to_parquet(output_path, compression='snappy', index=False)
                logger.info(f"[OK] StackOverflow sauvé: {len(survey_results['stackoverflow'])} lignes → {output_path}")
                results['stackoverflow'] = len(survey_results['stackoverflow'])
        
        return results
    
    def _consolidate_job_data(self, adzuna_df, glassdoor_df):
        """Consolide les données d'emploi"""
        # Harmonisation des colonnes communes
        common_cols = ['title', 'company', 'location', 'country_normalized', 'region', 
                      'skills_normalized', 'salary_eur_avg', 'source_type', 'processed_at',
                      'is_verified_company', 'sirene_match_score', 'sirene_match_method']
        
        available_cols = [col for col in common_cols if col in adzuna_df.columns and col in glassdoor_df.columns]
        
        if not available_cols:
            logger.warning("Aucune colonne commune trouvée pour la consolidation")
            return None
        
        df_consolidated = pd.concat([
            adzuna_df[available_cols],
            glassdoor_df[available_cols]
        ], ignore_index=True)
        
        return df_consolidated
    
    def _log_validation_stats(self, consolidated_df):
        """Affiche les statistiques de validation SIRENE"""
        french_jobs = consolidated_df[consolidated_df['country_normalized'] == 'FR']
        if len(french_jobs) > 0:
            verified_count = french_jobs['is_verified_company'].sum()
            total_french = len(french_jobs)
            logger.info(f"Validation globale: {verified_count}/{total_french} entreprises françaises vérifiées ({verified_count/total_french*100:.1f}%)")
    
    def run_pipeline(self):
        """Exécute le pipeline complet"""
        logger.info("Démarrage du pipeline de nettoyage modulaire - Bryan")
        
        # Test des dictionnaires
        self.test_dictionaries()
        
        # Traitement par module
        all_results = {}
        
        # 1. Données d'emploi
        job_results = self.process_job_data()
        all_results.update(job_results)
        
        # 2. Données GitHub
        github_results = self.process_github_data()
        all_results.update(github_results)
        
        # 3. Données Google Trends
        trends_results = self.process_trends_data()
        all_results.update(trends_results)
        
        # 4. Données de surveys
        survey_results = self.process_survey_data()
        all_results.update(survey_results)
        
        return all_results

def main():
    """Fonction principale"""
    try:
        pipeline = DataCleaningPipeline()
        results = pipeline.run_pipeline()
        
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
