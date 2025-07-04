#!/usr/bin/env python3
"""
Nettoyeur simplifié pour les données d'emploi (Adzuna et Glassdoor)
"""

import pandas as pd
import logging
from .base_cleaner import BaseDataCleaner

logger = logging.getLogger(__name__)

class JobDataCleaner(BaseDataCleaner):
    """Nettoyeur simplifié pour les données d'emploi"""
    
    def __init__(self, project_root, dictionaries, sirene_validator):
        super().__init__(project_root, dictionaries)
        self.sirene_validator = sirene_validator
    
    def clean_data(self):
        """Interface principale pour nettoyer les données d'emploi"""
        results = {}
        
        # Nettoyage Adzuna
        adzuna_clean = self._clean_adzuna()
        if adzuna_clean is not None:
            adzuna_validated = self._validate_french_companies(adzuna_clean)
            results['adzuna'] = adzuna_validated
        
        # Nettoyage Glassdoor
        glassdoor_clean = self._clean_glassdoor()
        if glassdoor_clean is not None:
            glassdoor_validated = self._validate_french_companies(glassdoor_clean)
            results['glassdoor'] = glassdoor_validated
        
        return results
    
    def _clean_adzuna(self):
        """Nettoie les données Adzuna"""
        logger.info("Nettoyage données Adzuna...")
        
        adzuna_files = list((self.project_root / "data" / "raw" / "adzuna").glob("*.csv"))
        if not adzuna_files:
            logger.warning("Aucune donnée Adzuna trouvée")
            return None
        
        all_data = []
        for file in adzuna_files:
            try:
                df = pd.read_csv(file)
                logger.info(f"{file.name}: {len(df)} lignes")
                all_data.append(df)
            except Exception as e:
                logger.error(f"Erreur lecture {file.name}: {e}")
        
        if not all_data:
            return None
        
        df = pd.concat(all_data, ignore_index=True)
        logger.info(f"Total Adzuna: {len(df)} lignes")
        
        return self._normalize_job_data(df)
    
    def _clean_glassdoor(self):
        """Nettoie les données Glassdoor"""
        logger.info("Nettoyage données Glassdoor...")
        
        glassdoor_file = self.project_root / "data" / "raw" / "glassdoor" / "glassdoor_tech_jobs_europe_2025-07-02_11-14.csv"
        if not glassdoor_file.exists():
            logger.warning("Fichier Glassdoor non trouvé")
            return None
        
        try:
            df = pd.read_csv(glassdoor_file)
            logger.info(f"Glassdoor: {len(df)} lignes")
            
            # Harmonisation colonnes
            if 'job_title' in df.columns:
                df = df.rename(columns={'job_title': 'title'})
            if 'country_code' in df.columns:
                df = df.rename(columns={'country_code': 'country'})
            
            return self._normalize_job_data(df)
            
        except Exception as e:
            logger.error(f"Erreur lecture Glassdoor: {e}")
            return None
    
    def _normalize_job_data(self, df):
        """Normalise les données d'emploi"""
        df_clean = df.copy()
        
        # Normalisation
        if 'skills' in df_clean.columns:
            df_clean['skills_normalized'] = df_clean['skills'].apply(self.normalize_skills)
        
        if 'country' in df_clean.columns:
            df_clean['country_normalized'] = df_clean['country'].apply(self.normalize_country)
        
        # Salaires
        if 'salary_min' in df_clean.columns:
            df_clean['salary_eur_min'] = df_clean.apply(
                lambda row: self.normalize_salary(row.get('salary_min'), row.get('currency')), axis=1)
        
        if 'salary_max' in df_clean.columns:
            df_clean['salary_eur_max'] = df_clean.apply(
                lambda row: self.normalize_salary(row.get('salary_max'), row.get('currency')), axis=1)
        
        if 'salary_eur_min' in df_clean.columns and 'salary_eur_max' in df_clean.columns:
            df_clean['salary_eur_avg'] = (df_clean['salary_eur_min'] + df_clean['salary_eur_max']) / 2
        
        # Métadonnées
        df_clean['source_type'] = 'job_board'
        df_clean['processed_at'] = pd.Timestamp.now()
        
        return self.remove_duplicates_safe(df_clean, "Jobs")
    
    def _validate_french_companies(self, df):
        """Valide les entreprises françaises via SIRENE"""
        logger.info("Validation entreprises françaises...")
        
        df_french = df[df['country_normalized'] == 'FR'].copy()
        df_others = df[df['country_normalized'] != 'FR'].copy()
        
        if len(df_french) == 0:
            df_others['is_verified_company'] = None
            df_others['sirene_match_score'] = None
            df_others['sirene_match_method'] = None
            return df_others
        
        logger.info(f"Validation de {len(df_french)} entreprises françaises...")
        
        df_french_validated = self.sirene_validator.validate_dataframe_optimized(
            df_french, company_col='company', chunk_size=50)
        
        df_others['is_verified_company'] = None
        df_others['sirene_match_score'] = None
        df_others['sirene_match_method'] = None
        
        df_complete = pd.concat([df_french_validated, df_others], ignore_index=True)
        
        verified = df_french_validated['is_verified_company'].sum()
        total = len(df_french_validated)
        logger.info(f"Entreprises vérifiées: {verified}/{total} ({verified/total*100:.1f}%)")
        
        return df_complete
