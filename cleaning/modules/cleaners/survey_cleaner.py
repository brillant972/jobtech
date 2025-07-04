#!/usr/bin/env python3
"""
Nettoyeur pour les données de surveys (Kaggle et StackOverflow)
"""

import pandas as pd
import logging
from pathlib import Path
from .base_cleaner import BaseDataCleaner

logger = logging.getLogger(__name__)

class SurveyDataCleaner(BaseDataCleaner):
    """Nettoyeur pour les données de surveys avec séparation par type"""
    
    def clean_data(self):
        """Nettoie et normalise les données de surveys séparément par type"""
        results = {}
        
        # Nettoyage Kaggle
        kaggle_europe, kaggle_raw = self._clean_kaggle_data()
        if kaggle_europe is not None:
            results['kaggle_europe'] = self._normalize_survey_data(kaggle_europe, "Kaggle Europe")
        if kaggle_raw is not None:
            results['kaggle_raw'] = self._normalize_survey_data(kaggle_raw, "Kaggle Raw")
        
        # Nettoyage StackOverflow
        stackoverflow_data = self._clean_stackoverflow_data()
        if stackoverflow_data is not None:
            results['stackoverflow'] = self._normalize_survey_data(stackoverflow_data, "StackOverflow")
        
        return results if results else None
    
    def _clean_kaggle_data(self):
        """Nettoie les données Kaggle avec séparation par type"""
        logger.info("Nettoyage données Kaggle...")
        
        kaggle_path = self.project_root / "data" / "raw" / "kaggle"
        if not kaggle_path.exists():
            logger.warning("[ATTENTION] Dossier Kaggle non trouvé")
            return None, None
        
        kaggle_files = list(kaggle_path.glob("*.csv"))
        
        # Séparation des fichiers par type
        europe_files = [f for f in kaggle_files if 'europe' in f.name]
        raw_files = [f for f in kaggle_files if 'raw' in f.name and 'europe' not in f.name]
        
        # Traitement Kaggle Europe
        df_europe = self._process_kaggle_files(europe_files, "kaggle_europe")
        
        # Traitement Kaggle Raw
        df_raw = self._process_kaggle_files(raw_files, "kaggle_raw")
        
        return df_europe, df_raw
    
    def _process_kaggle_files(self, files, kaggle_type):
        """Traite les fichiers Kaggle selon le type"""
        if not files:
            return None
        
        logger.info(f"Fichiers {kaggle_type}: {len(files)}")
        all_data = []
        
        for file in files:
            try:
                df = pd.read_csv(file)
                df['survey_source'] = kaggle_type
                df['source_file'] = file.name
                logger.info(f"{kaggle_type} {file.name}: {len(df)} lignes")
                all_data.append(df)
            except Exception as e:
                logger.error(f"[NOK] Erreur lecture {kaggle_type} {file.name}: {e}")
        
        if not all_data:
            return None
        
        df_consolidated = pd.concat(all_data, ignore_index=True)
        logger.info(f"Total {kaggle_type}: {len(df_consolidated)} lignes")
        
        return df_consolidated
    
    def _clean_stackoverflow_data(self):
        """Nettoie les données StackOverflow"""
        logger.info("Nettoyage données StackOverflow...")
        
        stackoverflow_path = self.project_root / "data" / "raw" / "stackoverflow"
        if not stackoverflow_path.exists():
            logger.warning("[ATTENTION] Dossier StackOverflow non trouvé")
            return None
        
        stackoverflow_files = list(stackoverflow_path.glob("*.csv"))
        
        # Prioriser les fichiers consolidés
        all_files = [f for f in stackoverflow_files if 'all_countries' in f.name or 'all' in f.name]
        individual_files = [f for f in stackoverflow_files if 
                           'stackoverflow_' in f.name and 'all' not in f.name]
        
        files_to_process = all_files if all_files else individual_files
        
        if not files_to_process:
            return None
        
        if all_files:
            logger.info(f"Fichiers StackOverflow consolidés: {len(all_files)}")
        else:
            logger.info(f"Fichiers StackOverflow individuels: {len(individual_files)}")
            logger.info("ATTENTION: Consolidation des fichiers individuels")
        
        all_data = []
        for file in files_to_process:
            try:
                df = pd.read_csv(file)
                df['survey_source'] = 'stackoverflow'
                df['source_file'] = file.name
                logger.info(f"StackOverflow {file.name}: {len(df)} lignes")
                all_data.append(df)
            except Exception as e:
                logger.error(f"[NOK] Erreur lecture StackOverflow {file.name}: {e}")
        
        if not all_data:
            return None
        
        df_consolidated = pd.concat(all_data, ignore_index=True)
        logger.info(f"StackOverflow consolidé: {len(df_consolidated)} lignes")
        
        return df_consolidated
    
    def _normalize_survey_data(self, df, survey_type):
        """Normalise les données de surveys"""
        if df is None or len(df) == 0:
            return None
        
        df_clean = df.copy()
        
        # Harmonisation des colonnes pays
        if 'country_code' in df_clean.columns and 'country' not in df_clean.columns:
            df_clean['country'] = df_clean['country_code']
        
        # Normalisation pays
        if 'country' in df_clean.columns:
            df_clean['country_normalized'] = df_clean['country'].apply(self.normalize_country)
        
        # Normalisation des compétences/langages
        skill_columns = ['skills', 'languages_worked', 'language']
        for col in skill_columns:
            if col in df_clean.columns:
                df_clean[f'{col}_normalized'] = df_clean[col].apply(self.normalize_skills)
        
        # Normalisation salaires
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
        
        # Suppression des doublons
        df_clean = self.remove_duplicates_safe(df_clean, survey_type)
        
        return df_clean
