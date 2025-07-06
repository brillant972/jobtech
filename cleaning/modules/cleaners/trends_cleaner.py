#!/usr/bin/env python3
"""
Nettoyeur pour les données Google Trends
"""

import pandas as pd
import logging
from pathlib import Path
from .base_cleaner import BaseDataCleaner

logger = logging.getLogger(__name__)

class TrendsDataCleaner(BaseDataCleaner):
    """Nettoyeur pour les données Google Trends avec séparation par type"""
    
    def clean_data(self):
        """Nettoie et normalise les données Google Trends avec séparation par type"""
        logger.info("Nettoyage données Google Trends...")
        
        trends_path = self.project_root / "data" / "raw" / "google_trends"
        if not trends_path.exists():
            logger.warning("[ATTENTION] Dossier Google Trends non trouvé")
            return None
        
        trends_files = list(trends_path.glob("*.csv"))
        
        # Séparation par type de fichier
        comparison_files = [f for f in trends_files if 'tech_comparisons' in f.name]
        
        # Fichiers trends : prioriser les fichiers "all" qui sont déjà consolidés
        all_country_files = [f for f in trends_files if 'all_countries' in f.name or 'all' in f.name]
        individual_country_files = [f for f in trends_files if 
                                   'trends_' in f.name and 
                                   'tech_comparisons' not in f.name and 
                                   'all' not in f.name]
        
        results = {}
        
        # Traitement des comparaisons tech
        if comparison_files:
            tech_comparisons = self._process_comparisons(comparison_files)
            if tech_comparisons is not None:
                results['tech_comparisons'] = tech_comparisons
        
        # Traitement des tendances pays
        country_trends = self._process_country_trends(all_country_files, individual_country_files)
        if country_trends is not None:
            results['country_trends'] = country_trends
        
        if not results:
            logger.warning("[ATTENTION] Aucune donnée Trends trouvée")
            return None
        
        return results
    
    def _process_comparisons(self, comparison_files):
        """Traite les comparaisons technologiques"""
        logger.info(f"Fichiers tech_comparisons: {len(comparison_files)}")
        all_data = []
        
        for file in comparison_files:
            try:
                df = pd.read_csv(file)
                df['source_file'] = file.name
                df['trends_data_type'] = 'tech_comparisons'
                logger.info(f"{file.name}: {len(df)} lignes (tech_comparisons)")
                all_data.append(df)
            except Exception as e:
                logger.error(f"[NOK] Erreur lecture {file.name}: {e}")
        
        if not all_data:
            return None
        
        df_consolidated = pd.concat(all_data, ignore_index=True)
        
        # Normalisation des pays et technologies
        if 'country' in df_consolidated.columns:
            df_consolidated['country_normalized'] = df_consolidated['country'].apply(self.normalize_country)
        if 'technology' in df_consolidated.columns:
            df_consolidated['technology_normalized'] = df_consolidated['technology'].apply(self.normalize_technology)
        
        df_consolidated['source_type'] = 'tech_comparisons'
        df_consolidated['processed_at'] = pd.Timestamp.now()
        
        # Suppression doublons
        df_consolidated = self.remove_duplicates_safe(df_consolidated, "Tech Comparisons")
        
        return df_consolidated
    
    def _process_country_trends(self, all_country_files, individual_country_files):
        """Traite les tendances par pays"""
        all_data = []
        
        if all_country_files:
            logger.info(f"Fichiers trends consolidés (all_countries): {len(all_country_files)}")
            for file in all_country_files:
                try:
                    df = pd.read_csv(file)
                    df['source_file'] = file.name
                    df['trends_data_type'] = 'country_trends_consolidated'
                    logger.info(f"{file.name}: {len(df)} lignes (déjà consolidé)")
                    all_data.append(df)
                except Exception as e:
                    logger.error(f"[NOK] Erreur lecture {file.name}: {e}")
        
        elif individual_country_files:
            logger.info(f"Fichiers trends individuels: {len(individual_country_files)}")
            logger.info("ATTENTION: Consolidation des fichiers individuels (pas de fichier 'all' trouvé)")
            
            for file in individual_country_files:
                try:
                    df = pd.read_csv(file)
                    df['source_file'] = file.name
                    df['trends_data_type'] = 'country_trends_individual'
                    logger.info(f"{file.name}: {len(df)} lignes (individuel)")
                    all_data.append(df)
                except Exception as e:
                    logger.error(f"[NOK] Erreur lecture {file.name}: {e}")
        
        if not all_data:
            return None
        
        df_consolidated = pd.concat(all_data, ignore_index=True)
        logger.info(f"Tendances pays: {len(df_consolidated)} lignes")
        
        # Normalisation des pays et mots-clés
        if 'country' in df_consolidated.columns:
            df_consolidated['country_normalized'] = df_consolidated['country'].apply(self.normalize_country)
        if 'keyword' in df_consolidated.columns:
            df_consolidated['keyword_normalized'] = df_consolidated['keyword'].apply(self.normalize_technology)
        
        df_consolidated['source_type'] = 'country_trends'
        df_consolidated['processed_at'] = pd.Timestamp.now()
        
        # Suppression doublons
        df_consolidated = self.remove_duplicates_safe(df_consolidated, "Country Trends")
        
        return df_consolidated
