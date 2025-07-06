#!/usr/bin/env python3
"""
Nettoyeur pour les données GitHub
"""

import pandas as pd
import logging
from pathlib import Path
from .base_cleaner import BaseDataCleaner

logger = logging.getLogger(__name__)

class GitHubDataCleaner(BaseDataCleaner):
    """Nettoyeur pour les données GitHub avec séparation par type"""
    
    def clean_data(self):
        """Nettoie et normalise les données GitHub avec séparation par type"""
        logger.info("Nettoyage données GitHub...")
        
        github_path = self.project_root / "data" / "raw" / "github"
        if not github_path.exists():
            logger.warning("[ATTENTION] Dossier GitHub non trouvé")
            return None
        
        github_files = list(github_path.glob("*.csv"))
        
        # Séparation par type de fichier
        language_files = [f for f in github_files if 'language_stats' in f.name]
        trending_files = [f for f in github_files if 'trending_repos' in f.name]
        
        result_dfs = {}
        
        # Traitement des statistiques de langages
        if language_files:
            language_stats = self._process_files(language_files, 'language_stats')
            if language_stats is not None:
                result_dfs['language_stats'] = language_stats
        
        # Traitement des repos trending
        if trending_files:
            trending_repos = self._process_files(trending_files, 'trending_repos')
            if trending_repos is not None:
                result_dfs['trending_repos'] = trending_repos
        
        if not result_dfs:
            logger.warning("[ATTENTION] Aucune donnée GitHub trouvée")
            return None
        
        return result_dfs
    
    def _process_files(self, files, github_type):
        """Traite les fichiers GitHub selon le type"""
        logger.info(f"Fichiers {github_type}: {len(files)}")
        all_data = []
        
        for file in files:
            try:
                df = pd.read_csv(file)
                df['source_file'] = file.name
                df['github_data_type'] = github_type
                logger.info(f"{file.name}: {len(df)} lignes ({github_type})")
                all_data.append(df)
            except Exception as e:
                logger.error(f"[NOK] Erreur lecture {file.name}: {e}")
        
        if not all_data:
            return None
        
        df_consolidated = pd.concat(all_data, ignore_index=True)
        logger.info(f"{github_type} consolidés: {len(df_consolidated)} lignes")
        
        # Normalisation des langages si colonne 'language' présente
        if 'language' in df_consolidated.columns:
            df_consolidated['language_normalized'] = df_consolidated['language'].apply(self.normalize_technology)
        
        # Enrichissement métadonnées
        df_consolidated['source_type'] = f'github_{github_type}'
        df_consolidated['processed_at'] = pd.Timestamp.now()
        
        # Suppression des doublons
        df_consolidated = self.remove_duplicates_safe(df_consolidated, f"GitHub {github_type}")
        
        return df_consolidated
