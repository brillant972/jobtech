#!/usr/bin/env python3
"""
Classe de base simplifiée pour les nettoyeurs de données
"""

import pandas as pd
import logging
from pathlib import Path
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

class BaseDataCleaner(ABC):
    """Classe de base simplifiée pour tous les nettoyeurs de données"""
    
    def __init__(self, project_root, dictionaries):
        self.project_root = Path(project_root)
        self.dictionaries = dictionaries
    
    @abstractmethod
    def clean_data(self):
        """Méthode abstraite pour nettoyer les données"""
        pass
    
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
    
    def normalize_salary(self, salary_value, currency):
        """Normalise un salaire en EUR"""
        if pd.isna(salary_value) or salary_value == 0:
            return None
        
        rates = {'EUR': 1.0, 'USD': 0.92, 'GBP': 1.17, 'CHF': 1.05}
        currency_clean = str(currency).upper() if currency else 'EUR'
        rate = rates.get(currency_clean, 1.0)
        
        try:
            return float(salary_value) * rate
        except:
            return None
    
    def normalize_skills(self, skills_str):
        """Normalise une liste de compétences"""
        if pd.isna(skills_str):
            return None
        
        try:
            if isinstance(skills_str, str):
                if skills_str.startswith('['):
                    skills_list = eval(skills_str)
                else:
                    skills_list = [s.strip() for s in skills_str.split(',')]
            else:
                skills_list = [str(skills_str)]
            
            normalized = [self.normalize_technology(skill) for skill in skills_list if skill]
            return normalized if normalized else None
            
        except Exception as e:
            logger.warning(f"Erreur normalisation skills '{skills_str}': {e}")
            return None
    
    def remove_duplicates(self, df, source_name):
        """Supprime les doublons"""
        initial_count = len(df)
        df_clean = df.drop_duplicates()
        final_count = len(df_clean)
        duplicates_removed = initial_count - final_count
        
        if duplicates_removed > 0:
            logger.info(f"{source_name} - Doublons supprimés: {duplicates_removed}")
        else:
            logger.info(f"{source_name} - Aucun doublon détecté")
        
        return df_clean
    
    def remove_duplicates_safe(self, df, source_name):
        """Supprime les doublons en gérant les colonnes avec des listes"""
        initial_count = len(df)
        
        # Convertir les colonnes avec des listes en strings pour la comparaison
        df_temp = df.copy()
        for col in df_temp.columns:
            if df_temp[col].dtype == 'object':
                # Convertir les listes en strings
                df_temp[col] = df_temp[col].apply(lambda x: str(x) if isinstance(x, list) else x)
        
        # Supprimer les doublons sur la version temporaire
        df_clean = df.loc[~df_temp.duplicated()]
        
        final_count = len(df_clean)
        duplicates_removed = initial_count - final_count
        
        if duplicates_removed > 0:
            logger.info(f"{source_name} - Doublons supprimés: {duplicates_removed}")
        else:
            logger.info(f"{source_name} - Aucun doublon détecté")
        
        return df_clean
