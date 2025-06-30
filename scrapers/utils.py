"""
Fonctions utilitaires pour les scrapers TalentInsight
"""
import os
import time
import random
import logging
import requests
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional
from config import DEFAULT_HEADERS, REQUEST_DELAY_MIN, REQUEST_DELAY_MAX, TECH_SKILLS

def setup_logging(scraper_name: str) -> logging.Logger:
    """Configure le logging pour un scraper"""
    os.makedirs('logs', exist_ok=True)
    
    logger = logging.getLogger(scraper_name)
    logger.setLevel(logging.INFO)
    
    # Handler fichier
    file_handler = logging.FileHandler(
        f'logs/{scraper_name}_{datetime.now().strftime("%Y%m%d")}.log',
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    
    # Handler console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Format
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    return logger

def safe_request(url: str, headers: Optional[Dict] = None, params: Optional[Dict] = None, 
                delay: bool = True, timeout: int = 30) -> Optional[requests.Response]:
    """Effectue une requête HTTP sécurisée avec gestion d'erreurs"""
    
    if delay:
        time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))
    
    request_headers = DEFAULT_HEADERS.copy()
    if headers:
        request_headers.update(headers)
    
    try:
        response = requests.get(
            url, 
            headers=request_headers, 
            params=params,
            timeout=timeout
        )
        response.raise_for_status()
        return response
        
    except requests.exceptions.Timeout:
        print(f"⏰ Timeout pour {url}")
        return None
    except requests.exceptions.ConnectionError:
        print(f"🔌 Erreur de connexion pour {url}")
        return None
    except requests.exceptions.HTTPError as e:
        print(f"❌ Erreur HTTP {e.response.status_code} pour {url}")
        return None
    except Exception as e:
        print(f"💥 Erreur inattendue pour {url}: {e}")
        return None

def extract_skills_from_text(text: str, skills_list: List[str] = None) -> List[str]:
    """Extrait les compétences techniques d'un texte"""
    if not text:
        return []
    
    if skills_list is None:
        skills_list = TECH_SKILLS
    
    text_lower = text.lower()
    found_skills = []
    
    for skill in skills_list:
        # Recherche exacte (mots complets)
        if f" {skill.lower()} " in f" {text_lower} ":
            found_skills.append(skill)
    
    return list(set(found_skills))  # Supprime les doublons

def clean_salary(salary_str: str) -> Optional[float]:
    """Nettoie et convertit une chaîne de salaire en nombre"""
    if not salary_str:
        return None
    
    # Supprimer les caractères non numériques sauf point et virgule
    import re
    cleaned = re.sub(r'[^\d.,]', '', str(salary_str))
    
    if not cleaned:
        return None
    
    try:
        # Remplacer virgule par point si nécessaire
        cleaned = cleaned.replace(',', '.')
        return float(cleaned)
    except ValueError:
        return None

def save_to_csv(data: List[Dict], filename: str, logger: logging.Logger = None) -> bool:
    """Sauvegarde des données en CSV avec pandas"""
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False, encoding='utf-8')
        
        msg = f"✅ Sauvegardé {len(data)} enregistrements dans {filename}"
        print(msg)
        if logger:
            logger.info(msg)
        
        return True
        
    except Exception as e:
        error_msg = f"❌ Erreur sauvegarde {filename}: {e}"
        print(error_msg)
        if logger:
            logger.error(error_msg)
        return False

def save_metadata(metadata: Dict, filename: str) -> bool:
    """Sauvegarde les métadonnées en JSON"""
    import json
    
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"📋 Métadonnées sauvegardées: {filename}")
        return True
        
    except Exception as e:
        print(f"❌ Erreur métadonnées {filename}: {e}")
        return False

def normalize_country_code(country: str) -> str:
    """Normalise les codes pays"""
    country_mapping = {
        'fr': 'FR', 'france': 'FR',
        'de': 'DE', 'germany': 'DE', 'deutschland': 'DE',
        'nl': 'NL', 'netherlands': 'NL', 'nederland': 'NL',
        'gb': 'GB', 'uk': 'GB', 'england': 'GB', 'united kingdom': 'GB',
        'it': 'IT', 'italy': 'IT', 'italia': 'IT'
    }
    
    return country_mapping.get(country.lower(), country.upper())

def create_output_filename(source: str, country: str, extension: str = 'csv') -> str:
    """Crée un nom de fichier standardisé"""
    date_str = datetime.now().strftime('%Y-%m-%d')
    return f"raw/{source}/{source}_{country.lower()}_jobs_{date_str}.{extension}"

def print_summary(source: str, total_records: int, countries: List[str]) -> None:
    """Affiche un résumé de la collecte"""
    print("\n" + "="*50)
    print(f"📊 RÉSUMÉ - {source.upper()}")
    print("="*50)
    print(f"🔢 Total enregistrements: {total_records}")
    print(f"🌍 Pays traités: {', '.join(countries)}")
    print(f"⏰ Collecté le: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*50)

def validate_required_fields(record: Dict, required_fields: List[str]) -> bool:
    """Valide qu'un enregistrement contient les champs requis"""
    for field in required_fields:
        if field not in record or record[field] is None:
            return False
    return True

def standardize_experience_level(text: str) -> str:
    """Standardise le niveau d'expérience"""
    if not text:
        return 'Unknown'
    
    text_lower = text.lower()
    
    if any(word in text_lower for word in ['junior', 'entry', 'débutant', 'graduate']):
        return 'Junior'
    elif any(word in text_lower for word in ['senior', 'lead', 'principal', 'expert']):
        return 'Senior'
    elif any(word in text_lower for word in ['mid', 'intermediate', 'confirmé']):
        return 'Mid-level'
    else:
        return 'Unknown'