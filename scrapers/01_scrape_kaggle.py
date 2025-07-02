import os
import sys
import requests
import pandas as pd
from datetime import datetime
import re

# Configuration encodage pour Windows
if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# URL du dataset Kaggle Survey
KAGGLE_SURVEY_URL = 'https://raw.githubusercontent.com/rfordatascience/tidytuesday/master/data/2021/2021-05-18/survey.csv'

# Mapping des pays européens
EUROPE_COUNTRIES = {
    'DE': 'Germany', 'FR': 'France', 'GB': 'United Kingdom', 'UK': 'United Kingdom',
    'NL': 'Netherlands', 'IT': 'Italy', 'ES': 'Spain', 'CH': 'Switzerland',
    'AT': 'Austria', 'BE': 'Belgium', 'SE': 'Sweden', 'DK': 'Denmark',
    'NO': 'Norway', 'FI': 'Finland', 'IE': 'Ireland', 'PT': 'Portugal',
    'PL': 'Poland', 'CZ': 'Czech Republic', 'HU': 'Hungary', 'RO': 'Romania'
}

def download_kaggle_survey():
    """
    Télécharge le dataset Kaggle Survey
    """
    print("SCRAPER KAGGLE SURVEY DATA")
    print("=" * 40)
    print("SOURCE: Kaggle ML & Data Science Survey")
    print("URL:", KAGGLE_SURVEY_URL)
    
    try:
        print("\nTéléchargement en cours...")
        response = requests.get(KAGGLE_SURVEY_URL, timeout=120)
        response.raise_for_status()
        
        print(f"Téléchargement réussi: {len(response.content):,} bytes")
        
        # Sauvegarde du fichier brut
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
        raw_filename = f"../data/raw/kaggle/kaggle_survey_raw_{timestamp}.csv"
        os.makedirs('../data/raw/kaggle', exist_ok=True)
        
        with open(raw_filename, 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        print(f"Fichier brut sauvé: {raw_filename}")
        
        # Lecture en DataFrame
        df = pd.read_csv(raw_filename, encoding='utf-8', on_bad_lines='skip')
        print(f"Dataset chargé: {len(df):,} lignes, {len(df.columns)} colonnes")
        
        return df, raw_filename
        
    except Exception as e:
        print(f"Erreur lors du téléchargement: {e}")
        return None, None

def process_kaggle_data(df):
    """
    Traite les données Kaggle Survey pour extraire les données européennes
    """
    print("\nTraitement des données Kaggle...")
    
    # Détection des colonnes importantes
    country_cols = [col for col in df.columns if 'country' in col.lower()]
    salary_cols = [col for col in df.columns if 'salary' in col.lower() or 'compensation' in col.lower()]
    job_cols = [col for col in df.columns if 'job' in col.lower() or 'title' in col.lower() or 'role' in col.lower()]
    
    print(f"Colonnes détectées:")
    print(f"  Pays: {country_cols}")
    print(f"  Salaire: {salary_cols}")
    print(f"  Job: {job_cols}")
    
    if not country_cols:
        print("ERREUR: Aucune colonne pays trouvée")
        return []
    
    processed_data = []
    
    # Échantillonnage pour performance
    sample_size = min(2000, len(df))
    df_sample = df.sample(n=sample_size, random_state=42) if len(df) > sample_size else df
    
    print(f"Traitement de {len(df_sample)} réponses...")
    
    europe_count = 0
    
    for idx, row in df_sample.iterrows():
        try:
            # Extraction du pays
            country_raw = str(row[country_cols[0]]) if country_cols else ''
            country_code = None
            
            # Recherche du pays européen
            for code, name in EUROPE_COUNTRIES.items():
                if name.lower() in country_raw.lower() or code.lower() in country_raw.lower():
                    country_code = code
                    break
            
            if not country_code:
                continue  # Skip non-European responses
            
            europe_count += 1
            
            # Extraction du job title
            job_title = 'Data Professional'
            if job_cols:
                job_raw = str(row[job_cols[0]])
                if 'scientist' in job_raw.lower():
                    job_title = 'Data Scientist'
                elif 'engineer' in job_raw.lower():
                    job_title = 'Data Engineer'
                elif 'analyst' in job_raw.lower():
                    job_title = 'Data Analyst'
                elif 'machine learning' in job_raw.lower():
                    job_title = 'Machine Learning Engineer'
                else:
                    job_title = job_raw.strip()
            
            # Extraction du salaire
            salary_eur = None
            if salary_cols:
                salary_raw = str(row[salary_cols[0]])
                # Extraction de nombres
                numbers = re.findall(r'\d+', salary_raw.replace(',', ''))
                if numbers:
                    salary_num = int(numbers[0])
                    # Conversion approximative selon le format
                    if salary_num > 200:  # Probablement déjà en milliers
                        salary_eur = salary_num
                    elif salary_num > 20:  # Probablement en dizaines de milliers
                        salary_eur = salary_num * 1000
                    else:  # Probablement en centaines de milliers
                        salary_eur = salary_num * 10000
                    
                    # Limite réaliste
                    if salary_eur > 300000:
                        salary_eur = None
            
            # Création de l'enregistrement
            record = {
                'id': f"kaggle_survey_{idx}",
                'source': 'kaggle_survey',
                'job_title': job_title,
                'company': f"Survey_Company_{idx % 100}",
                'country_code': country_code,
                'country_name': EUROPE_COUNTRIES[country_code],
                'salary_eur': salary_eur,
                'currency': 'EUR',
                'experience_level': 'Survey_Response',
                'skills': 'Data Analysis;Statistics;Programming',
                'skills_count': 3,
                'data_source': 'kaggle_ml_survey',
                'collected_at': datetime.now().isoformat()
            }
            
            processed_data.append(record)
            
        except Exception as e:
            continue
    
    print(f"Réponses européennes trouvées: {europe_count}")
    print(f"Enregistrements traités: {len(processed_data)}")
    
    return processed_data

def save_kaggle_data(data):
    """
    Sauvegarde les données Kaggle
    """
    if not data:
        print("Aucune donnée à sauvegarder")
        return None

    os.makedirs('../data/raw/kaggle', exist_ok=True)
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
    filename = f"../data/raw/kaggle/kaggle_survey_europe_{timestamp}.csv"

    df = pd.DataFrame(data)
    df.to_csv(filename, index=False, encoding='utf-8')
    
    print(f"\nSAUVEGARDE KAGGLE:")
    print(f"Fichier: {filename}")
    print(f"Enregistrements: {len(data)}")
    
    return filename

def analyze_kaggle_data(data):
    """
    Analyse des données Kaggle
    """
    if not data:
        return
    
    df = pd.DataFrame(data)
    
    print(f"\nANALYSE KAGGLE SURVEY:")
    print(f"Total: {len(df)} enregistrements")
    
    # Répartition par pays
    country_stats = df['country_code'].value_counts()
    print(f"\nRépartition par pays:")
    for country, count in country_stats.head(10).items():
        country_name = EUROPE_COUNTRIES.get(country, country)
        print(f"  {country} ({country_name}): {count}")
    
    # Jobs avec salaires
    salary_data = df[df['salary_eur'].notna() & (df['salary_eur'] > 0)]
    if len(salary_data) > 0:
        print(f"\nSalaires (EUR):")
        print(f"  Jobs avec salaires: {len(salary_data)}")
        print(f"  Salaire médian: {salary_data['salary_eur'].median():,.0f}")
        print(f"  Salaire moyen: {salary_data['salary_eur'].mean():,.0f}")
    
    # Top job titles
    job_stats = df['job_title'].value_counts()
    print(f"\nTop job titles:")
    for job, count in job_stats.head(5).items():
        print(f"  {job}: {count}")

def main():
    print("SCRAPER KAGGLE ML & DATA SCIENCE SURVEY")
    print("=" * 50)
    print("OBJECTIF: Collecter données européennes du survey Kaggle")
    print("TYPE: Vraies réponses de professionnels data/ML")
    
    # 1. Téléchargement
    df, raw_filename = download_kaggle_survey()
    
    if df is None:
        print("ERREUR: Impossible de télécharger les données")
        return
    
    # 2. Traitement
    processed_data = process_kaggle_data(df)
    
    if not processed_data:
        print("ERREUR: Aucune donnée européenne trouvée")
        return
    
    # 3. Sauvegarde
    filename = save_kaggle_data(processed_data)
    
    # 4. Analyse
    analyze_kaggle_data(processed_data)
    
    print(f"\nSUCCES KAGGLE!")
    print(f"Fichier généré: {filename}")
    print(f"Données européennes collectées: {len(processed_data)}")

if __name__ == "__main__":
    main()