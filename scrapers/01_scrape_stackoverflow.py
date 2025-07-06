"""
SCRAPER 2: STACK OVERFLOW DEVELOPER SURVEY
===========================================

QUE FAIT CE SCRAPER:
- Telecharge l'enquete annuelle Stack Overflow Developer Survey
- Traite ~90,000 reponses de developpeurs du monde entier
- Filtre sur les pays europeens cibles: France, Allemagne, Pays-Bas, UK, Italie
- Extrait les donnees sur salaires, technologies, experience

DONNEES COLLECTEES:
- Pays de residence
- Salaire annuel (converti en monnaie locale)
- Annees d'experience en programmation
- Langages/technologies utilisees
- Type de developpeur (Frontend, Backend, Full-stack, etc.)
- Niveau d'education
- Taille d'entreprise

SORTIE: fichiers CSV par pays + fichier global
AVANTAGE: Pas d'API necessaire, donnees tres fiables
"""

import os
import sys
import requests
import pandas as pd
import zipfile
from datetime import datetime
from typing import List, Dict
import io

# Configuration encodage pour Windows
if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# Mapping des pays cibles
TARGET_COUNTRIES = {
    'France': 'FR',
    'Germany': 'DE',
    'Netherlands': 'NL',
    'United Kingdom': 'GB',
    'United Kingdom of Great Britain and Northern Ireland': 'GB',
    'Italy': 'IT'
}

# URLs des enquetes Stack Overflow (plusieurs annees pour robustesse)
SURVEY_URLS = {
    2023: "https://info.stackoverflowsolutions.com/rs/719-EMH-566/images/stack-overflow-developer-survey-2023.zip",
    2022: "https://info.stackoverflowsolutions.com/rs/719-EMH-566/images/stack-overflow-developer-survey-2022.zip"
}

def download_stackoverflow_survey(year=2023):
    """
    Telecharge l'enquete Stack Overflow pour une annee donnee
    
    Args:
        year: Annee de l'enquete (2023, 2022)
    
    Returns:
        Chemin vers le fichier CSV extrait ou None si echec
    """
    
    print(f"\n=== TELECHARGEMENT STACK OVERFLOW SURVEY {year} ===")
    
    url = SURVEY_URLS.get(year)
    if not url:
        print(f"ERREUR: URL non disponible pour {year}")
        return None
    
    try:
        # Creer le dossier de destination
        os.makedirs('../data/raw/stackoverflow', exist_ok=True)
        
        print("Telechargement en cours...")
        response = requests.get(url, stream=True, timeout=120)
        response.raise_for_status()
        
        # Sauvegarder le ZIP en memoire
        zip_content = io.BytesIO()
        total_size = 0
        
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                zip_content.write(chunk)
                total_size += len(chunk)
                
                # Afficher progression tous les MB
                if total_size % (1024*1024) == 0:
                    print(f"  Telecharge: {total_size // (1024*1024)} MB")
        
        print(f"Telechargement termine: {total_size} bytes")
        
        # Extraire le ZIP
        zip_content.seek(0)
        
        with zipfile.ZipFile(zip_content, 'r') as zip_ref:
            # Lister les fichiers dans le ZIP
            file_list = zip_ref.namelist()
            print(f"Fichiers dans l'archive: {len(file_list)}")
            
            # Chercher le fichier CSV principal (le plus gros)
            csv_files = [f for f in file_list if f.endswith('.csv')]
            
            if not csv_files:
                print("ERREUR: Aucun fichier CSV trouve dans l'archive")
                return None
            
            # Prendre le fichier CSV le plus gros (generalement survey_results_public.csv)
            main_csv = None
            max_size = 0
            
            for csv_file in csv_files:
                file_info = zip_ref.getinfo(csv_file)
                if file_info.file_size > max_size:
                    max_size = file_info.file_size
                    main_csv = csv_file
            
            if main_csv:
                print(f"Fichier principal: {main_csv} ({max_size} bytes)")
                
                # Extraire le fichier CSV
                extract_path = f'../data/raw/stackoverflow/survey_{year}.csv'
                
                with zip_ref.open(main_csv) as source, open(extract_path, 'wb') as target:
                    target.write(source.read())
                
                print(f"SUCCES: Fichier extrait vers {extract_path}")
                return extract_path
            
        return None
        
    except Exception as e:
        print(f"ERREUR telechargement: {e}")
        return None

def process_stackoverflow_data(csv_path):
    """
    Traite les donnees Stack Overflow
    
    Args:
        csv_path: Chemin vers le fichier CSV
    
    Returns:
        Liste des enregistrements traites
    """
    
    print(f"\n=== TRAITEMENT DONNEES STACK OVERFLOW ===")
    
    try:
        print("Chargement du fichier CSV...")
        
        # Charger le CSV (peut etre tres gros)
        try:
            df = pd.read_csv(csv_path, encoding='utf-8', low_memory=False)
        except UnicodeDecodeError:
            print("Tentative avec encodage latin-1...")
            df = pd.read_csv(csv_path, encoding='latin-1', low_memory=False)
        
        print(f"Donnees chargees: {len(df)} reponses, {len(df.columns)} colonnes")
        
        # Examiner les colonnes disponibles
        columns = df.columns.tolist()
        print(f"Premieres colonnes: {columns[:10]}")
        
        # Mapping des colonnes (peut varier selon l'annee)
        column_mapping = {}
        
        # Chercher les colonnes importantes
        for col in columns:
            col_lower = col.lower()
            if 'country' in col_lower:
                column_mapping['country'] = col
            elif any(term in col_lower for term in ['compensation', 'salary', 'convertedcomp']):
                column_mapping['salary'] = col
            elif any(term in col_lower for term in ['yearscode', 'years_code', 'experience']):
                column_mapping['experience'] = col
            elif any(term in col_lower for term in ['language', 'tech']):
                if 'worked' in col_lower or 'have' in col_lower:
                    column_mapping['languages'] = col
            elif any(term in col_lower for term in ['devtype', 'developer']):
                column_mapping['dev_type'] = col
            elif any(term in col_lower for term in ['edlevel', 'education']):
                column_mapping['education'] = col
            elif any(term in col_lower for term in ['orgsize', 'company']):
                column_mapping['company_size'] = col
        
        print(f"Colonnes identifiees: {column_mapping}")
        
        # Filtrer les donnees europeennes
        if 'country' not in column_mapping:
            print("ERREUR: Colonne pays non trouvee")
            return []
        
        country_col = column_mapping['country']
        
        # Filtrer par pays europeens
        european_mask = df[country_col].isin(TARGET_COUNTRIES.keys())
        df_europe = df[european_mask].copy()
        
        print(f"Reponses europeennes: {len(df_europe)}")
        
        if len(df_europe) == 0:
            print("ATTENTION: Aucune reponse europeenne trouvee")
            print(f"Pays disponibles: {df[country_col].value_counts().head(10)}")
            return []
        
        # Traiter les donnees
        processed_data = []
        
        for _, row in df_europe.iterrows():
            record = process_survey_response(row, column_mapping)
            if record:
                processed_data.append(record)
        
        print(f"Enregistrements traites: {len(processed_data)}")
        return processed_data
        
    except Exception as e:
        print(f"ERREUR traitement: {e}")
        return []

def process_survey_response(row, column_mapping):
    """Traite une reponse individuelle de l'enquete"""
    
    try:
        # Pays
        country_name = row[column_mapping['country']]
        country_code = TARGET_COUNTRIES.get(country_name)
        
        if not country_code:
            return None
        
        # Salaire
        salary = None
        if 'salary' in column_mapping:
            salary_value = row[column_mapping['salary']]
            if pd.notna(salary_value) and salary_value > 0:
                salary = float(salary_value)
        
        # Experience
        experience = row.get(column_mapping.get('experience', ''), '')
        experience_level = categorize_experience(experience)
        
        # Technologies/langages
        languages = row.get(column_mapping.get('languages', ''), '')
        if pd.isna(languages):
            languages = ''
        
        # Type de developpeur
        dev_type = row.get(column_mapping.get('dev_type', ''), '')
        if pd.isna(dev_type):
            dev_type = ''
        
        # Education
        education = row.get(column_mapping.get('education', ''), '')
        if pd.isna(education):
            education = ''
        
        # Taille entreprise
        company_size = row.get(column_mapping.get('company_size', ''), '')
        if pd.isna(company_size):
            company_size = ''
        
        return {
            'source': 'stackoverflow',
            'country': country_code,
            'country_name': country_name,
            'salary_yearly': salary,
            'currency': get_currency_for_country(country_code),
            'years_experience': str(experience),
            'experience_level': experience_level,
            'languages_worked': str(languages)[:500],  # Limiter la taille
            'developer_type': str(dev_type),
            'education_level': str(education),
            'company_size': str(company_size),
            'collected_at': datetime.now().isoformat()
        }
        
    except Exception as e:
        print(f"Erreur traitement reponse: {e}")
        return None

def categorize_experience(experience_str):
    """Categorise le niveau d'experience"""
    
    if pd.isna(experience_str):
        return 'Unknown'
    
    exp_str = str(experience_str).lower()
    
    if any(term in exp_str for term in ['less than 1', '0', 'student']):
        return 'Junior'
    elif any(term in exp_str for term in ['1', '2', '3']):
        return 'Junior'
    elif any(term in exp_str for term in ['4', '5', '6', '7', '8', '9']):
        return 'Mid-level'
    elif any(term in exp_str for term in ['10', '15', '20', 'more than']):
        return 'Senior'
    else:
        return 'Unknown'

def get_currency_for_country(country_code):
    """Retourne la devise pour un pays"""
    currency_map = {
        'FR': 'EUR',
        'DE': 'EUR', 
        'NL': 'EUR',
        'IT': 'EUR',
        'GB': 'GBP'
    }
    return currency_map.get(country_code, 'EUR')

def save_stackoverflow_data(data, year):
    """Sauvegarde les donnees Stack Overflow"""
    
    if not data:
        return
    
    # Creer DataFrame
    df = pd.DataFrame(data)
    
    # Sauvegarde globale
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
    global_filename = f"../data/raw/stackoverflow/stackoverflow_all_countries_{year}_{timestamp}.csv"
    df.to_csv(global_filename, index=False, encoding='utf-8')
    
    print(f"SAUVEGARDE GLOBALE: {global_filename}")
    
    # Sauvegarde par pays
    countries_saved = []
    
    for country_code in df['country'].unique():
        country_data = df[df['country'] == country_code]
        
        if len(country_data) > 0:
            country_filename = f"../data/raw/stackoverflow/stackoverflow_{country_code.lower()}_{year}_{timestamp}.csv"
            country_data.to_csv(country_filename, index=False, encoding='utf-8')
            countries_saved.append(country_code)
            print(f"SAUVEGARDE {country_code}: {country_filename} ({len(country_data)} reponses)")
    
    return global_filename, countries_saved

def main():
    """Fonction principale du scraper Stack Overflow"""
    
    print("SCRAPER STACK OVERFLOW DEVELOPER SURVEY")
    print("=" * 45)
    print("OBJECTIF: Collecter donnees salaires et technologies")
    print("SOURCE: Enquete annuelle Stack Overflow (~90k reponses)")
    print("PAYS: France, Allemagne, Pays-Bas, Royaume-Uni, Italie")
    print("DONNEES: Salaires, experience, langages, types dev")
    
    # Essayer de telecharger l'enquete 2023 d'abord, puis 2022
    csv_path = None
    year_used = None
    
    for year in [2023, 2022]:
        print(f"\nTentative telechargement enquete {year}...")
        csv_path = download_stackoverflow_survey(year)
        
        if csv_path and os.path.exists(csv_path):
            year_used = year
            break
        else:
            print(f"Echec pour {year}, tentative annee suivante...")
    
    if not csv_path:
        print("\nERREUR: Impossible de telecharger l'enquete Stack Overflow")
        print("Verifiez votre connexion internet")
        return
    
    # Traiter les donnees
    processed_data = process_stackoverflow_data(csv_path)
    
    if not processed_data:
        print("ERREUR: Aucune donnee traitee")
        return
    
    # Sauvegarder
    global_file, countries = save_stackoverflow_data(processed_data, year_used)
    
    # Statistiques finales
    df = pd.DataFrame(processed_data)
    
    print(f"\n=== RESUME STACK OVERFLOW {year_used} ===")
    print(f"Total reponses collectees: {len(processed_data)}")
    print(f"Pays traites: {', '.join(countries)}")
    print(f"Fichier global: {global_file}")
    
    # Repartition par pays
    print(f"\nRepartition par pays:")
    for country, count in df['country'].value_counts().items():
        print(f"  {country}: {count} reponses")
    
    # Statistiques salaires
    salary_data = df[df['salary_yearly'].notna()]
    if len(salary_data) > 0:
        print(f"\nStatistiques salaires:")
        print(f"  Reponses avec salaire: {len(salary_data)}")
        print(f"  Salaire median: {salary_data['salary_yearly'].median():.0f}")
        print(f"  Salaire moyen: {salary_data['salary_yearly'].mean():.0f}")
    
    # Top langages
    all_languages = []
    for lang_str in df['languages_worked']:
        if lang_str and lang_str != 'nan':
            # Separer par point-virgule (format Stack Overflow)
            langs = [l.strip() for l in str(lang_str).split(';') if l.strip()]
            all_languages.extend(langs)
    
    if all_languages:
        from collections import Counter
        top_langs = Counter(all_languages).most_common(10)
        print(f"\nTop langages:")
        for lang, count in top_langs:
            print(f"  {lang}: {count} mentions")

if __name__ == "__main__":
    main()