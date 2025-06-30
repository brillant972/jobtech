import os
import sys
import requests
import pandas as pd
from datetime import datetime
from typing import List, Dict
import time
import random
import re

# Configuration encodage pour Windows
if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# Configuration API Adzuna
ADZUNA_APP_ID = os.getenv('ADZUNA_APP_ID')
ADZUNA_API_KEY = os.getenv('ADZUNA_API_KEY')

if not ADZUNA_APP_ID:
    print("Configuration manuelle des cles Adzuna:")
    ADZUNA_APP_ID = input("ADZUNA_APP_ID: ").strip()
    ADZUNA_API_KEY = input("ADZUNA_API_KEY: ").strip()

COUNTRIES = {
    'FR': 'fr',
    'DE': 'de',
    'NL': 'nl',
    'GB': 'gb',
    'IT': 'it'
}

TECH_SKILLS = [
    'Python', 'JavaScript', 'Java', 'C++', 'C#', 'PHP', 'Ruby', 'Go',
    'React', 'Vue.js', 'Angular', 'Node.js', 'Django', 'Flask',
    'PostgreSQL', 'MySQL', 'MongoDB', 'Redis',
    'Docker', 'Kubernetes', 'AWS', 'Azure', 'GCP',
    'Machine Learning', 'AI', 'DevOps', 'Scrum', 'Agile'
]

def extract_skills_from_text(text):
    if not text:
        return []
    text_lower = text.lower()
    return [skill for skill in TECH_SKILLS if skill.lower() in text_lower]

def clean_salary(salary_value):
    try:
        return float(salary_value)
    except:
        return None

def scrape_adzuna_country(country_code, max_pages=10):
    print(f"\n=== SCRAPING ADZUNA POUR {country_code.upper()} ===")
    all_jobs = []
    search_terms = ['developer', 'software engineer', 'programmeur', 'ingenieur logiciel']

    for search_term in search_terms:
        print(f"Recherche: '{search_term}'")

        for page in range(1, max_pages + 1):
            print(f"  Page {page}/{max_pages}")
            url = f"https://api.adzuna.com/v1/api/jobs/{country_code}/search/{page}"

            params = {
                'app_id': ADZUNA_APP_ID,
                'app_key': ADZUNA_API_KEY,
                'what': search_term,
                # 'category': '15',  # désactivé pour test
                'results_per_page': 50,
                'sort_by': 'date'
            }

            try:
                response = requests.get(url, params=params, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    jobs = data.get('results', [])

                    if not jobs:
                        print(f"    Aucun resultat page {page}, arret")
                        break

                    for job in jobs:
                        processed_job = process_job(job, country_code)
                        if processed_job:
                            all_jobs.append(processed_job)

                    print(f"    OK: {len(jobs)} emplois")

                elif response.status_code == 401:
                    print(f"    ERREUR 401: Cles API invalides")
                    return []

                elif response.status_code == 429:
                    print(f"    ERREUR 429: Trop de requetes, pause...")
                    time.sleep(60)
                    continue

                else:
                    print(f"    ERREUR {response.status_code}")
                    break

                time.sleep(random.uniform(1, 3))

            except Exception as e:
                print(f"    Exception: {e}")
                continue

        time.sleep(5)

    seen_ids = set()
    unique_jobs = []
    for job in all_jobs:
        job_id = job.get('id')
        if job_id not in seen_ids:
            seen_ids.add(job_id)
            unique_jobs.append(job)

    print(f"TOTAL {country_code.upper()}: {len(unique_jobs)} emplois uniques")
    return unique_jobs

def process_job(job_data, country_code):
    job_id = job_data.get('id')
    title = job_data.get('title', '').strip()

    company_data = job_data.get('company', {})
    company = company_data.get('display_name', '') if company_data else ''

    location_data = job_data.get('location', {})
    location = location_data.get('display_name', '') if location_data else ''

    salary_min = clean_salary(job_data.get('salary_min'))
    salary_max = clean_salary(job_data.get('salary_max'))

    currency_map = {
        'fr': 'EUR', 'de': 'EUR', 'nl': 'EUR', 'it': 'EUR', 'gb': 'GBP'
    }
    currency = currency_map.get(country_code, 'EUR')

    description = job_data.get('description', '')
    skills = extract_skills_from_text(f"{title} {description}")

    created_date = job_data.get('created', '')
    posted_date = created_date.split('T')[0] if created_date else ''

    contract_type = job_data.get('contract_type', '')

    return {
        'id': job_id,
        'source': 'adzuna',
        'title': title,
        'company': company,
        'location': location,
        'country': country_code.upper(),
        'salary_min': salary_min,
        'salary_max': salary_max,
        'currency': currency,
        'skills': ', '.join(skills),
        'skills_count': len(skills),
        'contract_type': contract_type,
        'description_excerpt': description[:200] + '...' if len(description) > 200 else description,
        'posted_date': posted_date,
        'url': job_data.get('redirect_url', ''),
        'collected_at': datetime.now().isoformat()
    }

def save_data(jobs_data, country_code):
    if not jobs_data:
        return None

    os.makedirs('raw/adzuna', exist_ok=True)
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
    filename = f"raw/adzuna/adzuna_{country_code.lower()}_{timestamp}.csv"

    df = pd.DataFrame(jobs_data)
    df.to_csv(filename, index=False, encoding='utf-8')
    print(f"SAUVEGARDE: {filename} ({len(jobs_data)} emplois)")
    return filename

def main():
    print("SCRAPER ADZUNA JOBS API")
    print("=" * 40)
    print("OBJECTIF: Collecter des offres d'emploi tech europeennes")
    print("SOURCE: API officielle Adzuna")
    print("PAYS: France, Allemagne, Pays-Bas, Royaume-Uni, Italie")
    print("DONNEES: Postes, entreprises, salaires, competences")

    if not ADZUNA_APP_ID or not ADZUNA_API_KEY:
        print("\nERREUR: Cles API Adzuna manquantes")
        print("Obtenez-les sur: https://developer.adzuna.com/")
        return

    print(f"\nConfiguration:")
    print(f"App ID: {ADZUNA_APP_ID[:8]}...")
    print(f"API Key: {ADZUNA_API_KEY[:8]}...")

    all_jobs = []
    success_countries = []

    for country_name, country_code in COUNTRIES.items():
        try:
            jobs = scrape_adzuna_country(country_code, max_pages=5)
            if jobs:
                save_data(jobs, country_code)
                all_jobs.extend(jobs)
                success_countries.append(country_name)
        except Exception as e:
            print(f"ERREUR {country_name}: {e}")
            continue

    if all_jobs:
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
        global_filename = f"raw/adzuna/adzuna_all_countries_{timestamp}.csv"
        df_global = pd.DataFrame(all_jobs)
        df_global.to_csv(global_filename, index=False, encoding='utf-8')

        print(f"\n=== RESUME ADZUNA ===")
        print(f"Total emplois collectes: {len(all_jobs)}")
        print(f"Pays reussis: {', '.join(success_countries)}")
        print(f"Fichier global: {global_filename}")

        by_country = df_global['country'].value_counts()
        print(f"\nRepartition par pays:")
        for country, count in by_country.items():
            print(f"  {country}: {count} emplois")

        all_skills = []
        for skills_str in df_global['skills']:
            if skills_str:
                all_skills.extend(skills_str.split(', '))

        if all_skills:
            from collections import Counter
            top_skills = Counter(all_skills).most_common(10)
            print(f"\nTop competences:")
            for skill, count in top_skills:
                print(f"  {skill}: {count} mentions")
    else:
        print("\nAUCUNE DONNEE COLLECTEE")
        print("Verifiez vos cles API et votre connexion")

if __name__ == "__main__":
    main()
