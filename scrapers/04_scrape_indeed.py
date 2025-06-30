"""
SCRAPER 4: INDEED JOBS (WEB SCRAPING)
=====================================

QUE FAIT CE SCRAPER:
- Web scraping des offres d'emploi sur Indeed
- Recherche emplois tech dans 5 pays europeens
- Extrait titres, entreprises, lieux, salaires, descriptions
- Utilise BeautifulSoup pour parser le HTML

ATTENTION:
- Indeed protege contre le scraping (rate limiting agressif)
- Peut etre bloque ou limite selon l'usage
- Respecte robots.txt et conditions d'utilisation

DONNEES COLLECTEES:
- Titre du poste
- Nom de l'entreprise  
- Localisation
- Salaire (si affiche)
- Description/snippet
- Date de publication
- URL de l'offre

SORTIE: fichiers CSV par pays + fichier global
STRATEGIE: Requetes lentes avec pauses pour eviter blocage
"""

import os
import sys
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from urllib.parse import urljoin, quote
import time
import random
import re

# Configuration encodage pour Windows
if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# URLs Indeed par pays
INDEED_URLS = {
    'FR': 'https://fr.indeed.com',
    'DE': 'https://de.indeed.com',
    'NL': 'https://nl.indeed.com', 
    'GB': 'https://uk.indeed.com',
    'IT': 'https://it.indeed.com'
}

# Termes de recherche par pays (adaptes aux langues)
SEARCH_TERMS = {
    'FR': ['développeur python', 'ingénieur logiciel', 'développeur web', 'data scientist'],
    'DE': ['software entwickler', 'python entwickler', 'web entwickler', 'data scientist'],
    'NL': ['software ontwikkelaar', 'python developer', 'web developer', 'data scientist'],
    'GB': ['software developer', 'python developer', 'web developer', 'data scientist'],
    'IT': ['sviluppatore software', 'sviluppatore python', 'sviluppatore web', 'data scientist']
}

# Headers pour simuler un navigateur real
def get_random_headers():
    """Retourne des headers aleatoires pour eviter detection"""
    
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0'
    ]
    
    return {
        'User-Agent': random.choice(user_agents),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none'
    }

def safe_request(url, params=None, max_retries=3):
    """Requete HTTP securisee avec retry et pause"""
    
    for attempt in range(max_retries):
        try:
            # Pause aleatoire entre requetes
            time.sleep(random.uniform(3, 7))
            
            headers = get_random_headers()
            response = requests.get(url, headers=headers, params=params, timeout=30)
            
            if response.status_code == 200:
                return response
            elif response.status_code == 429:
                print(f"    Rate limit (429), pause longue...")
                time.sleep(60)
                continue
            elif response.status_code == 403:
                print(f"    Bloque (403), changement headers...")
                time.sleep(30)
                continue
            else:
                print(f"    Status {response.status_code}")
                
        except Exception as e:
            print(f"    Erreur requete: {e}")
            
        if attempt < max_retries - 1:
            print(f"    Retry {attempt + 2}/{max_retries}")
            time.sleep(random.uniform(10, 20))
    
    return None

def scrape_indeed_country(country_code, max_pages=3):
    """
    Scrape Indeed pour un pays donne
    
    Args:
        country_code: Code pays (FR, DE, NL, GB, IT)
        max_pages: Nombre max de pages (limite volontairement basse)
    """
    
    print(f"\n=== SCRAPING INDEED {country_code} ===")
    
    base_url = INDEED_URLS.get(country_code)
    if not base_url:
        print(f"URL non trouvee pour {country_code}")
        return []
    
    search_terms = SEARCH_TERMS.get(country_code, ['developer'])
    all_jobs = []
    
    for search_term in search_terms:
        print(f"Recherche: '{search_term}'")
        
        jobs = scrape_indeed_search(base_url, search_term, country_code, max_pages)
        all_jobs.extend(jobs)
        
        # Pause importante entre termes de recherche
        print(f"  Pause 30s entre termes...")
        time.sleep(30)
    
    # Supprimer doublons (meme URL)
    seen_urls = set()
    unique_jobs = []
    
    for job in all_jobs:
        job_url = job.get('job_url', '')
        if job_url and job_url not in seen_urls:
            seen_urls.add(job_url)
            unique_jobs.append(job)
    
    print(f"TOTAL {country_code}: {len(unique_jobs)} emplois uniques")
    return unique_jobs

def scrape_indeed_search(base_url, search_term, country_code, max_pages):
    """Scrape une recherche specifique Indeed"""
    
    jobs = []
    
    for page in range(max_pages):
        start = page * 10  # Indeed: 10 resultats par page
        
        # Construire URL de recherche
        search_url = f"{base_url}/jobs"
        params = {
            'q': search_term,
            'start': start,
            'sort': 'date'
        }
        
        print(f"  Page {page + 1}/{max_pages}")
        
        response = safe_request(search_url, params=params)
        
        if not response:
            print(f"    Echec page {page + 1}")
            break
        
        # Parser le HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Trouver les cartes d'emploi (selectors peuvent changer)
        job_cards = find_job_cards(soup)
        
        if not job_cards:
            print(f"    Aucune offre trouvee page {page + 1}")
            break
        
        # Parser chaque carte
        page_jobs = 0
        for card in job_cards:
            job = parse_job_card(card, base_url, country_code)
            if job:
                jobs.append(job)
                page_jobs += 1
        
        print(f"    Collecte: {page_jobs} emplois")
        
        # Arret si pas assez de resultats
        if page_jobs < 5:
            print(f"    Peu de resultats, arret")
            break
    
    return jobs

def find_job_cards(soup):
    """Trouve les cartes d'emploi dans la page Indeed"""
    
    # Selecteurs CSS possibles pour les cartes d'emploi
    # (Indeed change regulierement sa structure)
    selectors = [
        '.jobsearch-SerpJobCard',
        '.job_seen_beacon',
        '[data-tn-component="organicJob"]',
        '.slider_container .slider_item',
        '.jobsearch-ResultJobCard',
        '[data-jk]'  # data-jk est souvent present
    ]
    
    job_cards = []
    
    for selector in selectors:
        cards = soup.select(selector)
        if cards:
            job_cards = cards
            print(f"    Selecteur trouve: {selector} ({len(cards)} cartes)")
            break
    
    if not job_cards:
        print(f"    ATTENTION: Aucun selecteur ne fonctionne")
        print(f"    Structure HTML peut avoir change")
        
        # Debug: sauvegarder HTML pour analyse
        debug_file = f"debug_indeed_{datetime.now().strftime('%H%M%S')}.html"
        with open(debug_file, 'w', encoding='utf-8') as f:
            f.write(str(soup))
        print(f"    HTML sauvegarde: {debug_file}")
    
    return job_cards

def parse_job_card(card, base_url, country_code):
    """Parse une carte d'emploi Indeed"""
    
    try:
        job_data = {}
        
        # Titre du poste (plusieurs selecteurs possibles)
        title_selectors = [
            'h2 a[data-tn-element="jobTitle"]',
            '.jobTitle a',
            'h2 a span[title]',
            'h2 a',
            '[data-tn-element="jobTitle"]'
        ]
        
        title = extract_text_by_selectors(card, title_selectors)
        if not title:
            return None  # Pas de titre = pas d'emploi valide
        
        # URL du poste
        url_selectors = [
            'h2 a',
            '.jobTitle a',
            '[data-tn-element="jobTitle"]'
        ]
        
        job_url = ''
        for selector in url_selectors:
            link_elem = card.select_one(selector)
            if link_elem and link_elem.get('href'):
                job_url = urljoin(base_url, link_elem['href'])
                break
        
        # Entreprise
        company_selectors = [
            '.companyName',
            '[data-testid="company-name"]',
            '.companyName a',
            '.companyName span'
        ]
        
        company = extract_text_by_selectors(card, company_selectors)
        
        # Localisation
        location_selectors = [
            '.companyLocation',
            '[data-testid="job-location"]',
            '.locationsContainer'
        ]
        
        location = extract_text_by_selectors(card, location_selectors)
        
        # Salaire
        salary_selectors = [
            '.salaryText',
            '[data-testid="salary-snippet"]',
            '.salary-snippet'
        ]
        
        salary_text = extract_text_by_selectors(card, salary_selectors)
        
        # Description/snippet
        desc_selectors = [
            '.summary',
            '[data-testid="job-snippet"]',
            '.jobSnippet'
        ]
        
        description = extract_text_by_selectors(card, desc_selectors)
        
        # Date de publication
        date_selectors = [
            '.date',
            '[data-testid="myJobsStateDate"]',
            '.datePosted'
        ]
        
        posted_date = extract_text_by_selectors(card, date_selectors)
        
        # Traitement des donnees
        return {
            'source': 'indeed',
            'title': clean_text(title),
            'company': clean_text(company),
            'location': clean_text(location),
            'country': country_code,
            'salary_text': clean_text(salary_text),
            'description': clean_text(description)[:300] + '...' if description else '',
            'posted_date': parse_indeed_date(posted_date),
            'job_url': job_url,
            'collected_at': datetime.now().isoformat()
        }
        
    except Exception as e:
        print(f"    Erreur parsing carte: {e}")
        return None

def extract_text_by_selectors(element, selectors):
    """Extrait du texte en essayant plusieurs selecteurs"""
    
    for selector in selectors:
        elem = element.select_one(selector)
        if elem:
            # Essayer attribut title d'abord, puis texte
            text = elem.get('title') or elem.get_text(strip=True)
            if text:
                return text
    
    return ''

def clean_text(text):
    """Nettoie un texte extrait"""
    if not text:
        return ''
    
    # Supprimer caracteres speciaux et normaliser espaces
    cleaned = re.sub(r'\s+', ' ', text.strip())
    return cleaned

def parse_indeed_date(date_text):
    """Parse les dates Indeed (format variable)"""
    if not date_text:
        return ''
    
    date_text = date_text.lower().strip()
    today = datetime.now()
    
    try:
        if any(word in date_text for word in ['aujourd\'hui', 'today']):
            return today.strftime('%Y-%m-%d')
        elif any(word in date_text for word in ['hier', 'yesterday']):
            return (today - timedelta(days=1)).strftime('%Y-%m-%d')
        elif 'il y a' in date_text or 'jours' in date_text:
            # Extraire le nombre de jours
            days_match = re.search(r'(\d+)', date_text)
            if days_match:
                days = int(days_match.group(1))
                return (today - timedelta(days=days)).strftime('%Y-%m-%d')
        elif 'days ago' in date_text:
            days_match = re.search(r'(\d+)', date_text)
            if days_match:
                days = int(days_match.group(1))
                return (today - timedelta(days=days)).strftime('%Y-%m-%d')
    except:
        pass
    
    return date_text  # Retourner tel quel si parsing echoue

def save_indeed_data(jobs_data, country_code):
    """Sauvegarde les donnees Indeed"""
    
    if not jobs_data:
        return None
    
    os.makedirs('raw/indeed', exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
    filename = f"raw/indeed/indeed_{country_code.lower()}_{timestamp}.csv"
    
    df = pd.DataFrame(jobs_data)
    df.to_csv(filename, index=False, encoding='utf-8')
    
    print(f"SAUVEGARDE: {filename} ({len(jobs_data)} emplois)")
    return filename

def main():
    """Fonction principale du scraper Indeed"""
    
    print("SCRAPER INDEED JOBS (WEB SCRAPING)")
    print("=" * 40)
    print("OBJECTIF: Collecter offres emploi tech europeennes")
    print("SOURCE: Web scraping Indeed.com")
    print("METHODE: BeautifulSoup + requetes respectueuses")
    print("ATTENTION: Indeed peut bloquer ou limiter l'acces")
    
    print(f"\nConfiguration:")
    print(f"Pages max par recherche: 3 (volontairement limite)")
    print(f"Pause entre requetes: 3-7 secondes")
    print(f"Pause entre pays: 60 secondes")
    
    # Demander confirmation
    confirm = input(f"\nContinuer avec le scraping Indeed? (o/N): ").strip().lower()
    if confirm not in ['o', 'oui', 'y', 'yes']:
        print("Scraping Indeed annule")
        return
    
    all_jobs = []
    success_countries = []
    
    # Scraper par pays (ordre aleatoire pour eviter patterns)
    countries = list(INDEED_URLS.keys())
    random.shuffle(countries)
    
    for i, country_code in enumerate(countries):
        print(f"\nPAYS {i+1}/{len(countries)}: {country_code}")
        
        try:
            jobs = scrape_indeed_country(country_code, max_pages=3)
            
            if jobs:
                # Sauvegarder par pays
                save_indeed_data(jobs, country_code)
                all_jobs.extend(jobs)
                success_countries.append(country_code)
            else:
                print(f"Aucun emploi collecte pour {country_code}")
                
        except Exception as e:
            print(f"ERREUR {country_code}: {e}")
            continue
        
        # Pause importante entre pays
        if i < len(countries) - 1:
            print(f"Pause 60s avant pays suivant...")
            time.sleep(60)
    
    # Sauvegarde globale
    if all_jobs:
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
        global_filename = f"raw/indeed/indeed_all_countries_{timestamp}.csv"
        
        df_global = pd.DataFrame(all_jobs)
        df_global.to_csv(global_filename, index=False, encoding='utf-8')
        
        print(f"\n=== RESUME INDEED ===")
        print(f"Total emplois collectes: {len(all_jobs)}")
        print(f"Pays reussis: {', '.join(success_countries)}")
        print(f"Fichier global: {global_filename}")
        
        # Statistiques
        by_country = df_global['country'].value_counts()
        print(f"\nRepartition par pays:")
        for country, count in by_country.items():
            print(f"  {country}: {count} emplois")
        
        # Entreprises les plus presentes
        companies = df_global['company'].value_counts().head(10)
        print(f"\nTop entreprises:")
        for company, count in companies.items():
            if company:
                print(f"  {company}: {count} offres")
        
    else:
        print(f"\n=== AUCUNE DONNEE COLLECTEE ===")
        print("Causes possibles:")
        print("- Indeed bloque les requetes")
        print("- Structure HTML changee")
        print("- Rate limiting trop agressif")
        print("- Probleme de connexion")

if __name__ == "__main__":
    main()