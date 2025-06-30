"""
SCRAPER 3: GITHUB TRENDING REPOSITORIES
========================================

QUE FAIT CE SCRAPER:
- Collecte les repositories GitHub tendance via l'API REST GitHub
- Identifie les langages de programmation populaires
- Analyse la localisation des proprietaires de projets
- Mesure la popularite des technologies (stars, forks)

DONNEES COLLECTEES:
- Nom du repository et description
- Langage principal du projet
- Nombre d'etoiles, forks, watchers
- Proprietaire et sa localisation
- Date de creation et derniere mise a jour
- Topics/tags du projet

OBJECTIF: Identifier les technologies montantes en Europe
SORTIE: fichiers CSV des repos tendance + statistiques langages
"""

import os
import sys
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import time
import re

# Configuration encodage pour Windows
if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# Configuration GitHub API
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')

# Si pas de token, demander (optionnel mais recommande)
if not GITHUB_TOKEN:
    print("Token GitHub non trouve dans .env")
    token_input = input("GITHUB_TOKEN (optionnel, Entree pour ignorer): ").strip()
    if token_input:
        GITHUB_TOKEN = token_input

# URLs de base GitHub
GITHUB_API_BASE = "https://api.github.com"

# Langages de programmation a analyser
PROGRAMMING_LANGUAGES = [
    'JavaScript', 'Python', 'Java', 'TypeScript', 'C#', 'PHP', 'C++',
    'Ruby', 'Go', 'Rust', 'Kotlin', 'Swift', 'Scala', 'R', 'Dart'
]

# Pays europeens et leurs indicateurs
EUROPEAN_LOCATIONS = {
    'france': 'FR', 'paris': 'FR', 'lyon': 'FR', 'marseille': 'FR',
    'germany': 'DE', 'berlin': 'DE', 'munich': 'DE', 'hamburg': 'DE',
    'netherlands': 'NL', 'amsterdam': 'NL', 'rotterdam': 'NL',
    'united kingdom': 'GB', 'london': 'GB', 'manchester': 'GB', 'edinburgh': 'GB',
    'italy': 'IT', 'rome': 'IT', 'milan': 'IT', 'turin': 'IT'
}

def get_github_headers():
    """Retourne les headers pour l'API GitHub"""
    headers = {
        'Accept': 'application/vnd.github.v3+json',
        'User-Agent': 'TalentInsight-DataCollector/1.0'
    }
    
    if GITHUB_TOKEN:
        headers['Authorization'] = f'token {GITHUB_TOKEN}'
    
    return headers

def make_github_request(url, params=None):
    """Fait une requete a l'API GitHub avec gestion des erreurs"""
    
    headers = get_github_headers()
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        # Gestion rate limiting
        if response.status_code == 403:
            rate_limit_remaining = response.headers.get('X-RateLimit-Remaining', '0')
            if rate_limit_remaining == '0':
                reset_time = int(response.headers.get('X-RateLimit-Reset', 0))
                wait_time = max(reset_time - int(datetime.now().timestamp()), 60)
                print(f"Rate limit atteint, attente {wait_time}s...")
                time.sleep(wait_time)
                return make_github_request(url, params)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Erreur API GitHub {response.status_code}: {response.text[:200]}")
            return None
            
    except Exception as e:
        print(f"Exception requete GitHub: {e}")
        return None

def search_trending_repositories(language=None, days=30, max_repos=100):
    """
    Recherche les repositories tendance
    
    Args:
        language: Langage a filtrer (optionnel)
        days: Periode en jours pour definir "tendance"
        max_repos: Nombre max de repos a recuperer
    """
    
    print(f"\nRecherche repos tendance: {language or 'tous langages'}")
    
    # Date limite pour repos "recents"
    since_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    
    # Construction de la requete de recherche
    query_parts = [
        f"created:>{since_date}",  # Crees recemment
        "stars:>5"  # Au moins 5 etoiles
    ]
    
    if language:
        query_parts.append(f"language:{language}")
    
    query = ' '.join(query_parts)
    
    url = f"{GITHUB_API_BASE}/search/repositories"
    params = {
        'q': query,
        'sort': 'stars',
        'order': 'desc',
        'per_page': min(100, max_repos)
    }
    
    data = make_github_request(url, params)
    
    if not data:
        return []
    
    repositories = data.get('items', [])
    print(f"  Trouve: {len(repositories)} repositories")
    
    # Traiter chaque repository
    processed_repos = []
    
    for repo in repositories:
        processed_repo = process_repository(repo)
        if processed_repo:
            processed_repos.append(processed_repo)
            
        # Petite pause pour eviter le rate limiting
        time.sleep(0.1)
    
    return processed_repos

def process_repository(repo_data):
    """Traite et normalise un repository GitHub"""
    
    # Donnees de base du repo
    repo_id = repo_data.get('id')
    name = repo_data.get('name', '')
    full_name = repo_data.get('full_name', '')
    description = repo_data.get('description') or ''  # Gerer les None
    
    # Metriques
    stars = repo_data.get('stargazers_count', 0)
    forks = repo_data.get('forks_count', 0)
    watchers = repo_data.get('watchers_count', 0)
    issues = repo_data.get('open_issues_count', 0)
    
    # Langage principal
    language = repo_data.get('language') or ''  # Gerer les None
    
    # Dates
    created_at = repo_data.get('created_at', '').split('T')[0] if repo_data.get('created_at') else ''
    updated_at = repo_data.get('updated_at', '').split('T')[0] if repo_data.get('updated_at') else ''
    
    # Proprietaire
    owner = repo_data.get('owner', {})
    owner_login = owner.get('login', '') if owner else ''
    owner_type = owner.get('type', '') if owner else ''  # User ou Organization
    
    # Recuperer la localisation du proprietaire (requete supplementaire)
    owner_location, owner_country = get_owner_location(owner_login)
    
    # Topics/tags
    topics = repo_data.get('topics', [])
    topics_str = ', '.join(topics) if topics else ''
    
    # License
    license_info = repo_data.get('license', {})
    license_name = license_info.get('spdx_id', '') if license_info else ''
    
    # URLs
    html_url = repo_data.get('html_url', '')
    clone_url = repo_data.get('clone_url', '')
    
    return {
        'id': repo_id,
        'source': 'github',
        'name': name,
        'full_name': full_name,
        'description': description[:300] + '...' if description and len(description) > 300 else description,
        'language': language,
        'stars_count': stars,
        'forks_count': forks,
        'watchers_count': watchers,
        'issues_count': issues,
        'created_at': created_at,
        'updated_at': updated_at,
        'owner_login': owner_login,
        'owner_type': owner_type,
        'owner_location': owner_location,
        'owner_country': owner_country,
        'topics': topics_str,
        'license': license_name,
        'url': html_url,
        'clone_url': clone_url,
        'collected_at': datetime.now().isoformat()
    }

def get_owner_location(username):
    """
    Recupere la localisation d'un utilisateur GitHub
    
    Returns:
        tuple: (location_string, country_code)
    """
    
    if not username:
        return '', ''
    
    url = f"{GITHUB_API_BASE}/users/{username}"
    user_data = make_github_request(url)
    
    if not user_data:
        return '', ''
    
    location = user_data.get('location', '')
    
    if not location:
        return '', ''
    
    # Detecter le pays depuis la localisation
    country_code = detect_country_from_location(location)
    
    return location, country_code

def detect_country_from_location(location_str):
    """Detecte le code pays depuis une string de localisation"""
    
    if not location_str:
        return ''
    
    location_lower = location_str.lower()
    
    # Chercher des correspondances avec les pays europeens
    for location_key, country_code in EUROPEAN_LOCATIONS.items():
        if location_key in location_lower:
            return country_code
    
    return ''

def analyze_language_popularity():
    """Analyse la popularite des langages de programmation"""
    
    print(f"\n=== ANALYSE POPULARITE LANGAGES ===")
    
    language_stats = []
    
    for language in PROGRAMMING_LANGUAGES:
        print(f"Analyse: {language}")
        
        # Rechercher repos tendance pour ce langage
        repos = search_trending_repositories(language=language, days=60, max_repos=30)
        
        if repos:
            # Calculer statistiques
            total_stars = sum(repo['stars_count'] for repo in repos)
            total_forks = sum(repo['forks_count'] for repo in repos)
            avg_stars = total_stars / len(repos) if repos else 0
            
            # Compter repos europeens
            european_repos = [repo for repo in repos if repo['owner_country']]
            european_count = len(european_repos)
            
            # Compter par pays
            country_counts = {}
            for repo in european_repos:
                country = repo['owner_country']
                if country:
                    country_counts[country] = country_counts.get(country, 0) + 1
            
            language_stats.append({
                'language': language,
                'trending_repos_count': len(repos),
                'total_stars': total_stars,
                'total_forks': total_forks,
                'avg_stars_per_repo': round(avg_stars, 2),
                'european_repos': european_count,
                'european_countries': ', '.join([f"{k}:{v}" for k, v in country_counts.items()]),
                'analysis_date': datetime.now().strftime('%Y-%m-%d')
            })
            
            print(f"  Repos: {len(repos)}, Stars moy: {avg_stars:.1f}, EU: {european_count}")
        else:
            print(f"  Aucun repo trouve")
        
        # Pause entre langages
        time.sleep(2)
    
    return language_stats

def save_github_data(repos_data, language_stats):
    """Sauvegarde les donnees GitHub"""
    
    # Creer le dossier
    os.makedirs('raw/github', exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
    
    # Sauvegarder repositories
    if repos_data:
        repos_filename = f"raw/github/github_trending_repos_{timestamp}.csv"
        df_repos = pd.DataFrame(repos_data)
        df_repos.to_csv(repos_filename, index=False, encoding='utf-8')
        print(f"SAUVEGARDE repos: {repos_filename}")
        
        # Filtrer repos europeens
        european_repos = [repo for repo in repos_data if repo['owner_country']]
        if european_repos:
            eu_filename = f"raw/github/github_trending_repos_europe_{timestamp}.csv"
            df_eu = pd.DataFrame(european_repos)
            df_eu.to_csv(eu_filename, index=False, encoding='utf-8')
            print(f"SAUVEGARDE repos EU: {eu_filename}")
    
    # Sauvegarder statistiques langages
    if language_stats:
        stats_filename = f"raw/github/github_language_stats_{timestamp}.csv"
        df_stats = pd.DataFrame(language_stats)
        df_stats.to_csv(stats_filename, index=False, encoding='utf-8')
        print(f"SAUVEGARDE stats: {stats_filename}")
    
    return repos_filename if repos_data else None

def main():
    """Fonction principale du scraper GitHub"""
    
    print("SCRAPER GITHUB TRENDING REPOSITORIES")
    print("=" * 40)
    print("OBJECTIF: Identifier technologies populaires et montantes")
    print("SOURCE: API GitHub REST")
    print("DONNEES: Repos tendance, langages, localisation developpeurs")
    
    if GITHUB_TOKEN:
        print(f"Token GitHub: {GITHUB_TOKEN[:8]}... (limite: 5000 req/h)")
    else:
        print("Pas de token GitHub (limite: 60 req/h)")
    
    # 1. Collecte repositories tendance generaux
    print(f"\n=== COLLECTE REPOS TENDANCE GENERAUX ===")
    general_repos = search_trending_repositories(days=30, max_repos=150)
    
    # 2. Analyse par langage
    language_stats = analyze_language_popularity()
    
    # 3. Sauvegarde
    main_file = save_github_data(general_repos, language_stats)
    
    # 4. Statistiques finales
    print(f"\n=== RESUME GITHUB ===")
    print(f"Total repositories: {len(general_repos)}")
    
    if general_repos:
        # Repos europeens
        european_repos = [repo for repo in general_repos if repo['owner_country']]
        print(f"Repositories europeens: {len(european_repos)}")
        
        # Top langages
        if general_repos:
            df_repos = pd.DataFrame(general_repos)
            top_languages = df_repos['language'].value_counts().head(10)
            print(f"\nTop langages (repos tendance):")
            for lang, count in top_languages.items():
                if lang:  # Ignorer les valeurs vides
                    print(f"  {lang}: {count} repos")
        
        # Pays europeens
        if european_repos:
            df_eu = pd.DataFrame(european_repos)
            eu_countries = df_eu['owner_country'].value_counts()
            print(f"\nRepartition pays europeens:")
            for country, count in eu_countries.items():
                print(f"  {country}: {count} repos")
    
    # Statistiques langages
    if language_stats:
        print(f"\nLanguages analyses: {len(language_stats)}")
        
        # Top langages par popularite
        sorted_langs = sorted(language_stats, key=lambda x: x['avg_stars_per_repo'], reverse=True)
        print(f"\nTop langages par popularite (stars moyennes):")
        for lang in sorted_langs[:8]:
            print(f"  {lang['language']}: {lang['avg_stars_per_repo']} etoiles/repo")
    
    if main_file:
        print(f"\nFichier principal: {main_file}")
    else:
        print(f"\nATTENTION: Peu ou pas de donnees collectees")
        print("Possible cause: Rate limiting GitHub ou probleme reseau")

if __name__ == "__main__":
    main()