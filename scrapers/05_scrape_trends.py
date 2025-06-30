"""
SCRAPER 5: GOOGLE TRENDS (POPULARITE TECHNOLOGIES)
==================================================

QUE FAIT CE SCRAPER:
- Analyse les tendances Google pour les technologies de programmation
- Mesure la popularite des langages/frameworks dans 5 pays europeens
- Utilise la bibliotheque pytrends (interface non-officielle)
- Compare les technologies entre elles

DONNEES COLLECTEES:
- Mot-cle technologique (ex: "Python programming")
- Pays d'analyse
- Score d'interet moyen (0-100)
- Direction de la tendance (croissante/decroissante)
- Periode d'analyse (12 derniers mois)

OBJECTIF: Identifier quelles technologies sont populaires par pays
SORTIE: fichiers CSV des tendances par pays + comparaisons

LIMITATIONS:
- Google peut limiter les requetes
- Donnees relatives (pas absolues)
- API non-officielle (peut changer)
"""

import os
import sys
import pandas as pd
from datetime import datetime, timedelta
import time
import random

# Configuration encodage pour Windows
if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# Import pytrends
try:
    from pytrends.request import TrendReq
    PYTRENDS_AVAILABLE = True
except ImportError:
    print("ATTENTION: pytrends non installe")
    print("Installez avec: pip install pytrends")
    PYTRENDS_AVAILABLE = False

# Pays europeens et leurs codes Google Trends
COUNTRIES = {
    'FR': 'FR',  # France
    'DE': 'DE',  # Allemagne
    'NL': 'NL',  # Pays-Bas
    'GB': 'GB',  # Royaume-Uni
    'IT': 'IT'   # Italie
}

# Technologies a analyser (groupees par 5 max - limitation Google)
TECH_GROUPS = [
    # Langages principaux
    ['Python programming', 'JavaScript programming', 'Java programming', 'TypeScript programming', 'C# programming'],
    
    # Frameworks web
    ['React framework', 'Angular framework', 'Vue.js framework', 'Node.js programming', 'Django framework'],
    
    # Technologies cloud/DevOps
    ['Docker container', 'Kubernetes', 'AWS cloud', 'Azure cloud', 'DevOps'],
    
    # Data Science & AI
    ['Machine Learning', 'Data Science', 'Artificial Intelligence', 'TensorFlow', 'PyTorch'],
    
    # Bases de donnees
    ['PostgreSQL', 'MongoDB', 'MySQL', 'Redis database', 'Elasticsearch']
]

def init_pytrends():
    """Initialise pytrends avec configuration"""
    if not PYTRENDS_AVAILABLE:
        return None
    
    try:
        # Configuration pytrends
        pytrends = TrendReq(
            hl='en-US',  # Langue interface
            tz=360,      # Timezone
            timeout=(10, 25)  # Timeout requetes
        )
        return pytrends
    except Exception as e:
        print(f"Erreur initialisation pytrends: {e}")
        return None

def analyze_tech_group(pytrends, tech_group, country_code, timeframe='today 12-m'):
    """
    Analyse un groupe de technologies pour un pays
    
    Args:
        pytrends: Instance TrendReq
        tech_group: Liste de mots-cles (max 5)
        country_code: Code pays (FR, DE, etc.)
        timeframe: Periode d'analyse
    """
    
    print(f"  Groupe: {', '.join(tech_group)}")
    
    try:
        # Construire la requete
        pytrends.build_payload(
            tech_group,
            cat=0,           # Toutes categories
            timeframe=timeframe,
            geo=country_code,
            gprop=''         # Web search
        )
        
        # Recuperer donnees d'interet dans le temps
        interest_data = pytrends.interest_over_time()
        
        if interest_data.empty:
            print(f"    Aucune donnee pour ce groupe")
            return []
        
        # Traiter chaque technologie
        results = []
        
        for tech in tech_group:
            if tech in interest_data.columns:
                tech_series = interest_data[tech]
                
                # Calculer statistiques
                avg_interest = tech_series.mean()
                max_interest = tech_series.max()
                min_interest = tech_series.min()
                
                # Calculer tendance (debut vs fin)
                first_quarter = tech_series.head(len(tech_series)//4).mean()
                last_quarter = tech_series.tail(len(tech_series)//4).mean()
                
                if last_quarter > first_quarter * 1.1:
                    trend_direction = 'increasing'
                elif last_quarter < first_quarter * 0.9:
                    trend_direction = 'decreasing'
                else:
                    trend_direction = 'stable'
                
                trend_strength = abs(last_quarter - first_quarter)
                
                results.append({
                    'keyword': tech,
                    'country': country_code,
                    'timeframe': timeframe,
                    'avg_interest': round(avg_interest, 2),
                    'max_interest': int(max_interest),
                    'min_interest': int(min_interest),
                    'trend_direction': trend_direction,
                    'trend_strength': round(trend_strength, 2),
                    'data_points': len(tech_series),
                    'analysis_date': datetime.now().strftime('%Y-%m-%d'),
                    'category': categorize_technology(tech)
                })
                
                print(f"    {tech}: {avg_interest:.1f} pts (tendance: {trend_direction})")
        
        return results
        
    except Exception as e:
        print(f"    Erreur groupe: {e}")
        return []

def categorize_technology(keyword):
    """Categorise une technologie"""
    keyword_lower = keyword.lower()
    
    if any(lang in keyword_lower for lang in ['python', 'javascript', 'java', 'typescript', 'c#']):
        return 'Programming Language'
    elif any(fw in keyword_lower for fw in ['react', 'angular', 'vue', 'node', 'django']):
        return 'Framework'
    elif any(cloud in keyword_lower for cloud in ['docker', 'kubernetes', 'aws', 'azure', 'devops']):
        return 'DevOps/Cloud'
    elif any(ai in keyword_lower for ai in ['machine learning', 'data science', 'ai', 'tensorflow']):
        return 'AI/Data Science'
    elif any(db in keyword_lower for db in ['postgresql', 'mongodb', 'mysql', 'redis']):
        return 'Database'
    else:
        return 'Technology'

def analyze_country_trends(pytrends, country_code):
    """Analyse toutes les technologies pour un pays"""
    
    print(f"\n=== ANALYSE TENDANCES {country_code} ===")
    
    all_results = []
    
    for i, tech_group in enumerate(TECH_GROUPS):
        print(f"Groupe {i+1}/{len(TECH_GROUPS)}")
        
        group_results = analyze_tech_group(pytrends, tech_group, country_code)
        all_results.extend(group_results)
        
        # Pause entre groupes pour eviter rate limiting
        if i < len(TECH_GROUPS) - 1:
            delay = random.uniform(5, 10)
            print(f"  Pause {delay:.1f}s...")
            time.sleep(delay)
    
    print(f"Total {country_code}: {len(all_results)} technologies analysees")
    return all_results

def compare_technologies(pytrends, tech_list, countries):
    """Compare des technologies entre pays"""
    
    print(f"\n=== COMPARAISON TECHNOLOGIES ===")
    print(f"Technologies: {', '.join(tech_list)}")
    
    comparison_results = []
    
    for country_code in countries:
        print(f"Comparaison pour {country_code}...")
        
        try:
            pytrends.build_payload(
                tech_list,
                timeframe='today 12-m',
                geo=country_code
            )
            
            interest_data = pytrends.interest_over_time()
            
            if not interest_data.empty:
                for tech in tech_list:
                    if tech in interest_data.columns:
                        avg_interest = interest_data[tech].mean()
                        
                        comparison_results.append({
                            'comparison_group': ' vs '.join(tech_list),
                            'technology': tech,
                            'country': country_code,
                            'avg_interest': round(avg_interest, 2),
                            'analysis_date': datetime.now().strftime('%Y-%m-%d')
                        })
            
            time.sleep(2)  # Pause entre pays
            
        except Exception as e:
            print(f"  Erreur pour {country_code}: {e}")
            continue
    
    return comparison_results

def save_trends_data(trends_data, country_code=None):
    """Sauvegarde les donnees de tendances"""
    
    if not trends_data:
        return None
    
    os.makedirs('raw/google_trends', exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
    
    if country_code:
        filename = f"raw/google_trends/trends_{country_code.lower()}_{timestamp}.csv"
    else:
        filename = f"raw/google_trends/trends_all_countries_{timestamp}.csv"
    
    df = pd.DataFrame(trends_data)
    df.to_csv(filename, index=False, encoding='utf-8')
    
    print(f"SAUVEGARDE: {filename} ({len(trends_data)} enregistrements)")
    return filename

def main():
    """Fonction principale du scraper Google Trends"""
    
    print("SCRAPER GOOGLE TRENDS (POPULARITE TECHNOLOGIES)")
    print("=" * 50)
    print("OBJECTIF: Analyser popularite technologies par pays")
    print("SOURCE: Google Trends via pytrends")
    print("DONNEES: Tendances recherche langages/frameworks")
    print("PERIODE: 12 derniers mois")
    
    if not PYTRENDS_AVAILABLE:
        print("\nERREUR: pytrends non disponible")
        print("Installez avec: pip install pytrends")
        return
    
    # Initialiser pytrends
    pytrends = init_pytrends()
    if not pytrends:
        print("ERREUR: Impossible d'initialiser pytrends")
        return
    
    print(f"\nConfiguration:")
    print(f"Pays analyses: {', '.join(COUNTRIES.keys())}")
    print(f"Groupes de technologies: {len(TECH_GROUPS)}")
    print(f"Technologies par groupe: max 5 (limitation Google)")
    
    # Demander confirmation
    confirm = input(f"\nLancer l'analyse Google Trends? (o/N): ").strip().lower()
    if confirm not in ['o', 'oui', 'y', 'yes']:
        print("Analyse Google Trends annulee")
        return
    
    all_trends = []
    success_countries = []
    
    # Analyser par pays
    for country_code in COUNTRIES.keys():
        try:
            country_trends = analyze_country_trends(pytrends, country_code)
            
            if country_trends:
                # Sauvegarder par pays
                save_trends_data(country_trends, country_code)
                all_trends.extend(country_trends)
                success_countries.append(country_code)
            else:
                print(f"Aucune donnee pour {country_code}")
                
        except Exception as e:
            print(f"ERREUR {country_code}: {e}")
            continue
        
        # Pause entre pays
        print(f"Pause 10s avant pays suivant...")
        time.sleep(10)
    
    # Comparaisons interessantes
    print(f"\n=== COMPARAISONS TECHNOLOGIES ===")
    
    comparisons = [
        ['Python programming', 'JavaScript programming', 'Java programming'],
        ['React framework', 'Angular framework', 'Vue.js framework'],
        ['Docker container', 'Kubernetes']
    ]
    
    comparison_data = []
    
    for comparison in comparisons:
        if len(comparison) <= 5:  # Limite Google
            comp_results = compare_technologies(pytrends, comparison, list(COUNTRIES.keys()))
            comparison_data.extend(comp_results)
    
    # Sauvegarde globale
    if all_trends:
        global_file = save_trends_data(all_trends)
        
        print(f"\n=== RESUME GOOGLE TRENDS ===")
        print(f"Total analyses: {len(all_trends)}")
        print(f"Pays reussis: {', '.join(success_countries)}")
        print(f"Fichier global: {global_file}")
        
        # Statistiques par pays
        df = pd.DataFrame(all_trends)
        by_country = df['country'].value_counts()
        print(f"\nAnalyses par pays:")
        for country, count in by_country.items():
            print(f"  {country}: {count} technologies")
        
        # Top technologies par interet moyen
        top_tech = df.nlargest(10, 'avg_interest')[['keyword', 'country', 'avg_interest']]
        print(f"\nTop technologies (interet moyen):")
        for _, row in top_tech.iterrows():
            print(f"  {row['keyword']} ({row['country']}): {row['avg_interest']} pts")
        
        # Technologies croissantes
        growing = df[df['trend_direction'] == 'increasing'].nlargest(5, 'trend_strength')
        if not growing.empty:
            print(f"\nTechnologies en croissance:")
            for _, row in growing.iterrows():
                print(f"  {row['keyword']} ({row['country']}): +{row['trend_strength']:.1f}")
    
    # Sauvegarder comparaisons
    if comparison_data:
        comp_file = f"raw/google_trends/tech_comparisons_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.csv"
        df_comp = pd.DataFrame(comparison_data)
        df_comp.to_csv(comp_file, index=False, encoding='utf-8')
        print(f"\nComparaisons sauvegardees: {comp_file}")
    
    if not all_trends:
        print(f"\n=== AUCUNE DONNEE COLLECTEE ===")
        print("Causes possibles:")
        print("- Rate limiting Google Trends")
        print("- Probleme de connexion")
        print("- Mots-cles non reconnus")
        print("- Restriction geographique")

if __name__ == "__main__":
    main()