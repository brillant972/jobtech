"""
Script principal pour lancer tous les scrapers TalentInsight
Version adaptee - 4 scrapers fonctionnels (sans Indeed)
"""
import os
import sys
import subprocess
import time
from datetime import datetime
import pandas as pd

def setup_directories():
    """Cree la structure de dossiers necessaire"""
    directories = [
        'data/raw/adzuna',
        'data/raw/stackoverflow', 
        'data/raw/github',
        'data/raw/google_trends',
        'logs'
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"Dossier cree: {directory}")

def check_dependencies():
    """Verifie les dependances Python"""
    required_packages = [
        ('requests', 'requests'),
        ('pandas', 'pandas'), 
        ('beautifulsoup4', 'bs4'),
        ('python-dotenv', 'dotenv'),
        ('pytrends', 'pytrends')
    ]
    
    missing = []
    for package_name, import_name in required_packages:
        try:
            __import__(import_name)
            print(f"OK: {package_name}")
        except ImportError:
            missing.append(package_name)
            print(f"MANQUANT: {package_name}")
    
    if missing:
        print(f"\nInstallez les packages manquants:")
        print(f"pip install {' '.join(missing)}")
        return False
    
    print("Toutes les dependances sont installees")
    return True

def configure_api_keys():
    """Configuration manuelle des cles API"""
    print("\n=== CONFIGURATION CLES API ===")
    
    # Adzuna (obligatoire)
    print("ADZUNA API (obligatoire):")
    adzuna_id = input("ADZUNA_APP_ID: ").strip()
    adzuna_key = input("ADZUNA_API_KEY: ").strip()
    
    # GitHub (optionnel)
    print("\nGITHUB TOKEN (optionnel pour eviter rate limiting):")
    github_token = input("GITHUB_TOKEN (Entree pour ignorer): ").strip()
    
    # Sauvegarder dans .env
    env_content = f"""# Configuration TalentInsight
ADZUNA_APP_ID={adzuna_id}
ADZUNA_API_KEY={adzuna_key}
"""
    
    if github_token:
        env_content += f"GITHUB_TOKEN={github_token}\n"
    
    env_content += """
# Configuration generale
TARGET_COUNTRIES=FR,DE,NL,GB,IT
"""
    
    with open('.env', 'w', encoding='utf-8') as f:
        f.write(env_content)
    
    print("Fichier .env cree avec succes")
    return adzuna_id, adzuna_key, github_token

def run_scraper(script_name, description):
    """Execute un scraper specifique"""
    print(f"\n{'='*50}")
    print(f"LANCEMENT: {description}")
    print(f"Script: {script_name}")
    print(f"Debut: {datetime.now().strftime('%H:%M:%S')}")
    print('='*50)
    
    start_time = time.time()
    
    try:
        result = subprocess.run(
            [sys.executable, script_name],
            cwd=os.getcwd(),
            capture_output=True,
            text=True,
            timeout=1800  # 30 minutes max
        )
        
        duration = time.time() - start_time
        
        if result.returncode == 0:
            print(f"SUCCES: {description}")
            print(f"Duree: {duration:.1f}s")
            
            # Compter fichiers generes
            files_count = count_generated_files(script_name)
            
            return {
                'script': script_name,
                'description': description,
                'status': 'success',
                'duration': duration,
                'files_generated': files_count,
                'output': result.stdout[-500:] if result.stdout else '',
                'error': None
            }
        else:
            print(f"ECHEC: {description}")
            print(f"Duree: {duration:.1f}s")
            print(f"Erreur: {result.stderr[:200]}...")
            
            return {
                'script': script_name,
                'description': description,
                'status': 'failed',
                'duration': duration,
                'files_generated': 0,
                'output': result.stdout[-300:] if result.stdout else '',
                'error': result.stderr[-300:] if result.stderr else ''
            }
            
    except subprocess.TimeoutExpired:
        print(f"TIMEOUT: {description} (30 min)")
        return {
            'script': script_name,
            'description': description,
            'status': 'timeout',
            'duration': 1800,
            'files_generated': 0,
            'output': '',
            'error': 'Timeout apres 30 minutes'
        }
        
    except Exception as e:
        print(f"ERREUR: {description} - {e}")
        return {
            'script': script_name,
            'description': description,
            'status': 'error',
            'duration': 0,
            'files_generated': 0,
            'output': '',
            'error': str(e)
        }

def count_generated_files(script_name):
    """Compte les fichiers generes par un scraper"""
    source_map = {
        '01_scrape_adzuna.py': 'adzuna',
        '01_scrape_stackoverflow.py': 'stackoverflow',
        '01_scrape_github.py': 'github',
        '01_scrape_trends.py': 'google_trends'
    }
    
    source_dir = source_map.get(script_name)
    if not source_dir:
        return 0
    
    directory = f"data/raw/{source_dir}"
    if not os.path.exists(directory):
        return 0
    
    today = datetime.now().strftime('%Y-%m-%d')
    files_today = [
        f for f in os.listdir(directory) 
        if today in f and (f.endswith('.csv') or f.endswith('.json'))
    ]
    
    return len(files_today)

def generate_report(results):
    """Genere un rapport final"""
    print(f"\n{'='*60}")
    print("RAPPORT DE COLLECTE TALENTINSIGHT")
    print('='*60)
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Scrapers executes: {len(results)}")
    
    # Statistiques
    successful = len([r for r in results if r['status'] == 'success'])
    failed = len([r for r in results if r['status'] == 'failed'])
    total_files = sum(r['files_generated'] for r in results)
    total_duration = sum(r['duration'] for r in results)
    
    print(f"Succes: {successful}")
    print(f"Echecs: {failed}")
    print(f"Fichiers generes: {total_files}")
    print(f"Duree totale: {total_duration/60:.1f} minutes")
    
    print(f"\nDETAIL PAR SCRAPER:")
    print("-" * 60)
    
    for result in results:
        status = "OK" if result['status'] == 'success' else "ECHEC"
        print(f"{status}: {result['description']}")
        print(f"  Duree: {result['duration']:.1f}s")
        print(f"  Fichiers: {result['files_generated']}")
        
        if result['error']:
            print(f"  Erreur: {result['error'][:80]}...")
        print()
    
    # Sauvegarder rapport
    if results:
        report_data = []
        for result in results:
            report_data.append({
                'timestamp': datetime.now().isoformat(),
                'script': result['script'],
                'description': result['description'],
                'status': result['status'],
                'duration_seconds': result['duration'],
                'files_generated': result['files_generated'],
                'error_message': result['error']
            })
        
        df_report = pd.DataFrame(report_data)
        report_file = f"logs/collection_report_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.csv"
        df_report.to_csv(report_file, index=False, encoding='utf-8')
        print(f"Rapport sauvegarde: {report_file}")

def main():
    """Fonction principale"""
    print("TALENTINSIGHT - COLLECTE AUTOMATISEE")
    print("=" * 45)
    print(f"Debut: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Version: 4 scrapers (Adzuna, StackOverflow, GitHub, GoogleTrends)")
    
    # 1. Preparation
    print("\nPREPARATION...")
    setup_directories()
    
    if not check_dependencies():
        print("ERREUR: Dependances manquantes")
        return
    
    # 2. Configuration API
    print("\nLes cles API sont necessaires pour:")
    print("- Adzuna: OBLIGATOIRE (collecte emplois)")
    print("- GitHub: OPTIONNEL (evite rate limiting)")
    
    config_choice = input("\nConfigurer les cles API maintenant? (o/N): ").strip().lower()
    if config_choice in ['o', 'oui', 'y', 'yes']:
        adzuna_id, adzuna_key, github_token = configure_api_keys()
        
        # Test rapide Adzuna
        if adzuna_id and adzuna_key:
            print("\nTest rapide Adzuna...")
            # Le test sera fait par le scraper lui-meme
    
    # 3. Configuration scrapers
    scrapers_config = [
        {
            'script': '01_scrape_adzuna.py',
            'description': 'Adzuna Jobs API',
            'duration_estimate': '3-5 minutes'
        },
        {
            'script': '01_scrape_stackoverflow.py',
            'description': 'Stack Overflow Survey',
            'duration_estimate': '4-6 minutes'
        },
        {
            'script': '01_scrape_github.py',
            'description': 'GitHub Trending',
            'duration_estimate': '2-4 minutes'
        },
        {
            'script': '01_scrape_trends.py',
            'description': 'Google Trends',
            'duration_estimate': '3-5 minutes'
        }
    ]
    
    print(f"\nSCRAPERS PROGRAMMES:")
    for i, config in enumerate(scrapers_config, 1):
        print(f"  {i}. {config['description']} (~{config['duration_estimate']})")
    
    print(f"\nDuree estimee totale: 12-20 minutes")
    
    # 4. Confirmation
    start_choice = input("\nLancer la collecte complete? (o/N): ").strip().lower()
    if start_choice not in ['o', 'oui', 'y', 'yes']:
        print("Collecte annulee")
        return
    
    # 5. Execution
    results = []
    total_start = time.time()
    
    for i, config in enumerate(scrapers_config, 1):
        print(f"\nPROGRESS: {i}/{len(scrapers_config)}")
        
        result = run_scraper(config['script'], config['description'])
        results.append(result)
        
        # Pause entre scrapers
        if i < len(scrapers_config):
            print("Pause 10 secondes...")
            time.sleep(10)
    
    total_duration = time.time() - total_start
    
    # 6. Rapport final
    generate_report(results)
    
    print(f"\nCOLLECTE TERMINEE!")
    print(f"Duree totale: {total_duration/60:.1f} minutes")
    print(f"Resultats: {len([r for r in results if r['status'] == 'success'])}/{len(results)} succes")
    
    # 7. Instructions suite
    print(f"\nFICHIERS GENERES dans data/raw/:")
    for source in ['adzuna', 'stackoverflow', 'github', 'google_trends']:
        source_dir = f"data/raw/{source}"
        if os.path.exists(source_dir):
            files = [f for f in os.listdir(source_dir) if f.endswith('.csv')]
            if files:
                print(f"  {source}/: {len(files)} fichiers CSV")
    
    print(f"\nTRANSMISSION A LA PERSONNE 2:")
    print(f"Dossier a traiter: data/raw/")
    print(f"Logs disponibles: logs/")
    print(f"Etape suivante: Nettoyage et normalisation des donnees")

def run_single_scraper():
    """Mode scraper individuel"""
    scrapers = {
        '1': ('01_scrape_adzuna.py', 'Adzuna Jobs API'),
        '2': ('01_scrape_stackoverflow.py', 'Stack Overflow Survey'),
        '3': ('01_scrape_github.py', 'GitHub Trending'),
        '4': ('01_scrape_trends.py', 'Google Trends')
    }
    
    print("\nMODE SCRAPER INDIVIDUEL")
    print("Choisissez un scraper:")
    
    for key, (script, desc) in scrapers.items():
        print(f"  {key}. {desc}")
    
    choice = input("\nVotre choix (1-4): ").strip()
    
    if choice in scrapers:
        script, description = scrapers[choice]
        setup_directories()
        result = run_scraper(script, description)
        
        if result['status'] == 'success':
            print(f"\nSUCCES: {description}")
            print(f"Fichiers generes: {result['files_generated']}")
        else:
            print(f"\nECHEC: {description}")
            if result['error']:
                print(f"Erreur: {result['error']}")
    else:
        print("Choix invalide")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == '--single':
            run_single_scraper()
        elif sys.argv[1] == '--help':
            print("TALENTINSIGHT SCRAPER")
            print("Usage:")
            print("  python run_all_scrapers.py          # Lance tous les scrapers")
            print("  python run_all_scrapers.py --single # Mode individuel")
            print("  python run_all_scrapers.py --help   # Aide")
        else:
            print(f"Argument inconnu: {sys.argv[1]}")
    else:
        main()