"""
Configuration et constantes pour les scrapers TalentInsight
"""
import os
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# API Keys
ADZUNA_APP_ID = os.getenv('ADZUNA_APP_ID')
ADZUNA_API_KEY = os.getenv('ADZUNA_API_KEY')
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')

# Configuration scraping
MAX_REQUESTS_PER_MINUTE = int(os.getenv('MAX_REQUESTS_PER_MINUTE', 30))
REQUEST_DELAY_MIN = int(os.getenv('REQUEST_DELAY_MIN', 1))
REQUEST_DELAY_MAX = int(os.getenv('REQUEST_DELAY_MAX', 3))

# Pays cibles
TARGET_COUNTRIES = os.getenv('TARGET_COUNTRIES', 'FR,DE,NL,GB,IT').split(',')

# Répertoires
RAW_DATA_DIR = os.getenv('RAW_DATA_DIR', 'raw')
CLEAN_DATA_DIR = os.getenv('CLEAN_DATA_DIR', 'datasets_clean')
LOGS_DIR = os.getenv('LOGS_DIR', 'logs')

# URLs de base
ADZUNA_BASE_URL = "https://api.adzuna.com/v1/api/jobs"
GITHUB_BASE_URL = "https://api.github.com"
STACKOVERFLOW_SURVEY_URL = "https://cdn.stackoverflow.co/files/jo7n4k8s/production/49915bfd46d0902c3564fd9a06b509d08a20488c.zip"

# Headers pour web scraping
DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1'
}

# Compétences techniques à rechercher
TECH_SKILLS = [
    # Langages de programmation
    'Python', 'JavaScript', 'Java', 'C++', 'C#', 'PHP', 'Ruby', 'Go', 'Rust',
    'TypeScript', 'Kotlin', 'Swift', 'Scala', 'R', 'MATLAB', 'Perl',
    
    # Frameworks web
    'React', 'Vue.js', 'Angular', 'Node.js', 'Django', 'Flask', 'Spring',
    'Express.js', 'Laravel', 'Ruby on Rails', 'ASP.NET', 'FastAPI',
    
    # Bases de données
    'PostgreSQL', 'MySQL', 'MongoDB', 'Redis', 'Elasticsearch', 'SQLite',
    'Oracle', 'SQL Server', 'Cassandra', 'Neo4j',
    
    # Cloud et DevOps
    'Docker', 'Kubernetes', 'AWS', 'Azure', 'GCP', 'Jenkins', 'GitLab CI',
    'Terraform', 'Ansible', 'Prometheus', 'Grafana',
    
    # Data Science & AI
    'Machine Learning', 'Deep Learning', 'TensorFlow', 'PyTorch', 'Pandas',
    'NumPy', 'Scikit-learn', 'Jupyter', 'Apache Spark', 'Hadoop',
    
    # Outils et méthodologies
    'Git', 'Scrum', 'Agile', 'REST API', 'GraphQL', 'Microservices',
    'Linux', 'Bash', 'PowerShell', 'Vim', 'VS Code'
]

# Mapping pays code -> nom
COUNTRY_MAPPING = {
    'FR': 'France',
    'DE': 'Germany', 
    'NL': 'Netherlands',
    'GB': 'United Kingdom',
    'IT': 'Italy'
}

# Mapping Adzuna codes pays
ADZUNA_COUNTRY_CODES = {
    'FR': 'fr',
    'DE': 'de',
    'NL': 'nl', 
    'GB': 'gb',
    'IT': 'it'
}