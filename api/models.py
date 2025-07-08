from django.db import models

class Adzuna(models.Model):
    id = models.BigIntegerField(primary_key=True)
    source = models.CharField(max_length=100, null=True, blank=True)
    title = models.TextField(null=True, blank=True)
    company = models.TextField(null=True, blank=True)
    location = models.TextField(null=True, blank=True)
    country = models.CharField(max_length=100, null=True, blank=True)
    salary_min = models.FloatField(null=True, blank=True)
    salary_max = models.FloatField(null=True, blank=True)
    currency = models.CharField(max_length=10, null=True, blank=True)
    skills = models.TextField(null=True, blank=True)
    skills_count = models.BigIntegerField(null=True, blank=True)
    contract_type = models.TextField(null=True, blank=True)
    description_excerpt = models.TextField(null=True, blank=True)
    posted_date = models.CharField(max_length=50, null=True, blank=True)
    url = models.URLField(max_length=500, null=True, blank=True)
    collected_at = models.CharField(max_length=50, null=True, blank=True)
    skills_normalized = models.TextField(null=True, blank=True)
    country_normalized = models.CharField(max_length=10, null=True, blank=True)
    salary_eur_min = models.FloatField(null=True, blank=True)
    salary_eur_max = models.FloatField(null=True, blank=True)
    salary_eur_avg = models.FloatField(null=True, blank=True)
    source_type = models.CharField(max_length=50, null=True, blank=True)
    processed_at = models.DateTimeField(null=True, blank=True)
    is_verified_company = models.CharField(max_length=10, null=True, blank=True)
    sirene_match_score = models.FloatField(null=True, blank=True)
    sirene_match_method = models.CharField(max_length=50, null=True, blank=True)

    class Meta:
        managed = False  # Django won't manage this table
        db_table = 'adzuna_jobs_clean'

class GithubStats(models.Model):
    language = models.CharField(max_length=100, primary_key=True)
    trending_repos_count = models.BigIntegerField(null=True, blank=True)
    total_stars = models.BigIntegerField(null=True, blank=True)
    total_forks = models.BigIntegerField(null=True, blank=True)
    avg_stars_per_repo = models.FloatField(null=True, blank=True)
    european_repos = models.BigIntegerField(null=True, blank=True)
    european_countries = models.TextField(null=True, blank=True)
    analysis_date = models.CharField(max_length=50, null=True, blank=True)
    source_file = models.CharField(max_length=255, null=True, blank=True)
    github_data_type = models.CharField(max_length=100, null=True, blank=True)
    language_normalized = models.CharField(max_length=100, null=True, blank=True)
    source_type = models.CharField(max_length=50, null=True, blank=True)
    processed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'github_language_stats_clean'

class GithubRepo(models.Model):
    id = models.BigIntegerField(primary_key=True)
    source = models.CharField(max_length=100, null=True, blank=True)
    name = models.CharField(max_length=255, null=True, blank=True)
    full_name = models.CharField(max_length=255, null=True, blank=True)
    description = models.TextField(null=True, blank=True)
    language = models.CharField(max_length=100, null=True, blank=True)
    stars_count = models.BigIntegerField(null=True, blank=True)
    forks_count = models.BigIntegerField(null=True, blank=True)
    watchers_count = models.BigIntegerField(null=True, blank=True)
    issues_count = models.BigIntegerField(null=True, blank=True)
    created_at = models.CharField(max_length=50, null=True, blank=True)
    updated_at = models.CharField(max_length=50, null=True, blank=True)
    owner_login = models.CharField(max_length=100, null=True, blank=True)
    owner_type = models.CharField(max_length=100, null=True, blank=True)
    owner_location = models.CharField(max_length=255, null=True, blank=True)
    owner_country = models.CharField(max_length=100, null=True, blank=True)
    topics = models.TextField(null=True, blank=True)
    license = models.CharField(max_length=100, null=True, blank=True)
    url = models.URLField(max_length=500, null=True, blank=True)
    clone_url = models.URLField(max_length=500, null=True, blank=True)
    collected_at = models.CharField(max_length=50, null=True, blank=True)
    source_file = models.CharField(max_length=255, null=True, blank=True)
    github_data_type = models.CharField(max_length=100, null=True, blank=True)
    language_normalized = models.CharField(max_length=100, null=True, blank=True)
    source_type = models.CharField(max_length=50, null=True, blank=True)
    processed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'github_trending_repos_clean'

class StackOverflow(models.Model):
    source = models.CharField(max_length=100, null=True, blank=True)
    country = models.CharField(max_length=100, null=True, blank=True)
    country_name = models.CharField(max_length=100, null=True, blank=True)
    salary_yearly = models.FloatField(null=True, blank=True)
    currency = models.CharField(max_length=10, null=True, blank=True)
    years_experience = models.CharField(max_length=50, null=True, blank=True)
    experience_level = models.CharField(max_length=100, null=True, blank=True)
    languages_worked = models.TextField(null=True, blank=True)
    developer_type = models.CharField(max_length=100, null=True, blank=True)
    education_level = models.CharField(max_length=100, null=True, blank=True)
    company_size = models.CharField(max_length=100, null=True, blank=True)
    collected_at = models.CharField(max_length=50, null=True, blank=True)
    survey_source = models.CharField(max_length=100, null=True, blank=True)
    source_file = models.CharField(max_length=255, null=True, blank=True)
    country_normalized = models.CharField(max_length=10, null=True, blank=True)
    languages_worked_normalized = models.TextField(null=True, blank=True)
    salary_yearly_eur_normalized = models.FloatField(null=True, blank=True)
    source_type = models.CharField(max_length=50, null=True, blank=True)
    processed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'stackoverflow_clean'

class Glassdoor(models.Model):
    id = models.CharField(max_length=100, primary_key=True)
    source = models.CharField(max_length=100, null=True, blank=True)
    title = models.TextField(null=True, blank=True)
    company = models.TextField(null=True, blank=True)
    location = models.TextField(null=True, blank=True)
    country = models.CharField(max_length=10, null=True, blank=True)
    country_name = models.CharField(max_length=100, null=True, blank=True)
    salary_min = models.BigIntegerField(null=True, blank=True)
    salary_max = models.BigIntegerField(null=True, blank=True)
    currency = models.CharField(max_length=10, null=True, blank=True)
    skills = models.TextField(null=True, blank=True)
    skills_count = models.BigIntegerField(null=True, blank=True)
    dataset_origin = models.CharField(max_length=100, null=True, blank=True)
    collected_at = models.CharField(max_length=50, null=True, blank=True)
    skills_normalized = models.TextField(null=True, blank=True)
    country_normalized = models.CharField(max_length=10, null=True, blank=True)
    salary_eur_min = models.FloatField(null=True, blank=True)
    salary_eur_max = models.FloatField(null=True, blank=True)
    salary_eur_avg = models.FloatField(null=True, blank=True)
    source_type = models.CharField(max_length=50, null=True, blank=True)
    processed_at = models.DateTimeField(null=True, blank=True)
    is_verified_company = models.CharField(max_length=10, null=True, blank=True)
    sirene_match_score = models.FloatField(null=True, blank=True)
    sirene_match_method = models.CharField(max_length=50, null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'glassdoor_jobs_clean'

class Kaggle(models.Model):
    id = models.CharField(max_length=100, primary_key=True)
    source = models.CharField(max_length=100, null=True, blank=True)
    job_title = models.TextField(null=True, blank=True)
    company = models.TextField(null=True, blank=True)
    country_code = models.CharField(max_length=10, null=True, blank=True)
    country_name = models.CharField(max_length=100, null=True, blank=True)
    salary_eur = models.FloatField(null=True, blank=True)
    currency = models.CharField(max_length=10, null=True, blank=True)
    experience_level = models.CharField(max_length=100, null=True, blank=True)
    skills = models.TextField(null=True, blank=True)
    skills_count = models.BigIntegerField(null=True, blank=True)
    data_source = models.CharField(max_length=100, null=True, blank=True)
    collected_at = models.CharField(max_length=50, null=True, blank=True)
    survey_source = models.CharField(max_length=100, null=True, blank=True)
    source_file = models.CharField(max_length=255, null=True, blank=True)
    country = models.CharField(max_length=10, null=True, blank=True)
    country_normalized = models.CharField(max_length=10, null=True, blank=True)
    skills_normalized = models.TextField(null=True, blank=True)
    salary_eur_eur_normalized = models.FloatField(null=True, blank=True)
    source_type = models.CharField(max_length=50, null=True, blank=True)
    processed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'kaggle_europe_clean'

# Tables de Google Trends (si elles existent)
class GoogleTrendsGroup(models.Model):
    comparison_group = models.CharField(max_length=255, null=True, blank=True)
    technology = models.CharField(max_length=255, null=True, blank=True)
    country = models.CharField(max_length=100, null=True, blank=True)
    avg_interest = models.FloatField(null=True, blank=True)
    analysis_date = models.CharField(max_length=50, null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'country_trends_clean'

class GoogleTrend(models.Model):
    comparison_group = models.CharField(max_length=255, null=True, blank=True)
    technology = models.CharField(max_length=255, null=True, blank=True)
    country = models.CharField(max_length=100, null=True, blank=True)
    avg_interest = models.FloatField(null=True, blank=True)
    analysis_date = models.CharField(max_length=50, null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'tech_comparisons_clean'

#Temporary
class SalaryStats(models.Model):
    id = models.BigAutoField(primary_key=True)
    country = models.CharField(max_length=2)
    skill = models.CharField(max_length=50)
    median_salary_eur = models.IntegerField()
    p25 = models.IntegerField()
    p75 = models.IntegerField()
    sample_size = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'f_salary_stats'
