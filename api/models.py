from django.db import models

from django.db import models

class Adzuna(models.Model):
    id = models.AutoField(primary_key=True)
    source = models.CharField(max_length=100)
    title = models.CharField(max_length=255)
    company = models.CharField(max_length=255)
    location = models.CharField(max_length=255)
    country = models.CharField(max_length=100)
    salary_min = models.FloatField(null=True, blank=True)
    salary_max = models.FloatField(null=True, blank=True)
    currency = models.CharField(max_length=10)
    skills = models.TextField()
    skills_count = models.IntegerField()
    contract_type = models.CharField(max_length=100)
    description_excerpt = models.TextField()
    posted_date = models.DateField()
    url = models.URLField()
    collected_at = models.DateTimeField()

class GithubStats(models.Model):
    language = models.CharField(max_length=100)
    trending_repos_count = models.IntegerField()
    total_stars = models.IntegerField()
    total_forks = models.IntegerField()
    avg_stars_per_repo = models.FloatField()
    european_repos = models.IntegerField()
    european_countries = models.TextField()
    analysis_date = models.DateField()

class GithubRepo(models.Model):
    id = models.AutoField(primary_key=True)
    source = models.CharField(max_length=100)
    name = models.CharField(max_length=255)
    full_name = models.CharField(max_length=255)
    description = models.TextField()
    language = models.CharField(max_length=100)
    stars_count = models.IntegerField()
    forks_count = models.IntegerField()
    watchers_count = models.IntegerField()
    issues_count = models.IntegerField()
    created_at = models.DateTimeField()
    updated_at = models.DateTimeField()
    owner_login = models.CharField(max_length=100)
    owner_type = models.CharField(max_length=100)
    owner_location = models.CharField(max_length=255, null=True)
    owner_country = models.CharField(max_length=100)
    topics = models.TextField()
    license = models.CharField(max_length=100)
    url = models.URLField()
    clone_url = models.URLField()
    collected_at = models.DateTimeField()

class GoogleTrendsGroup(models.Model):
    comparison_group = models.CharField(max_length=255)
    technology = models.CharField(max_length=255)
    country = models.CharField(max_length=100)
    avg_interest = models.FloatField()
    analysis_date = models.DateField()

class GoogleTrend(models.Model):
    keyword = models.CharField(max_length=255)
    country = models.CharField(max_length=100)
    timeframe = models.CharField(max_length=100)
    avg_interest = models.FloatField()
    max_interest = models.IntegerField()
    min_interest = models.IntegerField()
    trend_direction = models.CharField(max_length=50)
    trend_strength = models.CharField(max_length=50)
    data_points = models.IntegerField()
    analysis_date = models.DateField()
    category = models.CharField(max_length=255)

class StackOverflow(models.Model):
    source = models.CharField(max_length=100)
    country = models.CharField(max_length=100)
    country_name = models.CharField(max_length=100)
    salary_yearly = models.FloatField()
    currency = models.CharField(max_length=10)
    years_experience = models.FloatField()
    experience_level = models.CharField(max_length=100)
    languages_worked = models.TextField()
    developer_type = models.CharField(max_length=100)
    education_level = models.CharField(max_length=100)
    company_size = models.CharField(max_length=100)
    collected_at = models.DateTimeField()

class Glassdoor(models.Model):
    id = models.AutoField(primary_key=True)
    source = models.CharField(max_length=100)
    job_title = models.CharField(max_length=255)
    company = models.CharField(max_length=255)
    location = models.CharField(max_length=255)
    country_code = models.CharField(max_length=10)
    country_name = models.CharField(max_length=100)
    salary_min = models.FloatField()
    salary_max = models.FloatField()
    currency = models.CharField(max_length=10)
    skills = models.TextField()
    skills_count = models.IntegerField()
    dataset_origin = models.CharField(max_length=100)
    collected_at = models.DateTimeField()

class Kaggle(models.Model):
    id = models.AutoField(primary_key=True)
    source = models.CharField(max_length=100)
    job_title = models.CharField(max_length=255)
    company = models.CharField(max_length=255)
    country_code = models.CharField(max_length=10)
    country_name = models.CharField(max_length=100)
    salary_eur = models.FloatField()
    currency = models.CharField(max_length=10)
    experience_level = models.CharField(max_length=100)
    skills = models.TextField()
    skills_count = models.IntegerField()
    data_source = models.CharField(max_length=100)
    collected_at = models.DateTimeField()

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