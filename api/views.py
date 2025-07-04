from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import viewsets
from .models import *
from .serializers import *
from django.db.models import Avg, Min, Max
from django.db.models.functions import Cast
from django.db.models import FloatField
from django.db.models.expressions import F
import numpy as np

class SalaryComparisonBySkillView(APIView):
    def get(self, request):
        skill = request.query_params.get("skill", "").lower()
        country = request.query_params.get("country", "").upper()
        exp = request.query_params.get("experience_level", "")

        def filter_salary(model, country_field, skill_field, salary_field, exp_field=None):
            qs = model.objects.filter(**{f"{country_field}__iexact": country})
            qs = qs.filter(**{f"{skill_field}__icontains": skill})
            if exp and exp_field:
                qs = qs.filter(**{f"{exp_field}__iexact": exp})
            return qs.aggregate(avg=Avg(salary_field), min=Min(salary_field), max=Max(salary_field))

        return Response({
            "adzuna": filter_salary(Adzuna, "country", "skills", "salary_max"),
            "glassdoor": filter_salary(Glassdoor, "country_name", "skills", "salary_max"),
            "kaggle": filter_salary(Kaggle, "country_name", "skills", "salary_eur", "experience_level"),
            "stackoverflow": filter_salary(StackOverflow, "country", "languages_worked", "salary_yearly", "experience_level"),
        })

class SkillTrendView(APIView):
    def get(self, request):
        skill = request.query_params.get("skill", "").lower()
        country = request.query_params.get("country", "").upper()
        if not skill:
            return Response({"error": "Missing skill"}, status=400)

        trends = GoogleTrend.objects.filter(keyword__icontains=skill)
        if country:
            trends = trends.filter(country__iexact=country)
        trend_avg = trends.aggregate(avg=Avg("avg_interest"), max=Max("max_interest"))

        github_repos = GithubRepo.objects.filter(language__iexact=skill)
        if country:
            github_repos = github_repos.filter(owner_country__iexact=country)
        repo_count = github_repos.count()

        return Response({
            "google_trends": trend_avg,
            "github_repos_count": repo_count
        })

class SuggestedSkillsView(APIView):
    def get(self, request):
        country = request.query_params.get("country", "").upper()

        def extract_skills(model, field):
            qs = model.objects.all()
            if country:
                qs = qs.filter(**{f"{field}__iexact": country})
            return qs.values_list("skills", flat=True)

        def get_skill_set(queryset):
            skills_set = set()
            for row in queryset:
                if row:
                    skills = [s.strip().lower() for s in row.split(",")]
                    skills_set.update(skills)
            return skills_set

        job_skills = get_skill_set(
            extract_skills(Adzuna, "country") |
            extract_skills(Glassdoor, "country_name")
        )
        dev_skills = get_skill_set(
            extract_skills(GithubRepo, "owner_country") |
            extract_skills(StackOverflow, "country")
        )

        missing_skills = sorted(job_skills - dev_skills)
        return Response({"suggested_skills": missing_skills[:20]})

class TopSkillsByCountryView(APIView):
    def get(self, request):
        country = request.query_params.get("country", "").upper()
        if not country:
            return Response({"error": "Missing country"}, status=400)

        from collections import Counter
        skill_counter = Counter()

        for model, field in [(Adzuna, "country"), (Glassdoor, "country_name"), (Kaggle, "country_name")]:
            queryset = model.objects.filter(**{f"{field}__iexact": country}).values_list("skills", flat=True)
            for skills_text in queryset:
                if skills_text:
                    skills = [s.strip().lower() for s in skills_text.split(",")]
                    skill_counter.update(skills)

        most_common = skill_counter.most_common(10)
        return Response([{"skill": s, "count": c} for s, c in most_common])

class TopSalaryCountriesView(APIView):
    def get(self, request):
        skill = request.query_params.get("skill", "").lower()
        if not skill:
            return Response({"error": "Missing skill"}, status=400)

        # Fusionne les sources ici (exemple avec Adzuna, Glassdoor, Kaggle)
        all_data = []

        def filter_and_average(model, skill_field, salary_field, country_field):
            entries = model.objects.filter(
                skills__icontains=skill
            ).values(country_field).annotate(avg_salary=Avg(salary_field)).order_by('-avg_salary')
            for entry in entries:
                all_data.append({
                    "country": entry[country_field],
                    "avg_salary": entry["avg_salary"],
                    "source": model.__name__,
                })

        filter_and_average(Adzuna, "skills", "salary_max", "country")
        filter_and_average(Glassdoor, "skills", "salary_max", "country_name")
        filter_and_average(Kaggle, "skills", "salary_eur", "country_name")

        # Group by country
        from collections import defaultdict
        grouped = defaultdict(list)
        for item in all_data:
            grouped[item["country"]].append(item["avg_salary"])

        avg_per_country = [
            {"country": k, "avg_salary": sum(v) / len(v)} for k, v in grouped.items()
        ]
        sorted_result = sorted(avg_per_country, key=lambda x: x["avg_salary"], reverse=True)[:5]

        return Response(sorted_result)


class AdzunaViewSet(viewsets.ModelViewSet):
    queryset = Adzuna.objects.all()
    serializer_class = AdzunaSerializer

class GithubStatsViewSet(viewsets.ModelViewSet):
    queryset = GithubStats.objects.all()
    serializer_class = GithubStatsSerializer

class GithubRepoViewSet(viewsets.ModelViewSet):
    queryset = GithubRepo.objects.all()
    serializer_class = GithubRepoSerializer

class GoogleTrendsGroupViewSet(viewsets.ModelViewSet):
    queryset = GoogleTrendsGroup.objects.all()
    serializer_class = GoogleTrendsGroupSerializer

class GoogleTrendViewSet(viewsets.ModelViewSet):
    queryset = GoogleTrend.objects.all()
    serializer_class = GoogleTrendSerializer

class StackOverflowViewSet(viewsets.ModelViewSet):
    queryset = StackOverflow.objects.all()
    serializer_class = StackOverflowSerializer

class GlassdoorViewSet(viewsets.ModelViewSet):
    queryset = Glassdoor.objects.all()
    serializer_class = GlassdoorSerializer

class KaggleViewSet(viewsets.ModelViewSet):
    queryset = Kaggle.objects.all()
    serializer_class = KaggleSerializer


class AverageSalaryView(APIView):
    def get(self, request):
        country = request.query_params.get("country", None)
        experience = request.query_params.get("experience_level", None)

        def median(qs, field):
            values = qs.exclude(**{f"{field}__isnull": True}).values_list(field, flat=True)
            values = sorted(values)
            return float(np.median(values)) if values else None

        # Adzuna
        adzuna_qs = Adzuna.objects.all()
        if country:
            adzuna_qs = adzuna_qs.filter(country__iexact=country)
        adzuna_avg = adzuna_qs.annotate(
            avg_salary=Cast((F("salary_min") + F("salary_max")) / 2, FloatField())
        ).aggregate(
            avg=Avg("avg_salary"),
            min=Min("salary_min"),
            max=Max("salary_max")
        )
        adzuna_median = median(adzuna_qs.annotate(avg_salary=(F("salary_min") + F("salary_max")) / 2), "avg_salary")

        # StackOverflow
        so_qs = StackOverflow.objects.all()
        if country:
            so_qs = so_qs.filter(country__iexact=country)
        if experience:
            so_qs = so_qs.filter(experience_level__iexact=experience)
        so_stats = so_qs.aggregate(
            avg=Avg("salary_yearly"),
            min=Min("salary_yearly"),
            max=Max("salary_yearly")
        )
        so_median = median(so_qs, "salary_yearly")

        # Glassdoor
        glassdoor_qs = Glassdoor.objects.all()
        if country:
            glassdoor_qs = glassdoor_qs.filter(country_name__iexact=country)
        glassdoor_avg = glassdoor_qs.annotate(
            avg_salary=Cast((F("salary_min") + F("salary_max")) / 2, FloatField())
        ).aggregate(
            avg=Avg("avg_salary"),
            min=Min("salary_min"),
            max=Max("salary_max")
        )
        glassdoor_median = median(glassdoor_qs.annotate(avg_salary=(F("salary_min") + F("salary_max")) / 2), "avg_salary")

        # Kaggle
        kaggle_qs = Kaggle.objects.all()
        if country:
            kaggle_qs = kaggle_qs.filter(country_name__iexact=country)
        if experience:
            kaggle_qs = kaggle_qs.filter(experience_level__iexact=experience)
        kaggle_stats = kaggle_qs.aggregate(
            avg=Avg("salary_eur"),
            min=Min("salary_eur"),
            max=Max("salary_eur")
        )
        kaggle_median = median(kaggle_qs, "salary_eur")

        # Combine for total average
        averages = [
            val for val in [
                adzuna_avg["avg"],
                so_stats["avg"],
                glassdoor_avg["avg"],
                kaggle_stats["avg"]
            ] if val is not None
        ]
        combined_avg = sum(averages) / len(averages) if averages else None

        return Response({
            "adzuna": {
                "average": adzuna_avg["avg"],
                "median": adzuna_median,
                "min": adzuna_avg["min"],
                "max": adzuna_avg["max"],
            },
            "stackoverflow": {
                "average": so_stats["avg"],
                "median": so_median,
                "min": so_stats["min"],
                "max": so_stats["max"],
            },
            "glassdoor": {
                "average": glassdoor_avg["avg"],
                "median": glassdoor_median,
                "min": glassdoor_avg["min"],
                "max": glassdoor_avg["max"],
            },
            "kaggle": {
                "average": kaggle_stats["avg"],
                "median": kaggle_median,
                "min": kaggle_stats["min"],
                "max": kaggle_stats["max"],
            },
            "combined_average": combined_avg
        })


#Temporary
class SalaryStatsView(APIView):
    def get(self, request):
        skill = request.GET.get('skill')
        country = request.GET.get('country')
        data = SalaryStats.objects.filter(skill=skill, country=country).values()
        return Response(data)