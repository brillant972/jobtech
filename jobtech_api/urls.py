"""
URL configuration for jobtech_api project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from api.views import *
from django.urls import include, path
from rest_framework.routers import DefaultRouter

router = DefaultRouter()
router.register(r'adzuna', AdzunaViewSet)
router.register(r'github/stats', GithubStatsViewSet)
router.register(r'github/repos', GithubRepoViewSet)
router.register(r'google/trends_group', GoogleTrendsGroupViewSet)
router.register(r'google/trend', GoogleTrendViewSet)
router.register(r'stackoverflow', StackOverflowViewSet)
router.register(r'glassdoor', GlassdoorViewSet)
router.register(r'kaggle', KaggleViewSet)

urlpatterns = [
    path('', include(router.urls)),
    path('analytics/average-salaries/', AverageSalaryView.as_view(), name='average-salaries'),
    path('api/v1/salary-daily/', SalaryStatsView.as_view()),
    path('analytics/top-salary-countries/', TopSalaryCountriesView.as_view(), name='top-salary-countries'),
    path('analytics/top-skills-by-country/', TopSkillsByCountryView.as_view()),
    path('analytics/suggested-skills/', SuggestedSkillsView.as_view()),
    path('analytics/skill-trend/', SkillTrendView.as_view()),
    path('analytics/salary-comparison/', SalaryComparisonBySkillView.as_view()),
]



