from rest_framework import serializers
from .models import *

class AdzunaSerializer(serializers.ModelSerializer):
    class Meta:
        model = Adzuna
        fields = '__all__'

class GithubStatsSerializer(serializers.ModelSerializer):
    class Meta:
        model = GithubStats
        fields = '__all__'

class GithubRepoSerializer(serializers.ModelSerializer):
    class Meta:
        model = GithubRepo
        fields = '__all__'

class GoogleTrendsGroupSerializer(serializers.ModelSerializer):
    class Meta:
        model = GoogleTrendsGroup
        fields = '__all__'

class GoogleTrendSerializer(serializers.ModelSerializer):
    class Meta:
        model = GoogleTrend
        fields = '__all__'

class StackOverflowSerializer(serializers.ModelSerializer):
    class Meta:
        model = StackOverflow
        fields = '__all__'

class GlassdoorSerializer(serializers.ModelSerializer):
    class Meta:
        model = Glassdoor
        fields = '__all__'

class KaggleSerializer(serializers.ModelSerializer):
    class Meta:
        model = Kaggle
        fields = '__all__'
