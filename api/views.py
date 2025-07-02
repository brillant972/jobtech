from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response
from .models import SalaryStats

class SalaryStatsView(APIView):
    def get(self, request):
        skill = request.GET.get('skill')
        country = request.GET.get('country')
        data = SalaryStats.objects.filter(skill=skill, country=country).values()
        return Response(data)