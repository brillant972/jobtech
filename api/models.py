from django.db import models

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