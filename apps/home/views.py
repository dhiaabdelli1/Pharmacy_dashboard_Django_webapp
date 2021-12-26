# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from django import template
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse, HttpResponseRedirect
from django.template import loader
from django.urls import reverse

from django.db import connections
import os
import pyodbc
import pandas as pd

def dictfetchall(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]

driver = 'SQL Server' 
server = os.environ['USERDOMAIN']
staging_database = 'pharma_SA'
dw_database='pharma_DW'

connection = pyodbc.connect('DRIVER={'+driver+'};SERVER='+server+';DATABASE='+dw_database+';Trusted_Connection=yes;')
cursor=connection.cursor()

ClientQuery="""
SELECT [ClientPK]
      ,[CNSS]
  FROM [pharma_DW].[dbo].[DimClient]
"""

ClientDF = pd.read_sql_query(ClientQuery,connection)



################################################################################################
 
VenteQuery = """SELECT * 
  FROM [pharma_DW].[dbo].[FactVente] V 
  INNER JOIN DimClient C on V.ClientPK = C.ClientPK
  INNER JOIN DimArticle A on V.ArticlePK = A.ArticlePK 
   INNER JOIN DimMutuelle M on V.MutuellePK = M.MutuellePK 
    INNER JOIN DimDate D on V.DatePK = D.DatePK  """
VenteDF = pd.read_sql_query(VenteQuery,connection)


MonthlySalesQuery = """SELECT Date, A.PrixVenteTTC*V.Qte as Montant

  FROM [pharma_DW].[dbo].[FactVente] V 
  INNER JOIN DimClient C on V.ClientPK = C.ClientPK
  INNER JOIN DimArticle A on V.ArticlePK = A.ArticlePK 
   INNER JOIN DimMutuelle M on V.MutuellePK = M.MutuellePK 
    INNER JOIN DimDate D on V.DatePK = D.DatePK """


MonthlySalesDF = pd.read_sql_query(MonthlySalesQuery,connection)

from datetime import datetime

MonthlySalesDF['YearMonth'] = pd.to_datetime(MonthlySalesDF['Date']).apply(lambda x: '{year}-{month}'.format(year=x.year,month=x.month))

MonthlySalesDF = MonthlySalesDF.groupby('YearMonth')['Montant'].sum()

import json


MonthlySalesData = []

for index, value in MonthlySalesDF.iteritems():
    MonthlySalesData.append({'y':index,'a':value})


MonthlySalesData = json.dumps(MonthlySalesData)



CategorySalesQuery = """SELECT TOP 5 A.LibelleCategorie, SUM(ROUND(A.PrixVenteTTC*V.Qte,2)) as Montant
  FROM [pharma_DW].[dbo].[FactVente] V 
  INNER JOIN DimClient C on V.ClientPK = C.ClientPK
  INNER JOIN DimArticle A on V.ArticlePK = A.ArticlePK 
   INNER JOIN DimMutuelle M on V.MutuellePK = M.MutuellePK 
    INNER JOIN DimDate D on V.DatePK = D.DatePK GROUP BY A.LibelleCategorie ORDER BY Montant DESC
"""

CategorySalesDF = pd.read_sql_query(CategorySalesQuery,connection)


CategorySalesData = []

for index,row in CategorySalesDF.iterrows():
    CategorySalesData.append({'label':row[0],'value':row[1]})




CategorySalesData = json.dumps(CategorySalesData)


ProductSalesQuery = """SELECT TOP 10 A.Designation,A.LibelleCategorie,A.PrixVenteTTC, SUM(A.PrixVenteTTC*V.Qte) as Montant, SUM(V.Qte) as Quantity
  FROM [pharma_DW].[dbo].[FactVente] V 
  INNER JOIN DimClient C on V.ClientPK = C.ClientPK
  INNER JOIN DimArticle A on V.ArticlePK = A.ArticlePK 
   INNER JOIN DimMutuelle M on V.MutuellePK = M.MutuellePK 
    INNER JOIN DimDate D on V.DatePK = D.DatePK GROUP BY A.Designation, A.LibelleCategorie, A.PrixVenteTTC ORDER BY Montant DESC"""


ProductSalesDF = pd.read_sql_query(ProductSalesQuery,connection)


def df_to_json(df):
    json_records = df.reset_index().to_json(orient ='records')
    data = []
    data = json.loads(json_records)
    return data




LaboSalesQuery = """SELECT  A.LibelleForme,SUM(A.PrixVenteTTC*V.Qte) as Montant, Month, Year
  FROM [pharma_DW].[dbo].[FactVente] V 
  INNER JOIN DimArticle A on V.ArticlePK = A.ArticlePK 
  INNER JOIN DimDate D on V.DatePK = D.DatePK 
  WHERE A.LibelleForme in (SELECT TOP 5 A.LibelleForme
  FROM [pharma_DW].[dbo].[FactVente] V 
  INNER JOIN DimArticle A on V.ArticlePK = A.ArticlePK GROUP BY A.LibelleForme ORDER BY SUM(A.PrixVenteTTC*V.Qte) DESC ) 
  GROUP BY A.LibelleForme, Month, Year ORDER BY Month,Year"""



LaboSalesDF = pd.read_sql_query(LaboSalesQuery,connection)

LaboSalesDF['YearMonth'] = LaboSalesDF.Year.astype(str) + '-' + LaboSalesDF.Month.astype(str)

LaboSalesDF = LaboSalesDF.sort_values(by = ['Year','Month'])


list=[]
dict={}

for date in LaboSalesDF.YearMonth.unique():
    dict={}
    df=LaboSalesDF[LaboSalesDF['YearMonth'] == date]
    dict['y']=date
    for row in df.iterrows():
        dict[row[1][0]]=row[1][1]
    list.append(dict)


totalSalesQuery = """
SELECT  SUM(A.PrixVenteTTC*V.Qte) as TOTAL
  FROM [pharma_DW].[dbo].[FactVente] V 
  INNER JOIN DimArticle A on V.ArticlePK = A.ArticlePK ;
  """

cursor.execute(totalSalesQuery)
total_sales = cursor.fetchone()


@login_required(login_url="/login/")
def sales(request):
    context = {'segment': 'sales', 
    'monthlysales':MonthlySalesData, 
    'categorysales':CategorySalesData,
    'productsales':df_to_json(ProductSalesDF),
    'total_sales': int(total_sales[0]),
    'labosales':json.dumps(list)
    }
    html_template = loader.get_template('home/sales.html')
    return HttpResponse(html_template.render(context, request))




################################################################################################


@login_required(login_url="/login/")
def index(request):
    context = {'segment': 'index'}

    html_template = loader.get_template('home/index.html')
    return HttpResponse(html_template.render(context, request))




@login_required(login_url="/login/")
def pages(request):
    context = {}
    # All resource paths end in .html.
    # Pick out the html file name from the url. And load that template.
    try:

        load_template = request.path.split('/')[-1]

        if load_template == 'admin':
            return HttpResponseRedirect(reverse('admin:index'))
        context['segment'] = load_template

        html_template = loader.get_template('home/' + load_template)
        return HttpResponse(html_template.render(context, request))

    except template.TemplateDoesNotExist:

        html_template = loader.get_template('home/page-404.html')
        return HttpResponse(html_template.render(context, request))

    except:
        html_template = loader.get_template('home/page-500.html')
        return HttpResponse(html_template.render(context, request))
