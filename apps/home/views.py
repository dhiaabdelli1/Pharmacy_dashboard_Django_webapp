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

from apps.home import queries

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



ClientDF = pd.read_sql_query(queries.ClientQuery,connection)



################################################################################################
 

VenteDF = pd.read_sql_query(queries.VenteQuery,connection)


MonthlySalesDF = pd.read_sql_query(queries.MonthlySalesQuery,connection)
MonthlyPurchasesDF = pd.read_sql_query(queries.MonthlyPurchaseQuery,connection)

from datetime import datetime

MonthlySalesDF['YearMonth'] = pd.to_datetime(MonthlySalesDF['Date']).apply(lambda x: '{year}-{month}'.format(year=x.year,month=x.month))

MonthlySalesDF = MonthlySalesDF.groupby('YearMonth')['Montant'].sum()

MonthlyPurchasesDF['YearMonth'] = pd.to_datetime(MonthlyPurchasesDF['Date']).apply(lambda x: '{year}-{month}'.format(year=x.year,month=x.month))

MonthlyPurchasesDF= MonthlyPurchasesDF.groupby('YearMonth')['Montant'].sum()

import json


MonthlySalesData = []

for index, value in MonthlySalesDF.iteritems():
    MonthlySalesData.append({'y':index,'a':value})


MonthlySalesData = json.dumps(MonthlySalesData)

MonthlyPurchasesData = []

for index, value in MonthlyPurchasesDF.iteritems():
  MonthlyPurchasesData.append({'y':index,'a':value})
MonthlyPurchasesData = json.dumps(MonthlyPurchasesData )


CategorySalesDF = pd.read_sql_query(queries.CategorySalesQuery,connection)


CategorySalesData = []

for index,row in CategorySalesDF.iterrows():
    CategorySalesData.append({'label':row[0],'value':row[1]})

CategorySalesData = json.dumps(CategorySalesData)


CategoryPurchasesDF = pd.read_sql_query(queries.CategoryPurchaseQuery,connection)


CategoryPurchasesData = []

for index,row in CategoryPurchasesDF.iterrows():
    CategoryPurchasesData.append({'label':row[0],'value':row[1]})
CategoryPurchasesData = json.dumps(CategoryPurchasesData)


ProductSalesDF = pd.read_sql_query(queries.ProductSalesQuery,connection)
ProductPurchasesDF=pd.read_sql_query(queries.ProductPurchasesQuery, connection)

def df_to_json(df):
    json_records = df.reset_index().to_json(orient ='records')
    data = []
    data = json.loads(json_records)
    return data


LaboSalesDF = pd.read_sql_query(queries.LaboSalesQuery,connection)

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

######
FournisseurDF = pd.read_sql_query(queries.queryFournisseur,connection)

FournisseurDF['YearMonth'] = FournisseurDF.Year.astype(str) + '-' + FournisseurDF.Month.astype(str)

FournisseurDF= FournisseurDF.sort_values(by = ['Year','Month'])


F_list=[]
F_dict={}

for date in FournisseurDF.YearMonth.unique():
    F_dict={}
    df=FournisseurDF[FournisseurDF['YearMonth'] == date]
    F_dict['y']=date
    for row in df.iterrows():
        F_dict[row[1][0]]=row[1][1]
    F_list.append(F_dict)
print(F_list)

###


cursor.execute(queries.totalSalesQuery)
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

CategoryStockDF=pd.read_sql_query(queries.CategoryStockQuery,connection)
CategoryStockDF['YearMonth'] = CategoryStockDF.Year.astype(str) + '-' + CategoryStockDF.Month.astype(str)
CategoryStockDF = CategoryStockDF.sort_values(by = ['Year','Month'])

InOutDF=pd.read_sql_query(queries.InOutQuery,connection)
InOutDF['YearMonth'] = InOutDF.Year.astype(str) + '-' + InOutDF.Month.astype(str)
InOutDF = InOutDF.sort_values(by = ['Year','Month'])
InOutDF=InOutDF.replace({'Entrée':'In','Sortie':'Out'})

inout_list=[]
inout_dict={}

print(InOutDF)

for date in InOutDF.YearMonth.unique():
    inout_dict={}
    df=InOutDF[InOutDF['YearMonth'] == date]
    inout_dict['y']=date
    for row in df.iterrows():
        inout_dict[row[1][2]]=row[1][3]
    inout_list.append(inout_dict)

print(inout_list)


stock_list=[]
stock_dict={}

for date in CategoryStockDF.YearMonth.unique():
    stock_dict={}
    df=CategoryStockDF[CategoryStockDF['YearMonth'] == date]
    stock_dict['y']=date
    for row in df.iterrows():
        stock_dict[row[1][0]]=row[1][1]
    stock_list.append(stock_dict)


Stock2016DF=pd.read_sql_query(queries.Stock2016Query,connection)


@login_required(login_url="/login/")
def stock(request):
    context = {'segment': 'stock',
    'CategStock': json.dumps(stock_list),
    'inout': json.dumps(inout_list),
    'stock2016': df_to_json(Stock2016DF)
    }
    html_template = loader.get_template('home/stock.html')
    return HttpResponse(html_template.render(context, request))


login_required(login_url="/login/")
def purchases(request):
    context = {'segment': 'purchases',
               'monthlypurchases': MonthlyPurchasesData,
               'categoryPurchases' : CategoryPurchasesData ,
               'purchasesProduct': df_to_json(ProductPurchasesDF),
               'Fournisseur': json.dumps(F_list),
               }
    html_template = loader.get_template('home/purchases.html')
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
