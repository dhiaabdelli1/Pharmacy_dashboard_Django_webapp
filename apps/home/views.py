from django import template
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse, HttpResponseRedirect
from django.http.response import JsonResponse
from django.template import loader
from django.urls import reverse

from django.db import connections
import os
from pandas.core import groupby
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


for date in InOutDF.YearMonth.unique():
    inout_dict={}
    df=InOutDF[InOutDF['YearMonth'] == date]
    inout_dict['y']=date
    for row in df.iterrows():
        inout_dict[row[1][2]]=row[1][3]
    inout_list.append(inout_dict)




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

MonthlyInOutDF=pd.read_sql_query(queries.MonthlyInOutQuery,connection)
MonthlyInOutDF['YearMonth'] = MonthlyInOutDF.Year.astype(str) + '-' + MonthlyInOutDF.Month.astype(str)
MonthlyInOutDF = MonthlyInOutDF.sort_values(by = ['Year','Month'])
MonthlyInOutDF=MonthlyInOutDF.replace({'Entrée':'In','Sortie':'Out'})





MonthlyInData = []

for index, value in MonthlyInOutDF.iteritems():
    MonthlyInData.append({'y':index,'a':value})



ms_list_in=[]
ms_dict_in={}
ms_list_out=[]
ms_dict_out={}

for date in MonthlyInOutDF.YearMonth.unique():
    ms_dict_in={}
    ms_dict_out={}
    df_in=MonthlyInOutDF[MonthlyInOutDF['YearMonth'] == date][MonthlyInOutDF['TypeMouvement'] == 'In']
    df_out=MonthlyInOutDF[MonthlyInOutDF['YearMonth'] == date][MonthlyInOutDF['TypeMouvement'] == 'Out']
    ms_dict_in['y']=date
    ms_dict_out['y']=date
    for row in df_in.iterrows():
        ms_dict_in['a']=row[1][3]
    for row in df_out.iterrows():
        ms_dict_out['a']=row[1][3]
    ms_list_in.append(ms_dict_in)
    ms_list_out.append(ms_dict_out)

monthlyIns=json.dumps(ms_list_in)
monthlyOuts=json.dumps(ms_list_out)






CategoryStockDF = pd.read_sql(queries.queryTopCategoriesInStock,connection)


CategoryStockData = []

for index,row in CategoryStockDF.iterrows():
    CategoryStockData.append({'label':row[0],'value':row[1]})

CategoryStockData = json.dumps(CategoryStockData)


print(CategoryStockData)



@login_required(login_url="/login/")
def stock(request):
    context = {'segment': 'stock',
    'CategStock': json.dumps(stock_list),
    'inout': json.dumps(inout_list),
    'stock2016': df_to_json(Stock2016DF),
    'monthlyIns':monthlyIns,
    'monthlyOuts':monthlyOuts,
    'topcatinstock':CategoryStockData
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



######## SALES FORECASTING ##########

sales_df = pd.read_csv('./staticfiles/sales.csv',parse_dates=['Date'],index_col=['Date'],encoding='utf-16')
sales_df.rename(columns={'TotalTTC':'total_sales'},inplace=True)
sales_ts = sales_df['total_sales'].resample('D', level=0).sum()
sales_ts= sales_ts.replace(to_replace=0,value=sales_df['total_sales'].mean())
sales_ts.index = sales_ts.index.strftime('%Y-%m-%d')


@login_required(login_url="/login/")
def sales_forecasting(request):
    context = {'segment': 'sales-forecasting','salesdata':sales_ts.to_json()}
    html_template = loader.get_template('home/sales-forecast.html')
    return HttpResponse(html_template.render(context, request))


# a fetch api to get the estimated value to print in the template 
# a function to get date then go to prediction and return the vlaue 


from django.http import JsonResponse

from .mlmodels import mlmodels as sf

@login_required(login_url="/login/")
def getEstimatedSales(request):
    date = json.load(request)['post_data']
    estimation = sf.predictSales(date)
    return JsonResponse({'date':date,'estimation':estimation[-1]})




@login_required(login_url="/login/")
def getEstimatedSalesByRange(request):
    params = json.load(request)['post_data']
    estimation = sf.predictSales(params['end'])
    estimation = pd.Series(estimation)
    estimation.index = estimation.index.strftime('%Y-%m-%d')
    estimation = estimation[params['start']:params['end']]
    data = []
    for index, value in estimation.iteritems():
        data.append({'y':index,'x':value})

    return JsonResponse(data,safe=False)




###################### PRICE PREDICTION ##################

import sklearn
import joblib
from sklearn.preprocessing import StandardScaler



@login_required(login_url="/login/")
def price_prediction(request):
    context = {'segment': 'price-prediction'}
    html_template = loader.get_template('home/price-prediction.html')
    return HttpResponse(html_template.render(context, request))

@login_required(login_url="/login/")
def getPredictedPrice(request):
    params = json.load(request)['post_data']
    predicted = sf.predictPrice(params['actualprice'])
    return JsonResponse(predicted, safe=False)


############################ CNSS CLASSIFIER ###########################

def fetchDataset():
    FactVente = pd.read_csv(
        './apps/home/data/FactVente.csv', encoding='utf-16')
    DimArticle = pd.read_csv(
        './apps/home/data/DimArticle.csv', encoding='utf-16')
    DimClient = pd.read_csv(
        './apps/home/data/DimClient.csv', encoding='utf-16')
    VenteDF = pd.merge(DimArticle, FactVente, on='ArticlePK')
    VenteDF = pd.merge(VenteDF, DimClient, on='ClientPK')
    VenteDF = VenteDF[['TotalTTC', 'Qte',
                       'TotalRemise', 'LibelleCategorie', 'CNSS']]
    return VenteDF


def preprocess(row, df):
    df.drop(columns=['CNSS'], inplace=True)
    VenteDF = df.append(row, ignore_index=True)
    df = pd.get_dummies(df, columns=['LibelleCategorie'])
    scaler = StandardScaler()
    client = df.iloc[-1].tolist()
    list = []
    list.append(client)
    scaler.fit(list)
    list = scaler.transform(list)
    return list


def FormView(request):
    categs = ['GTTE BUVABLE', 'SIROP ANTITUSSIF', 'POMMADE', 'CP ANTIBIOTIQUE',
              'CP', 'COLLUTOIRE', 'SIROP', 'PDE OPHT', 'GTTE NASALE', 'SUPPO',
              'SIROP   ANTIBIOTIQUE', 'COLLYRE', 'SACHET BUVABLE', 'GRANULES',
              'GTTE AURIC', 'GYNECO', 'TOILETTE', 'LAIT_FARINE', 'DIETETIQUE',
              'PANSEMENT', 'DENTAIRE', 'SERINGUE', 'ACCESS', 'SERUM', 'INJ',
              'ORTHOPEDIE', 'VET', 'HYGIENE INTIME', 'AMP BUVABLE',
              'GTTE HOMEOPATHIE', 'DOSE', 'INJ ANTIBIOTIQUE', 'US_EXT',
              'TEXTILE', 'NATURE', 'MAISON', 'VACCIN', 'FORMULE', 'INHALATION',
              'SACHET ANTIBIOTIQUE', 'PRESERVATIF', 'GEL-SOLUTE BUCCAL',
              'CP HOMEOPATHIE', 'B. BOUCHE', 'BEBE', 'TEINTURE MERE', 'SABOT',
              'Divers', 'CAPSULE', 'RECIP']

    context = {'categs': categs}
    if request.method == 'POST':
        totalttc = request.POST.get('totalttc')
        qte = request.POST.get('qte')
        totalremise = request.POST.get('totalremise')
        libellecategorie = request.POST.get('libellecategorie')
        new_row = {'TotalTTC': totalttc, 'Qte': qte,
                   'TotalRemise': totalremise, 'LibelleCategorie': libellecategorie}

        # fetching original dataset
        VenteDF = fetchDataset()

        # preprocessing
        list = preprocess(new_row, VenteDF)

        # importing model and making prediction
        knn_model = joblib.load('./staticfiles/knn.sav')
        prediction = knn_model.predict(list)
        context['prediction'] = prediction[0]
        html_template = loader.get_template('home/knn-predict.html')
        return HttpResponse(html_template.render(context, request))
    else:
        context['Prediction'] = ''
        html_template = loader.get_template('home/knn-predict.html')
        return HttpResponse(html_template.render(context, request))




