from django.urls import path, re_path
from apps.home import views

urlpatterns = [

    # The home page
    path('', views.index, name='home'),
    path('sales/', views.sales, name='sales'),
    path('sales-forecasting/', views.sales_forecasting, name='sales-forecast'),
    path('sales-forecasting/getEstimatedSales/', views.getEstimatedSales, name='api-getestimatedsales'),
    path('sales-forecasting/getEstimatedSalesByRange/', views.getEstimatedSalesByRange, name='api-getestimatedsalesbyrange'),
    path('stock/', views.stock, name='stock'),
    path('purchases/', views.purchases, name='purchases'),
    path('price-prediction/',views.price_prediction, name='price-prediction' ),
    path('price-prediction/getPredictedPrice/',views.getPredictedPrice, name='api-getpredictedprice'),
    path('cnss-classifier/', views.FormView, name='cnss-classifier')

    # Matches any htnml file
    #re_path(r'^.*\.*', views.pages, name='pages'),

]
