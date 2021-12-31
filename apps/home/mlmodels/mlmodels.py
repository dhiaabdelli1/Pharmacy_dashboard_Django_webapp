import joblib

def predictSales(date):
    sarimax_model = joblib.load('./staticfiles/forecasting-sales.sav')
    
    forecasted = sarimax_model.forecast(steps=date)
    return forecasted



def predictPrice(purchase_price):
    linear_model = joblib.load('./staticfiles/price-prediction.sav')
    predicted = linear_model.predict([[purchase_price]])
    return predicted[0][0]


