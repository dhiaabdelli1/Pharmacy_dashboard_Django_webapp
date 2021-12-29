import joblib

def predictSales(date):
    sarimax_model = joblib.load('./staticfiles/forecasting-sales.sav')
    forecasted = sarimax_model.forecast(steps=date)
    return forecasted
