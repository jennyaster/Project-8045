import pandas
from pandas.tools import plotting
from statsmodels.formula.api import ols
import matplotlib.pyplot as plt

data=pandas.read_csv('/Users/John/Desktop/Airline.csv',index_Col=0)
Y = data['recommended']
X = data [['rating_valuemoney','rating_inflightEnt','rating_cabinstaff','rating_seatcomfort','rating_foodbeverage',,'rating_overall']]
model = ols(Y,X).fit()

print(model.summary())

