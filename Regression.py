import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from sklearn import linear_model
from sklearn import datasets

#Load Train and Test datasets
#Identify feature and response variable(s) and values must be numeric and numpy arrays
x_train=input_variables_values_training_datasets
y_train=target_variables_values_training_datasets
x_test=input_variables_values_test_datasets


# Create linear regression object
linear = linear_model.LinearRegression()


# Train the model using the training sets and check score
linear.fit(x_train, y_train)
linear.score(x_train, y_train)


#Equation coefficient and Intercept
print('Coefficient: \n', linear.coef_)
print('Intercept: \n', linear.intercept_)


#Predict Output
predicted= linear.predict(x_test)
