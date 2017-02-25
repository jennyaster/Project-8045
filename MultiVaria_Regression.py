import py2neo
from py2neo import Graph, Node, Relationship
import pandas as pd
import numpy as np
import statsmodels.api as sm

py2neo.authenticate("localhost:7474", "neo4j", "classroom")
graph = Graph("http://localhost:7474/db/data/")

#Pull data from neo4j

query = """MATCH (n: Review)
RETURN n.rating_valuemoney, n.rating_inflightEnt, n.rating_cabinstaff,
    n.rating_seatcomfort, n.rating_foodbeverage, n.rating_overall, n.sentiment, n.recommended
"""

result = graph.run(query)

labels = ["rating_valuemoney","rating_inflightEnt","rating_cabinstaff","rating_seatcomfort","rating_foodbeverage","rating_overall","sentiment","recommended"]
rows1 = []

for index,result in enumerate(result):
    row = [result["n.rating_valuemoney"],result["n.rating_inflightEnt"],result["n.rating_cabinstaff"],result["n.rating_seatcomfort"],result["n.rating_foodbeverage"],result["n.rating_overall"],result["n.sentiment"],result["n.recommended"]]
    if "nan" not in row:
        for index,entry in enumerate(row):
            entry = (float(entry))
            row[index] = entry
        rows1.append(row)
    elif row[7] is "nan":
        pass
    else:
        count = 0
        for index,entry in enumerate(row):
            if entry == "nan":
                if index != 5:
                    entry = float(row[7]) * 5
                else:
                    entry = float(row[7]) * 10
                count += 1
                row[index] = entry
            else:
                entry = float(entry)
                row[index] = entry
        if count < 5:
            rows1.append(row)

dataframe1 = pd.DataFrame.from_records(rows1, columns=labels)

def regression(dataframe):
    data = dataframe1
    y = data['recommended']
    X = data [['rating_valuemoney','rating_inflightEnt','rating_cabinstaff','rating_seatcomfort','rating_foodbeverage','rating_overall','sentiment']]
    X = sm.add_constant(X)
    results = sm.OLS(y,X).fit()

    print results.summary()

regression(dataframe1)
