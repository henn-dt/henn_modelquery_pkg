import ModelQuery as query
import pandas as pd



class MyObject():
    def __init__(self, name = None):
        self.name = name



technologies= {
    'Category':["Spark", "PySpark", "Hadoop", "Python", "Pandas", "P and as", "Spark"],
    'Fee' :[22000, 25000, 23000, 24000, 26000, 28000, 22000],
    'Discount':[1000, 2300, 1000, 1200, 2500, 3200, 1500],
    'Duration':['35days', '35days', '40days', '30days', '25days', '12days', '35days'],
    'CostGroup':['35days', '35days', '40days', '30days', '25days', '12days', '400']
    
          }

technologies["Object"] = []

for cat in technologies["Category"]:
    technologies["Object"].append(MyObject(cat))

label = query.Label([query.Labels.Category, "Fee", query.Labels.Category ] , query.LabelCondition.ByValue)

groups = label.analyse(technologies)

print("groups")
print(groups)