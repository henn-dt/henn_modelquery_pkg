import pandas as pd
import ModelQuery as query

class MyObject():
    def __init__(self, name = None):
        self.name = name



technologies= {
    'Category':["Spark", "PySpark", "Hadoop", "Python", "Pandas", "P and as", "Spark"],
    'Fee' :[22000, 25000, 23000, 24000, 26000, 28000, 20000],
    'Discount':[1000, 2300, 1000, 1200, 2500, 3200, 400],
    'Duration':['35days', '35days', '40days', '30days', '25days', '12days', "35days"]
          }

technologies["Object"] = []

for cat in technologies["Category"]:
    technologies["Object"].append(MyObject(cat))

df = pd.DataFrame(technologies)

#print(df)

df1 = df.groupby(df["Fee"])

#print(df1.describe())

label = query.labelDataframe(df, query.Labels.Category, query.LabelCondition.ByValue)

print(label.keys())

for key, value in label.items():
    print(type(key))
    for k in key:
        print(type(k))
    print(type(value))


print("filter in place")

value = 'Spark'

df = pd.DataFrame(technologies)

results = df.query("Category ==  'Spark' ", inplace = True)

print(df)

#print(label.describe())

#filter = query.filterDataframe(df, "Duration", query.FilterCondition.NumberGreater, 25)

#print(filter)