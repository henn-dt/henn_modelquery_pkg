import pandas as pd
import henn.model_query as query

technologies= {
    'Category':["Spark", "PySpark", "Hadoop", "Python", "Pandas", "P and as"],
    'Fee' :[22000, 25000, 23000, 24000, 26000, 28000],
    'Discount':[1000, 2300, 1000, 1200, 2500, 3200],
    'Duration':['35days', '35days', '40days', '30days', '25days', '12days']
          }

df = pd.DataFrame(technologies)

#print(df)

df1 = df.groupby(df["Fee"])

#print(df1.describe())

label = query.labelDataframe(df, query.Labels.Category, query.LabelCondition.ByValue)

#print(label.describe())

filter = query.filterDataframe(df, "Duration", query.FilterCondition.NumberGreater, 25)

print(filter)