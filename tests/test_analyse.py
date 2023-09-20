import ModelQuery as query
import pandas as pd



class MyObject():
    def __init__(self, name = None):
        self.name = name



technologies= {
    'Category':["Spark", "PySpark", "Hadoop", "Python", "Pandas", "P and as"],
    'Fee' :[22000, 25000, 23000, 24000, 26000, 28000],
    'Discount':[1000, 2300, 1000, 1200, 2500, 3200],
    'Duration':['35days', '35days', '40days', '30days', '25days', '12days']
          }

technologies["Object"] = []

for cat in technologies["Category"]:
    technologies["Object"].append(MyObject(cat))

df = pd.DataFrame(technologies)

filter_fee = query.Filter("Fee", query.FilterCondition.NumberSmaller, 26000 )
filter_discount = query.Filter("Discount", query.FilterCondition.NumberGreaterEquals, 1200)
filter_category = query.Filter("Category", query.FilterCondition.ValueEquals, "Spark")

price_set = query.Filterset(query.Operators.All, queries=[filter_fee, filter_discount])

filter_set = query.Filterset(query.Operators.Any, queries=filter_category, querysets=price_set)

#print(filter_set.as_Json())


res_cat = filter_category.analyse(df)

res_disc = filter_discount.analyse(df)

res_fee = filter_fee.analyse(df)


res_price = pd.merge(res_disc, res_fee, how = "inner")

#print(pd.merge(res_cat, res_price, how = "outer"))

result = filter_set.Analyse(df)

print(result)