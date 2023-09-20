import json
import pandas as pd
import time
import ModelQuery as query
import serializers.APS as APS
import ModelCats as Cats

starttime = time.time()
startprocess = time.process_time()

with open("/workspaces/henn_modelquery_pkg/tests/objects_tree.json", "r") as objects_tree, \
        open("/workspaces/henn_modelquery_pkg/tests/viewable_tree.json", "r") as viewable_tree:

    objects = json.loads(objects_tree.read())
    tree = json.loads(viewable_tree.read())
    
    _model = APS.Serialize_fromModelDerivative(objects, tree)

    # print(_model[50:55])

df = pd.DataFrame(_model)

#print (df.loc[df["category"] == Cats.ModelCategories.Covering,  "type"][100:120])

print(df.describe())

endtime = time.time()
endprocess = time.process_time()

print ("execution time: " , endtime - starttime, " seconds")
print("")
print("process time: ", endprocess - startprocess, " seconds")


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