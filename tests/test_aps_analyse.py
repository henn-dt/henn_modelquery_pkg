import json
import pandas as pd
import time
import ModelQuery as query
import serializers.APS as APS
from ModelCats import *

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

print(df.head())

endtime = time.time()
endprocess = time.process_time()

print ("execution time: " , endtime - starttime, " seconds")
print("")
print("process time: ", endprocess - startprocess, " seconds")

filter_category = query.Filter("category", query.FilterCondition.ValueEquals, ModelCategories.Wall)

filter_set = query.Filterset(query.Operators.Any, queries=filter_category)

#print(filter_set.as_Json())

print("analyse filter")
res_cat = filter_category.analyse(df)

print(res_cat)

#print(pd.merge(res_cat, res_price, how = "outer"))


print("analyse filterset")
result = filter_set.Analyse(df)

print(result)

endtime = time.time()
endprocess = time.process_time()
print ("execution time: " , endtime - starttime, " seconds")
print("")
print("process time: ", endprocess - startprocess, " seconds")