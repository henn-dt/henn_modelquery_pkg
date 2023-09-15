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
