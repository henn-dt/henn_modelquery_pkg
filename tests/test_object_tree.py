import json
import pandas as pd
import henn.model_query as query

with open("/workspaces/henn_modelquery_pkg/tests/objects_tree.json") as file:

    data = json.load(file)

    data = pd.json_normalize(data, meta="objectid")

    #df = pd.DataFrame.from_dict(data)

    print(data)