import henn.model_query as query

rule = query.Rule(query.Labelset(), "walls")

print(rule.type)