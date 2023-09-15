import ModelQuery as query

rule = query.Rule(query.Labelset(), "walls")

print(rule.type)