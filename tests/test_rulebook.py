import ModelQuery as query
import ModelCats as cats


myLabelSet = query.Labelset(query.Label(query.Labels.Category))

print(myLabelSet.as_Json())

myrule = query.Rule(myLabelSet, cats.ModelCategories.Beam)

print(myrule.as_Json())

myFilterSet = query.Filterset()

print (myFilterSet.as_Json())

mycats = cats.ModelCategories.Ceiling | cats.ModelCategories.Beam

myotherrule = query.Rule(myFilterSet, mycats)

print(myotherrule.as_Dict())

print(myotherrule.as_Json())

ruleAsDict = myotherrule.as_Dict()

ruleFromDict = query.Rule.from_Dict(ruleAsDict)

print(ruleFromDict.as_Dict())

print("rule from json")

jsonfromRule = myotherrule.as_Json()

RuleFromJson = query.Rule.from_Json(jsonfromRule)

print(RuleFromJson)