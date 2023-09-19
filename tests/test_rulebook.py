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

print(jsonfromRule)

RuleFromJson = query.Rule.from_Json(jsonfromRule)

print(RuleFromJson.as_Dict())

print(RuleFromJson.as_Json())

print("")
print ("now rulebooks")
print("")

rulebook = query.Rulebook(RuleFromJson)

print(rulebook.as_Dict())

print(rulebook.graph)


print(rulebook.as_Json())


print("adding connections")

OtherRule = query.Rule(query.Labelset())

rulebook = query.Rulebook([RuleFromJson, OtherRule])

rulebook.connections.append(query.RuleConnection(rulebook.rules[0], rulebook.rules[1]))

print(rulebook.as_Dict())

print("")
print(rulebook.as_Json())

print("rulebook from json")

print(query.Rulebook.from_Json(rulebook.as_Json()).as_Dict())