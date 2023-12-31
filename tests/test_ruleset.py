import ModelQuery as query
import copy

print("##############################################################")
print("#######################Rulesets###############################")
print("")


print("some rules")

myrule = query.Query("one parameter", "ValueEquals", "one value)")

jsonfromrule = myrule.as_Json()

jsonrule = query.Query.from_Json(jsonfromrule)

jsonrule.param = "two parameter"
jsonrule.value = "two value"

dictfromrule = myrule.as_Dict()

dictrule = query.Query.from_Dict(dictfromrule)
dictrule.param = "three param"
dictrule.value = "three value"


sampledict = dictrule.as_Dict()

myruleset = query.Queryset(jsonrule)

print(vars(myruleset))

anotherruleset = query.Queryset([jsonrule, dictrule, query.Query.from_Dict(sampledict)], myruleset)

print(vars(anotherruleset))

print("################### ruleset to dict #####################")

print(anotherruleset.as_Dict())

print ("")
print("###########################################################")
print("ruleset to json")

print (anotherruleset.as_Json())

jsonruleset = anotherruleset.as_Json()

print("########################ruleset from dictionary")

rulesetfromdict = query.Queryset.from_Dict(anotherruleset.as_Dict())

print(vars(rulesetfromdict))

print(rulesetfromdict.as_Dict())

print("ruleset from json")

rulesetfromjson = query.Queryset.from_Json(jsonruleset)

print(vars(rulesetfromjson))

print("ruleset from json as json")

print(rulesetfromjson.as_Json())

print("")

print("a filter")

myfilter = query.Filter("another param", query.FilterCondition.StringBeginsWith, "bleh")

dictfromFilter = myfilter.as_Dict()

filterfromDict = query.Filter.from_Dict(dictfromFilter)

anotherfilter = query.Filter("param three", query.FilterCondition.ValueInList, [10, 5, 25])

jsonfromfilter = anotherfilter.as_Json()

filterfromJson = query.Filter.from_Json(jsonfromfilter)

print([])

print("create a filterset")

filterset = query.Filterset(query.Operators.Any, filterfromDict)

print(vars(filterset))

print("")
print("filter as dict")

print (filterset.as_Dict())

print("")
print("filterset with nested filterset")

otherfilterset = query.Filterset(query.Operators.All, [filterfromDict, filterfromJson, query.Filter.from_Dict(sampledict)], filterset)

print(otherfilterset.as_Dict())

dictfromfilterset = otherfilterset.as_Dict()

print("")
print("dict from filterset")
print(dictfromfilterset)


print("")
print("json from filterset")

jsonfromfilterset = otherfilterset.as_Json()
print(jsonfromfilterset)


print("filterset from dict")
print(query.Filterset.from_Dict(dictfromfilterset).as_Dict())


print("filterset from json")
print(query.Filterset.from_Json(jsonfromfilterset).as_Dict())

print("")
print("####################### LABELSets ##################################")
print("")

mylabel = query.Label(query.Labels.Category)

otherlabel = query.Label(query.Labels.CostGroup, query.LabelCondition.ByValue)

mylabelset = query.Labelset(mylabel)

print(vars(mylabelset))

nestedlabelset = query.Labelset([mylabel, otherlabel] , mylabelset)

print(vars(nestedlabelset))

print("")
print("labelssets as dictionaries")

print(mylabelset.as_Dict())

print("")

print(nestedlabelset.as_Dict())


print("##################################")
print("")
print("labelssets from dics")

labelsetFromDict = query.Labelset.from_Dict(nestedlabelset.as_Dict())

print(labelsetFromDict.as_Dict())

print("")
print("labelssets as jsons")

print(mylabelset.as_Json())

print("")

print(nestedlabelset.as_Json())


print("")
print("labelsets from json")
print("")

labelsetfromJson = query.Labelset.from_Json(nestedlabelset.as_Json())

print(labelsetfromJson.as_Dict())


print("test filtersets")

redundantrule = copy.copy(myfilter)

testfilterset = query.Filterset(query.Operators.All, [redundantrule, myfilter])

print(testfilterset.as_Json())

#checkEq = redundantrule.__eq__(myfilter)

#print(checkEq)

print("try to re-add an equal rule")

testfilterset.queries.append(redundantrule)

print(testfilterset.as_Json())
