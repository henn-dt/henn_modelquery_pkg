import ModelQuery as query



print( "create a rule with init")

rule = query.Query("material" , "equals", "STB")

print (vars(rule))

print ("#### convert a rule to dictionary")

_dict = rule.as_Dict()

print (_dict)

print("#### convert a rule to json")

_json = rule.as_Json()

print (_json)

print("#### create a rule from dictionary")

sampledict = {"name": "Rule", "param": "structure", "condition": "does not equal", "value": "MW"}
wrongdict = {"name": "Rule", "param": "structure", "condition": "does not equal"}
dictrule = query.Query.from_Dict(_dict)

print (vars(dictrule))

print("#### create a rule from json")

jsonrule = query.Query.from_Json(_json)
print (vars(jsonrule))

print("#### multiple instances")

rulelist = []

rulelist.append(query.Query.from_Dict(_dict))
rulelist.append(query.Query.from_Dict(sampledict))
rulelist.append(query.Query.from_Dict(wrongdict))

for rule in rulelist:
    try:
        print(vars(rule))
    except Exception as err:
        print(err)

print("")
print("########## now a Filter ###################")

myFilter = query.Filter("material", query.FilterCondition.NumberBetween, [5,10])
print (vars(myFilter))

print("change condition")
myFilter.condition = query.FilterCondition.NumberStrictlyBetween

print (vars(myFilter))

print ("filter to dictionary")

print(myFilter.as_Dict())

print("filter to json")

print(myFilter.as_Json())

print("filter from dictionary")

gooddict = myFilter.as_Dict()

filterfromDict = query.Filter.from_Dict(gooddict)

print (type(filterfromDict))
print(vars(filterfromDict))

print("a bad dictionary")

baddict = {"name": "Filter", "param": "material", "condition": "NumberStrictlBetween", "value": [5, 10]}

try:
    filterfrombadDict = query.Filter.from_Dict(baddict) 
    print (vars(filterfrombadDict))
except Exception as err:
    print(err)



print("filter from json")

goodjson = myFilter.as_Json()

filterfromJson = query.Filter.from_Json(goodjson)

print (type(filterfromJson))
print(vars(filterfromJson))



print("create a label")

mylabel = query.Label(query.Labels.Category, query.LabelCondition.ByValue)
print(vars(mylabel))

print("label to json")

print (mylabel.as_Json())

print("label to dictionary")

print (mylabel.as_Dict())

print ("label from dictionary")

dictfromlabel = mylabel.as_Dict()

labelfromdict = query.Label.from_Dict(dictfromlabel)

print(vars(labelfromdict))

print("label from json")

jsonfromlabel = mylabel.as_Json()

labelfromjson = query.Label.from_Json(jsonfromlabel)

print(vars(labelfromjson))

print("##############################################################")
print("#######################Rulesets###############################")
print("")

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


print("########################ruleset from dictionary")

rulesetfromdict = query.Queryset.from_Dict(anotherruleset.as_Dict())

print(vars(rulesetfromdict))

print(rulesetfromdict.as_Dict())

print("ruleset from json")

rulesetfromjson = query.Queryset.from_Json(rulesetfromdict.as_Json())

print(vars(rulesetfromjson))

print("")
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

jsonfromfilterset = otherfilterset.as_Json()

print("")
print("dict from filterset")
print(dictfromfilterset)

print("")
print("json from filterset")
print(jsonfromfilterset)