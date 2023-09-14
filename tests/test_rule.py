import henn.ModelQuery as query



print( "create a rule with init")

rule = query.Query("material" , "equals", "STB")

anotherrule = query.Query(prop = "value")

rulewithenum = query.Query(condition=query.LabelCondition.ByValue, a=1)

print (vars(rule))

print ("#### convert a rule to dictionary")

_dict = dict(rule)

print (_dict)

print ("print anotherrule to dictionary and as json")
print(anotherrule.as_Dict())
print(anotherrule.as_Json())

print("#### convert a rule to json")

_json = rule.as_Json()

print(_json)

print("")
print("print rule with enum to dict and json")
print(rulewithenum.as_Json())
print(rulewithenum.as_Dict())


print("")
print("rules from dictionaries")

_dict = dict(rule)
_anotherdict = anotherrule.as_Dict()
_dichtwithenum = rulewithenum.as_Dict()

_rulefromdict = query.Query.from_Dict(_dict)
_rulefromnotherdict = query.Query.from_Dict(_anotherdict)
_rulefromdictwithenum = query.Query.from_Dict(_dichtwithenum)

print(vars(_rulefromdict))
print("-------------------")
print(vars(_rulefromnotherdict))
print("-------------------")
print(vars(_rulefromdictwithenum))

print("")
print("rules from json")

rulefromjson = query.Query.from_Json(_json)

print (vars(rulefromjson))

rulewithenumfromjson = query.Query.from_Json(rulewithenum.as_Json())

print(vars(rulewithenumfromjson))     # will not return Enum from specific subclass
