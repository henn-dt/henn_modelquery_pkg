import henn.ModelQuery as query


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