from Core import *
from ModelCalc import *
from ModelCats import *
from ModelQuery import *


#################### rules as we had them in carbonitor ###################

stahlbeton = Filterset(Operators.All, [ Filter("Structural Material", FilterCondition.StringContains, "STB"), 
                                      Filter("Structural Material", FilterCondition.StringDoesNotContain, "FT") ])



fassade = Filterset(Operators.All, queries= Filter("name", FilterCondition.StringContains, "Curtain Wall"))

bycostgroup = Labelset(Label(Labels.CostGroup, LabelCondition.ByValue))

ribbed_concrete = Filterset(Operators.Any, querysets = [Filterset(Operators.All, [Filter("Category", FilterCondition.ValueEquals, ModelCategories.Beam), 
                                                                                Filter("Material", FilterCondition.StringContains, ["STB", "Steel"] )]),
                                                        Filterset(Operators.All, [Filter("Category", FilterCondition.ValueEquals, ModelCategories.Slab), 
                                                                                Filter("Material", FilterCondition.StringContains, "STB"),
                                                                                Filter("Thickness", FilterCondition.NumberSmallerEquals, 0.27)])
                                                        ])

#print(ribbed_concrete.as_Dict())


#print(ribbed_concrete.as_Json())


flat_concrete = Filterset(Operators.All, [ Filter("Structural Material", FilterCondition.StringContains, "STB"), 
                                      Filter("Structural Material", FilterCondition.StringDoesNotContain, "FT"),
                                       Filter("Thickness", FilterCondition.NumberGreaterEquals, 0.27) ])

precast_concrete = Filterset(Operators.All, [ Filter("Structural Material", FilterCondition.StringContains, "STB"), 
                                      Filter("Structural Material", FilterCondition.StringContains, "FT"),
                                       Filter("Thickness", FilterCondition.NumberGreaterEquals, 0.27) ])


timber_concrete_composite = Filterset(Operators.Any, querysets = [Filterset(Operators.All, [Filter("Category", FilterCondition.ValueEquals, ModelCategories.Beam), 
                                                                                Filter("Material", FilterCondition.StringContains, ["Holz"] )]),
                                                        Filterset(Operators.All, [Filter("Category", FilterCondition.ValueEquals, ModelCategories.Slab), 
                                                                                Filter("Material", FilterCondition.StringContains, "STB"),
                                                                                Filter("Thickness", FilterCondition.NumberSmallerEquals, 0.27)])
                                                        ])

_CLT = Filterset(Operators.All, [ Filter("Structural Material", FilterCondition.StringContains, "Holz") ])


sets = [ribbed_concrete, flat_concrete, precast_concrete, timber_concrete_composite, _CLT]

rules = [Rule(qset, ModelCategories.Beam |+ ModelCategories.Slab) for qset in sets]

my_rulebook = Rulebook(rules)

print(my_rulebook.as_Json())