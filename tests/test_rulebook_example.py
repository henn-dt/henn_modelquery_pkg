from Core import *
from ModelCalc import *
from ModelCats import *
from ModelQuery import *


#################### rules as we had them in carbonitor ###################

stahlbeton = Filterset(Operators.All, [ Filter("Structural Material", FilterCondition.StringContains, "STB"), 
                                      Filter("Structural Material", FilterCondition.StringDoesNotContain, "FT") ])

fassade = Filterset(Operators.All, queries= Filter("name", FilterCondition.StringContains, "Curtain Wall"))

bycostgroup = Labelset(Label(Labels.CostGroup, LabelCondition.ByValue))

sets = [stahlbeton, fassade, bycostgroup]

rules = [Rule(qset, ModelCategories.Wall |+ ModelCategories.Slab) for qset in sets]

my_rulebook = Rulebook(rules, RuleConnection(rules[0], rules[2]))

print(my_rulebook.as_Json())