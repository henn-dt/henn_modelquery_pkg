from Core import *
from ModelCalc import *
from ModelCats import *
from ModelQuery import *


#################### rules for fire ratings ###################

fire_rating = Labelset(Label(Labels.FireRating, LabelCondition.ByValue))

structural_walls = Filterset(Operators.All, [Filter("Structural Material", FilterCondition.StringContains, "STB")])

sets = [structural_walls, fire_rating, fire_rating]

rules = [Rule(qset, ModelCategories.Slab |+ ModelCategories.Slab) for qset in sets]

my_rulebook = Rulebook(rules, [RuleConnection(rules[0], rules[1])])

print(my_rulebook.as_Json())