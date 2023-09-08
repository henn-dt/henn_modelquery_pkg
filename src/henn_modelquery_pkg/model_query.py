import pandas as pd
from enum import Flag, Enum, auto
from typing import List, Optional, Set, Tuple, Union
import json

"""
A module that defines queries capabilites for apps that need to filter information
in building information models. 

methods are built assuming pandas DataFrame as inputs.

"""

######## Variables and Constants #####################################

class RuleType(Enum):
    Rule = auto()
    Filter = auto()
    Label = auto()
    Ruleset = auto()

class RulesetType(Enum):
    Filterset = auto()
    Labelset = auto()

class FilterCondition(Enum):    
    NoCondition = 0
    ValueEquals = auto()
    ValueNotEquals = auto()
    ValueInList = auto()
    ValueNotInList = auto()
    StringBeginsWith = auto()
    StringDoesNotBeginWith = auto()
    StringEndsWith = auto()
    StringDoesNotEndWith = auto()
    StringContains = auto()
    StringDoesNotContain = auto()
    NumberGreater = auto()
    NumberGreaterEquals = auto()
    NumberSmaller = auto()
    NumberSmallerEquals = auto()
    NumberBetween = auto()
    NumberStrictlyBetween = auto()
    ValueExists = auto()          # filter rules that triggers true if the parameter has any value except None, null, empty strings, or 0
    ValueIsNone = auto()          # filter rules that returns true if the value is None, null, empty string, or 0

class LabelCondition(Enum):
    NoCondition = 0
    ByValue = auto()             

"""
future conditions to implement:
"""
"""    
    ByStartingCharacter = auto()
    ByEndingCharachter = auto()
    ByInterval = auto()
    ByMatch = auto()
"""

class Labels (Enum):
    NoLabel = 0
    Model = auto()
    Category = auto()
    Type = auto()
    CostGroup = auto()
    Level = auto()
    Elevation = auto()
    Material = auto()
    FireRating = auto()

class Operators (Enum):
    All = 1
    Any = 0


################################### HELPERS ####################################################################

def ensure_list(s: Optional[Union[str, List[str], Tuple[str], Set[str]]]) -> List[str]:
    """works for string and list/tuple/sets of strings. converts None to an empty list"""
    # Ref: https://stackoverflow.com/a/56641168/
    return s if isinstance(s, list) else list(s) if isinstance(s, (tuple, set)) else [] if s is None else [s]


def enumToString(input):
    output = []

    if isinstance(input, list):
        for e in input:
            output.append(enumToString(e))

    elif isinstance(input, Enum):
        output = input.name
    else:
        output = input

    return output

def dictEnumValuesToString(input : dict):
    for key , value in input.items():
        input[key] = enumToString(value)
    return input


def check_reqs(input, reqs):
    properties = ensure_list(reqs)
    error = "input doesn't have the required keys"    
    missingkeys = []

    for p in properties:
        if p in list(input.keys()):
            continue
        missingkeys.append(p)

    if len(missingkeys) > 0:
        return error + "(" + str(missingkeys) + ")"

################################# END OF HELPERS ################################################################


class Rule:         # a parent class for performing different kind of simple queries

    def __init__(self, param = None, condition = None, value = None, **kwargs):
        self.name = str(self.__class__.__name__)
        self.param = param if param is not None else None
        self.condition = condition if condition is not None else None
        self.value = value if value is not None else None
        self.__dict__.update(**kwargs)

    def __iter__(self):             
        for key in self.__dict__:
            yield key, getattr(self, key)

    def as_Dict(self):
        return dict(self)

    def as_Json(self):       
        output = self.as_Dict()

        dictEnumValuesToString(output)

#        for key in output.keys():
#            if isinstance(output[key], Enum):
#                output[key] = output[key].name

        return json.dumps(output)

    @classmethod
    def from_Dict(cls, input):    
        """
        method to create a rule from a dictionary string
        input = the dictionary. must contain a "param", a "condition", and a "value" key.
        """

        prop = list(Rule().as_Dict().keys())
                
        check_reqs(input, prop)
                
        obj = cls()

        for key, value in input.items():
            if key == "name":
                continue
            setattr(obj, key, value)

        return obj


    @classmethod
    def from_Json(cls, input):
        """
        method to create a rule from a json string
        input = the json string. must contain a "param", a "condition", and a "value" key.
        """
        error = "input is not a valid json"

        try:
            input = json.loads(input)
        except Exception as err:
            print(error + str(err.args))
            return 
        
        return cls.from_Dict(input)


class Filter(Rule):     # a Rule that returns a yes or no result
    def __init__(self, param : str = None , condition : FilterCondition = FilterCondition.NoCondition, value = None):               
        super().__init__(param, condition, value)

    @classmethod
    def from_Json(cls, input):

        obj = super().from_Json(input)
        if not isinstance(obj.condition, FilterCondition):
            obj.condition = FilterCondition[obj.condition]       
        return obj



class Label(Rule):      # a Rule that return groups of elements based on the values of a parameter 
    def __init__(self, param : Labels = Labels.NoLabel, condition : LabelCondition = LabelCondition.NoCondition, value = None):
        super().__init__(param, condition, value)

    @classmethod
    def from_Json(cls, input):
        obj = super().from_Json(input)
        if not isinstance(obj.condition, LabelCondition):
            obj.condition = LabelCondition[obj.condition]
        if not isinstance(obj.param, Labels):
            obj.param = Labels[obj.param]
        return obj



class Ruleset(Rule):      # a parent class for implementing multiple nested queries

    def __init__(self,  rules : List[Rule] = None , rulesets = None):
        __rules = ensure_list(rules)
        __rulesets = ensure_list(rulesets)
        super().__init__(param=list(set(r.param for r in __rules)), rules=__rules, rulesets=__rulesets)


#####################   Recursive methods #####################

    def as_Dict(self):
        output = super().as_Dict()
        if len(self.rules) > 0:
            output["rules"] = [r.as_Dict() for r in self.rules]
        
        if len(self.rulesets) > 0:
            output["rulesets"] = [ruleset.as_Dict() for ruleset in self.rulesets]
        return output
    
    def as_Json(self):
        
        def recursiveEnumToString(ruleset : dict):
            if len(ruleset["rulesets"]) > 0:
                ruleset["ruleset"] = [recursiveEnumToString(subruleset) for subruleset in ruleset["rulesets"]]
            if len(output["rules"]) > 0:
                ruleset["rules"] = [dictEnumValuesToString(rule) for rule in ruleset["rules"]]
            dictEnumValuesToString(ruleset)
            return ruleset
            
        output = self.as_Dict()
        recursiveEnumToString(output)

        dictEnumValuesToString(output)

        return json.dumps(output)

    
    @classmethod
    def from_Dict(cls, input):
        prop = list(Ruleset().as_Dict().keys())

        check_reqs(input, prop )
    
        __rules = [Rule.from_Dict(r) for r in input["rules"]]
        __rulesets = [Ruleset.from_Dict(ruleset) for ruleset in input["rulesets"]]     
     
        obj = cls(__rules, __rulesets)

        for key, value in input.items():
            if key in prop:
                continue
            setattr(obj, key, value)

        return obj
    
    @classmethod
    def from_Json(cls, input):
        error = "input is not a valid json"

        try:
            input = json.loads(input)
        except Exception as err:
            print(error + str(err.args))
            return   

        return cls.from_Dict(input)
    



class Filterset(Ruleset):       # a ruleset for performing nested filter operations, using an Operator (and, or, ...) to aggregate results

    def __init__(self, condition : Operators = Operators.All, rule : List[Filter] = None, ruleset = None):       
        test = []

        __condition = condition

        if rule is not None:
            test = rule if isinstance(rule, list) else [rule]
            if not all(isinstance(r, Filter) for r in test):
                return "input rules are not Filters"
        
        if ruleset is not None:
            test = [_ruleset.rules for _ruleset in ruleset] if isinstance(ruleset, list) else ruleset.rules
            if not all(isinstance(r, Filter) for r in test):
                return "input rules within ruleset are not all Filters"

        super().__init__(rule, ruleset)
        self.condition = __condition


class Labelset(Ruleset):
    def __init__(self, rule : List[Label] = None, ruleset = None):
        test = []

        if rule is not None:
            test = rule if isinstance(rule, list) else [rule]
            if not all(isinstance(r, Label) for r in test):
                return print("input rules are not Label")
        
        if ruleset is not None:
            test = [_ruleset.rules for _ruleset in ruleset] if isinstance(ruleset, list) else ruleset.rules
            if not all(isinstance(r, Label) for r in test):
                return print("input rules within ruleset are not all Label")
            
        super().__init__(rule, ruleset)









