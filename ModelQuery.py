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


class LabelCondition(Enum):
    NoCondition = 0
    ByValue = auto()             
    ByStartingCharacter = auto()
    """Value should be a positive integer. If no value is provided, it will group by the first character"""
    ByEndingCharacter = auto()
    """Value should be a positive integer. If no value is provided, it will group by the last character"""


"""
future conditions to implement:
"""
"""    
    ByInterval = auto()
    Value should be either a single number or a list of numbers. If no value is provided, will group by positive and negatives values
    ByMatch = auto()
"""

  


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


################################# BASE QUERY FUNCTIONS ##########################################################


def filterDataframe(input : pd.DataFrame , param,  criteria:FilterCondition, value, inplace = False):
    """
    Filters a pandas dataframe based on a single filter condition
    Returns a new dataframe with only the elements that pass the filter.
    Set inplace = True to change the input dataframe.
    """
#    _param = repr(param) if " " in param else param
#    _value = repr(value) if " " in value else value
    _param = param
    _value = value
    match(FilterCondition(criteria)):
        case FilterCondition.NoCondition:
            return True
        
        case FilterCondition.ValueEquals:
            return input.query("{0} == @_value".format(_param), inplace=inplace)
        
        case FilterCondition.ValueNotEquals:
            return input.query("{0} != @_value".format(_param), inplace=inplace)
        
        case FilterCondition.ValueInList:
            if isinstance (value, list):
                _value = (v for v in value)
                return input.query("{0} in @_value".format(_param), inplace=inplace)
            return filterDataframe(input, param, FilterCondition.ValueEquals, value, inplace)
        
        case FilterCondition.ValueNotInList:
            if isinstance (value, list):
                _value = (v for v in value)
                return input.query("{0} not in @_value".format(_param), inplace=inplace)
            return filterDataframe(input, param, FilterCondition.ValueNotEquals, value, inplace)
        
        case FilterCondition.StringBeginsWith:
            return input.query("{0}.str.startswith(@_value)".format(_param), inplace=inplace)  
             
        case FilterCondition.StringEndsWith:
            return input.query("{0}.str.endswith(@_value)".format(_param), inplace=inplace)    

        case FilterCondition.StringDoesNotBeginWith:
            return input.query(" not {0}.str.startswith(@_value)".format(_param), inplace=inplace)     

        case FilterCondition.StringDoesNotEndWith:   
            return input.query(" not {0}.str.endswith(@_value)".format(_param), inplace=inplace)  

        case FilterCondition.StringContains:
            return input.query("{0}.str.contains(@_value)".format(_param), inplace=inplace)
        
        case FilterCondition.StringDoesNotContain:
            return input.query("not {0}.str.contains(@_value)".format(_param), inplace=inplace)

        case FilterCondition.NumberGreater:
            try:
                return input.query("{0} > @_value".format(_param), inplace=inplace)
            except:
                pass
            input["compare"] = input[param].str.extract('(\d+)').astype(float)
            return input.query("{0} > @_value".format("compare"), inplace=inplace)

        case FilterCondition.NumberGreaterEquals:
            try:
                return input.query("{0} >= @_value".format(_param), inplace=inplace)
            except:
                pass
            input["compare"] = input[param].str.extract('(\d+)').astype(float)
            return input.query("{0} >= @_value".format("compare"), inplace=inplace)
        
        case FilterCondition.NumberSmaller:
            try:
                return input.query("{0} < @_value".format(_param), inplace=inplace)
            except:
                pass
            input["compare"] = input[param].str.extract('(\d+)').astype(float)
            return input.query("{0} < @_value".format("compare"), inplace=inplace)
        
        case FilterCondition.NumberSmallerEquals:
            try:
                return input.query("{0} <= @_value".format(_param), inplace=inplace)
            except:
                pass
            input["compare"] = input[param].str.extract('(\d+)').astype(float)
            return input.query("{0} <= @_value".format("compare"), inplace=inplace)       

        case FilterCondition.NumberBetween:
            if isinstance(value, list):
                min = min(value)
                max = max(value)
                try:
                    return input.query("{0} <= @max and {0} >= @min".format(_param), inplace=inplace)
                except:
                    pass
                input["compare"] = input[param].str.extract('(\d+)').astype(float)
                return input.query("{0} <= @max and {0} >= @min".format("compare"), inplace=inplace)  
            return filterDataframe(input, param, FilterCondition.ValueEquals, value, inplace)
        

        case FilterCondition.NumberStrictlyBetween:
            if isinstance(value, list):
                min = min(value)
                max = max(value)
                try:
                    return input.query("{0} < @max and {0} > @min".format(_param), inplace=inplace)
                except:
                    pass
                input["compare"] = input[param].str.extract('(\d+)').astype(float)
                return input.query("{0} < @max and {0} > @min".format("compare"), inplace=inplace)  
            return filterDataframe(input, param, FilterCondition.ValueEquals, value, inplace)  

        case FilterCondition.ValueExists:
            return input.query("not {0}isnull()".format(_param))

        case FilterCondition.ValueIsNone:
            return input.query("{0}isnull()".format(_param))



def filterValue(input , criteria:FilterCondition, value):
    """ given an input value, a criteria, and a value or list of values, returns true if the input fulfills the criteria """
    match(FilterCondition(criteria)):
        case FilterCondition.NoCondition:
            return True
        case FilterCondition.ValueEquals:
            return input == value
        case FilterCondition.ValueNotEquals:
            return input != value
        case FilterCondition.ValueInList:
            if isinstance(value, list):
                return input in value
            return input == value
        case FilterCondition.ValueNotInList:
            if isinstance(value, list):
                return input not in value
            return input != value
        case FilterCondition.StringBeginsWith:
            return str(value).startswith(str(input))
        case FilterCondition.StringDoesNotBeginWith:
            return not str(value).startswith(str(input))
        case FilterCondition.StringEndsWith:
            return str(value).endswith(str(input))
        case FilterCondition.StringDoesNotEndWith:
            return not str(value).endswith(str(input))
        case FilterCondition.StringContains:
            return str(input) in str(value)
        case FilterCondition.StringDoesNotContain:
            return str(input) not in str(value)
        case FilterCondition.NumberGreater:
            return input > value
        case FilterCondition.NumberGreaterEquals:
            return input >= value
        case FilterCondition.NumberSmaller:
            return input < value
        case FilterCondition.NumberSmallerEquals:
            return input <= value
        case FilterCondition.NumberBetween:
            if isinstance(value, list):
                return min(value) <= input <= max(value)
            return value == input
        case FilterCondition.NumberStrictlyBetween:
            if isinstance(value, list):
                return min(value) < input < max(value)
            return value == input  
        case FilterCondition.ValueExists:
            if input:
                return True
            return False
        case FilterCondition.ValueIsNone:
            if not input:
                return True
            return False

def labelDataframe(input : pd.DataFrame, label : Labels, criteria:LabelCondition , value = None):
    """return groupby items of the input Dataframe, based on Labels (column) and criteria """
    match(LabelCondition(criteria)):
        case LabelCondition.NoCondition:
            return input
        case LabelCondition.ByValue:
            return input.groupby([label.name], group_keys=True, sort=False, dropna=False)
        case LabelCondition.ByStartingCharacter:
            i = value - 1 if isinstance(value, int) else 0
            i = i if i >= 0 else 0
            input["compare"] = input.label.str[0:i]
            return labelDataframe(input, "compare", LabelCondition.ByValue)
        case LabelCondition.ByEndingCharacter:
            i = value - 1 if isinstance(value, int) else 0
            i = i if i >= 0 else 0
            string = input.label.str
            input["compare"] = string[len(string)- i - 1 : len(string)]
            return labelDataframe(input, "compare", LabelCondition.ByValue)   

################################ END OF BASE QUERY FUNCTIONS ####################################################


class Query:         # a parent class for performing different kind of simple queries

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

        prop = list(Query().as_Dict().keys())
                
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


class Filter(Query):     # a Rule that returns a yes or no result
    def __init__(self, param : str = None , condition : FilterCondition = FilterCondition.NoCondition, value = None):               
        super().__init__(param, condition, value)

    @classmethod
    def from_Json(cls, input):

        obj = super().from_Json(input)
        if not isinstance(obj.condition, FilterCondition):
            obj.condition = FilterCondition[obj.condition]       
        return obj
    
    def analyse(self, input):
        if isinstance (input, pd.DataFrame):
            return filterDataframe(input, self.param, self.condition, self.value)
        return filterValue(input, self.condition, self.value)
    




class Label(Query):      # a Rule that return groups of elements based on the values of a parameter 
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
    



class Queryset(Query):      # a parent class for implementing multiple nested queries

    def __init__(self,  queries : List[Query] = None , querysets = None):
        __queries = ensure_list(queries)
        __querysets = ensure_list(querysets)
        super().__init__(param=list(set(r.param for r in __queries)), queries=__queries, querysets=__querysets)


#####################   Recursive methods #####################

    def as_Dict(self):
        output = super().as_Dict()
        if len(self.queries) > 0:
            output["queries"] = [r.as_Dict() for r in self.queries]
        
        if len(self.querysets) > 0:
            output["querysets"] = [queryset.as_Dict() for queryset in self.querysets]
        return output
    
    def as_Json(self):
        
        def recursiveEnumToString(queryset : dict):
            if len(queryset["querysets"]) > 0:
                queryset["querysets"] = [recursiveEnumToString(subset) for subset in queryset["querysets"]]
            if len(output["queries"]) > 0:
                queryset["queries"] = [dictEnumValuesToString(rule) for rule in queryset["queries"]]
            dictEnumValuesToString(queryset)
            return queryset
            
        output = self.as_Dict()
        recursiveEnumToString(output)

        dictEnumValuesToString(output)

        return json.dumps(output)

    
    @classmethod
    def from_Dict(cls, input):
        prop = list(Queryset().as_Dict().keys())

        check_reqs(input, prop )
    
        __queries = [Query.from_Dict(r) for r in input["queries"]]
        __querysets = [Queryset.from_Dict(queryset) for queryset in input["querysets"]]     
     
        obj = cls(__queries, __querysets)

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
    



class Filterset(Queryset):       # a ruleset for performing nested filter operations, using an Operator (and, or, ...) to aggregate results

    def __init__(self, condition : Operators = Operators.All, queries : List[Filter] = None, querysets = None):       
        test = []

        __condition = condition

        if queries is not None:
            test = queries if isinstance(queries, list) else [queries]
            if not all(isinstance(r, Filter) for r in test):
                return "input queries are not Filters"
        
        if querysets is not None:
            test = [_ruleset.queries for _ruleset in querysets] if isinstance(querysets, list) else querysets.queries
            if not all(isinstance(r, Filter) for r in test):
                return "input queries within querysets are not all Filters"

        super().__init__(queries, querysets)
        self.condition = __condition


class Labelset(Queryset):
    def __init__(self, queries : List[Label] = None, querysets = None):
        test = []

        if queries is not None:
            test = queries if isinstance(queries, list) else [queries]
            if not all(isinstance(r, Label) for r in test):
                return print("input queries are not Label")
        
        if querysets is not None:
            test = [_ruleset.queries for _ruleset in querysets] if isinstance(querysets, list) else querysets.queries
            if not all(isinstance(r, Label) for r in test):
                return print("input queries within querysets are not all Label")
            
        super().__init__(queries, querysets)


######################################## Rules and Rulebook #################################################

class Rule:
    """a rule contains a single Queryset (including Labelsets or Filtersets), optional categories, and a target group """
    def __init__(self, ruleset, categories, group = None):
        self.ruleset = ruleset
        self.categories = categories
        self.group = group if group is not None else None
        self.type = type(self.ruleset).__name__

