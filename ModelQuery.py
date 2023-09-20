import sys
import pandas as pd
from enum import Flag, Enum, auto
from typing import List, Optional, Set, Tuple, Union
import json
import ModelCats


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

    elif isinstance(input, Flag):
        _list = [name for name, member in input.__class__.__members__.items() if member in input and member != 0]
        output = _list

    elif isinstance(input, Enum):

        output = input.name
    else:
        output = input

    return output

def dictEnumValuesToString(input : dict):

    for key , value in input.items():
        if isinstance(value, dict):
            input[key] = dictEnumValuesToString(value)
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


def filterDataframe(input : pd.DataFrame , param,  condition:FilterCondition, value, inplace = False):
    """
    Filters a pandas dataframe based on a single filter condition
    Returns a new dataframe with only the elements that pass the filter.
    Set inplace = True to change the input dataframe.
    """
#    _param = repr(param) if " " in param else param
#    _value = repr(value) if " " in value else value
    _param = param
    _value = value
    match(FilterCondition(condition)):
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



def filterValue(input , condition:FilterCondition, value):
    """ given an input value, a criteria, and a value or list of values, returns true if the input fulfills the criteria """
    match(FilterCondition(condition)):
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

def labelDataframe(input : pd.DataFrame, label : Labels, condition:LabelCondition , value = None):
    """
    returns a dictionary of dataframes, 
    where key is a tuple ( 'label_value' , )
    and value is a dataframe with the grouped elements.

    Parameters:
    input : the dataframe to be analysed
    label : the argument to group by. accept single values or lists. converts Enums to their .name
    condition (enum.LabelCondition): the condition to group by
    value : for criterias that need to specify a value, the required value. 
    """
    label = label if isinstance(label, list) else [label]
    labelby = [l.name if isinstance(l, Enum) else l for l in label]
    match(LabelCondition(condition)):
        case LabelCondition.NoCondition:
            return { tuple() : input }
        case LabelCondition.ByValue:

            return dict(tuple(input.groupby(labelby,  sort=False, dropna=False)))
        
        case LabelCondition.ByStartingCharacter:
            _labelby = []
            for l in labelby:
                _label = "compare_{0}".format(l)
                _labelby.append(_label)
                i = value - 1 if isinstance(value, int) else 0
                i = i if i >= 0 else 0
                input[_label] = input.l.str[0:i]
            return labelDataframe(input, _labelby, LabelCondition.ByValue)
        
        case LabelCondition.ByEndingCharacter:
            _labelby = []
            for l in labelby:
                _label = "compare_{0}".format(l)
                _labelby.append(_label)                
                i = value - 1 if isinstance(value, int) else 0
                i = i if i >= 0 else 0
                string = input.l.str
                input[_label] = string[len(string)- i - 1 : len(string)]
            return labelDataframe(input, _labelby, LabelCondition.ByValue)   


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

#    def __repr__(self):
#        return "this {!r} checks the parameter {!r} for the condition {!r} against the value {!r}".format(self.name, self.param, self.condition, self.value)
    
    def __hash__(self):
        return hash(self.__iter__())
    
    def __eq__(self, other):
        return tuple(self) == tuple(other)

    def as_Dict(self):
        return dict(self)
    
    def serializable_attr(self):
        return (dict((i.replace(self.__class__.__name__, "").lstrip("_"), value) for i, value in self.__dict__.items()))

    def as_Json_value(self):
        output = self.as_Dict()
        return dictEnumValuesToString(output)


    def as_Json(self):       
        return json.dumps(self.as_Json_value())
    

    @classmethod
    def from_Dict(cls, input):    
        """
        method to create a rule from a dictionary string
        input = the dictionary. must contain a "param", a "condition", and a "value" key.
        """
        _class = getattr(sys.modules[__name__], input["name"])()
        prop = list(_class.as_Dict().keys())                
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
            input = json.loads(str(input))
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
    
    def analyse(self, _input, inplace = False):
        if isinstance(_input, dict):
            try:
                _input = pd.DataFrame(_input)
            except:
                pass
        if isinstance (_input, pd.DataFrame):
            return filterDataframe(_input, self.param, self.condition, self.value, inplace)
        return filterValue(_input, self.condition, self.value)
    




class Label(Query):      # a Rule that return groups of elements based on the values of a parameter 
    def __init__(self, param : Labels = Labels.NoLabel, condition : LabelCondition = LabelCondition.NoCondition, value = None):
        super().__init__(param, condition, value)

    @classmethod
    def from_Json(cls, _input):
        obj = super().from_Json(_input)
        if not isinstance(obj.condition, LabelCondition):
            obj.condition = LabelCondition[obj.condition]
        if not isinstance(obj.param, Labels):
            obj.param = Labels[obj.param]
        return obj
    
    def analyse(self, _input):
        if isinstance(_input, dict):
            try:
                _input = pd.DataFrame(_input)
            except:
                pass
        if not isinstance(_input, pd.DataFrame):
            return
        return labelDataframe(_input, self.param, self.condition, self.value)



class Queryset(Query):      # a parent class for organising multiple nested queries. Does not have solver methods on its own. 

    def __init__(self,  queries : List[Query] = None , querysets = None):
        __queries = list(set(ensure_list(queries)))
        __querysets = list(set(ensure_list(querysets)))
        super().__init__(param=list(set(r.param for r in __queries)), queries=__queries, querysets=__querysets)


#####################   Recursive methods #####################

    def as_Dict(self):
        output = super().as_Dict()

        if len(self.queries) > 0:
            output["queries"] = [r.as_Dict() for r in self.queries]
        
        if len(self.querysets) > 0:
            output["querysets"] = [queryset.as_Dict() for queryset in self.querysets]

        return output
    
    
    def as_Json_value(self):
        def recursiveEnumToString(queryset : dict):
            if len(queryset["querysets"]) > 0:
                queryset["querysets"] = [recursiveEnumToString(subset) for subset in queryset["querysets"]]
            if len(output["queries"]) > 0:
                queryset["queries"] = [dictEnumValuesToString(rule) for rule in queryset["queries"]]
            dictEnumValuesToString(queryset)
            return queryset
            
        output = self.as_Dict()
        recursiveEnumToString(output)
        return dictEnumValuesToString(output)

    
    def as_Json(self):

        return json.dumps(self.as_Json_value())

    @classmethod
    def from_Dict(cls, input):

        _class = getattr(sys.modules[__name__], input["name"])()

        prop = list(_class.as_Dict().keys())

        check_reqs(input, prop )
         
        __querysets = None
        if "querysets" in input:
            for q in input["querysets"]:
                _qclass = getattr(sys.modules[__name__], q["name"])()
                __querysets = [_qclass.from_Dict(queryset) for queryset in input["querysets"]]


        __queries = None
        for q in input["queries"]:
            _qclass = getattr(sys.modules[__name__], q["name"])()
            __queries = [_qclass.from_Dict(query) for query in input["queries"]]


        obj = cls(queries = __queries, querysets = __querysets)

        for key, value in input.items():
            if key in prop:
                continue
            setattr(obj, key, value)

        return obj
    
    @classmethod
    def from_Json(cls, input):
        error = "input is not a valid json"

        try:
            input = json.loads(str(input))
        except Exception as err:
            print(error + str(err.args))
            return   

        return cls.from_Dict(input)
    
######################################## Set Methods #######################################################

    def Analyse(self, _data, inplace = False):
        """main method to analyse a Filterset or Labelset. """
        _input = _data
        _query = self

        def analyseFilterSet(qset):
        
            def analyseDataFrame():
                _output = None

                def checkout(exit = False, msg = ""):
                    if msg != "" : print(msg)

                    if _output is None or exit is True:
                        """returning an empty dataframe"""
                        return _input.head(0)
                    return _output

                def aggregate(l : List[pd.DataFrame]):
                    if len(l) == 0:
                        return checkout(True, msg = "aggregate list has length 0")

                    match qset.condition:
                        case Operators.Any:
                            MergeHow = "outer"
                            if all(len(x) == 0 for x in l ):
                                return checkout(True, msg = "outer join has length 0")                        
                        case Operators.All:
                            MergeHow = "inner"
                            if any(len(x) == 0 for x in l ):
                                return checkout(True, msg = "inner join has length 0")

                    out = l[0]
                    for d in l[1:]:
                        out = out.merge(d, how = MergeHow) 
                        
                    if len(out) == 0 and qset.condition is Operators.All:
                        return checkout(True, msg = "inner join has length 0")
                    return out

                
                # recursive logic for nested filtersets:
                
                if len(qset.querysets) > 0:
                    nestedResults = [] 
                    
                    for _qset in qset.querysets:
                        result = analyseFilterSet(_qset)
                        nestedResults.append(result)
                        
                    _output = aggregate(nestedResults)

                    # solve aggregate results
                    if qset.condition == Operators.All and _output is None:
                        return checkout(True, msg = "output is empty")

                # check queries
                queriesResults = [] if _output is None else [_output]

                if len(qset.queries) >0:
                    for q in qset.queries:
                        result = q.analyse(_input, inplace)           # here the filtering happens
                        queriesResults.append(result)
                
                _output = aggregate(queriesResults)

                # returns a dataframe
                result = checkout()
                return result



            def analyseSingleValue():
                _output = None

                # helper functions

                def checkout(exit = False, msg = ""):
                    if msg != "" : print(msg)

                    if _output is None or exit is True:
                        """returning false"""
                        return False
                    return _output

                def aggregate(l : List[bool]):
                    if len(l) == 0:
                        return checkout(True, msg = "aggregate list has length 0")

                    match qset.condition:
                        case Operators.Any:
                            if True not in l:
                                return checkout(True, msg = "all values are False")                        
                        case Operators.All:
                            if False in l:
                                return checkout(True, msg = "some values are False")

                # recursive logic

                if len(qset.querysets) > 0:
                    nestedResults = [] 
                    
                    for _qset in qset.querysets:
                        result = analyseFilterSet(_qset)
                        nestedResults.append(result)
                        
                    _output = aggregate(nestedResults)

                # solve aggregate results
                    if qset.condition == Operators.All and _output is None:
                        return checkout(True, msg = "output is empty")            
                

                # check rules

                queriesResults = [] if _output is None else [_output]

                if len(qset.queries) >0:
                    for q in qset.queries:
                        result = q.analyse(_input)
                        queriesResults.append(result)
                
                _output = aggregate(queriesResults)

                # aggregate


                # return True or False
                result = checkout()
                return result


            match _input:
                case pd.DataFrame():
                    result = analyseDataFrame()
                    return result
                case _:
                    return analyseSingleValue()
            

        def analyseLabelSet(labelby = None, execute = True):

            # nested labelset are mostly important for the order of operations. In the end a single operation should be performed on the "topmost" labelset

            # recursive logic
            if not execute:
                return labelby
            
            # group by
            results = "some smart thing"

            # return groups 
            return results

        match _query:
            case Labelset():
                return analyseLabelSet(_query)
            case Filterset():
                print("analyse Filterset")
                result = analyseFilterSet(_query)
                print("result of analysis")
                print(result)
                return result

    



class Filterset(Queryset):       # a ruleset for performing nested filter operations, using an Operator (and, or, ...) to aggregate results

    def __init__(self, condition : Operators = Operators.All, queries : List[Filter] = None, querysets = None):       
        test = []

        __condition = condition

        if queries is not None:
            test = queries if isinstance(queries, list) else [queries]
            if not all(isinstance(r, Filter) for r in test):
                pass
 #               raise "input queries are not Filters"
        
        if querysets is not None:
            test = [_ruleset.queries for _ruleset in querysets] if isinstance(querysets, list) else querysets.queries
            if not all(isinstance(r, Filter) for r in test):
                pass
                # raise "input queries within querysets are not all Filters"

        super().__init__(queries, querysets)
        self.condition = __condition

  
    @classmethod
    def from_Dict(cls, input):
        obj = super().from_Dict(input)
        obj.condition = input["condition"]
        return obj
    
    @classmethod
    def from_Json(cls, input):
    
        error = "input is not a valid json"

        try:
            input = json.loads(str(input))
        except Exception as err:
            print(error + str(err.args))
            return  
        
        def fsetToEnum(input):
            input["condition"] = Operators[input["condition"]]

            if "queryset" in input and input["querysets"] is not []:
                for qset in input["querysets"]:
                    qset = fsetToEnum(qset)

            if "queries" in input and input["queries"] is not []:
                for q in input["queries"]:
                    q["condition"] = FilterCondition[q["condition"]]
            
            return input
        
        """ convert strings to enums """        
        input = fsetToEnum(input)


        return cls.from_Dict(input)


class Labelset(Queryset):
    def __init__(self, queries : List[Label] = None, querysets = None):
        test = []

        if queries is not None:
            test = queries if isinstance(queries, list) else [queries]
            if not all(isinstance(r, Label) for r in test):
                pass

        
        if querysets is not None:
            test = [_ruleset.queries for _ruleset in querysets] if isinstance(querysets, list) else querysets.queries
            if not all(isinstance(r, Label) for r in test):
                pass

            
        super().__init__(queries, querysets)

    @classmethod
    def from_Dict(cls, input):
        obj = super().from_Dict(input)
        obj.param = input["param"]
        obj.condition = input["condition"]
        return obj
    


    @classmethod
    def from_Json(cls, input):
    
        error = "input is not a valid json"

        try:
            input = json.loads(str(input))
        except Exception as err:
            print(error + str(err.args))
            return  
        
        def fsetToEnum(input):


            if "queryset" in input and input["querysets"] is not []:
                for qset in input["querysets"]:
                    qset = fsetToEnum(qset)

            if "queries" in input and input["queries"] is not []:
                for q in input["queries"]:
                    q["param"] = Labels[q["param"]]
                    q["condition"] = LabelCondition[q["condition"]]
            
            return input
        
        """ convert strings to enums """        
        input = fsetToEnum(input)


        return cls.from_Dict(input)






######################################## Rules and Rulebook #################################################

class Rule:
    """a rule contains a single Queryset (including Labelsets or Filtersets), optional categories, and a target group """
    def __init__(self, query, categories : ModelCats.ModelCategories = ModelCats.ModelCategories.NoCategory, group = None):
        self.name = str(self.__class__.__name__)
        self.query = query
        self.categories = categories
        self.group = group if group is not None else None
        self.type = str(self.query.__class__.__name__)

    def __iter__(self):             
        for key in self.__dict__:
            yield key, getattr(self, key)

#    def __repr__(self):
#        return "this {!r} checks the ruleset {!r} for the category {!r} and assigns results to the group {!r}".format(self.name, self.ruleset, self.categories, self.group)
    
    def __hash__(self):
        return hash(self.__iter__())
    
    def __eq__(self, other):
        return tuple(self) == tuple(other)
               
    def serializable_attr(self):
        return (dict((i.replace(self.__class__.__name__, "").lstrip("_"), value) for i, value in self.__dict__.items()))
    
    def as_Dict(self):
        return dict(self)

    def as_Json_value(self):       
        output = self.as_Dict()
        for key, value in output.items():
            if isinstance(value, Query):
                output[key] = value.as_Json_value()
        return dictEnumValuesToString(output)


    def as_Json(self):
        return json.dumps(self.as_Json_value())
    

    @classmethod
    def from_Dict(cls, input):    
        """
        method to create a rule from a dictionary string
        input = the dictionary. must contain a "query", a "categories", and a "group" key.
        """

        prop = list(Rule(Queryset()).as_Dict().keys())
                
        check_reqs(input, prop)                
        obj = cls(input["query"])

        for key, value in input.items():
            if key == "name":
                continue
            setattr(obj, key, value)

        return obj


    @classmethod
    def from_Json(cls, input):
        """
        method to create a rule from a json string
        input = the json string. must contain a "query", a "categories", and a "group" key.
        """
        error = "input is not a valid json"

        try:
            input = json.loads(str(input))
        except Exception as err:
            print(error + str(err.args))
            return 
        

        obj = cls.from_Dict(input)
        
        if not isinstance(obj.categories, ModelCats.ModelCategories):
            _cats = obj.categories if isinstance(obj.categories, list) else [obj.categories]
            _enums = None
            for c in _cats:                
                _enums = ModelCats.ModelCategories[c] if _enums == None else _enums | ModelCats.ModelCategories[c]
            obj.categories = _enums

        
        if not isinstance(obj.query, Queryset):
            _query = json.dumps(input["query"])
            match obj.query["name"]:
                case "Filterset":
                    obj.query = Filterset().from_Json(_query)
                case "Labelset":
                    obj.query = Labelset().from_Json(_query)
                case "Queryset":
                    obj.query = Queryset().from_Json(_query)
                case _:
                    pass
            

        return obj
    

class RuleConnection:
    """a class that describe a relationship between two Rules. This would be an Edge in a Graph."""
    def __init__(self, source : Rule, target : Rule):
        self.source = source
        self.target = target

    @classmethod
    def from_rules_and_index(cls, rules, source : int = 0, target : int = 1):       
        return cls(rules[int(source)], rules[int(target)])


        


class Rulebook:
    """a class that contains several rules and their relationships to each other"""
    def __init__(self, rules : List[Rule] = None, connections : List[RuleConnection] = None, inplace = False):
        self.rules = ensure_list(rules) if rules is not None else []
        self.connections = ensure_list(connections) if connections is not None else []
        self.inplace = inplace

    @property
    def graph(self):
        if self.connections is []:
            return {}
        __graph = {}
        for r in self.rules:
            __graph[self.rules.index(r)] = []
        for c in self.connections:
            if self.rules.index(c.source) in __graph:
                __graph[self.rules.index(c.source)].append(self.rules.index(c.target))
        return __graph

    def __iter__(self):             
        for key in self.__dict__:
            yield key, getattr(self, key)

#    def __repr__(self):
#        return "this {!r} checks the parameter {!r} for the condition {!r} against the value {!r}".format(self.name, self.param, self.condition, self.value)
    
    def __hash__(self):
        return hash(self.__iter__())
    
    def __eq__(self, other):
        return tuple(self) == tuple(other)

    def as_Dict(self):
        return dict(self)
    
    def serializable_attr(self):
        return (dict((i.replace(self.__class__.__name__, "").lstrip("_"), value) for i, value in self.__dict__.items()))

    def as_Json_value(self):
        output = {}
        output["rules"] = [rule.as_Json_value() for rule in self.rules]
        output["graph"] = self.graph

        return output

    def as_Json(self):       
        return json.dumps(self.as_Json_value())

    @classmethod
    def from_Dict(cls, input):
        """
        method to create a rule from a dictionary string
        input = the dictionary. must contain a "rules" key.
        """

        prop = list(Rulebook().as_Dict().keys())
                
        check_reqs(input, prop)                
        obj = cls(input["rules"])

        for key, value in input.items():
            if key == "name":
                continue
            setattr(obj, key, value)

        return obj
    
    @classmethod
    def from_Json(cls, input):
        """
        method to create a rule from a json string
        input = the json string. must contain a "query", a "categories", and a "group" key.
        """
        error = "input is not a valid json"

        try:
            input = json.loads(str(input))
        except Exception as err:
            print(error + str(err.args))
            return 

        obj = cls()

        for r in input["rules"]:
            obj.rules.append(Rule.from_Json(json.dumps(r)))

        for source, targets in input["graph"].items():
            if targets == []:
                continue
            for target in targets:
                obj.connections.append(RuleConnection.from_rules_and_index(obj.rules, source, target))

        return obj        

