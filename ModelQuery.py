import sys
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
    """properties common to almost any model. Each element to be filtered should have these properties - be enriched with "NaN" if needed"""
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
    Will group elements that match any of the criteria, and produce one spillover group for "unmatched" elements
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

def addLabelColumnToDataFrame(input : pd.DataFrame, label : Labels, condition:LabelCondition , value = None):

    label = label if isinstance(label, list) else [label]
    labelby = [l.name if isinstance(l, Enum) else l for l in label]

    match(LabelCondition(condition)):
        case LabelCondition.NoCondition:
            pass
        case LabelCondition.ByValue:   
            pass
        case LabelCondition.ByStartingCharacter:
            _labelby = []
            for l in labelby:
                _label = "label_{0}".format(l)
                _labelby.append(_label)
                i = value - 1 if isinstance(value, int) else 0
                i = i if i >= 0 else 0
                input[_label] = input.l.str[0:i]
            labelby = _labelby
        
        case LabelCondition.ByEndingCharacter:
            _labelby = []
            for l in labelby:
                _label = "label_{0}".format(l)
                _labelby.append(_label)                
                i = value - 1 if isinstance(value, int) else 0
                i = i if i >= 0 else 0
                string = input.l.str
                input[_label] = string[len(string)- i - 1 : len(string)]
            labelby = _labelby
            
    return input , labelby
         
def labelDataframe(input : pd.DataFrame, label : Labels, condition:LabelCondition , value = None):

    input, labelby = addLabelColumnToDataFrame(input, label, condition, value)

    return dict(tuple(input.groupby(labelby,  sort=False, dropna=False)))

################################ END OF BASE QUERY FUNCTIONS ####################################################

class Query:         # a parent class for performing different kind of simple queries

    def __init__(self, param = None, condition = None, value = None, **kwargs):
        self.name = str(self.__class__.__name__)
        self.param = param if param is not None else None
        self.condition = condition if condition is not None else None
        self.value = value if value is not None else None
        self.__dict__.update(**kwargs)
    
    @property
    def param(self):
        return self._param
    
    @param.setter
    def param(self, value):
        self._param = value
        
    
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
        return {'name' : self.name, 'param' : self.param, 'condition' : self.condition, 'value' : self.value}
    
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

    def __init__(self,  queries : List[Query] = [] , querysets = []):

        super().__init__()

        self.queries = list(set(ensure_list(queries)))
        self.querysets = list(set(ensure_list(querysets)))

    @Query.param.getter
    def param(self):
        params = [q.param for q in self.queries] if self.queries is not [] else []
        params += [q.param for q in self.querysets] if self.querysets is not [] else []
        output = []
        [output.append(p) for p in params if p not in output]
        return output

#####################   Recursive methods #####################

    def as_Dict(self):
        output = super().as_Dict()
        
        output["queries"] = [r.as_Dict() for r in self.queries] if len(self.queries) > 0 else []
        output["querysets"] = [queryset.as_Dict() for queryset in self.querysets] if len(self.querysets) > 0 else []

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
    
######################## Analyse Set Methods ##########################
######################## Recursion Ahoy ###############################

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
            

        def analyseLabelSet(_input, qset, labelby = None, execute = True):
            # if nested labelset all share the same condition, they are then only important for the order of operations.
            # in this case the recursive process should just collect the params and add them to the params of the topmost label and
            # perform a single groupby. 
            # if the nested labels have different conditions, then the nested labels need to add sorting columns to the dataset.  
            # 
            # nested labelset are mostly important for the order of operations. 
            # In the end a single groupby operation should be performed on the "topmost" labelset. By adding labelling columns the topmost
            # operation is a simple "groupby" by a list of columns. 

#            print("running analyse on labelset set to {0}".format(execute))
            labelby = [] if labelby is None else labelby


            # recursive logic
            if len(qset.querysets) > 0:
                for q in qset.querysets:
                    _input, labelby = analyseLabelSet(_input, q, labelby, False)
                
            
            # enriches the dataframe and defines the new labelby

            if len(qset.queries) > 0:
                for q in qset.queries:
                    _input, _labelby = addLabelColumnToDataFrame(_input, q.param, q.condition, q.value)
 #                   print("input is {0} and labelby is {1}".format(_input, _labelby))

                    labelby += _labelby



#                    print("list before uniq is {0}".format(labelby))


            # return labelby and updated dataset 
            if not execute:
                return _input, labelby
            
#            print("label by {0}".format(labelby))
            
            # group by
            result = labelDataframe(_input, labelby, LabelCondition.ByValue)

            # return results
            return result

        match _query:
            case Labelset():
                print("analyse Labelset")
                result = analyseLabelSet(_input, _query)



            case Filterset():
                print("analyse Filterset")
                result = analyseFilterSet(_query)


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