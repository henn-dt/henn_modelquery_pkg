from ModelQuery import *
from ModelCats import *
from ModelCalc import *

######################################## Rules and Rulebook #################################################

class Rule:
    """a rule contains a single Queryset (including Labelsets or Filtersets), optional categories, and a target calculation group """
    def __init__(self, query : Queryset, categories : ModelCategories = ModelCategories.NoCategory, group : CalcGroup = None):
        self.name = str(self.__class__.__name__)
        self.__query = query
        self.categories = categories
        self.group = group if group is not None else None

    @property
    def query(self):
        if isinstance(self.__query, (list, tuple)):
            return self.__query[0]
        return self.__query
    
    @query.setter
    def query(self, value : Queryset):
        self.__query = value

    @property
    def type(self):
        return str(self.query.__class__.__name__)
    
    @property
    def param(self):
        return self.query.param

    def __iter__(self):             
        for key in self.__dict__:
            yield key, getattr(self, key)
   
#    def __hash__(self):
#        return hash(self.__iter__())
    
    def __eq__(self, other):
        return tuple(self) == tuple(other)
    
    def as_Dict(self):
        return {'name' : self.name, 'query' : self.query, 'categories' : self.categories, 'group' : self.group}

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
        obj = cls(input["query"], input["categories"], input["group"])

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
        
        if not isinstance(obj.categories, ModelCategories):
            _cats = obj.categories if isinstance(obj.categories, list) else [obj.categories]
            _enums = None
            for c in _cats:                
                _enums = ModelCategories[c] if _enums == None else _enums | ModelCategories[c]
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
    

    def Analyse(self, input : pd.DataFrame):
        """
        base method to analyse a rule and all its underlying qsets.
        returns a Ledger(Dataframe) of elements.
        if a CalcGroup is defined, enriches the LedgerItems in the Ledger with the calculated value.
        """
        output = pd.DataFrame()

        # filters out categories
        if not self.categories == ModelCategories.NoCategory:
            pass

        # analyses according to qset
        if self.type == "Queryset" :
            pass
        else:
            output = self.query.Analyse(input)

        # runs the calculation
        if self.group is not None:
            output = self.group.Calculate(output)

        return output

    

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
    def __init__(self, rules : List[Rule] = None, connections : List[RuleConnection] = None, inplace = True):
        self.rules = ensure_list(rules) if rules is not None else []
        self.connections = ensure_list(connections) if connections is not None else []
        self.inplace = inplace
        """
        rules are checked in a given order, following the connections. 
        if inplace is True, checked elements will be assigned to all groups for which they trigger a Query.
        if inplace is False, every checked rule will generate a new Ledger (DataFrame) which includes only the elements not yet assigned to a Group.
        Within a connection branch, elements that are deeper down the branch will be prioritised when assigning Groups to elements. 
        """

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
    
    @property
    def param(self):
        if not len(self.rules) > 0:
            return list()
        params = list()
        for rule in self.rules:
            params += rule.param
        return list(set(tuple(params)))

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

