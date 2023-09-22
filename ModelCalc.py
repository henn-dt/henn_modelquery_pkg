import pandas as pd
import numpy as np
import math
import pint as pt
from enum import Enum, auto
from typing import List

#### CONSTANTS ####

class Operations(Enum):
    Sum = auto()
    Multiply = auto()

defaultConstants = {
    "count" : 1,
    "specific_weight" : 1,
    "thickness" : 1
 }

def calcCount(UnitCount = None, AmountPerUnit = None):
    _input = UnitCount if UnitCount is not None else 1
    _constant = AmountPerUnit if AmountPerUnit is not None else defaultConstants["count"]
    return _input * _constant

def calcVolume(UnitArea = None, UnitThickness = None):
    _input = UnitArea if UnitArea is not None else 1
    _constant = UnitThickness if UnitThickness is not None else defaultConstants["thickness"]
    return _input * _constant

def calcWeight(UnitVolume = None, SpecificWeight = None):
    _input = UnitVolume if UnitVolume is not None else 1
    _constant = SpecificWeight if SpecificWeight is not None else defaultConstants["specific_weight"]
    return _input * _constant



defaultQuantities = {
    "count" : np.NaN,
    "length" : np.NaN, 
    "width" : np.NaN, 
    "depth" : np.NaN, 
    "height" : np.NaN, 
    "thickness" : np.NaN, 
    "weight" : np.NaN,
    "area" : np.NaN, 
    "volume" : np.NaN,
    "angle" : np.NaN
    }

#### END OF CONSTANTS ####




class Quantities:
    "a class that contains both the values needed to perform a calculation (input) and the one expected as a result (output)"
    def __init__(self, **kwargs):
        self.name = str(self.__class__.__name__)
        self.__dict__.update(**kwargs)
        for key, value in defaultQuantities.items():
            if key in self.__dict__:
                continue
            setattr(self, key, value)

    def __iter__(self):             
        for key in self.__dict__:
            yield key, getattr(self, key)


class Calc(object):
    """
    a class that describes a calculation to be performed
    """
    def __init__(self, type : Operations, vars = None ):
        self.type = type
        self.vars = vars if isinstance(vars, list) else [vars]

    def Calculate(self, input):
        values = [getattr(input, v) for v in vars]
        match type:
            case Operations.Sum:
                return sum(values)
            case Operations.Multiply:
                return math.prod(values)

class CalcGroup(object):
    """
    a class that describes a set of calculations to be performed
    """
    def __init__(self, groups : List[str] = None, calcs : List[Calc] = None):             
        groups = tuple() if groups is None else groups
        calcs = Calc() if calcs is None else calcs                     # calcs can also be a list of lists of Calc, in case of multiple operations on the same group.
        self.group = {}
        for group, calc in zip(groups, calcs):
            self.group[group] = calc

    def Calculate(self, input):
        output = input
        return output



class LedgerItem(object):
    """
    a class that describes an item to be calculated and the results of the calculation
    """
    def __init__(self):
        pass

class Ledger(pd.DataFrame):
    """
    a dataframe class that holds all elements to be calculated.
    """
    def __init__(self) -> None:
        super().__init__()

    def add_properties(self, properties : list = None):
        pass




