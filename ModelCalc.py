import pandas as pd
import numpy as np
from enum import Enum, auto

defaultQuantities = {
    "count" : 1, 
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

class Quantities:
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



