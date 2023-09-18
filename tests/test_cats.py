import ModelCats as cats
from enum import Flag, Enum




def catnames(cats):
    return [name for name, member in cats.__class__.__members__.items() if member in cats]

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
        input[key] = enumToString(value)
    return input



mycats = cats.ModelCategories.Beam | cats.ModelCategories.Building

mycat = cats.ModelCategories.Column

revitcat = cats.RevitCategories.BasicWall

print (mycats.__class__)

#print(catnames(mycats))

print (enumToString(mycats))

print (enumToString(revitcat))