
import pandas as pd
from enum import Flag, auto
import json

import ModelCats

"""module for utility methods for Autodesk Platform Services"""


######################## HELPERS ###############################

def unpivot_viewable_tree(viewable_tree):
    """
    inputs a read, loaded viewable tree, outputs a list of dictionaries with these keys:
    {  objectid:
       object:
       categoryid:
       category:
       familyid:
       family:
       typeid:
       type:
    }
    """

    dataframe = pd.json_normalize(
                                    viewable_tree, 
                                    ["data", "objects", "objects", "objects"],  
                                    [["model" , "objects", "objects", "name"], 
                                    ["model" , "objects", "objects", "objectid"]]
                                   )

    dataframe = dataframe.reset_index()

    def unpivotDataframe(dataframe):
        out = []

        for index, row in dataframe.iterrows():
            _categoryid = row["model.objects.objects.objectid"]
            _category = row["model.objects.objects.name"]
            _familyid = row["objectid"]
            _family = row["name"]

            try:        
                for types in row["objects"]:
                    _typeid = types["objectid"]
                    _type = types["name"]
                    for elements in types["objects"]:
                        newElement = {}
                        newElement["objectid"] = elements["objectid"]
                        newElement["object"] = elements["name"]
                        newElement["categoryid"] = _categoryid
                        newElement["categoryname"] = _category
                        try:
                            newElement["category"] = ModelCats.RevitCategories[_category]
                        except:
                            newElement["category"] = ModelCats.RevitCategories.NoCategory
                        newElement["familyid"] = _familyid
                        newElement["family"] = _family
                        newElement["typeid"] = _typeid
                        newElement["type"] = _type
                        out.append(newElement)
            except:
                newElement = {}
                newElement["objectid"] = row["objectid"]
                newElement["object"] = row["name"]
                newElement["categoryid"] = _categoryid
                newElement["categoryname"] = _category
                try:
                    newElement["category"] = ModelCats.RevitCategories[_category]
                except:
                    newElement["category"] = ModelCats.RevitCategories.NoCategory
                newElement["familyid"] = None
                newElement["family"] = None                
                newElement["typeid"] = None
                newElement["type"] = None
                out.append(newElement)
                continue

        return out
    
    return unpivotDataframe(dataframe)

def FlattenDictionary(nestedDict, flatDict = {}):
    group = ""
    for key, value in nestedDict.items():
        if not isinstance(value, dict):
            if flatDict.get(key):
                flatDict["_".join([group, key])] = value    # small hack to deal with parameter with repeated names
            flatDict[key] = value
        else:
            group = key
            FlattenDictionary(value, flatDict)
    return flatDict

#################################  END OF HELPERS ################################


#################################   SERIALIZER ####################################

def SerializeCategories(element):
    try:
        category = element["category"]
    except:
        return
    if type(category) is not ModelCats.RevitCategories:
        return
    match category:
        case ModelCats.RevitCategories.NoCategory:
            return ModelCats.ModelCategories.NoCategory

        case ModelCats.RevitCategories.BasicWall:
            return ModelCats.ModelCategories.Wall

        case ModelCats.RevitCategories.Doors:
            return ModelCats.ModelCategories.Door

        case ModelCats.RevitCategories.Topography:
            return ModelCats.ModelCategories.Topography

        case ModelCats.RevitCategories.Fascias | \
                ModelCats.RevitCategories.RoofSoffits |  \
                ModelCats.RevitCategories.Roofs:
            return ModelCats.ModelCategories.Roof

        case ModelCats.RevitCategories.MechanicalEquipment | \
            ModelCats.RevitCategories.GenericModels:
            opening_strings = ["sud", "s+d", "bauangaben", "ausspar",
                                 "schlitz", "durchbruch", "opening", "void",    
                                 "wd", "dd", "ws", "bs", "bd"]
            if any(x in element["family"].lower() for x in opening_strings) or \
                any(x in element["type"].lower() for x in opening_strings):
                return ModelCats.ModelCategories.Opening
            return ModelCats.ModelCategories.GenericObject

        case ModelCats.RevitCategories.Furniture | \
            ModelCats.RevitCategories.Casework:
            return ModelCats.ModelCategories.Furniture

        case ModelCats.RevitCategories.StructuralFraming:
            return ModelCats.ModelCategories.Beam

        case ModelCats.RevitCategories.Ceilings:
            return ModelCats.ModelCategories.Ceiling

        case ModelCats.RevitCategories.PlumbingFixtures:
            return ModelCats.ModelCategories.PlumbingFixture

        case ModelCats.RevitCategories.SlabEdges | \
            ModelCats.RevitCategories.StructuralFoundations:
            return ModelCats.ModelCategories.Slab

        case ModelCats.RevitCategories.Floors:
            finish_strings = ["fb", "_b" ]
            structure_strings = ["stb", "_de"]
            if any(x in element["type"].lower() for x in finish_strings) and \
                all(x not in element["type"] for x in structure_strings):  
                return ModelCats.ModelCategories.Covering
            return ModelCats.ModelCategories.Slab

        case ModelCats.RevitCategories.Railings | \
                ModelCats.RevitCategories.TopRails | \
                    ModelCats.RevitCategories.Handrails :
            return ModelCats.ModelCategories.Railing

        case ModelCats.RevitCategories.Stairs | \
                    ModelCats.RevitCategories.Landings | \
                        ModelCats.RevitCategories.Supports:
            return ModelCats.ModelCategories.Stair

        case _:
            return ModelCats.ModelCategories.NoCategory


def Serialize_fromModelDerivative( objects_tree, viewable_tree):
    """ inputs a json object tree and viewable tree as queries from APS,
        outputs a list of dictionaries with these properties:
        {   
            objectid:
            object:
            categoryid:
            category:
            familyid:
            family:
            typeid:
            type:
            externalId:           ### note the capital I ---- keeping with APS nomenclature here
            name:
            properties [ {property_name : property_value}, ... ] 
            modelcategory :
        } 
    """


#    objects = json.loads(objects_tree)
#    collection = objects["data"].get("collection")

#    objects = json.loads(viewable_tree)
#    viewtree = unpivot_viewable_tree(objects)

    collection = objects_tree["data"].get("collection")
    viewtree = unpivot_viewable_tree(viewable_tree)

    out = []

    for element in collection:

        vt_element = next (( e for e in viewtree \
                    if e["objectid"] == element["objectid"]),
                    None)     # single element from unpivoted viewable tree

        serialized = None
        if vt_element:
            serialized = vt_element
            serialized["properties"] = FlattenDictionary(element["properties"])
            serialized["externalId"] = element["externalId"]
            serialized["name"] = element["name"]
            serialized["app_category"] = serialized["category"]
            serialized["category"] = SerializeCategories(serialized)
            serialized["element"] = serialized
            """dumps the whole element as a property of the element itself. 
            allows for quick retrieval of the original element after disposing of the
            dictionary / transforming it to DataFrame"""
            out.append(serialized)
    
    return out