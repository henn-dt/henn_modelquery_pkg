# HENN Model Process
This package contains methods for common operations performed on building models:

ModelCats
- conversion of format-specific categories to a common pool of categories
(e.g. from IfcWall and Revit Basic Wall to a common "Wall" Category.)

ModelQuery
- define common query procedures for filtering and classifying elements based on their properties,
as well as how to organize queries into systems to fully break down the input models

ModelCalc
- common math operations performed on building elements. 
includes methods to deal with common format transformations - from area to volume based on thickness,
from volume to weight based on specific weights, etc. 


To use the package, add this line to the requirements.txt of your app:

    git+{url of the repo}.git

Then import henn_modelquery_pkg in your python files. 