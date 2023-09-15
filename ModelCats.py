from enum import Flag, Enum, auto

######## Variables and Constants #####################################

class ModelCategories(Flag):
    NoCategory = 0
    """container categories"""
    Building = auto()
    Level = auto()
    Layer = auto()
    Project = auto()
    Location = auto()
    System = auto()
    Zone = auto()
    Group = auto()

    """site categories"""
    Topography = auto()
    Site = auto()
    Planting = auto()

    """main architectural categories"""
    Beam = auto()
    Part = auto()
    Column = auto()
    Ceiling = auto()
    Covering = auto()
    CurtainWall = auto()
    Panel = auto()
    Mullion = auto()
    Door = auto()
    Furniture = auto()
    FurnitureSystem = auto()
    GenericObject = auto()
    Opening = auto()
    Railing = auto()
    Ramp = auto()
    Roof = auto()
    Room = auto()
    ShadingDevice = auto()
    Slab = auto()
    Footing = auto()
    Stair = auto()
    Wall = auto()
    Window = auto()

    """mep components"""
    LightFixture = auto()
    PlumbingFixture = auto()
    TransportElement = auto()
    Duct = auto()
    Terminal = auto()


class RevitCategories(Enum):
    NoCategory = auto()
    Topography = "Topography"
    Fascias = "Fascias"
    RoofSoffits = "Roof Sottifts"
    MechanicalEquipment = "Mechanical Equipment"
    Furniture = "Furniture"
    StructuralFraming = "Structural Framing"
    Casework = "Casework"
    PlumbingFixtures = "Plumbing Fixtures"
    SlabEdges = "Slab Edges"
    TopRails = "Top Rails"
    Supports = "Supports"
    Handrails = "Handrails"
    Landings = "Landings"
    Runs = "Runs"
    Stairs = "Stairs"
    GenericModels = "Generic Models"
    Windows = "Windows"
    Railings = "Railings"
    ElectricalEquipment = "Electrical Equipment"
    SpecialtyEquipment = "Specialty Equipment"
    Ceilings = "Ceilings"
    WallSweeps = "Wall Sweeps"
    Site = "Site"
    Planting = "Planting"
    Roofs = "Roofs"
    Doors = "Doors"
    CurtainWallMullions = "Curtain Wall Mullions"
    BasicWall = "Basic Wall"
    CurtainPanels = "Curtain Panels"
    Floors = "Floors"
    StructuralFoundations = "Structural Foundations"
    Rooms = "Rooms"
    StructuralColumns = "Structural Columns"
    Walls = "Walls"


class IfcCategories(Enum):
    IfcWall = auto()


