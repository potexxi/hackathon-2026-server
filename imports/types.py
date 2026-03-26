from enum import Enum


class OSMColumns(Enum):
    ID = 'id'
    NAME = 'name'
    TYP = 'wasser_typ'
    TRINKBAR = 'ist_trinkwasser'
    LAT = 'lat'
    LON = 'lon'
    GEOM = 'geom'
    TAGS = 'tags'
    DIST = 'distanz_meter'

    @classmethod
    def ALL(cls):
        return [c.value for c in cls]
