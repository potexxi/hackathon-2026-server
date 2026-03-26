class OSMColumns:
    ID = 'id'
    NAME = 'name'
    TYP = 'wasser_typ'
    TRINKBAR = 'ist_trinkwasser'
    LAT = 'lat'
    LON = 'lon'
    GEOM = 'geom'
    DIST = 'distanz_meter'

    @classmethod
    def ALL_String(cls):
        return f"{cls.ID}, {cls.NAME}, {cls.TYP}, {cls.TRINKBAR}, {cls.LAT}, {cls.LON}, {cls.GEOM}"

    ALL_LIST = [ID, NAME, TYP, TRINKBAR, LAT, LON, GEOM, DIST]
