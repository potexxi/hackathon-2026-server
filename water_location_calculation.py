import duckdb
import pandas as pd
from imports.types import OSMColumns

# API: https://www.openstreetmap.org

con = duckdb.connect(":memory:")
con.execute("INSTALL spatial; LOAD spatial;")

wasser_filter = """
    (tags->'amenity' IN ('drinking_water', 'water_point', 'fountain', 'watering_place')) OR
    (tags->'natural' IN ('spring')) OR
    (tags->'man_made' IN ('water_well', 'water_tap', 'pump'))
"""



class OSMWaterManager:
    """Class for managing the water sources"""
    def __init__(self, db_path: str = "./austria.db"):
        """
        :param
        db_path: path to converted database
        """
        self.db_path = db_path
        self.con = None

    def connect(self):
        """Connect to the database"""
        self.con = duckdb.connect(self.db_path)
        self.con.execute("INSTALL spatial; LOAD spatial;")

    def create_from_pbf(self, pbf_path, force: bool = False):
        pass

def zeige_erste_wasserquellen(pbf_path: str, limit: int = 500) -> pd.DataFrame:
    query = f"""
        SELECT 
            id,
            lat,
            lon,
            COALESCE(CAST(tags['name'] AS VARCHAR), 'Unbekannte Quelle') as name,
            CASE 
                WHEN CAST(tags['amenity'] AS VARCHAR) = 'drinking_water' THEN 'Trinkwasserbrunnen'
                WHEN CAST(tags['natural'] AS VARCHAR) = 'spring'         THEN 'Quelle'
                WHEN CAST(tags['amenity'] AS VARCHAR) = 'water_point'    THEN 'Wasserentnahmestelle'
                WHEN CAST(tags['man_made'] AS VARCHAR) = 'water_well'    THEN 'Brunnen'
                WHEN CAST(tags['amenity'] AS VARCHAR) = 'fountain'       THEN 'Brunnen / Fontäne'
                WHEN CAST(tags['amenity'] AS VARCHAR) = 'watering_place' THEN 'Tränke / Wanne'
                WHEN CAST(tags['man_made'] AS VARCHAR) = 'water_tap'     THEN 'Wasserhahn'
                WHEN CAST(tags['man_made'] AS VARCHAR) = 'pump'          THEN 'Pumpe'
                ELSE 'Sonstige Wasserquelle'
            END as wasser_typ,
            CAST(tags['drinking_water'] AS VARCHAR) as ist_trinkwasser,
            tags
        FROM ST_ReadOSM('{pbf_path}')
        WHERE 
            CAST(tags['amenity'] AS VARCHAR) IN ('drinking_water', 'water_point', 'fountain', 'watering_place') OR
            CAST(tags['natural'] AS VARCHAR) IN ('spring') OR
            CAST(tags['man_made'] AS VARCHAR) IN ('water_well', 'water_tap', 'pump')
        LIMIT {limit}
    """

    df = con.execute(query).df()

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)

    print(df[OSMColumns.LAT])
    return df


# Ausführen
zeige_erste_wasserquellen("./austria-260325.osm.pbf")
input()
