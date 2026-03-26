import duckdb
import pandas as pd
from imports.types import OSMColumns
import os

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

    def _connect(self):
        """Connect to the database"""
        self.con = duckdb.connect(self.db_path)
        self.con.execute("INSTALL spatial; LOAD spatial;")

    def create_from_pbf(self, pbf_path: str, force: bool = False):
        """Erstellt die optimierte .db Datei mit deinem spezifischen Wasser-Filter."""
        if os.path.exists(self.db_path) and not force:
            print(f"--- Info: {self.db_path} existiert bereits. Überspringe Import. ---")
            return

        print(f"--- Starte Import von {pbf_path} ---")
        # Verbindung für den Import öffnen
        conn = duckdb.connect(self.db_path)
        conn.execute("INSTALL spatial; LOAD spatial;")

        # Wir nutzen deine CASE-Logik und Spaltennamen aus OSMColumns
        # Wichtig: ST_Point(lon, lat) wird direkt für den Index erstellt
        # In deiner create_from_pbf Methode:
        create_query = f"""
            CREATE OR REPLACE TABLE wasserstellen AS 
            SELECT 
                id AS {OSMColumns.ID},
                lat AS {OSMColumns.LAT},
                lon AS {OSMColumns.LON},
                COALESCE(CAST(tags['name'] AS VARCHAR), 'Unbekannte Quelle') AS {OSMColumns.NAME},
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
                END AS {OSMColumns.TYP},
                CAST(tags['drinking_water'] AS VARCHAR) AS {OSMColumns.TRINKBAR},
                ST_Point(lon, lat) AS {OSMColumns.GEOM}
            FROM ST_ReadOSM('{pbf_path}')
            WHERE 
                CAST(tags['amenity'] AS VARCHAR) IN ('drinking_water', 'water_point', 'fountain', 'watering_place') OR
                CAST(tags['natural'] AS VARCHAR) IN ('spring') OR
                CAST(tags['man_made'] AS VARCHAR) IN ('water_well', 'water_tap', 'pump')
        """

        conn.execute(create_query)

        print("--- Erstelle räumlichen Index ---")
        conn.execute(f"CREATE INDEX IF NOT EXISTS spatial_idx ON wasserstellen USING RTREE ({OSMColumns.GEOM});")
        conn.close()
        print(f"--- Datenbank {self.db_path} erfolgreich erstellt! ---")

    def find_nearby(self, lat: float, lon: float, radius_m: int = 5000) -> pd.DataFrame:
        """Sucht im Umkreis und gibt ein DataFrame mit Distanz zurück."""
        self._connect()

        # Nutzt den R-Tree Index für maximale Geschwindigkeit
        # ST_Distance_Spheroid berechnet die Entfernung in Metern auf der Erdkugel
        # In deiner find_nearby Methode:
        search_query = f"""
            SELECT 
                {OSMColumns.ALL_String()},
                round(ST_Distance_Spheroid({OSMColumns.GEOM}, ST_Point({lon}, {lat}))) AS {OSMColumns.DIST}
            FROM wasserstellen
            WHERE ST_DWithin(
                ST_Transform({OSMColumns.GEOM}, 'EPSG:4326', 'EPSG:3857'), 
                ST_Transform(ST_Point({lon}, {lat}), 'EPSG:4326', 'EPSG:3857'), 
                {radius_m}
            )
            ORDER BY {OSMColumns.DIST} ASC
        """
        return self.con.execute(search_query).df()


manager = OSMWaterManager("wasser_austria.db")
manager.create_from_pbf("./austria-260325.osm.pbf")

# Suche z.B. in Wien (Stephansplatz)
ergebnisse = manager.find_nearby(lat=48.2084, lon=16.3731, radius_m=1000)

# Anzeige mit deinen OSMColumns (Autocompletion-Safe)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
print(ergebnisse[OSMColumns.ALL_LIST])
