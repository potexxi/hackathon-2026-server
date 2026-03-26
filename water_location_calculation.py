import duckdb
import pandas as pd
import logging
from imports.types import OSMColumns
import os


def get_relativ_path(path: str):
    base_path = os.path.dirname(os.path.abspath(__file__))
    return os.path.normpath(os.path.join(base_path, path))


logger = logging.getLogger(__name__)


class OSMWaterManager:
    """Class for managing the water sources"""
    def __init__(self, db_path: str = "./austria.db"):
        """
        :param
        db_path: path to converted database
        """
        self.db_path = db_path
        self.con = None
        logger.info(f"OSMWaterManager initialisiert mit Datenbank: {db_path}")

    def _connect(self):
        """Connect to the database"""
        logger.debug(f"Verbinde mit Datenbank: {self.db_path}")
        self.con = duckdb.connect(self.db_path)
        self.con.execute("INSTALL spatial; LOAD spatial;")
        logger.info("Datenbankverbindung erfolgreich hergestellt")

    def create_from_pbf(self, pbf_path: str, force: bool = False):
        """Erstellt die optimierte .db Datei mit deinem spezifischen Wasser-Filter."""
        if os.path.exists(self.db_path) and not force:
            logger.info(f"{self.db_path} existiert bereits – Import übersprungen (force=False)")
            return

        logger.info(f"Starte Import von: {pbf_path}")
        conn = duckdb.connect(self.db_path)
        conn.execute("INSTALL spatial; LOAD spatial;")

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

        logger.info("Führe CREATE TABLE aus...")
        conn.execute(create_query)

        logger.info("Erstelle räumlichen R-Tree Index...")
        conn.execute(f"CREATE INDEX IF NOT EXISTS spatial_idx ON wasserstellen USING RTREE ({OSMColumns.GEOM});")
        conn.close()
        logger.info(f"Datenbank '{self.db_path}' erfolgreich erstellt")

    def find_nearby(self, lat: float, lon: float, radius_m: int = 5000, amount=20) -> pd.DataFrame:
        """Sucht im Umkreis und gibt ein DataFrame mit Distanz zurück."""
        logger.info(f"Suche Wasserstellen – lat={lat}, lon={lon}, radius={radius_m}m")
        self._connect()

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
            ORDER BY ST_Distance_Spheroid({OSMColumns.GEOM}, ST_Point({lon}, {lat})) ASC
            LIMIT 20
        """

        ergebnisse = self.con.execute(search_query).df()
        logger.info(f"{len(ergebnisse)} Wasserstellen gefunden")
        return ergebnisse

#  Potentieller ablauf
# manager = OSMWaterManager("wasser_austria.db")
# manager.create_from_pbf("./austria-260325.osm.pbf") # Nur 1 mal runnen
# ergebnisse = manager.find_nearby(lat=48.2084, lon=16.3731, ammount=20, radius_m=1000)
