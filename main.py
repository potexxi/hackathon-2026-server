from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import os
import sys
import logging
import water_location_calculation

app = FastAPI(title="Water Location API", version="1.0")


def get_relativ_path(path: str):
    base_path = os.path.dirname(os.path.abspath(__file__))
    return os.path.normpath(os.path.join(base_path, path))


log_dir = get_relativ_path("./Logs")
os.makedirs(log_dir, exist_ok=True)

log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

if not root_logger.handlers:
    main_file_handler = logging.FileHandler(os.path.join(log_dir, "app.log"), encoding='utf-8')
    main_file_handler.setFormatter(log_formatter)
    root_logger.addHandler(main_file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)

osm_logger = logging.getLogger("water_location_calculation")
osm_logger.setLevel(logging.INFO)

if not osm_logger.handlers:
    osm_file_handler = logging.FileHandler(os.path.join(log_dir, "OSMLogs.log"), encoding='utf-8')
    osm_file_handler.setFormatter(log_formatter)
    osm_logger.addHandler(osm_file_handler)
    osm_logger.propagate = False

logger = logging.getLogger(__name__)


# --- MODELS ---
class WaterRequest(BaseModel):
    lat: float
    lon: float
    radius: Optional[int] = 5000


class WaterResponse(BaseModel):
    found: bool
    count: int
    results: list


# --- OSM MANAGER (einmal laden, nicht bei jedem Request neu) ---
pbf_path = get_relativ_path("./austria-260325.osm.pbf")
manager = water_location_calculation.OSMWaterManager(get_relativ_path("./austria.db"))
manager.create_from_pbf(pbf_path)
logger.info("✅ OSMWaterManager geladen und bereit.")


# --- ENDPOINT ---
@app.post("/water/nearby", response_model=WaterResponse)
def find_nearby_water(request: WaterRequest):
    logger.info(f"📍 Suche gestartet für Koordinaten: {request.lat}, {request.lon} | Radius: {request.radius}m")

    try:

        result = manager.find_nearby(request.lat, request.lon, request.radius)

        if result is None or result.empty:
            logger.warning("Keine Wasserstellen im Umkreis gefunden.")
            return WaterResponse(found=False, count=0, results=[])

        logger.info(f"Treffer gefunden: {len(result)} Wasserstellen.")

        # Geometrie-Spalte entfernen (nicht JSON-tauglich)
        if 'geom' in result.columns:
            result = result.drop(columns=['geom'])
            logger.info("Spalte 'geom' entfernt (Bytearray Fix).")

        result_dict = result.to_dict(orient='records')
        return WaterResponse(found=True, count=len(result_dict), results=result_dict)

    except Exception as e:
        logger.exception(f"Unerwarteter Fehler: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- HEALTH CHECK ---
@app.get("/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)