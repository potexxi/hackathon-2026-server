import os
import sys
import json
import logging
import water_location_calculation


# --- HILFSFUNKTION FÜR PFADE ---
def get_relativ_path(path: str):
    base_path = os.path.dirname(os.path.abspath(__file__))
    return os.path.normpath(os.path.join(base_path, path))


# --- ZENTRALES LOGGING SETUP ---
log_dir = get_relativ_path("./Logs")
os.makedirs(log_dir, exist_ok=True)
os.makedirs(get_relativ_path("./Files"), exist_ok=True)

# Gemeinsames Format für alle Logs
log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

# 1. SETUP: HAUPT-LOG (app.log) & KONSOLE
# Der Root-Logger fängt alles auf, was nicht speziell umgeleitet wird
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# Handler für die Datei app.log
main_file_handler = logging.FileHandler(os.path.join(log_dir, "app.log"), encoding='utf-8')
main_file_handler.setFormatter(log_formatter)
root_logger.addHandler(main_file_handler)

# Handler für die Konsole (damit du im Terminal siehst, was passiert)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(log_formatter)
root_logger.addHandler(console_handler)

# 2. SETUP: SPEZIAL-LOG (OSMLogs.log)
# Wir holen uns gezielt den Logger der Library (muss dem Dateinamen entsprechen)
osm_logger = logging.getLogger("water_location_calculation")
osm_logger.setLevel(logging.INFO)

# Handler für die Datei OSMLogs.log
osm_file_handler = logging.FileHandler(os.path.join(log_dir, "OSMLogs.log"), encoding='utf-8')
osm_file_handler.setFormatter(log_formatter)
osm_logger.addHandler(osm_file_handler)

# Optional: Wenn OSM-Logs NUR in OSMLogs.log stehen sollen (nicht auch in app.log),
# dann setze propagate auf False:
osm_logger.propagate = False

# Unser Logger für diese main.py Datei
logger = logging.getLogger(__name__)

# --- HAUPTPROGRAMM ---
if __name__ == "__main__":
    # Check ob genug Argumente da sind (Skriptname + 2 Koordinaten)
    if len(sys.argv) < 3:
        logger.error("Fehlende Argumente! Nutzung: python main.py <lat> <lon> <optional: radius>")
        sys.exit(1)

    try:
        # 1. Input verarbeiten
        lat = float(sys.argv[1])
        lon = float(sys.argv[2])
        radius = int(sys.argv[3]) if len(sys.argv) > 3 else 5000
        logger.info(f"📍 Suche gestartet für Koordinaten: {lat}, {lon}")

        # 2. Library nutzen
        pbf_path = get_relativ_path("./austria-260325.osm.pbf")
        manager = water_location_calculation.OSMWaterManager()

        # Diese Aufrufe schreiben jetzt automatisch in OSMLogs.log
        manager.create_from_pbf(pbf_path)
        result = manager.find_nearby(lat, lon, radius)

        # 3. DataFrame verarbeiten (Bytearray-Fix für JSON)
        if result is not None and not result.empty:
            logger.info(f"Treffer gefunden: {len(result)} Wasserstellen.")

            # Geometrie-Spalte entfernen, da sie nicht JSON-tauglich ist
            if 'geom' in result.columns:
                result = result.drop(columns=['geom'])
                logger.info("Spalte 'geom' entfernt (Bytearray Fix).")

            # 4. Speichern
            output_path = get_relativ_path("Files/output.json")
            result_dict = result.to_dict(orient='records')

            with open(output_path, 'w', encoding="utf-8") as f:
                json.dump(result_dict, f, ensure_ascii=False, indent=4)

            logger.info(f"✅ JSON erfolgreich gespeichert unter: {output_path}")
        else:
            logger.warning("Keine Wasserstellen im Umkreis gefunden.")

    except ValueError:
        logger.error("Latitude und Longitude müssen gültige Zahlen sein!")
        sys.exit(1)
    except Exception as e:
        # logger.exception loggt den kompletten Fehler-Stacktrace (sehr hilfreich!)
        logger.exception(f"Unerwarteter Fehler: {e}")
        sys.exit(1)