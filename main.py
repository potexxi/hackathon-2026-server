from fastapi import FastAPI

app = FastAPI()

@app.get("/water")
def get_water():
    return [
        {"lat": 48.2082, "lon": 16.3738},
        {"lat": 48.2100, "lon": 16.3700}
    ]