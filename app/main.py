# app/main.py
from fastapi import FastAPI
from app.api.v1.ingestion import router as ingestion_router

app = FastAPI(title="Fraud Detection Ingestion API")

# Connect the modular router
app.include_router(ingestion_router, prefix="/api/v1", tags=["Ingestion"])

@app.get("/health")
async def health_check():
    return {"status": "healthy"}