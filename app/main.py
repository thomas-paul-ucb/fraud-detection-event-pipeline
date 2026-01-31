# app/main.py
from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.api.v1.ingestion import router as ingestion_router
from app.services.kafka_service import kafka_service

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Connect to Kafka
    await kafka_service.start()
    yield
    # Shutdown: Disconnect from Kafka
    await kafka_service.stop()

app = FastAPI(title="Fraud Detection Ingestion API", lifespan=lifespan)

app.include_router(ingestion_router, prefix="/api/v1", tags=["Ingestion"])

@app.get("/health")
async def health_check():
    return {"status": "healthy"}