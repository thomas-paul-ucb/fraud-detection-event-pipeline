# app/api/v1/ingestion.py
from fastapi import APIRouter
from app.models.transaction import Transaction
from datetime import datetime

router = APIRouter()

@router.post("/ingest")
async def ingest_transaction(tx: Transaction):
    if not tx.timestamp:
        tx.timestamp = datetime.utcnow()
    print(f"✅ Received: {tx.transaction_id}")
    return {"status": "accepted", "tx_id": tx.transaction_id}