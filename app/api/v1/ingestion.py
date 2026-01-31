# app/api/v1/ingestion.py
from fastapi import APIRouter, HTTPException
from app.models.transaction import Transaction
from app.services.kafka_service import kafka_service
from datetime import datetime

router = APIRouter()

@router.post("/ingest")
async def ingest_transaction(tx: Transaction):
    try:
        if not tx.timestamp:
            tx.timestamp = datetime.utcnow()
            
        # Convert Pydantic model to dictionary for Kafka
        # We use .model_dump() in Pydantic v2
        tx_data = tx.model_dump()
        tx_data['timestamp'] = tx_data['timestamp'].isoformat() # Date must be string for JSON
        
        # PUSH TO KAFKA
        print(f"DEBUG: Attempting to send {tx.transaction_id} to Kafka...") # ADD THIS
        await kafka_service.send_transaction(tx_data)
        print(f"DEBUG: Successfully sent {tx.transaction_id}!") # ADD THIS

        return {"status": "accepted", "tx_id": tx.transaction_id}
    except Exception as e:
        print(f"❌ Error pushing to Kafka: {e}")
        raise HTTPException(status_code=500, detail=str(e))