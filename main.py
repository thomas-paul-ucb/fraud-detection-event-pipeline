from fastapi import FastAPI
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional

# 1. Define the Data Contract
class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    amount: float = Field(gt=0, description="The amount must be greater than zero")
    merchant: str
    currency: str = "USD"
    timestamp: Optional[datetime] = None

# 2. Initialize FastAPI
app = FastAPI(title="Fraud Detection Ingestion API")


@app.post("/ingest")
async def ingest_transaction(tx: Transaction):
    # For now, we simulate success. 
    # Logic for Kafka will go here in the next phase.
    
    # We add a server-side timestamp if one wasn't provided
    if not tx.timestamp:
        tx.timestamp = datetime.utcnow()
        
    print(f"Received transaction: {tx.transaction_id} from {tx.user_id}")
    
    return {
        "status": "accepted", 
        "message": f"Transaction {tx.transaction_id} queued for processing"
    }