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