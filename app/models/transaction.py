from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional
import uuid

class Transaction(BaseModel):
    transaction_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    amount: float = Field(gt=0)
    merchant: str
    currency: str = "USD"
    timestamp: Optional[datetime] = None
    # Add these two for the Geographic Impossibility rule
    lat: Optional[float] = None
    lon: Optional[float] = None