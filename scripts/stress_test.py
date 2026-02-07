import requests
import uuid
import time

API_URL = "http://localhost:8000/api/v1/ingest"

def test_traveler():
    user = "globetrotter_7"
    
    # 1. Transaction in New York
    print("Sending NY transaction...")
    requests.post(API_URL, json={
        "transaction_id": str(uuid.uuid4()),
        "user_id": user,
        "amount": 50.0,
        "merchant": "NY_Cafe",
        "lat": 40.7128, "lon": -74.0060  # New York
    })
    
    time.sleep(1) # Wait a second
    
    # 2. Transaction in London
    print("Sending London transaction...")
    requests.post(API_URL, json={
        "transaction_id": str(uuid.uuid4()),
        "user_id": user,
        "amount": 100.0,
        "merchant": "London_Pub",
        "lat": 51.5074, "lon": -0.1278  # London (~5,500 km away!)
    })

if __name__ == "__main__":
    test_traveler()