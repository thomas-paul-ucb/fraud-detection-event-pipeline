import requests
import time
import random
from datetime import datetime

# Updated to match your Swagger screenshot path
API_URL = "http://localhost:8000/api/v1/ingest"

def run_stress_test(count=50):
    print(f"🔥 Starting stress test: Sending {count} transactions to {API_URL}")
    
    for i in range(count):
        payload = {
            "transaction_id": f"tx_{random.randint(1000, 9999)}",
            "user_id": f"user_{random.randint(1, 50)}",
            "amount": random.uniform(10.0, 6000.0), # High amounts to trigger fraud logic
            "merchant": random.choice(["Amazon", "Apple Store", "Steam", "Unknown_Shop"]),
            "currency": "USD",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "lat": 40.7128,
            "lon": -74.0060
        }
        
        try:
            response = requests.post(API_URL, json=payload)
            print(f"[{i}] Sent! Status: {response.status_code}")
        except Exception as e:
            print(f"❌ Error: {e}")
        
        time.sleep(0.5) # Half-second delay between hits

if __name__ == "__main__":
    run_stress_test()