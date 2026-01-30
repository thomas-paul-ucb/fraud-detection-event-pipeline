import requests
import uuid
import random
import time

API_URL = "http://localhost:8000/api/v1/ingest"

def generate_transaction():
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(100, 999)}",
        "amount": round(random.uniform(5.0, 500.0), 2),
        "merchant": random.choice(["Amazon", "Uber", "Starbucks", "Target"]),
        "currency": "USD"
    }

def run_test(count=10):
    print(f"🚀 Starting stress test: sending {count} transactions...")
    for i in range(count):
        data = generate_transaction()
        response = requests.post(API_URL, json=data)
        print(f"Sent {i+1}/{count}: {response.status_code}")
        time.sleep(0.1) # A small delay so we can see it happening

if __name__ == "__main__":
    run_test(50)