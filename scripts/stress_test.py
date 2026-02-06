import requests
import time
import uuid

API_URL = "http://localhost:8000/api/v1/ingest"

def test_spending_spike():
    target_user = "spike_tester_88"
    
    # 1. Establish a "normal" spending habit (4 small transactions)
    print(f"--- Establishing history for {target_user} ---")
    for i in range(4):
        payload = {
            "transaction_id": str(uuid.uuid4()),
            "user_id": target_user,
            "amount": 20.0,  # Normal small amount
            "currency": "USD",
            "merchant": "CoffeeShop"
        }
        requests.post(API_URL, json=payload)
        print(f"Normal transaction {i+1} sent.")
        time.sleep(0.1)

    # 2. Send the "Spike" (1 large transaction)
    print(f"--- Sending the spike! ---")
    spike_payload = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": target_user,
        "amount": 500.0,  # 25x the average!
        "currency": "USD",
        "merchant": "HighEndJewelry"
    }
    resp = requests.post(API_URL, json=spike_payload)
    print(f"Spike sent. Status: {resp.status_code}")

if __name__ == "__main__":
    test_spending_spike()