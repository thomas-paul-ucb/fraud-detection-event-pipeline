import requests
import time
import uuid

# Use localhost to match your working version
API_URL = "http://localhost:8000/api/v1/ingest"

def run_velocity_attack():
    # We use a single ID to force an alert
    target_user = "user_99_ATTACKER"
    print(f"🔥 Starting velocity attack on {target_user}...")
    
    for i in range(15):
        payload = {
            "transaction_id": str(uuid.uuid4()),
            "user_id": target_user,
            "amount": 100.00,
            "currency": "USD",
            "merchant": "Amazon"  
        }
        try:
            resp = requests.post(API_URL, json=payload)
            # This print will tell us immediately if it's fixed (look for 200)
            print(f"Sent tx {i+1}: Status {resp.status_code}")
        except Exception as e:
            print(f"Error: {e}")
        
        time.sleep(0.1) 

if __name__ == "__main__":
    run_velocity_attack()