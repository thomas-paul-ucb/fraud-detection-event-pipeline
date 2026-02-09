import requests
import time
import random
from datetime import datetime

# Matches your fixed endpoint
API_URL = "http://localhost:8000/api/v1/ingest"

def run_stress_test(count=20):
    # We use a specific ID to ensure the 'velocity' logic triggers
    target_user = "FINAL_BOSS_TEST"
    print(f"🔥 Starting stress test: Sending {count} transactions to {API_URL}")
    print(f"🎯 Target user for velocity fraud: {target_user}")
    
    for i in range(count):
        # Every 5th transaction, we jump the location to trigger impossible travel
        is_travel_fraud = (i % 5 == 0)
        lat = 51.5074 if is_travel_fraud else 40.7128
        lon = -0.1278 if is_travel_fraud else -74.0060

        payload = {
            "transaction_id": f"tx_stress_{i}_{random.randint(100, 999)}",
            "user_id": target_user,
            "amount": random.uniform(100.0, 9000.0), 
            "merchant": random.choice(["Global_Electronics", "Luxury_Watches", "Casino_Royale"]),
            "currency": "USD",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "lat": lat,
            "lon": lon
        }
        
        try:
            response = requests.post(API_URL, json=payload, timeout=5)
            print(f"[{i}] Sent! User: {target_user} | Lat: {lat} | Status: {response.status_code}")
        except Exception as e:
            print(f"❌ Error: {e}")
        
        time.sleep(0.2) # Faster hits to ensure velocity detection

if __name__ == "__main__":
    run_stress_test()