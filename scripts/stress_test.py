import requests
import uuid
import time

API_URL = "http://localhost:8000/api/v1/ingest"

def run_comprehensive_test():
    # 1. Test Banned User
    print("Testing Banned User...")
    requests.post(API_URL, json={
        "transaction_id": str(uuid.uuid4()),
        "user_id": "user_666",
        "amount": 10.0, "currency": "USD", "merchant": "CoffeeShop"
    })

    # 3. Test Velocity
    # print("Testing Velocity...")
    # for i in range(7):
    #     requests.post(API_URL, json={
    #         "transaction_id": str(uuid.uuid4()),
    #         "user_id": "speedy_gonzales",
    #         "amount": 5.0, "currency": "USD", "merchant": "GasStation"
    #     })
    #     time.sleep(0.1)

if __name__ == "__main__":
    run_comprehensive_test()