from locust import HttpUser, task, between

class FraudPipelineUser(HttpUser):
    wait_time = between(0.01, 0.05)

    @task
    def ingest_transaction(self):
        self.client.post("/api/v1/ingest", json={
            "user_id": "load_test_user",
            "amount": 150.0,
            "merchant": "Test_Merchant",
            "currency": "USD",
            "lat": 40.7128,
            "lon": -74.0060
        })