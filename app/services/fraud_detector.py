import os
import asyncio
import json
from aiokafka import AIOKafkaConsumer
import redis.asyncio as redis
from datetime import datetime
from prometheus_client import start_http_server, Counter

# Define Metrics
FRAUD_ALERTS = Counter(
    'fraud_alerts_total', 
    'Total fraud alerts', 
    ['user_id', 'type']
)
TX_PROCESSED = Counter('transactions_processed_total', 'Total transactions checked')

class FraudDetector:
    def __init__(self):
        self.kafka_server = os.getenv("KAFKA_URL", "127.0.0.1:9092")
        self.redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
        self.topic = "pending-transactions"
        # Start Prometheus metrics server on port 8001
        start_http_server(8001)
        
    async def start(self):
        self.r = redis.Redis(host=self.redis_host, decode_responses=True)
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.kafka_server,
            group_id="fraud-detector-group",
            auto_offset_reset='earliest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        await self.consumer.start()
        print("🕵️ Fraud Detector Worker is watching the stream (Metrics on :8001)...")

        try:
            async for msg in self.consumer:
                TX_PROCESSED.inc() # Count every message seen
                tx = msg.value
                user_id = tx.get('user_id', 'unknown_user')
                amount = float(tx['amount'])
                new_lat = tx.get('lat')
                new_lon = tx.get('lon')
                
                # --- RULE 1: Block-list Check ---
                if await self.r.sismember("banned_users", user_id):
                    FRAUD_ALERTS.labels(user_id=str(user_id), type='block_list').inc()
                    print(f"🚫 BLOCK-LIST ALERT: Banned user {user_id}!")
                    continue

                # --- RULE 2: Geographic Impossibility ---
                if new_lat is not None and new_lon is not None:
                    geo_key = "global_user_locations"
                    state_key = f"user_state:{user_id}"
                    prev_state = await self.r.hgetall(state_key)
                    
                    if prev_state:
                        await self.r.geoadd(geo_key, (float(prev_state['lon']), float(prev_state['lat']), f"{user_id}_old"))
                        await self.r.geoadd(geo_key, (new_lon, new_lat, user_id))
                        dist = await self.r.geodist(geo_key, f"{user_id}_old", user_id, unit="km")
                        
                        if dist and float(dist) > 500:
                            FRAUD_ALERTS.labels(user_id=str(user_id), type='travel_impossibility').inc()
                            print(f"🌍 TRAVEL ALERT: User {user_id} moved {float(dist):.2f}km!")

                    await self.r.hset(state_key, mapping={"lat": new_lat, "lon": new_lon})

                # --- RULE 3: Spending Spike Check ---
                history_key = f"user_history:{user_id}"
                history = await self.r.lrange(history_key, 0, 4)
                if history:
                    avg_spend = sum(float(x) for x in history) / len(history)
                    if amount > avg_spend * 5 and len(history) >= 3:
                        FRAUD_ALERTS.labels(user_id=str(user_id), type='spending_spike').inc()
                        print(f"💰 SPIKE ALERT: User {user_id} spent ${amount} (Avg: ${avg_spend:.2f})")
                await self.r.lpush(history_key, amount)
                await self.r.ltrim(history_key, 0, 9)

                # --- RULE 4: Velocity Check ---
                v_key = f"user_velocity:{user_id}"
                count = await self.r.incr(v_key)
                if count == 1: await self.r.expire(v_key, 60)
                if count > 5:
                    FRAUD_ALERTS.labels(user_id=str(user_id), type='velocity').inc()
                    print(f"🚨 VELOCITY ALERT: User {user_id} ({count} tx/min)")
                else:
                    print(f"✅ Transaction processed for {user_id}. Count: {count}")
                    
        finally:
            await self.consumer.stop()

if __name__ == "__main__":
    detector = FraudDetector()
    asyncio.run(detector.start())