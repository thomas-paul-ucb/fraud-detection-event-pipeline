import asyncio
import json
from aiokafka import AIOKafkaConsumer
import redis.asyncio as redis

class FraudDetector:
    def __init__(self):
        self.kafka_server = "127.0.0.1:9092"
        self.redis_host = "127.0.0.1"
        self.topic = "pending-transactions"
        
    async def start(self):
        # Connect to Redis
        self.r = redis.Redis(host=self.redis_host, decode_responses=True)
        
        # Connect to Kafka
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.kafka_server,
            group_id="fraud-detector-group",
            auto_offset_reset='earliest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        await self.consumer.start()
        print("🕵️ Fraud Detector Worker is watching the stream...")

        try:
            async for msg in self.consumer:
                tx = msg.value
                user_id = tx['user_id']
                
                # DESIGN CONCEPT: Rate Limiting / Velocity Check
                # We use a Redis key for each user
                key = f"user_velocity:{user_id}"
                
                # Increment count
                count = await self.r.incr(key)
                
                # If first time seeing this user in this window, set expiry (60s)
                if count == 1:
                    await self.r.expire(key, 60)
                
                if count > 5:
                    print(f"🚨 ALERT: High Velocity detected for User {user_id}! ({count} tx/min)")
                else:
                    print(f"✅ Transaction processed for {user_id}. Count: {count}")
                    
        finally:
            await self.consumer.stop()

if __name__ == "__main__":
    detector = FraudDetector()
    asyncio.run(detector.start())