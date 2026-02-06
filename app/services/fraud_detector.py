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
        self.r = redis.Redis(host=self.redis_host, decode_responses=True)
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
                amount = float(tx['amount'])
                
                # --- RULE 1: Block-list Check ---
                is_banned = await self.r.sismember("banned_users", user_id)
                if is_banned:
                    print(f"🚫 BLOCK-LIST ALERT: Banned user {user_id} attempted a transaction!")
                    continue

                # --- RULE 2: Spending Spike Check (New!) ---
                history_key = f"user_history:{user_id}"
                # Get the last 5 transaction amounts
                history = await self.r.lrange(history_key, 0, 4)
                
                if history:
                    avg_spend = sum(float(x) for x in history) / len(history)
                    if amount > avg_spend * 5 and len(history) >= 3: # Min 3 tx to have a valid avg
                        print(f"💰 SPIKE ALERT: User {user_id} spent ${amount} (Avg: ${avg_spend:.2f})")

                # Store this transaction in the history list
                await self.r.lpush(history_key, amount)
                # Keep only the last 10 transactions to save memory
                await self.r.ltrim(history_key, 0, 9)

                # --- RULE 3: Velocity Check ---
                key = f"user_velocity:{user_id}"
                count = await self.r.incr(key)
                if count == 1: await self.r.expire(key, 60)
                
                if count > 5:
                    print(f"🚨 VELOCITY ALERT: High Velocity for User {user_id}! ({count} tx/min)")
                else:
                    print(f"✅ Transaction processed for {user_id}. Count: {count}")
                    
        finally:
            await self.consumer.stop()

if __name__ == "__main__":
    detector = FraudDetector()
    asyncio.run(detector.start())