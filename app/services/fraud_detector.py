import asyncio
import json
from aiokafka import AIOKafkaConsumer
import redis.asyncio as redis
from datetime import datetime

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
                new_lat = tx.get('lat')
                new_lon = tx.get('lon')
                
                # --- RULE 1: Block-list Check (Sets) ---
                if await self.r.sismember("banned_users", user_id):
                    print(f"🚫 BLOCK-LIST ALERT: Banned user {user_id}!")
                    continue

                # --- RULE 2: Geographic Impossibility (Geo/Hashes) ---
                if new_lat is not None and new_lon is not None:
                    geo_key = "global_user_locations"
                    state_key = f"user_state:{user_id}"
                    
                    prev_state = await self.r.hgetall(state_key)
                    
                    if prev_state:
                        # 1. Update the "old" marker to where the user WAS
                        await self.r.geoadd(geo_key, (float(prev_state['lon']), float(prev_state['lat']), f"{user_id}_old"))
                        # 2. Add the "new" marker to where the user IS NOW
                        await self.r.geoadd(geo_key, (new_lon, new_lat, user_id))
                        
                        # 3. Calculate distance
                        dist = await self.r.geodist(geo_key, f"{user_id}_old", user_id, unit="km")
                        
                        if dist:
                            print(f"DEBUG: Distance calculated: {float(dist):.2f} km")
                            if float(dist) > 500:
                                print(f"🌍 TRAVEL ALERT: User {user_id} moved {float(dist):.2f}km instantly!")

                    # Update the state hash for the next comparison
                    await self.r.hset(state_key, mapping={"lat": new_lat, "lon": new_lon})

                # --- RULE 3: Spending Spike Check (Lists) ---
                history_key = f"user_history:{user_id}"
                history = await self.r.lrange(history_key, 0, 4)
                if history:
                    avg_spend = sum(float(x) for x in history) / len(history)
                    if amount > avg_spend * 5 and len(history) >= 3:
                        print(f"💰 SPIKE ALERT: User {user_id} spent ${amount} (Avg: ${avg_spend:.2f})")
                await self.r.lpush(history_key, amount)
                await self.r.ltrim(history_key, 0, 9)

                # --- RULE 4: Velocity Check (Strings/Counters) ---
                v_key = f"user_velocity:{user_id}"
                count = await self.r.incr(v_key)
                if count == 1: await self.r.expire(v_key, 60)
                if count > 5:
                    print(f"🚨 VELOCITY ALERT: User {user_id} ({count} tx/min)")
                else:
                    print(f"✅ Transaction processed for {user_id}. Count: {count}")
                    
        finally:
            await self.consumer.stop()

if __name__ == "__main__":
    detector = FraudDetector()
    asyncio.run(detector.start())