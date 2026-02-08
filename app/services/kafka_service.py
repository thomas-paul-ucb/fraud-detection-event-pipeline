# app/services/kafka_service.py
import os
from aiokafka import AIOKafkaProducer
import json
import asyncio

class KafkaService:
    def __init__(self):
        self.producer = None
        self.server = os.getenv("KAFKA_URL", "127.0.0.1:9092")
        self.topic = "pending-transactions"

    async def start(self):
        """Initialize the Kafka Producer"""
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.server,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        await self.producer.start()
        print(f"🚀 Kafka Producer started on {self.server}")

    async def stop(self):
        """Shutdown the Kafka Producer"""
        if self.producer:
            await self.producer.stop()
            print("🛑 Kafka Producer stopped")

    async def send_transaction(self, tx_data: dict):
        """Push transaction data to the topic"""
        if not self.producer:
            raise Exception("Kafka Producer not started")
        
        await self.producer.send_and_wait(self.topic, tx_data)

# Create a single instance to be used across the app
kafka_service = KafkaService()