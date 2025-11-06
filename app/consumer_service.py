# Kafka consumer logic for DB Monitor Server
import asyncio
from aiokafka import AIOKafkaConsumer
from config import KAFKA_BROKER, KAFKA_TOPIC
from extensions import AsyncSessionLocal
from models import KafkaEvent


async def consumer_task():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id='fastapi-consumer-group',
        auto_offset_reset='earliest'
    )
    
    try:
        await consumer.start()
        print("Kafka consumer started")
        
        async for msg in consumer:
            try:
                data = msg.value.decode("utf-8")
                print(f"Received: {data}")
                
                # Database operation
                async with AsyncSessionLocal() as session:
                    event = KafkaEvent(value=data)
                    session.add(event)
                    await session.commit()
                    
            except Exception as e:
                print(f"Error processing message: {e}")
                
    except asyncio.CancelledError:
        print("Consumer task received cancellation signal")
        raise
    finally:
        print("Stopping Kafka consumer")
        await consumer.stop()
