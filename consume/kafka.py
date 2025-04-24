import json
import logging
from aiokafka import AIOKafkaConsumer
from config.logging import Logger
from datastore.redis_store import save_seismic_data
from consume.websocket_manager import WebSocketManager
import traceback
from starlette.websockets import WebSocketDisconnect

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class AsyncConsumer:
    def __init__(self, kafka_broker, topic, group_id, websocket_manager: WebSocketManager):
        self.kafka_broker = kafka_broker
        self.topic = topic
        self.group_id = group_id
        self.logger = Logger().setup_logger(service_name="consumer")
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.kafka_broker,
            group_id=self.group_id,
            auto_offset_reset="latest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        self.websocket_manager = websocket_manager

    async def start(self):
        await self.consumer.start()

    async def stop(self):
        await self.consumer.stop()

    async def consume(self):
        """Continuously consume messages, save them to Redis, and put them in the queue for SSE."""
        try:
            async for message in self.consumer:
                raw_data = message.value

                # seismic_data = {
                #     "dt": int(str(raw_data.get("dt", 0))[:10]),
                #     "network": raw_data.get("network", "unknown"),
                #     "station": raw_data.get("station", "unknown"),
                #     "channel": raw_data.get("channel", "unknown"),
                #     "data": raw_data.get("data", 0.0)
                # }

                # key = f"seismic:{seismic_data['dt']}_{seismic_data['station']}_{seismic_data['channel']}"

                # await save_seismic_data(key, seismic_data)
                # try:
                #     await self.websocket_manager.broadcast(json.dumps(seismic_data))
                # except WebSocketDisconnect:
                #     self.logger.warning("WebSocket disconnected. Skipping message broadcast.")

                # try:
                #     await self.consumer.commit()
                # except Exception as e:
                #     self.logger.error(f"Error committing Kafka offset: {e}")

                data_list = raw_data if isinstance(raw_data, list) else [raw_data]

                for data_point in data_list:
                    seismic_data = {
                        "dt": int(str(data_point.get("dt", 0))[:10]),
                        "network": data_point.get("network", "unknown"),
                        "station": data_point.get("station", "unknown"),
                        "channel": data_point.get("channel", "unknown"),
                        "data": data_point.get("data", 0.0)
                    }

                    key = f"seismic:{seismic_data['dt']}_{seismic_data['station']}_{seismic_data['channel']}"
                    await save_seismic_data(key, seismic_data)

                    try:
                        await self.websocket_manager.broadcast(json.dumps(seismic_data))
                    except WebSocketDisconnect:
                        self.logger.warning("WebSocket disconnected. Skipping message broadcast.")

                try:
                    await self.consumer.commit()
                except Exception as e:
                    self.logger.error(f"Error committing Kafka offset: {e}")
                    
        except Exception as e:
            self.logger.error(f" [x] Error in consumer: {e}")
            self.logger.error(traceback.format_exc())
