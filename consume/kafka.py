import json
import logging
from aiokafka import AIOKafkaConsumer
from config.logging import Logger
from consume.websocket_manager import WebSocketManager
import traceback
from starlette.websockets import WebSocketDisconnect
from consume.station_latlon import STATION_LATLON

station_lookup = {station["name"]: {"lat": station["lat"], "lon": station["lon"]} for station in STATION_LATLON}

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
        """Continuously consume messages and stream to Websocket."""
        try:
            async for message in self.consumer:
                raw_data = message.value

                data_list = raw_data.get("data","") if isinstance(raw_data.get("data",""), list) else [raw_data.get("data","")]

                for data_point in data_list:
                    seismic_data = {
                        "dt": data_point.get("dt", "null"),
                        "network": data_point.get("network", "unknown"),
                        "station": data_point.get("station", "unknown"),
                        "channel": data_point.get("channel", "unknown"),
                        "data": data_point.get("data", 0)
                    }

                    station_info = station_lookup.get(seismic_data["station"])
                    if station_info:
                        seismic_data["lat"] = station_info["lat"]
                        seismic_data["lon"] = station_info["lon"]
                    else:
                        seismic_data["lat"] = None
                        seismic_data["lon"] = None

                    try:
                        await self.websocket_manager.broadcast(json.dumps(seismic_data))
                        # self.logger.info(f"Data sent to WebSocket: {seismic_data}")
                    except WebSocketDisconnect:
                        self.logger.warning("WebSocket disconnected. Skipping message broadcast.")

                try:
                    await self.consumer.commit()
                except Exception as e:
                    self.logger.error(f"Error committing Kafka offset: {e}")
                    
        except Exception as e:
            self.logger.error(f" [x] Error in consumer: {e}")
            self.logger.error(traceback.format_exc())
