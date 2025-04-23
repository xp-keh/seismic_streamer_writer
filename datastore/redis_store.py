import aioredis
import logging
import json
from config.utils import get_env_value
import logging

logger = logging.getLogger("redis_store")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

REDIS_HOST = get_env_value("REDIS_HOST")
REDIS_PORT = get_env_value("REDIS_PORT")
REDIS_DB = get_env_value("REDIS_DB")
REDIS_TTL = 7200

redis_client = None

async def init_redis():
    global redis_client
    redis_client = await aioredis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}", decode_responses=True)

async def save_seismic_data(key: str, data: dict):
    if redis_client is None:
        await init_redis()
    try:
        await redis_client.setex(key, REDIS_TTL, json.dumps(data)) # type: ignore
    except Exception as e:
        logger.error(f"Failed to save data to Redis: {e}")

async def get_all_seismic_data():
    if redis_client is None:
        await init_redis()
    else: 
        keys = await redis_client.keys("seismic:*")
        data = [json.loads(await redis_client.get(key)) for key in keys]
        logging.info(f"Fetched {len(data)} records from Redis.")
        return data

async def clear_redis():
    if redis_client is None: 
        await init_redis()
    
    try:
        await redis_client.flushdb() # type: ignore
        logging.info("✅ Redis flush successful.")
    except Exception as e:
        logging.error(f"❌ Redis flush failed: {e}")