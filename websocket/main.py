from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import redis
import asyncio
import json
import os
import logging
from dotenv import load_dotenv

load_dotenv()

# Setup logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

r = redis.Redis(
    host=os.getenv('REDIS_HOST', 'redis'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    password=os.getenv('REDIS_PASSWORD', None),
    decode_responses=True
)

@app.websocket("/ws/video/{video_id}")
async def video_ws(websocket: WebSocket, video_id: str):
    await websocket.accept()
    logger.info(f"WebSocket connected for video {video_id}")
    
    pubsub = r.pubsub()
    pubsub.subscribe(f"video_status:{video_id}")
    logger.info(f"Subscribed to Redis channel video_status:{video_id}")

    try:
        while True:
            message = pubsub.get_message()
            if message and message['type'] == 'message':
                data = json.loads(message['data'])
                logger.info(f"Sending update to client: {data}")
                await websocket.send_json(data)
            await asyncio.sleep(0.2)
    except WebSocketDisconnect:
        pubsub.unsubscribe(f"video_status:{video_id}")
        logger.info(f"WebSocket disconnected for video {video_id} and unsubscribed from Redis")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        pubsub.unsubscribe(f"video_status:{video_id}")