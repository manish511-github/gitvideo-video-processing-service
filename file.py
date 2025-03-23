import asyncio
from bullmq import Queue, Job
import redis.asyncio as redis
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Redis client
redis_client = redis.Redis(
    host='localhost',
    port=6379,
    db=0,
    password='mysecurepassword',  # Replace with your actual Redis password
    decode_responses=True
)

# Create a BullMQ queue
queue = Queue('video-changes', {'connection': redis_client})

# Sample job data
job_data = {
    'changeId': 'test-123',
    'videoId': 1,
    'operation': 'insert',
    'sourceVideoId': 'vid2',
    'start': 0.0,
    'end': 3.0,
    'at': 1.0,
    'currentTimeline': [
        {'sourceVideoId': 'vid1', 'sourceStartTime': 0.0, 'sourceEndTime': 12.0, 'globalStartTime': 0.0}
    ]
}

async def wait_for_job_completion(job_id):
    """Poll job status until it's finished."""
    while True:
        job = await Job.fromId(queue, job_id)  # ✅ Use correct method `fromId`
        logger.info(f"Job {job.id} added to queue with data: {job_data}")
        if job is None:
            print("Job not found!")
            return None
        
        if job.finishedOn:  # ✅ Corrected `finishedOn` instead of `finished_on`
            print(f"Job {job_id} completed with result: {job.returnvalue}")
            return job.returnvalue

        print(f"Waiting for job {job_id} to complete...")
        await asyncio.sleep(1)  # Poll every 1 second

async def add_job():
    job = await queue.add('change', job_data)
    print(f"Job {job.id} added to queue")

    # Poll until job is finished
    result = await wait_for_job_completion(job.id)
    print("Final Result:", result)

asyncio.run(add_job())