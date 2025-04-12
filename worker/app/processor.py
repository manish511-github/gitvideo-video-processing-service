import boto3
import os
import subprocess
import time
import redis
import shutil
import json
import logging
from kafka_producer import send_kafka_event
# Logger Configuration
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(lineno)d %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
# from bullmq import Worker
# AWS Setup
s3 = boto3.client(
    's3',
    region_name=os.getenv('AWS_REGION'),
    endpoint_url=os.getenv('AWS_ENDPOINT_URL'),
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)

# Redis Setup
redis_conn = redis.Redis(
    host=os.getenv('REDIS_HOST'),
    port=os.getenv('REDIS_PORT'),
    password=os.getenv('REDIS_PASSWORD'),
    decode_responses=True
)

def update_redis_status(video_id, status, progress=None):
    update = {
        "video_id": video_id,
        "status": status,
        "progress": progress if progress is not None else 0
    }
    redis_conn.hset(f"video:{video_id}", mapping=update)
    redis_conn.publish(f"video_status:{video_id}", json.dumps(update))

def get_video_duration(input_path):
    result = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1", input_path],
         stdout = subprocess.PIPE,
         stderr = subprocess.STDOUT
        )
    return float(result.stdout.decode().strip())

def process_video(file_name):
    input_path = None
    output_dir = None
    # file_name = None
    base_name = None
    
    try:
        # import rpdb
        # rpdb.set_trace()
        logger.info("Received video processing task.")
        # logger.debug(f"Upload object: {upload_obj}")
        
        # file_name = upload_obj['fileName']
        logger.info(f"Processing file: {file_name}")

        input_path = f"/tmp/{file_name}"
        base_name = os.path.splitext(os.path.basename(file_name))[0]
        output_dir = f"/tmp/{base_name}_hls"
        output_playlist = f"{output_dir}/playlist.m3u8"

        os.makedirs(os.path.dirname(input_path), exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)

        s3.download_file("video-raw", file_name, input_path)
        logger.info(f"Downloaded {file_name} from S3")
        logger.info("Video Processing Task Start")
        send_kafka_event("video.processing.started",{
            "file": file_name
        })
        update_redis_status(file_name, "processing", progress=0)
        #FFmpeg (simulate progress tracking)
        segment_count =0
        total_segments=0
        
        total_duration = get_video_duration(input_path)

        process = subprocess.Popen([
            "ffmpeg",
            "-i", input_path,
            "-c:v", "libx264",
            "-preset", "fast",
            "-f", "hls",
            "-hls_time", "1",
            "-hls_segment_filename", f"{output_dir}/segment_%03d.ts",
            output_playlist
        ], stderr=subprocess.PIPE, universal_newlines=True)
        
        last_progress = 0
        for line in subprocess.stderr:
            if "time" in line:
                # Extract timestamp from line
                try:
                    time_str = line.split("time=")[1].split(" ")[0].strip()
                    h, m, s = map(float,time_str.split(':'))
                    current_seconds = h * 3600 + m * 60 + s
                    progress = int((current_seconds / total_duration) * 100)
                    if progress >= last_progress + 5:  # only update if 5% ahead
                        last_progress = progress
                        update_redis_status(file_name, "processing", progress=progress)
                        send_kafka_event("video.processing.progress", {
                            "video_file": file_name,
                            "progress": progress
                        })

                except Exception as parse_err:
                    logger.debug(f"Failded to parse progres: {parse_err}")
        process.wait()
        if process.returncode !=0:
            raise Exception("FFmpeg failed during processing")

        logger.info(f"FFmpeg processing complete for {file_name}")
        
        segment_files = [f for f in os.listdir(output_dir) if f.endswith(".ts") or f.endswith(".m3u8")]
        total_segments = len([f for f in segment_files if f.endswith(".ts")])
        for idx, file in enumerate(segment_files):
            s3_key = f"{base_name}/{file}"
            s3.upload_file(os.path.join(output_dir, file), "video-processed", s3_key)

            if file.endswith(".ts"):
                segment_count+= 1
                progress = int((segment_count/total_segments) * 100)
                update_redis_status(file_name, "processing", progress=progress)

                if progress % 10 ==0:
                    send_kafka_event("video.processing.progress", {
                        "video_file": file_name,
                        "progress": progress
                    })

        playlist_url = f"{base_name}/playlist.m3u8"
        send_kafka_event("video.processing.completed", {
            "video_file": file_name,
            "playlist_url":playlist_url
        })


        # for root, _, files in os.walk(output_dir):
        #     for file in files:
        #         local_path = os.path.join(root, file)
        #         s3_key = f"{base_name}/{file}"
        #         s3.upload_file(local_path, "video-processed", s3_key)
        #         logger.debug(f"Uploaded {file} to video-processed/{s3_key}")

    except Exception as e:
        logger.error(f"Failed to process {file_name}: {e}")
    finally:
        if input_path and os.path.exists(input_path):
            os.remove(input_path)
        if output_dir and os.path.exists(output_dir):
            shutil.rmtree(output_dir)
        logger.info(f"Cleaned up temporary files for {file_name}")


def sqs_worker():
    sqs = boto3.client('sqs', endpoint_url=os.getenv('AWS_ENDPOINT_URL'))

    while True:
        try:
            logger.info("Polling SQS for messages...")
            response = sqs.receive_message(
                QueueUrl="http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/processing-queue",
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20
            )
            if 'Messages' in response:
                for msg in response['Messages']:
                    logger.info("Received message from SQS.")
                    upload_obj = json.loads(msg['Body'])
                    file_name = upload_obj['Records'][0]['s3']['object']['key']
                    # import rpdb
                    # rpdb.set_trace()
                    process_video(file_name)
                    sqs.delete_message(
                        QueueUrl="http://localstack:4566/000000000000/processing-queue",
                        ReceiptHandle=msg['ReceiptHandle']
                    )
                    logger.info("Deleted message from SQS.")
            else:
                logger.debug("No messages received, sleeping...")
                time.sleep(5)

        except Exception as e:
            logger.error(f"SQS Error: {str(e)}")
            time.sleep(10)

# def redis_worker():
#     # Your existing BullMQ worker
#     # worker = Worker('video-changes', process_change, {'connection': redis_conn})
#     asyncio.get_event_loop().run_forever()

if __name__ == "__main__":
    import threading
    # Start both workers in separate threads
    threading.Thread(target=sqs_worker, daemon=True).start()
    # threading.Thread(target=redis_worker, daemon=True).start()
    
    # Keep main thread alive
    while True:
        logger.debug("🌀 Main loop alive...")
        time.sleep(1)