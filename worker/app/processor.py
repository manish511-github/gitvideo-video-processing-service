import boto3
import os
import subprocess
import time
import redis
import shutil
import json
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

def process_video(upload_obj):
    input_path = None
    output_dir = None
    file_name = None
    base_name = None
    
    try:
        print("hello first")
        print(upload_obj)
        file_name = upload_obj['fileName']
        print(file_name)
        input_path = f"/tmp/{file_name}"
        base_name = os.path.splitext(os.path.basename(file_name))[0]
        output_dir = f"/tmp/{base_name}_hls"
        output_playlist = f"{output_dir}/playlist.m3u8"
        
        print("hello seoncd")
        # Ensure directories exist
        os.makedirs(os.path.dirname(input_path), exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)
        
        # Download video from S3
        s3.download_file("video-raw", file_name, input_path)

        # FFmpeg processing -> make into ts segments
        subprocess.run([
            "ffmpeg",
            "-i", input_path,
            "-c:v", "libx264",
            "-preset", "fast",
            "-f", "hls",
            "-hls_time", "1",
            "-hls_segment_filename", f"{output_dir}/segment_%03d.ts",
            output_playlist
        ], check=True)

        print(f"[+] FFmpeg processing complete for {file_name}")

        # Upload all generated files to video-processed
        for root, _, files in os.walk(output_dir):
            for file in files:
                local_path = os.path.join(root, file)
                s3_key = f"{base_name}/{file}"
                s3.upload_file(local_path, "video-processed", s3_key)
    except Exception as e:
        print(f"[!] Failed to process {file_name}: {e}")
    finally:
        # Clean up local temp files
        if input_path and os.path.exists(input_path):
            os.remove(input_path)
        if output_dir and os.path.exists(output_dir):
            shutil.rmtree(output_dir)
        print(f"[~] Cleaned up temporary files for {file_name}")


def sqs_worker():
    sqs = boto3.client('sqs', endpoint_url=os.getenv('AWS_ENDPOINT_URL'))
    
    while True:
        try:
            response = sqs.receive_message(
                QueueUrl="http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/processing-queue",
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20
            )            
            if 'Messages' in response:
                for msg in response['Messages']:
                    upload_obj = json.loads(msg['Body'])  # Parse JSON string to dict
                    process_video(upload_obj)
                    sqs.delete_message(
                        QueueUrl="http://localstack:4566/000000000000/processing-queue",
                        ReceiptHandle=msg['ReceiptHandle']
                    )
            else:
                time.sleep(5)
                
        except Exception as e:
            print(f"SQS Error: {str(e)}")
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
        print("🌀 Main loop alive...")
        time.sleep(1)