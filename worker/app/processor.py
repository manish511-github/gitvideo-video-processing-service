import boto3
import os
import subprocess
import time
import redis
import shutil
import json
from queue import Queue
import logging
import threading
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

class VideoProcessor:
    def __init__(self):
      self.upload_queue = Queue()
      self.processing_lock = threading.Lock()
      self.current_progress=0

    def update_redis_status(self, video_id, status, progress=None):
        with self.processing_lock:
            # Ensure progress never decrese
            if progress is not None and progress > self.current_progress:
                self.current_progress = progress
            elif  progress is None:
                progress = self.current_progress
            update = {
                "video_id": video_id,
                "status": status,
                "progress": progress
            }
            redis_conn.hset(f"video:{video_id}", mapping=update)
            redis_conn.publish(f"video_status:{video_id}",json.dumps(update))
           
    def get_video_duration(self, input_path):
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", input_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True
        )
        return float(result.stdout.decode().strip())
    
    def upload_segment(self, file_path, s3_key):
        try:
            s3.upload_file(file_path, "video-processed", s3_key)
            return True
        except Exception as e:
            logger.error(f"Failed to upload segment {s3_key}: {e}")
            return False
        
    def process_video(self,upload_obj):
        file_name = upload_obj['fileName']
        commit_id = upload_obj['commitId']
        self.current_progress = 0 # Reset progress for new job
        input_path = f"/tmp/{file_name}"
        base_name = os.path.splitext(os.path.basename(file_name))[0]
        output_dir = f"/tmp/{base_name}_hls"
        try:
            # Setup directories
            os.makedirs(output_dir, exist_ok=True)
            # Downlaod orignal file
            s3.download_file("video-raw", file_name, input_path)
            self.update_redis_status(file_name, "downloaded", 5)

            #Get video duration
            duration = self.get_video_duration(input_path)
            self.update_redis_status(file_name,"processing", 10)

                # Start FFmpeg process
            ffmpeg_cmd = [
                "ffmpeg", "-i", input_path,
                "-c:v", "libx264", "-preset", "fast",
                "-f", "hls", "-hls_time", "1",
                "-hls_segment_filename", f"{output_dir}/segment_%03d.ts",
                f"{output_dir}/playlist.m3u8"
            ]

            process = subprocess.Popen(
                ffmpeg_cmd,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )

            # Start progress monitoring thread
            progress_thread = threading.Thread(
                target = self.monitor_encoding_progress,
                args= (process, file_name, duration)
            )
            progress_thread.start()

            #Start segment upload thread
            upload_thread = threading.Thread(
                target=self.upload_segments,
                args= (output_dir, base_name,file_name,commit_id)
            )
            upload_thread.start()

            #Wait for completion
            process.wait()
            progress_thread.join(timeout=30)

            #Finalise upload
            upload_thread.join(timeout=120)
            self.update_redis_status(file_name, "completed", 100)


        except Exception as e:
            logger.error(f"Processing failed: {e}")
            self.update_redis_status(file_name, "failed", self.current_progress)
        finally:
            # Cleanup
            if os.path.exists(input_path):
                os.remove(input_path)
            if os.path.exists(output_dir):
                shutil.rmtree(output_dir)
    
    def monitor_encoding_progress(self, process, video_id, total_duration):
        """Tracks encoding progress from FFmpeg output (0-95%)"""
        last_update = 0
        for line in process.stderr:
            if "time=" in line:
                try:
                    time_str = line.split("time=")[1].split()[0]
                    h, m, s = map(float, time_str.split(':'))
                    current_sec = h * 3600 + m*60 +s
                    progress = min(95, int((current_sec/ total_duration) * 95))

                    if progress > last_update +2:
                        self.update_redis_status(video_id, "processing", progress)
                        last_update = progress
                except Exception as e:
                    logger.debug(f"Progress parse error : {e}")
    
    def upload_segments(self, output_dir, base_name, video_id,commit_id):
        """Handles segment uploads (adds 5% to progress)"""
        uploaded_files = set()
        total_segments = 0

        while True:
            #Wait for segments to appear
            segments = [f for f in os.listdir(output_dir)
                        if f.endswith('.ts') and f not in uploaded_files]
            # Get total segments from playlist
            playlist = f"{output_dir}/playlist.m3u8"
            if os.path.exists(playlist):
                with open(playlist) as f:
                    total_segments = sum (1 for line in f if '.ts' in line)
            
            #Upload new segments
            for seg in segments:
                seg_path = os.path.join(output_dir, seg)
                s3_key = f"{commit_id}/{base_name}/{seg}"  # updated path

                if self.upload_segment(seg_path, s3_key):
                    uploaded_files.add(seg)

                    #Calculate progress
                    if total_segments > 0:
                        upload_progress = 95 + int((len(uploaded_files)/total_segments)* 5)
                        self.update_redis_status(video_id, "processing", upload_progress)
            
            if os.path.exists(playlist):
                playlist_key = f"{commit_id}/{base_name}/playlist.m3u8"
                if self.upload_segment(playlist,playlist_key):
                    send_kafka_event(topic="video.processed",message={
                        "video_id": video_id,
                        "commit_id": commit_id,
                        "playlist_url": playlist_key
                    })
                    logger.info(f"Uploaded playlist to {playlist_key}")
                    
            #Exit Condition
            if total_segments > 0 and len(uploaded_files) >=total_segments:
                break

            time.sleep(2)

    def sqs_worker(self):
        """Main worker loop that process SQS messages"""
        sqs = boto3.client('sqs',
        region_name=os.getenv('AWS_REGION'),
        endpoint_url=os.getenv('AWS_ENDPOINT_URL'),
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )
        while True:
            try:
                response = sqs.receive_message(
                    QueueUrl="http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/processing-queue",
                    MaxNumberOfMessages = 1,
                    WaitTimeSeconds = 20
                )

                if 'Messages' in response:
                    msg = response['Messages'][0]

                    upload_obj = json.loads(msg['Body'])

                    # file_name = body['Records'][0]['s3']['object']['key']
                    
                    logger.info(f"Procesing {upload_obj}")
                    self.process_video(upload_obj)

                    sqs.delete_message(
                        QueueUrl="http://localstack:4566/000000000000/processing-queue",
                        ReceiptHandle=msg['ReceiptHandle']
                    )
                else:
                    time.sleep(5)
            except Exception as e:
                logger.error(f"SQS Error: {e}")
                time.sleep(10)

if __name__ == "__main__":
    # AWS and Redis setup
    s3 = boto3.client('s3',
    region_name=os.getenv('AWS_REGION'),
    endpoint_url=os.getenv('AWS_ENDPOINT_URL'),
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    
    redis_conn = redis.Redis(
        host=os.getenv('REDIS_HOST'),
        port=os.getenv('REDIS_PORT'),
        password=os.getenv('REDIS_PASSWORD')
    )
    
    # Start processor
    processor = VideoProcessor()
    processor.sqs_worker()