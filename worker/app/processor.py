import boto3
import os
import subprocess
import time
import redis
import shutil
import json
import logging
import threading
from queue import Queue
from datetime import datetime
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

class VideoProcessor:
    def __init__(self):
        self.upload_queue = Queue()
        self.processing_lock = threading.Lock()
        self.current_progress = 0
        self.shared_path = os.getenv('SHARED_VOLUME_PATH', '/shared')
        self.cleanup_lock = threading.Lock()
        self.max_metadata_retries = 2
        self.metadata_timeout = 300  # 5 minutes

    def update_redis_status(self, video_id, status, progress=None):
        """Update processing status in Redis"""
        with self.processing_lock:
            if progress is not None and progress > self.current_progress:
                self.current_progress = progress
            elif progress is None:
                progress = self.current_progress
                
            update = {
                "video_id": video_id,
                "status": status,
                "progress": progress,
                "timestamp": datetime.utcnow().isoformat()
            }
            redis_conn.hset(f"video:{video_id}", mapping=update)
            redis_conn.publish(f"video_status:{video_id}", json.dumps(update))

    def get_video_duration(self, input_path):
        """Get video duration using ffprobe"""
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", input_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True
        )
        return float(result.stdout.decode().strip())

    def upload_segment(self, file_path, s3_key):
        """Upload a single segment to S3"""
        try:
            s3.upload_file(file_path, "video-processed", s3_key)
            return True
        except Exception as e:
            logger.error(f"Failed to upload segment {s3_key}: {e}")
            return False

    def _cleanup_files(self, *paths):
        """Thread-safe file cleanup"""
        with self.cleanup_lock:
            for path in paths:
                try:
                    if os.path.exists(path):
                        if os.path.isfile(path):
                            os.remove(path)
                            logger.info(f"Deleted file: {path}")
                        elif os.path.isdir(path):
                            shutil.rmtree(path)
                            logger.info(f"Deleted directory: {path}")
                except Exception as e:
                    logger.error(f"Error cleaning up {path}: {e}")

    def _retry_metadata_extraction(self, video_id,commit_id, file_path, max_retries):
        """Handle metadata extraction with retries"""
        for attempt in range(max_retries + 1):
            try:
                send_kafka_event(
                    topic="video.metadata.requests",
                    message={
                        "commit_id": commit_id,
                        'video_id': video_id,
                        'file_path': file_path,
                        'attempt': attempt,
                        'timestamp': datetime.utcnow().isoformat()
                    }
                )

                if self._wait_for_metadata_completion(video_id):
                    return True

                if attempt < max_retries:
                    logger.warning(f"Retrying metadata extraction (attempt {attempt + 1})")
                    time.sleep(5)  # Delay between retries

            except Exception as e:
                logger.error(f"Metadata attempt {attempt} failed: {e}")

        return False

    def _wait_for_metadata_completion(self, video_id):
        """Wait for metadata service to complete"""
        start_time = time.time()
        
        while time.time() - start_time < self.metadata_timeout:
            metadata_status = redis_conn.hget(f"video:{video_id}", "metadata_status")
            
            if metadata_status == "completed":
                logger.info(f"Metadata processing completed for {video_id}")
                return True
            elif metadata_status == "failed":
                logger.error(f"Metadata processing failed for {video_id}")
                return False
                
            time.sleep(2)
        
        logger.error(f"Metadata processing timed out for {video_id}")
        return False

    def _quarantine_file(self, file_path):
        """Move failed files to quarantine area"""
        quarantine_dir = f"{self.shared_path}/quarantine"
        os.makedirs(quarantine_dir, exist_ok=True)
        quarantine_path = f"{quarantine_dir}/{os.path.basename(file_path)}"
        shutil.move(file_path, quarantine_path)
        logger.warning(f"Moved to quarantine: {quarantine_path}")
        return quarantine_path

    def monitor_encoding_progress(self, process, video_id, total_duration):
        """Track encoding progress from FFmpeg output"""
        last_update = 0
        for line in process.stderr:
            if "time=" in line:
                try:
                    time_str = line.split("time=")[1].split()[0]
                    h, m, s = map(float, time_str.split(':'))
                    current_sec = h * 3600 + m * 60 + s
                    progress = min(95, int((current_sec / total_duration) * 95))

                    if progress > last_update + 2:
                        self.update_redis_status(video_id, "processing", progress)
                        last_update = progress
                except Exception as e:
                    logger.debug(f"Progress parse error: {e}")

    def upload_segments(self, output_dir, base_name, video_id, commit_id, duration):
        """Handle segment uploads in parallel"""
        uploaded_files = set()
        total_segments = 0

        while True:
            segments = [f for f in os.listdir(output_dir)
                      if f.endswith('.ts') and f not in uploaded_files]
            
            playlist = f"{output_dir}/playlist.m3u8"
            if os.path.exists(playlist):
                with open(playlist) as f:
                    total_segments = sum(1 for line in f if '.ts' in line)
            
            for seg in segments:
                seg_path = os.path.join(output_dir, seg)
                s3_key = f"{commit_id}/{base_name}/{seg}"

                if self.upload_segment(seg_path, s3_key):
                    uploaded_files.add(seg)
                    if total_segments > 0:
                        upload_progress = 95 + int((len(uploaded_files)/total_segments)*5)
                        self.update_redis_status(video_id, "uploading", upload_progress)
            
            if os.path.exists(playlist):
                playlist_key = f"{commit_id}/{base_name}/playlist.m3u8"
                if self.upload_segment(playlist, playlist_key):
                    send_kafka_event(
                        topic="video.processed",
                        message={
                            "video_id": video_id,
                            "commit_id": commit_id,
                            "playlist_url": playlist_key,
                            "duration": duration
                        }
                    )
                    logger.info(f"Uploaded playlist to {playlist_key}")
            
            if total_segments > 0 and len(uploaded_files) >= total_segments:
                break

            time.sleep(2)

    def process_video(self, upload_obj):
        """Main video processing pipeline"""
        file_name = upload_obj['fileName']
        commit_id = upload_obj['commitId']
        self.current_progress = 0
        
        input_path = f"{self.shared_path}/{file_name}"
        base_name = os.path.splitext(os.path.basename(file_name))[0]
        output_dir = f"{self.shared_path}/{base_name}_hls"
        
        # State tracking
        processing_success = False
        metadata_success = False

        try:
            # 1. Setup and download
            os.makedirs(output_dir, exist_ok=True)
            s3.download_file("video-raw", file_name, input_path)
            self.update_redis_status(file_name, "downloaded", 5)

            # 2. Start metadata extraction in parallel thread
            metadata_thread = threading.Thread(
                target=self._retry_metadata_extraction,
                args=(file_name,commit_id, input_path,self.max_metadata_retries)
            )
            metadata_thread.start()

            # 3. Process video
            duration = self.get_video_duration(input_path)
            process = subprocess.Popen(
                [
                    "ffmpeg", "-i", input_path,
                    "-c:v", "libx264", "-preset", "fast",
                    "-f", "hls", "-hls_time", "1",
                    "-hls_segment_filename", f"{output_dir}/segment_%03d.ts",
                    f"{output_dir}/playlist.m3u8"
                ],
                stderr=subprocess.PIPE,
                universal_newlines=True
            )

            # 4. Start monitoring threads
            progress_thread = threading.Thread(
                target=self.monitor_encoding_progress,
                args=(process, file_name, duration)
            )
            upload_thread = threading.Thread(
                target=self.upload_segments,
                args=(output_dir, base_name, file_name, commit_id, duration)
            )
            
            progress_thread.start()
            upload_thread.start()

            # 5. Wait for video processing to complete
            process.wait()
            progress_thread.join(timeout=30)
            upload_thread.join(timeout=120)
            processing_success = True

            # 6. Clean processed segments immediately
            self._cleanup_files(output_dir)
            self.update_redis_status(file_name, "processing_complete", 100)

            # 7. Wait for metadata completion
            metadata_thread.join(timeout=self.metadata_timeout * (self.max_metadata_retries + 1))
            metadata_success = redis_conn.hget(f"video:{file_name}", "metadata_status") == "completed"

            # 8. Final cleanup decision
            if metadata_success:
                self._cleanup_files(input_path)
            else:
                self._quarantine_file(input_path)

        except Exception as e:
            logger.error(f"Processing failed: {e}")
            self.update_redis_status(file_name, "failed", self.current_progress)
            send_kafka_event(
                topic="video.processing.errors",
                message={
                    'video_id': file_name,
                    'error': str(e),
                    'timestamp': datetime.utcnow().isoformat()
                }
            )
            self._cleanup_files(output_dir, input_path)
            
        finally:
            # Final status update
            status = {
                'processing_status': 'completed' if processing_success else 'failed',
                'metadata_status': 'completed' if metadata_success else 'failed',
                'timestamp': datetime.utcnow().isoformat()
            }
            redis_conn.hset(f"video:{file_name}", mapping=status)

    def sqs_worker(self):
        """Main worker loop that processes SQS messages"""
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
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=20
                )

                if 'Messages' in response:
                    msg = response['Messages'][0]
                    upload_obj = json.loads(msg['Body'])
                    
                    logger.info(f"Processing {upload_obj}")
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
        password=os.getenv('REDIS_PASSWORD'),
        decode_responses=True
    )
    
    # Start processor
    processor = VideoProcessor()
    processor.sqs_worker()