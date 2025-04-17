import os
import json
import subprocess
from confluent_kafka import Consumer, Producer
import redis
import logging
from datetime import datetime
import boto3  # Import boto3 if you need to download from S3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('metadata_service.log')
    ]
)
logger = logging.getLogger(__name__)

# Assuming you have configured your S3 client elsewhere if needed
# s3 = boto3.client('s3')

class MetadataService:
    def __init__(self):
        logger.info("Initializing MetadataService")
        
        try:
            # Kafka Configuration
            self.consumer = Consumer({
                'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
                'group.id': 'metadata-service',
                'auto.offset.reset': 'earliest'
            })
            self.producer = Producer({
                'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS')
            })
            logger.info("Kafka producers and consumers initialized")

            # Redis connection
            self.redis = redis.Redis(
                host=os.getenv('REDIS_HOST'),
                port=os.getenv('REDIS_PORT'),
                password=os.getenv('REDIS_PASSWORD'),
                decode_responses=True
            )
            # Test Redis connection
            self.redis.ping()
            logger.info("Redis connection established and verified")

            self.request_topic = os.getenv('KAFKA_METADATA_TOPIC')
            self.result_topic = os.getenv('KAFKA_RESULT_TOPIC')
            logger.info(f"Configured to consume from {self.request_topic} and produce to {self.result_topic}")

        except Exception as e:
            logger.error(f"Failed to initialize MetadataService: {str(e)}")
            raise

    def _parse_frame_rate(self, frame_rate_str):
        """Parses frame rate string (e.g., '30/1') to float."""
        try:
            num, den = map(int, frame_rate_str.split('/'))
            result = num / den
            logger.debug(f"Parsed frame rate {frame_rate_str} to {result}")
            return result
        except Exception as e:
            logger.warning(f"Failed to parse frame rate {frame_rate_str}: {e}")
            return 0.0

    def get_comprehensive_video_metadata(self, input_path):
        """Extracts exhaustive video metadata using FFmpeg"""
        logger.info(f"Starting comprehensive metadata extraction for {input_path}")
        
        try:
            # Log the FFprobe command being executed
            ffprobe_cmd = [
                "ffprobe", "-v", "quiet", "-print_format", "json",
                "-show_format", "-show_streams", "-show_frames",
                "-show_packets", "-show_chapters", "-show_programs",
                "-show_private_data", input_path
            ]
            logger.debug(f"Executing FFprobe command: {' '.join(ffprobe_cmd)}")

            # Run FFprobe to get all available metadata
            result = subprocess.run(
                ffprobe_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True
            )
            
            # Log stderr if there's any output
            if result.stderr:
                logger.debug(f"FFprobe stderr output: {result.stderr.decode()}")

            raw_metadata = json.loads(result.stdout.decode())
            logger.debug(f"Raw metadata size: {len(result.stdout)} bytes")

            # Get encoding information
            logger.debug("Getting encoding information")
            encoding_info = self._get_encoding_info(input_path)

            # Parse into structured format
            metadata = {
                "technical": self._parse_technical_metadata(raw_metadata),
                "media": self._parse_media_metadata(raw_metadata),
                "encoding": encoding_info,
                "analysis": self._analyze_video_characteristics(input_path),
                "system": {
                    "processing_time": datetime.utcnow().isoformat(),
                    "ffmpeg_version": self._get_ffmpeg_version(),
                    "hardware_acceleration": self._detect_hardware_acceleration()
                }
            }

            logger.info(f"Successfully extracted metadata for {input_path}")
            logger.debug(f"Metadata summary - Duration: {metadata['technical']['general'].get('duration')}s, "
                        f"Resolution: {metadata['technical']['video_streams'][0]['width']}x{metadata['technical']['video_streams'][0]['height']}")

            return metadata

        except subprocess.CalledProcessError as e:
            logger.error(f"FFprobe command failed for {input_path}. Return code: {e.returncode}, Error: {e.stderr.decode()}")
            return None
        except Exception as e:
            logger.error(f"Failed to extract comprehensive metadata for {input_path}: {e}", exc_info=True)
            return None

    def _parse_technical_metadata(self, raw_metadata):
        """Extracts technical video specifications"""
        logger.debug("Parsing technical metadata")
        
        tech_data = {
            "general": {},
            "video_streams": [],
            "audio_streams": [],
            "subtitles": [],
            "data_streams": []
        }

        try:
            # Format-level information
            if "format" in raw_metadata:
                tech_data["general"] = {
                    "format_name": raw_metadata["format"].get("format_name"),
                    "format_long_name": raw_metadata["format"].get("format_long_name"),
                    "duration": float(raw_metadata["format"].get("duration", 0)),
                    "size": int(raw_metadata["format"].get("size", 0)),
                    "bit_rate": int(raw_metadata["format"].get("bit_rate", 0)),
                    "probe_score": raw_metadata["format"].get("probe_score"),
                    "tags": raw_metadata["format"].get("tags", {})
                }
                logger.debug(f"Found format: {tech_data['general']['format_name']}")

            # Stream information
            stream_count = len(raw_metadata.get("streams", []))
            logger.debug(f"Processing {stream_count} streams")
            
            for stream in raw_metadata.get("streams", []):
                stream_type = stream.get("codec_type")
                stream_info = {
                    "index": stream.get("index"),
                    "codec_name": stream.get("codec_name"),
                    "codec_long_name": stream.get("codec_long_name"),
                    "profile": stream.get("profile"),
                    "bit_rate": int(stream.get("bit_rate", 0)),
                    "time_base": stream.get("time_base"),
                    "start_time": stream.get("start_time"),
                    "duration_ts": stream.get("duration_ts"),
                    "nb_frames": stream.get("nb_frames"),
                    "disposition": stream.get("disposition", {}),
                    "sample_aspect_ratio": stream.get("sample_aspect_ratio"),
                    "display_aspect_ratio": stream.get("display_aspect_ratio"),
                    "r_frame_rate": stream.get("r_frame_rate"),
                    "avg_frame_rate": stream.get("avg_frame_rate"),
                    "tags": stream.get("tags", {})
                }

                if stream_type == "video":
                    video_info = {
                        **stream_info,
                        "width": stream.get("width"),
                        "height": stream.get("height"),
                        "coded_width": stream.get("coded_width"),
                        "coded_height": stream.get("coded_height"),
                        "pix_fmt": stream.get("pix_fmt"),
                        "field_order": stream.get("field_order"),
                        "color_range": stream.get("color_range"),
                        "color_space": stream.get("color_space"),
                        "color_transfer": stream.get("color_transfer"),
                        "color_primaries": stream.get("color_primaries"),
                        "chroma_location": stream.get("chroma_location"),
                        "frame_rate": self._parse_frame_rate(stream.get("avg_frame_rate", "0/0")),
                        "bits_per_raw_sample": stream.get("bits_per_raw_sample")
                    }
                    tech_data["video_streams"].append(video_info)
                    logger.debug(f"Added video stream: {video_info['codec_name']} {video_info['width']}x{video_info['height']}")

                elif stream_type == "audio":
                    audio_info = {
                        **stream_info,
                        "sample_rate": stream.get("sample_rate"),
                        "channels": stream.get("channels"),
                        "channel_layout": stream.get("channel_layout"),
                        "bits_per_sample": stream.get("bits_per_sample")
                    }
                    tech_data["audio_streams"].append(audio_info)
                    logger.debug(f"Added audio stream: {audio_info['codec_name']} {audio_info['channels']}ch")

                elif stream_type == "subtitle":
                    tech_data["subtitles"].append(stream_info)
                    logger.debug(f"Added subtitle stream: {stream_info['codec_name']}")
                else:
                    tech_data["data_streams"].append(stream_info)
                    logger.debug(f"Added data stream: {stream_info['codec_name']}")

            logger.info(f"Processed technical metadata with {len(tech_data['video_streams'])} video, "
                      f"{len(tech_data['audio_streams'])} audio streams")
            return tech_data

        except Exception as e:
            logger.error(f"Error parsing technical metadata: {e}", exc_info=True)
            raise

    def _parse_media_metadata(self, raw_metadata):
        """Extracts media-specific metadata and tags"""
        logger.debug("Parsing media metadata")
        
        media_data = {
            "general": {},
            "video": [],
            "audio": []
        }

        try:
            # Format tags
            if "format" in raw_metadata and "tags" in raw_metadata["format"]:
                media_data["general"] = {
                    "title": raw_metadata["format"]["tags"].get("title"),
                    "artist": raw_metadata["format"]["tags"].get("artist"),
                    "album": raw_metadata["format"]["tags"].get("album"),
                    "date": raw_metadata["format"]["tags"].get("date"),
                    "creation_time": raw_metadata["format"]["tags"].get("creation_time"),
                    "encoder": raw_metadata["format"]["tags"].get("encoder"),
                    "comment": raw_metadata["format"]["tags"].get("comment"),
                    "genre": raw_metadata["format"]["tags"].get("genre"),
                    "copyright": raw_metadata["format"]["tags"].get("copyright"),
                    "Performer": raw_metadata["format"]["tags"].get("Performer"),
                    "composer": raw_metadata["format"]["tags"].get("composer"),
                    "disc": raw_metadata["format"]["tags"].get("disc"),
                    "track": raw_metadata["format"]["tags"].get("track")
                }
                logger.debug(f"Found media tags: {list(media_data['general'].keys())}")

            # Stream tags
            for stream in raw_metadata.get("streams", []):
                if "tags" not in stream:
                    continue

                tags = stream["tags"]
                stream_tags = {
                    "language": tags.get("language"),
                    "handler_name": tags.get("handler_name"),
                    "vendor_id": tags.get("vendor_id"),
                    "title": tags.get("title"),
                    "DESCRIPTION": tags.get("DESCRIPTION"),
                    "comment": tags.get("comment")
                }

                if stream.get("codec_type") == "video":
                    media_data["video"].append(stream_tags)
                    logger.debug(f"Added video tags for stream {stream.get('index')}")
                elif stream.get("codec_type") == "audio":
                    media_data["audio"].append(stream_tags)
                    logger.debug(f"Added audio tags for stream {stream.get('index')}")

            logger.info(f"Processed media metadata with {len(media_data['video'])} video and {len(media_data['audio'])} audio tags")
            return media_data

        except Exception as e:
            logger.error(f"Error parsing media metadata: {e}", exc_info=True)
            raise

    def _get_encoding_info(self, input_path):
        """Gets encoding parameters from the first video frame"""
        logger.debug(f"Getting encoding info for {input_path}")
        
        try:
            ffprobe_cmd = [
                "ffprobe", "-v", "quiet", "-select_streams", "v:0",
                "-show_frames", "-read_intervals", "%+#1", "-show_entries",
                "frame=color_space,color_primaries,color_transfer,side_data_list,pict_type,top_field_first,interlaced_frame,repeat_pict",
                "-of", "json", input_path
            ]
            logger.debug(f"Executing FFprobe command for encoding info: {' '.join(ffprobe_cmd)}")

            result = subprocess.run(
                ffprobe_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True
            )
            
            frame_data = json.loads(result.stdout.decode())
            if frame_data.get("frames"):
                logger.debug(f"Retrieved encoding info: {frame_data['frames'][0]}")
                return frame_data["frames"][0]
            
            logger.debug("No encoding info found in first frame")
            return {}
            
        except subprocess.CalledProcessError as e:
            logger.warning(f"Couldn't get encoding info for {input_path}: {e.stderr.decode()}")
            return {}
        except Exception as e:
            logger.warning(f"Error getting encoding info for {input_path}: {e}")
            return {}

    def _analyze_video_characteristics(self, input_path):
        """Performs basic video analysis"""
        logger.debug(f"Analyzing video characteristics for {input_path}")
        analysis = {}
        
        try:
            # Get keyframe information
            keyframes_cmd = [
                "ffprobe", "-v", "quiet", "-select_streams", "v:0",
                "-show_frames", "-show_entries", "frame=key_frame,pict_type,coded_picture_number",
                "-of", "csv", input_path
            ]
            logger.debug(f"Executing keyframe analysis command: {' '.join(keyframes_cmd)}")

            keyframes = subprocess.run(
                keyframes_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            ).stdout

            keyframe_count = keyframes.count(",1,")
            frame_types = {
                "I": keyframes.count("I"),
                "P": keyframes.count("P"),
                "B": keyframes.count("B")
            }
            total_frames = len(keyframes.splitlines()) - 1 if keyframes.strip() else 0 # Exclude header

            analysis.update({
                "keyframe_count": keyframe_count,
                "frame_types": frame_types,
                "total_frames": total_frames,
                "keyframe_interval": total_frames / keyframe_count if keyframe_count > 0 and total_frames > 0 else 0
            })

            logger.debug(f"Frame analysis - Total: {total_frames}, Keyframes: {keyframe_count}, Types: {frame_types}")

            # Get motion vectors (if available)
            try:
                motion_cmd = [
                    "ffmpeg", "-i", input_path, "-vf", "extractplanes=y",
                    "-f", "null", "-"
                ]
                logger.debug(f"Attempting motion analysis with command: {' '.join(motion_cmd)}")
                
                subprocess.run(
                    motion_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=True
                )
                analysis["motion_analysis"] = "supported"
                logger.debug("Motion analysis supported")
            except subprocess.CalledProcessError:
                analysis["motion_analysis"] = "unsupported"
                logger.debug("Motion analysis not supported")
            except Exception as e:
                analysis["motion_analysis"] = "error"
                logger.warning(f"Motion analysis failed: {e}")

            logger.info(f"Completed video analysis with {total_frames} frames")
            return analysis

        except Exception as e:
            logger.error(f"Video analysis failed for {input_path}: {e}", exc_info=True)
            analysis["error"] = str(e)
            return analysis

    def _get_ffmpeg_version(self):
        """Gets FFmpeg version information"""
        logger.debug("Getting FFmpeg version")
        
        try:
            result = subprocess.run(
                ["ffmpeg", "-version"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            version = result.stdout.splitlines()[0].strip()
            logger.debug(f"FFmpeg version: {version}")
            return version
        except Exception as e:
            logger.warning(f"Couldn't get FFmpeg version: {e}")
            return "unknown"

    def _detect_hardware_acceleration(self):
        """Detects available hardware acceleration"""
        logger.debug("Detecting hardware acceleration support")
        
        try:
            result = subprocess.run(
                ["ffmpeg", "-hwaccels"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            hwaccels = [line.strip() for line in result.stdout.splitlines()[1:] if line.strip()]
            logger.debug(f"Detected hardware acceleration methods: {hwaccels}")
            return hwaccels
        except Exception as e:
            logger.warning(f"Couldn't detect hardware acceleration: {e}")
            return []

    def extract_metadata(self, file_path):
        """
        Extracts comprehensive metadata from the video file.
        """
        logger.info(f"Starting metadata extraction for {file_path}")
        metadata = self.get_comprehensive_video_metadata(file_path)
        
        if metadata:
            logger.info(f"Successfully extracted metadata for {file_path}")
        else:
            logger.error(f"Failed to extract metadata for {file_path}")
            
        return metadata

    def process_message(self, msg):
        """Process incoming Kafka message"""
        logger.debug("Processing new Kafka message")
        
        message = None  # Initialize message to None
        try:
            if msg is None:
                logger.warning("Received None message")
                return
                
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                return

            # Correct the JSON loading: use loads() for bytes
            message_str = msg.value().decode('utf-8') # Decode bytes to string
            message = json.loads(message_str)
            logger.debug(f"Received message: {message_str[:200]}...")  # Log first 200 chars to avoid huge logs

            video_id = message['video_id']
            file_path = message['file_path']
            logger.info(f"Processing video {video_id} from {file_path}")

            # If you need to download from S3 first:
            # input_path = f"/tmp/{video_id}"
            # s3.download_file("your-s3-bucket", file_path, input_path)
            # file_path = input_path

            # Update redis status
            self.redis.hset(f"video:{video_id}", "status", "extracting_metadata")
            logger.debug(f"Updated Redis status for {video_id} to 'extracting_metadata'")

            # Extract comprehensive metadata
            metadata = self.extract_metadata(file_path)

            # Clean up temporary file if downloaded
            # if 'input_path' in locals() and os.path.exists(input_path):
            #     os.remove(input_path)

            if not metadata:
                raise Exception("Comprehensive metadata extraction returned empty result")

            # Prepare result message
            result = {
                'video_id': video_id,
                'metadata': metadata,
                'status': 'metadata_extracted',
                'timestamp': datetime.utcnow().isoformat()
            }
            
            logger.debug(f"Producing result to {self.result_topic}")
            self.producer.produce(
                self.result_topic,
                value=json.dumps(result)
            )
            self.producer.flush()
            logger.info(f"Successfully produced metadata result for {video_id}")

            # Update Redis
            self.redis.hset(f"video:{video_id}", mapping={
                'status': 'metadata_complete',
                'metadata': json.dumps(metadata)
            })
            self.redis.hset(f"video:{video_id}", "metadata_status","completed")
            logger.info("Redis HSET done")            
            logger.debug(f"Updated Redis status for {video_id} to 'metadata_complete'")

        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON message: {e}, raw message: {msg.value()}")
        except KeyError as e:
            logger.error(f"Missing required field in message: {e}")
            if message:
                error_result = {
                    'video_id': message.get('video_id'),
                    'error': f"Missing required field: {str(e)}",
                    'status': 'failed',
                    'timestamp': datetime.utcnow().isoformat()
                }
                self._send_error_result(error_result)
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
            if message:
                error_result = {
                    'video_id': message.get('video_id'),
                    'error': str(e),
                    'status': 'failed',
                    'timestamp': datetime.utcnow().isoformat()
                }
                self._send_error_result(error_result)
            else:
                logger.error("Could not access message to send error result.")

    def _send_error_result(self, error_result):
        """Helper method to send error results to Kafka"""
        logger.error(f"Sending error result: {error_result}")
        try:
            self.producer.produce(
                self.result_topic,
                value=json.dumps(error_result)
            )
            self.producer.flush()
            logger.info("Error result successfully sent to Kafka")
        except Exception as e:
            logger.error(f"Failed to send error result to Kafka: {e}")

    def run(self):
        """Main Service Loop"""
        logger.info("Starting MetadataService main loop")
        
        try:
            self.consumer.subscribe([self.request_topic])
            logger.info(f"Subscribed to Kafka topic {self.request_topic}")
            
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue
                    
                try:
                    self.process_message(msg)
                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)
                    
        except KeyboardInterrupt:
            logger.info("Received KeyboardInterrupt, shutting down gracefully...")
        except Exception as e:
            logger.error(f"Fatal error in main loop: {e}", exc_info=True)
        finally:
            logger.info("Cleaning up resources...")
            self.consumer.close()
            logger.info("Consumer closed")
            logger.info("MetadataService shutdown complete")

if __name__ == "__main__":
    try:
        logger.info("Starting MetadataService")
        service = MetadataService()
        service.run()
    except Exception as e:
        logger.error(f"Failed to start MetadataService: {e}", exc_info=True)
        raise