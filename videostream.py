from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import boto3
from fastapi.responses import PlainTextResponse

router = APIRouter(prefix="/video")

# Configure LocalStack S3 client (using your existing Redis credentials style)
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1'
)

BUCKET_NAME = "test-bucket"  # Match your LocalStack bucket

class TimelineSegment(BaseModel):
    source_video_id: str  # e.g., "video1.mp4"
    start_time: float     # Start time in seconds
    end_time: float       # End time in seconds

class TimelineRequest(BaseModel):
    timeline: list[TimelineSegment]

@router.post("/generate-hls-manifest")
async def generate_hls_manifest(request: TimelineRequest):
    """Generate HLS manifest from a timeline of segments."""
    hls_manifest = "#EXTM3U\n#EXT-X-VERSION:7\n"
    
    for segment in request.timeline:
        video_url = f"http://localhost:4566/{BUCKET_NAME}/sources/{segment.source_video_id}"
        duration = segment.end_time - segment.start_time
        
        hls_manifest += f"#EXTINF:{duration},\n"
        hls_manifest += f"{video_url}#t={segment.start_time},{segment.end_time}\n"
    
    hls_manifest += "#EXT-X-ENDLIST\n"
    
    # Save to LocalStack S3
    manifest_key = "manifests/dynamic_timeline.m3u8"
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=manifest_key,
            Body=hls_manifest,
            ContentType="application/vnd.apple.mpegurl"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"S3 Upload Error: {str(e)}")
    
    return {
        "manifest_url": f"http://localhost:4566/{BUCKET_NAME}/{manifest_key}"
    }

@router.get("/get-manifest/{manifest_key}", response_class=PlainTextResponse)
async def get_manifest(manifest_key: str):
    """Fetch a manifest from S3 (debug endpoint)."""
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=manifest_key)
        return response['Body'].read().decode('utf-8')
    except Exception as e:
        raise HTTPException(status_code=404, detail="Manifest not found")