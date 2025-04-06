from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
import subprocess
import os
from pathlib import Path
import uuid

router = APIRouter(prefix="/hls", tags=["HLS Streaming"])

@router.post("/generate-precise-hls")
async def generate_precise_hls(timeline: list[dict]):
    """Generate HLS with exact duration matching the requested segments"""
    session_id = str(uuid.uuid4())
    output_dir = f"hls_output/{session_id}"
    os.makedirs(output_dir, exist_ok=True)

    try:
        # 1. Calculate total duration
        total_duration = sum(seg['end_time'] - seg['start_time'] for seg in timeline)
        
        # 2. Create input file for FFmpeg with precise segments
        concat_file = Path(f"{output_dir}/input.txt")
        with open(concat_file, 'w') as f:
            for segment in timeline:
                video_path = segment['source_video_id']
                if not Path(video_path).exists():
                    raise HTTPException(404, f"Video file not found: {video_path}")
                
                start = segment['start_time']
                duration = segment['end_time'] - start
                f.write(f"file '{Path(video_path).resolve()}'\n")
                f.write(f"inpoint {start}\n")
                f.write(f"duration {duration}\n")

        # 3. Generate HLS with exact duration
        playlist_path = f"{output_dir}/playlist.m3u8"
        cmd = [
            'ffmpeg', '-y',
            '-f', 'concat',
            '-safe', '0',
            '-i', str(concat_file),
            '-c:v', 'libx264',
            '-preset', 'fast',
            '-t', str(total_duration),  # Force exact output duration
            '-c:a', 'aac',
            '-f', 'hls',
            '-hls_time', '4',
            '-hls_list_size', '0',
            '-hls_segment_type', 'mpegts',
            '-hls_flags', 'independent_segments',
            '-hls_segment_filename', f'{output_dir}/segment_%03d.ts',
            playlist_path
        ]
        
        # Run FFmpeg
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise HTTPException(500, f"FFmpeg error: {result.stderr}")
        
        # Verify duration
        duration_check = subprocess.run([
            'ffprobe', '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            playlist_path
        ], capture_output=True, text=True)
        
        actual_duration = float(duration_check.stdout.strip())
        if abs(actual_duration - total_duration) > 0.1:
            raise HTTPException(500, f"Duration mismatch: expected {total_duration}, got {actual_duration}")

        return {
            "playlist_url": f"/hls/playlist/{session_id}/playlist.m3u8",
            "session_id": session_id,
            "expected_duration": total_duration,
            "actual_duration": actual_duration
        }

    except Exception as e:
        raise HTTPException(500, f"Error: {str(e)}")

@router.get("/hls/playlist/{session_id}/{filename}")
async def serve_hls_playlist(session_id: str, filename: str):
    """Serve the generated HLS playlist"""
    file_path = Path(f"hls_output/{session_id}/{filename}")
    if not file_path.exists():
        raise HTTPException(404, "File not found")
    
    return FileResponse(
        file_path,
        media_type="application/vnd.apple.mpegurl",
        headers={
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "no-cache"
        }
    )