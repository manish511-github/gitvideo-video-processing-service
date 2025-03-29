from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
import subprocess
import os
from pathlib import Path
import uuid

router = APIRouter(prefix="/hls", tags=["HLS Streaming"])

@router.post("/generate")
async def generate_hls_timeline(timeline: list[dict]):
    """Generate precise HLS with exact duration matching"""
    session_id = str(uuid.uuid4())
    output_dir = Path(f"hls_output/{session_id}")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # 1. Process segments with timestamp reset
        temp_files = []
        total_duration = 0.0
        
        for i, segment in enumerate(timeline):
            video_file = Path(segment['source_video_id']).resolve()
            if not video_file.exists():
                raise HTTPException(404, f"Video file not found: {video_file}")
            
            start = segment['start_time']
            end = segment['end_time']
            duration = end - start
            total_duration += duration
            
            temp_file = output_dir / f"segment_{i}.mp4"
            cmd = [
                'ffmpeg', '-y',
                '-ss', str(start),
                '-to', str(end),
                '-i', str(video_file),
                '-c:v', 'libx264',  # Re-encode to reset timestamps
                '-preset', 'ultrafast',
                '-x264-params', 'force-cfr=1',
                '-vsync', 'cfr',
                '-c:a', 'aac',
                '-filter_complex', '[0:v]setpts=N/FRAME_RATE/TB[v];[0:a]asetpts=N/SR/TB[a]',
                '-map', '[v]',
                '-map', '[a]',
                '-avoid_negative_ts', 'make_zero',
                str(temp_file)
            ]
            subprocess.run(cmd, check=True, capture_output=True)
            temp_files.append(temp_file)

        # 2. Generate concat list
        concat_file = output_dir / "input.txt"
        with open(concat_file, 'w') as f:
            for file in temp_files:
                f.write(f"file '{file.absolute()}'\n")

        # 3. Generate HLS with strict duration control
        output_prefix = output_dir / "stream"
        cmd = [
            'ffmpeg', '-y',
            '-f', 'concat',
            '-safe', '0',
            '-i', str(concat_file),
            '-c:v', 'libx264',
            '-preset', 'fast',
            '-x264-params', 'force-cfr=1',
            '-vsync', 'cfr',
            '-c:a', 'aac',
            '-f', 'hls',
            '-hls_time', '4',
            '-hls_list_size', '0',
            '-hls_flags', 'independent_segments+discont_start',
            '-hls_segment_type', 'mpegts',
            '-hls_segment_filename', f'{output_prefix}_%03d.ts',
            '-hls_playlist_type', 'vod',
            '-t', str(total_duration),  # Force exact duration
            f'{output_prefix}.m3u8'
        ]
        
        subprocess.run(cmd, check=True, capture_output=True)
        
        # Verify duration
        verify_cmd = [
            'ffprobe',
            '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            f'{output_prefix}.m3u8'
        ]
        result = subprocess.run(verify_cmd, capture_output=True, text=True)
        actual_duration = float(result.stdout.strip())
        
        if abs(actual_duration - total_duration) > 0.1:
            raise HTTPException(500, f"Duration mismatch: expected {total_duration}s, got {actual_duration}s")
            
        return {
            "playlist_url": f"/hls/stream/{session_id}/stream.m3u8",
            "session_id": session_id,
            "expected_duration": total_duration,
            "actual_duration": actual_duration
        }

    except subprocess.CalledProcessError as e:
        error_msg = f"FFmpeg error: {e.stderr.decode()}" if e.stderr else "FFmpeg failed"
        raise HTTPException(500, error_msg)
    except Exception as e:
        raise HTTPException(500, f"Error: {str(e)}")

    
@router.get("/stream/{session_id}/{filename}")
async def serve_hls(session_id: str, filename: str):
    file_path = Path(f"hls_output/{session_id}/{filename}")
    if not file_path.exists():
        raise HTTPException(404, "File not found")
    
    return FileResponse(
        file_path,
        media_type="application/vnd.apple.mpegurl" if filename.endswith('.m3u8') else "video/MP2T",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Access-Control-Allow-Origin": "*"
        }
    )