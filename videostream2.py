from fastapi import APIRouter
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles
import subprocess
import os
from pathlib import Path
import uuid
import m3u8

vd_router = APIRouter(prefix="/stream", tags=["HLS Streaming"])
vd_router.mount("/hls", StaticFiles(directory="hls"), name="hls")
vd_router.mount("/trimmed", StaticFiles(directory="trimmed"), name="trimmed")

VIDEO_SOURCES = {
    "video1": "hls/video1/video1.m3u8",
    "video2": "hls/video2/video2.m3u8",
}

TRIMMED_DIR = "trimmed"
os.makedirs(TRIMMED_DIR, exist_ok=True)

def extract_segment(m3u8_path: str, start_time: float, end_time: float):
    playlist = m3u8.load(m3u8_path)
    segments = []
    current_time = 0.0

    for segment in playlist.segments:
        seg_start = current_time
        seg_end = current_time + segment.duration

        if seg_end > start_time and seg_start < end_time:
            segments.append({
                "uri": segment.uri,
                "duration": segment.duration,
                "start_time": max(start_time - seg_start, 0),
                "end_time": min(end_time - seg_start, segment.duration),
                "base_path": os.path.dirname(m3u8_path),
                "video_id": os.path.basename(os.path.dirname(m3u8_path))  # Add video_id
            })

        current_time += segment.duration
        if current_time >= end_time:
            break

    return segments

@vd_router.get("/custom-playlist")
def generate_custom_playlist():
    timeline = [
        {"video": "video1", "start": 6, "end": 20},
        {"video": "video2", "start": 6, "end": 15}
    ]

    playlist_content = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        "#EXT-X-INDEPENDENT-SEGMENTS",
        "#EXT-X-TARGETDURATION:2",  # Fixed target duration
        "#EXT-X-MEDIA-SEQUENCE:0"
    ]
    
    segments_to_add = []

    for entry in timeline:
        video = entry["video"]
        start = entry["start"]
        end = entry["end"]
        m3u8_path = VIDEO_SOURCES[video]
        segs = extract_segment(m3u8_path, start, end)

        for s in segs:
            input_path = os.path.join(s["base_path"], s["uri"])
            duration = s["duration"]
            
            if s["start_time"] > 0 or s["end_time"] < duration:
                duration = s["end_time"] - s["start_time"]
                trimmed_filename = f"{s['video_id']}_{uuid.uuid4().hex}.ts"
                trimmed_path = os.path.join(TRIMMED_DIR, trimmed_filename)

                subprocess.run([
                    "ffmpeg", "-y",
                    "-ss", str(s["start_time"]),
                    "-i", input_path,
                    "-t", str(duration),
                    "-c", "copy",
                    "-avoid_negative_ts", "make_zero",
                    trimmed_path
                ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                
                segment_uri = f"/trimmed/{trimmed_filename}"
            else:
                segment_uri = f"/hls/{s['video_id']}/{s['uri']}"

            segments_to_add.append({
                "uri": segment_uri,
                "duration": duration,
                "video_id": s["video_id"]
            })

    # Add segments with proper discontinuity tags
    for i, seg in enumerate(segments_to_add):
        # Add discontinuity when video_id changes
        if i > 0 and seg['video_id'] != segments_to_add[i-1]['video_id']:
            playlist_content.append("#EXT-X-DISCONTINUITY")
        
        playlist_content.append(f"#EXTINF:{seg['duration']:.3f},")
        playlist_content.append(seg['uri'])

    playlist_content.append("#EXT-X-ENDLIST")

    return Response(
        content="\n".join(playlist_content),
        media_type="application/vnd.apple.mpegurl",
        headers={
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "no-cache",
            "Content-Type": "application/vnd.apple.mpegurl"
        }
    )