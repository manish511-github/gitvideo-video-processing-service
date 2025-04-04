from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles

import subprocess
import os
from pathlib import Path
import uuid
import m3u8


vd_router = APIRouter(prefix="/stream", tags=["HLS Streaming"])

vd_router.mount("/hls", StaticFiles(directory="hls"), name="hls")

VIDEO_SOURCES = {
    "video1": "hls/video1/video1.m3u8",
    "video2": "hls/video2/video2.m3u8",
}

def extract_segment(m3u8_path : str, start_time : float, end_time: float):
    playlist = m3u8.load(m3u8_path)
    segments = []
    current_time = 0

    for segment in playlist.segments:
        seg_duration = segment.duration
        if current_time + seg_duration >= start_time and current_time < end_time:
            segments.append({
                "uri": segment.uri,
                "duration": seg_duration
            })
        current_time += seg_duration
        if current_time>=end_time:
            break
    return segments

@vd_router.get("/custom-playlist")
def generate_custom_playlist():
    # Example timeline
    timeline = [
        {"video": "video1", "start": 6, "end": 11},
        {"video": "video2", "start": 2, "end": 5}
    ]
    playlist_content = "#EXTM3U\n#EXT-X-VERSION:3\n"
    target_duration = 0
    segments_to_add = []

    for entry in timeline:
        video = entry["video"]
        start = entry["start"]
        end = entry["end"]
        m3u8_path = VIDEO_SOURCES[video]
        segs = extract_segment(m3u8_path, start, end)
        segments_to_add.extend([
            {
                "uri":f"/hls/{video}/{s['uri']}",
                "duration": s["duration"]
            } for s in segs
        ])

        max_seg = max([s["duration"] for s in segs], default=0)
        if max_seg > target_duration:
            target_duration = max_seg
    playlist_content += f"#EXT-X-TARGETDURATION:{int(target_duration)+1}\n"
    playlist_content += "#EXT-X-MEDIA-SEQUENCE:0\n"
    for seg in segments_to_add:
        playlist_content += f"#EXTINF:{seg['duration']},\n{seg['uri']}\n"

    playlist_content += "#EXT-X-ENDLIST\n"
    return Response(content=playlist_content, media_type="application/vnd.apple.mpegurl")


