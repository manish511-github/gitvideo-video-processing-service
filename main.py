from fastapi import FastAPI
import redis.asyncio as redis
from bullmq import Worker
from dataclasses import dataclass
from typing import Dict, List
import logging
import threading
from datetime import datetime
import asyncio

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Initialize async Redis client with password
redis_client = redis.Redis(
    host='localhost',
    port=6379,
    db=0,
    password='mysecurepassword',  # Replace with your actual Redis password
    decode_responses=True
)

@dataclass
class SourceVideo:
    id: int
    path: str
    frame_rate: float
    duration: float

@dataclass
class Segment:
    source_id: int 
    source_start_time: float
    source_end_time: float
    global_start_time: float

    def duration(self) -> float:
        return self.source_end_time - self.source_start_time

class Timeline:
    def __init__(self):
        self.segments: List[Segment] = []
        self.sources: Dict[str, SourceVideo] = {}

    def add_source(self, source: SourceVideo):
        self.sources[source.id] = source

    def total_duration(self) -> float:
        return sum(seg.duration() for seg in self.segments)

    def update_global_starts(self):
        current_global_start = 0.0
        for segment in self.segments:
            segment.global_start_time = current_global_start
            current_global_start += segment.duration()

class VideoVersionControl:
    def __init__(self):
        self.timeline = Timeline()
        self.history = []

    def commit(self, operation_type: str, params: dict):
        self.history.append((operation_type, params, datetime.now()))
        self.timeline.update_global_starts()

    def cut(self, t1: float, t2: float):
        new_segments = []
        for seg in self.timeline.segments:
            seg_start = seg.global_start_time
            seg_end = seg_start + seg.duration()
            if seg_end < t1 or seg_start > t2:
                new_segments.append(seg)
            else:
                if seg_start < t1:
                    new_seg = Segment(seg.source_id, seg.source_start_time,
                                      seg.source_start_time + (t1 - seg_start),
                                      seg.global_start_time)
                    new_segments.append(new_seg)
                if seg_end > t2:
                    new_seg = Segment(seg.source_id,
                                      seg.source_start_time + (t2 - seg_start),
                                      seg.source_end_time, 0.0)
                    new_segments.append(new_seg)
        self.timeline.segments = new_segments
        self.commit("cut", {"t1": t1, "t2": t2})

    def insert(self, new_seg: Segment, t: float):
        new_segments = []
        inserted = False
        for seg in self.timeline.segments:
            seg_start = seg.global_start_time
            seg_end = seg_start + seg.duration()
            if not inserted and t <= seg_start:
                new_segments.append(new_seg)
                new_segments.append(seg)
                inserted = True
            elif seg_start <= t < seg_end:
                split_point = seg.source_start_time + (t - seg_start)
                if split_point > seg.source_start_time:
                    before = Segment(seg.source_id, seg.source_start_time,
                                     split_point, seg.global_start_time)
                    new_segments.append(before)
                new_segments.append(new_seg)
                if split_point < seg.source_end_time:
                    after = Segment(seg.source_id, split_point,
                                    seg.source_end_time, 0.0)
                    new_segments.append(after)
                inserted = True
            else:
                new_segments.append(seg)
        if not inserted:
            new_segments.append(new_seg)
        self.timeline.segments = new_segments
        self.commit("insert", {"segment": (new_seg.source_id, new_seg.source_start_time, new_seg.source_end_time), "t": t})

    def merge(self, other_timeline: Timeline):
        for seg in other_timeline.segments:
            if seg.source_id not in self.timeline.sources:
                raise ValueError(f"Source {seg.source_id} not registered in main timeline. Add it first.")
        self.timeline.segments.extend(other_timeline.segments)
        self.commit("merge", {"segments": [(seg.source_id, seg.source_start_time, seg.source_end_time) 
                                           for seg in other_timeline.segments]})

    def update(self, t1: float, t2: float, new_seg: Segment):
        if t2 <= t1:
            raise ValueError("t2 must be greater than t1")
        if new_seg.source_id not in self.timeline.sources:
            raise ValueError(f"Source {new_seg.source_id} not registered in timeline. Add it first.")
        self.cut(t1, t2)
        self.insert(new_seg, t1)

vvc = VideoVersionControl()

async def process_change(job, jobid):
    logger.info("hello")
    try:
        print(f"Processing job: {job.data}")

        # Extract job data
        change_id = job.data["changeId"]
        video_id = job.data["videoId"]
        operation = job.data["operation"]
        source_video_id = job.data.get("sourceVideoId")
        start = job.data.get("start", 0.0)
        end = job.data.get("end", 0.0)
        at = job.data.get("at", 0.0)

        # Perform the operation
        if operation == "cut":
            vvc.cut(start, end)
        elif operation == "insert":
            new_seg = Segment(source_video_id, start, end, 0.0)
            vvc.insert(new_seg, at)
        elif operation == "merge":
            other_timeline = Timeline()
            other_timeline.segments.append(Segment(source_video_id, start, end, 0.0))
            vvc.merge(other_timeline)
        elif operation == "update":
            new_seg = Segment(source_video_id, start, end, 0.0)
            vvc.update(at, end, new_seg)
        else:
            raise ValueError(f"Unknown operation: {operation}")

        # Return the updated timeline
        timeline = [
            {
                "sourceVideoId": seg.source_id,
                "sourceStartTime": seg.source_start_time,
                "sourceEndTime": seg.source_end_time,
                "globalStartTime": seg.global_start_time,
            }
            for seg in vvc.timeline.segments
        ]
        return {"changeId": change_id, "timeline": timeline}
    except Exception as e:
        print(f"Error processing job: {e}")
        return {"changeId": change_id, "error": str(e)}

# Function to start the BullMQ worker
def start_worker():
    # Create a new event loop for the worker thread
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Initialize the BullMQ worker
    worker = Worker('video-changes', process_change, {'connection': redis_client})

    # Log that the worker has started
    logger.info("BullMQ worker started")

    # Run the event loop
    loop.run_forever()

# Start the BullMQ worker in a separate thread
worker_thread = threading.Thread(target=start_worker)
worker_thread.daemon = True  # Daemonize thread to stop it when the main program exits
worker_thread.start()

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)