from fastapi import FastAPI
import redis
from bullmq import Worker
from dataclasses import dataclass
from typing import List

app = FastAPI()
redis_client = redis.Redis(host='localhost', port=6379, db=0)

@dataclass
class Segment:
    source_video_id: str
    source_start_time: float
    source_end_time: float
    global_start_time: float

    def duration(self) -> float:
        return self.source_end_time - self.source_start_time

class VideoVersionControl:
    def __init__(self, segments: List[dict]):
        self.segments = [Segment(seg['sourceVideoId'], seg['sourceStartTime'], seg['sourceEndTime'], seg['globalStartTime']) 
                         for seg in segments]

    def cut(self, t1: float, t2: float):
        new_segments = []
        for seg in self.segments:
            seg_start = seg.global_start_time
            seg_end = seg_start + seg.duration()
            if seg_end <= t1 or seg_start >= t2:
                new_segments.append(seg)
            else:
                if seg_start < t1:
                    new_seg = Segment(seg.source_video_id, seg.source_start_time,
                                      seg.source_start_time + (t1 - seg_start), seg_start)
                    new_segments.append(new_seg)
                if seg_end > t2:
                    new_seg = Segment(seg.source_video_id,
                                      seg.source_start_time + (t2 - seg_start),
                                      seg.source_end_time, 0.0)
                    new_segments.append(new_seg)
        self.segments = self.update_global_starts(new_segments)

    def insert(self, new_seg: Segment, t: float):
        new_segments = []
        inserted = False
        for seg in self.segments:
            seg_start = seg.global_start_time
            seg_end = seg_start + seg.duration()
            if not inserted and t <= seg_start:
                new_segments.append(new_seg)
                new_segments.append(seg)
                inserted = True
            elif seg_start <= t < seg_end:
                split_point = seg.source_start_time + (t - seg_start)
                if split_point > seg.source_start_time:
                    before = Segment(seg.source_video_id, seg.source_start_time,
                                     split_point, seg_start)
                    new_segments.append(before)
                new_segments.append(new_seg)
                if split_point < seg.source_end_time:
                    after = Segment(seg.source_video_id, split_point,
                                    seg.source_end_time, 0.0)
                    new_segments.append(after)
                inserted = True
            else:
                new_segments.append(seg)
        if not inserted:
            new_segments.append(new_seg)
        self.segments = self.update_global_starts(new_segments)

    def update(self, t1: float, t2: float, new_seg: Segment):
        self.cut(t1, t2)
        self.insert(new_seg, t1)

    def merge(self, other_segments: List[Segment]):
        self.segments.extend(other_segments)
        self.segments = self.update_global_starts(self.segments)

    def update_global_starts(self, segments: List[Segment]) -> List[Segment]:
        current_global_start = 0.0
        for seg in segments:
            seg.global_start_time = current_global_start
            current_global_start += seg.duration()
        return segments

    def get_timeline(self):
        return [{'sourceVideoId': seg.source_video_id, 'sourceStartTime': seg.source_start_time,
                 'sourceEndTime': seg.source_end_time, 'globalStartTime': seg.global_start_time}
                for seg in self.segments]

async def process_change(job):
    try:
        change_id = job.data['changeId']
        video_id = job.data['videoId']
        operation = job.data['operation']
        source_video_id = job.data.get('sourceVideoId')
        start = job.data.get('start', 0.0)
        end = job.data.get('end', 0.0)
        at = job.data.get('at', 0.0)
        current_timeline = job.data['currentTimeline']

        vvc = VideoVersionControl(current_timeline)

        if operation == 'cut':
            vvc.cut(start, end)
        elif operation == 'insert':
            new_seg = Segment(source_video_id, start, end, 0.0)
            vvc.insert(new_seg, at)
        elif operation == 'update':
            new_seg = Segment(source_video_id, start, end, 0.0)
            vvc.update(at, end, new_seg)  # 'at' as t1, 'end' as t2
        elif operation == 'merge':
            other_segments = [Segment(source_video_id, start, end, 0.0)]  # Simplified merge with one segment
            vvc.merge(other_segments)

        return {'changeId': change_id, 'timeline': vvc.get_timeline()}
    except Exception as e:
        return {'changeId': change_id, 'error': str(e)}

worker = Worker('video-changes', process_change, {'connection': redis_client})

@app.get("/health")
def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)