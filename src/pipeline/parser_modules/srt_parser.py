import pysrt
from io import StringIO

def parse_srt(content: bytes, file_path: str) -> dict:
    try:
        srt_text = content.decode('utf-8')
        subs = pysrt.from_string(srt_text)
        return {
            "subtitle_count": len(subs),
            "duration_seconds": (subs[-1].end - subs[0].start).seconds if subs else 0,
            "first_line": subs[0].text if subs else None
        }
    except:
        return {"error": "Invalid SRT"}