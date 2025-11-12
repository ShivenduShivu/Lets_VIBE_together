from PIL import Image
from PIL.ExifTags import TAGS
from typing import Dict, Any, Optional
import datetime

def parse_exif(content: bytes, file_path: str) -> Optional[Dict[str, Any]]:
    try:
        from io import BytesIO
        img = Image.open(BytesIO(content))
        exif_data = img._getexif()
        if not exif_data:
            return None

        parsed = {}
        for tag_id, value in exif_data.items():
            tag = TAGS.get(tag_id, tag_id)
            if tag == 'GPSInfo':
                parsed['GPS'] = _parse_gps(value)
            elif tag == 'DateTimeOriginal':
                parsed['DateTimeOriginal'] = value
            else:
                parsed[tag] = value
        return parsed
    except Exception:
        return None

def _parse_gps(gps_info):
    def _convert_to_degrees(value):
        d, m, s = value
        return float(d) + float(m)/60 + float(s)/3600
    return {
        'lat': _convert_to_degrees(gps_info[2]),
        'lon': _convert_to_degrees(gps_info[4]),
        'lat_ref': gps_info[1],
        'lon_ref': gps_info[3]
    }