"""
Component 2: Parser Manager
Delegates raw bytes to correct parser based on file_type.
"""
from typing import Optional, Dict, Any
import src.pipeline.parser_modules.html_parser as html_parser
import src.pipeline.parser_modules.exif_parser as exif_parser
import src.pipeline.parser_modules.text_parser as text_parser
import src.pipeline.parser_modules.srt_parser as srt_parser

class ParserManager:
    def __init__(self):
        self.parsers = {
            'html': self._parse_html,
            'image': self._parse_image,
            'text': self._parse_text,
            'subtitle': self._parse_subtitle,
        }

    def parse(self, raw_content: bytes, file_type: str, file_path: str) -> Optional[Dict[str, Any]]:
        parser = self.parsers.get(file_type)
        if not parser:
            print(f"No parser for type: {file_type}")
            return None
        return parser(raw_content, file_path)

    def _parse_html(self, content: bytes, path: str) -> Dict:
        return html_parser.parse_html(content, path)

    def _parse_image(self, content: bytes, path: str) -> Dict:
        exif = exif_parser.parse_exif(content, path)
        return {"exif": exif} if exif else {"exif": None, "size_bytes": len(content)}

    def _parse_text(self, content: bytes, path: str) -> Dict:
        return text_parser.parse_text(content, path)

    def _parse_subtitle(self, content: bytes, path: str) -> Dict:
        return srt_parser.parse_srt(content, path)