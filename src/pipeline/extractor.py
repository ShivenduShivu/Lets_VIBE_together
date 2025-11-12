"""
Component 1: Extractor
Discovers files in data/raw/, reads raw content (bytes), identifies file type by extension.
Moves processed files to avoid re-processing.
"""
import os
import shutil
from pathlib import Path
from typing import List, Tuple, Optional

class Extractor:
    def __init__(self, raw_data_dir: str = "data/raw"):
        self.raw_dir = Path(raw_data_dir)
        self.processed_dir = self.raw_dir.parent / "processed"
        self.processed_dir.mkdir(exist_ok=True)

    def discover_files(self) -> List[str]:
        """Find all files in raw_dir that are not hidden and have known extensions."""
        if not self.raw_dir.exists():
            print(f"Raw directory {self.raw_dir} does not exist.")
            return []

        # Supported extensions (add more as you implement parsers)
        valid_extensions = {'.html', '.jpg', '.jpeg', '.png', '.txt', '.srt'}
        
        files = [
            str(f) for f in self.raw_dir.iterdir()
            if f.is_file() and f.suffix.lower() in valid_extensions and not f.name.startswith('.')
        ]
        return files

    def _get_file_type(self, file_path: str) -> str:
        """Map file extension to parser key."""
        ext = Path(file_path).suffix.lower()
        mapping = {
            '.html': 'html',
            '.jpg': 'image', '.jpeg': 'image', '.png': 'image',
            '.txt': 'text',
            '.srt': 'subtitle'
        }
        return mapping.get(ext, 'unknown')

    def read_file(self, file_path: str) -> Tuple[str, bytes]:
        """
        Read file as bytes and return (file_type, content).
        file_type is used by ParserManager to pick the right parser.
        """
        file_type = self._get_file_type(file_path)
        with open(file_path, 'rb') as f:
            content = f.read()
        return file_type, content

    def move_file(self, file_path: str, to_error: bool = False) -> None:
        """
        Move processed file to processed/ or error/ subfolder.
        Prevents re-processing.
        """
        source = Path(file_path)
        if not source.exists():
            return

        if to_error:
            dest_dir = self.raw_dir.parent / "error"
        else:
            dest_dir = self.processed_dir

        dest_dir.mkdir(exist_ok=True)
        dest = dest_dir / source.name

        # Avoid name collisions
        counter = 1
        original_dest = dest
        while dest.exists():
            dest = dest_dir / f"{source.stem}_{counter}{source.suffix}"
            counter += 1

        try:
            shutil.move(str(source), str(dest))
            print(f"Moved: {source.name} → {dest.parent.name}/")
        except Exception as e:
            print(f"Failed to move {source.name}: {e}")