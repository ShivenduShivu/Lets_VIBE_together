def parse_text(content: bytes, file_path: str) -> dict:
    try:
        text = content.decode('utf-8')
    except:
        text = "<binary>"
    return {
        "content": text,
        "line_count": len(text.splitlines()),
        "char_count": len(text)
    }