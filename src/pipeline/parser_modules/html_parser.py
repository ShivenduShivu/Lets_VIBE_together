from bs4 import BeautifulSoup
from typing import Dict, Any

def parse_html(content: bytes, file_path: str) -> Dict[str, Any]:
    soup = BeautifulSoup(content.decode('utf-8', errors='ignore'), 'html.parser')
    
    data = {
        "title": soup.title.string.strip() if soup.title else None,
        "headings": [h.get_text(strip=True) for h in soup.find_all(['h1', 'h2', 'h3'])],
        "links": [a.get('href') for a in soup.find_all('a', href=True)],
        "text": soup.get_text(separator=' ', strip=True)[:1000],
    }
    
    # Extract custom fields (like category, author)
    for span in soup.find_all('span', class_='category'):
        data['category'] = span.get_text(strip=True)
    for span in soup.find_all('span', class_='author'):
        data['author'] = span.get_text(strip=True)
    
    return data