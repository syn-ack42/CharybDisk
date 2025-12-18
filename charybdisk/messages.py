import base64
import json
from dataclasses import dataclass
from typing import Any, Dict, Union


@dataclass
class FileMessage:
    file_name: str
    create_timestamp: str
    content: bytes
    file_id: str
    chunk_index: int
    total_chunks: int
    original_size: int


def encode_message(message: FileMessage) -> bytes:
    payload: Dict[str, Any] = {
        'file_name': message.file_name,
        'create_timestamp': message.create_timestamp,
        'content': base64.b64encode(message.content).decode('utf-8'),
        'file_id': message.file_id,
        'chunk_index': message.chunk_index,
        'total_chunks': message.total_chunks,
        'original_size': message.original_size,
    }
    return json.dumps(payload).encode('utf-8')


def decode_message(raw: Union[str, bytes]) -> FileMessage:
    if isinstance(raw, bytes):
        raw = raw.decode('utf-8')
    data = json.loads(raw)
    return FileMessage(
        file_name=data['file_name'],
        create_timestamp=data.get('create_timestamp', ''),
        content=base64.b64decode(data['content'].encode('utf-8')),
        file_id=data.get('file_id', data.get('file_name')),
        chunk_index=int(data.get('chunk_index', 0)),
        total_chunks=int(data.get('total_chunks', 1)),
        original_size=int(data.get('original_size', len(base64.b64decode(data['content'].encode('utf-8'))))),
    )
