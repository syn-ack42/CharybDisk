import base64
import json
from dataclasses import dataclass
from typing import Any, Dict, Union


@dataclass
class FileMessage:
    file_name: str
    create_timestamp: str
    content: bytes


def encode_message(message: FileMessage) -> bytes:
    payload: Dict[str, Any] = {
        'file_name': message.file_name,
        'create_timestamp': message.create_timestamp,
        'content': base64.b64encode(message.content).decode('utf-8'),
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
    )

