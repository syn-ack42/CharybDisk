import logging
import os
import re
import shutil
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from charybdisk.messages import FileMessage

logger = logging.getLogger('charybdisk.file_preparer')

DEFAULT_MAX_TRANSFER_FILE_SIZE = 3 * 1024 * 1024


@dataclass
class PreparedFile:
    original_path: str
    message: FileMessage
    backup_name: Optional[str]


def remove_text_between_T_and_extension(file_name: str) -> str:
    name, ext = os.path.splitext(file_name)
    pattern = r'_T\d+'  # Match "_T" followed by digits (timestamp suffixes)
    new_name = re.sub(pattern, '', name) + ext
    return new_name


def build_backup_name(file_path: str, create_timestamp: str) -> str:
    no_ts_fname = remove_text_between_T_and_extension(file_path)
    safe_timestamp = re.sub(r'[^0-9]', '', create_timestamp) or datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
    base, ext = os.path.splitext(os.path.basename(no_ts_fname))
    return f"{base}_T{safe_timestamp}{ext}"


class FilePreparer:
    """
    Prepares files for transfer: guards against locked/oversized files and builds the transfer payload.
    """

    def __init__(self, max_transfer_file_size: Optional[int] = DEFAULT_MAX_TRANSFER_FILE_SIZE) -> None:
        self.max_transfer_file_size = max_transfer_file_size

    def is_file_open(self, file_path: str) -> bool:
        if os.path.exists(file_path):
            try:
                with open(file_path, 'a'):
                    return False
            except IOError:
                return True
        return False

    def prepare(self, file_path: str, max_size_override: Optional[int] = None) -> Optional[PreparedFile]:
        # Check if the file is open
        if self.is_file_open(file_path):
            logger.warning(f"File '{file_path}' is currently open by another process, skipping.")
            return None

        if not os.path.exists(file_path):
            logger.warning(f"File '{file_path}' disappeared before processing.")
            return None

        # Check file size
        file_size = os.path.getsize(file_path)
        size_limit = max_size_override if max_size_override is not None else self.max_transfer_file_size
        if size_limit is not None and file_size > size_limit:
            logger.warning(f"File '{file_path}' is larger than {size_limit} bytes. Skipping.")
            return None

        if file_size == 0:
            logger.warning(f"File '{file_path}' is empty. Skipping.")
            return None

        try:
            with open(file_path, 'rb') as file:
                content = file.read()
        except OSError as e:
            logger.error(f"OS error occurred while reading file '{file_path}': {e}, continuing")
            return None

        create_timestamp = datetime.fromtimestamp(os.path.getctime(file_path)).isoformat()
        message = FileMessage(
            file_name=os.path.basename(file_path),
            create_timestamp=create_timestamp,
            content=content,
            file_id=f"{os.path.basename(file_path)}-{uuid.uuid4().hex}",
            chunk_index=0,
            total_chunks=1,
            original_size=len(content),
        )
        backup_name = build_backup_name(file_path, create_timestamp)
        return PreparedFile(original_path=file_path, message=message, backup_name=backup_name)


def backup_file(file_path: str, backup_directory: str, new_file_name: str) -> None:
    try:
        safe_file_name = re.sub(r'[<>:\"/\\\\|?*]', '_', new_file_name).rstrip('. ')
        if not os.path.exists(backup_directory):
            os.makedirs(backup_directory)
        shutil.move(file_path, os.path.join(backup_directory, safe_file_name))
        logger.info(f"Moved file '{os.path.basename(file_path)}' to backup directory: '{backup_directory}'")
    except Exception as e:
        logger.error(f"Error moving file '{file_path}' to backup directory '{backup_directory}': {e}")
        raise IOError(f"Error moving file '{file_path}' to backup directory '{backup_directory}'")
