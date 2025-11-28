import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger('charybdisk.file_writer')


def write_file_safe(output_directory: str, file_name: str, content: bytes, output_suffix: Optional[str] = None) -> Optional[Path]:
    filepath = os.path.join(output_directory, file_name)
    if output_suffix:
        if not output_suffix.startswith('.'):
            output_suffix = f'.{output_suffix}'
        new_filepath = Path(filepath).with_suffix(output_suffix)
        logger.debug(f'write_file_safe() changing file suffix; old file {filepath}, new file {new_filepath}')
        filepath = str(new_filepath)

    if not os.path.exists(output_directory):
        try:
            os.makedirs(output_directory)
            logger.warning(f"Output directory '{output_directory}' did not exist, created it.")
        except Exception as e:
            logger.error(f"Error creating output directory '{output_directory}': {e}")
            return None

    target_path = Path(filepath)

    if target_path.exists():
        try:
            with open(target_path, 'rb') as existing_file:
                existing_content = existing_file.read()
            if existing_content == content:
                logger.info(f"File '{target_path}' already exists with identical content. Skipping write.")
                return target_path
            else:
                timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
                new_filepath = target_path.with_name(f"{target_path.stem}_{timestamp}{target_path.suffix}")
                logger.info(f"File '{target_path}' already exists with different content. Writing new file with timestamp.")
                target_path = new_filepath
        except Exception as e:
            logger.error(f"Error reading existing file '{target_path}': {e}")
            return None

    try:
        with open(target_path, 'wb') as file:
            file.write(content)
        return target_path
    except Exception as e:
        logger.error(f"Error writing file '{target_path}' (original: '{file_name}') to directory '{output_directory}': {e}")
        return None

