import os
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from charybdisk.file_preparer import FilePreparer
from charybdisk.file_writer import write_file_safe


def test_file_preparer_skips_large_files():
    with TemporaryDirectory() as tmpdir:
        p = Path(tmpdir) / "big.bin"
        p.write_bytes(b"a" * 10)
        preparer = FilePreparer(max_transfer_file_size=5)
        assert preparer.prepare(str(p)) is None


def test_file_preparer_builds_message():
    with TemporaryDirectory() as tmpdir:
        p = Path(tmpdir) / "small.txt"
        p.write_text("hi")
        preparer = FilePreparer(max_transfer_file_size=None)
        prepared = preparer.prepare(str(p))
        assert prepared is not None
        assert prepared.message.file_name == "small.txt"
        assert prepared.message.content == b"hi"
        assert prepared.message.total_chunks == 1


def test_write_file_safe_creates_directories_and_writes():
    with TemporaryDirectory() as tmpdir:
        out_dir = Path(tmpdir) / "out"
        target = write_file_safe(str(out_dir), "f.txt", b"data", None)
        assert target and target.exists()
        assert target.read_bytes() == b"data"


def test_write_file_safe_deduplicates_same_content():
    with TemporaryDirectory() as tmpdir:
        out_dir = Path(tmpdir)
        target1 = write_file_safe(str(out_dir), "same.txt", b"data", None)
        target2 = write_file_safe(str(out_dir), "same.txt", b"data", None)
        assert target1 == target2


def test_write_file_safe_versions_on_difference():
    with TemporaryDirectory() as tmpdir:
        out_dir = Path(tmpdir)
        first = write_file_safe(str(out_dir), "diff.txt", b"one", None)
        time.sleep(0.001)
        second = write_file_safe(str(out_dir), "diff.txt", b"two", None)
        assert first and second
        assert first != second
        assert first.read_bytes() == b"one"
        assert second.read_bytes() == b"two"
