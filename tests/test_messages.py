from charybdisk.messages import FileMessage, decode_message, encode_message


def test_encode_decode_round_trip():
    original = FileMessage(
        file_name="example.txt",
        create_timestamp="2024-01-01T00:00:00",
        content=b"hello world",
        file_id="example-1",
        chunk_index=0,
        total_chunks=1,
        original_size=11,
    )
    encoded = encode_message(original)
    decoded = decode_message(encoded)

    assert decoded.file_name == original.file_name
    assert decoded.create_timestamp == original.create_timestamp
    assert decoded.content == original.content
