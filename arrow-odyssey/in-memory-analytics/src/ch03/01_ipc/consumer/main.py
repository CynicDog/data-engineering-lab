import requests
import pyarrow as pa
import time
import io
import sys


def consume():
    url = "http://producer:8000/data"
    print(f"[{time.strftime('%H:%M:%S')}] Connecting to producer...")

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        print(f"[{time.strftime('%H:%M:%S')}] Connection established, awaiting stream...")

        class StreamWrapper(io.RawIOBase):
            def __init__(self, iterable):
                self.iterator = iterable
                self.leftover = b""

            def readable(self):
                return True

            def readinto(self, b):
                try:
                    # Get next chunk if we don't have leftover data
                    chunk = self.leftover or next(self.iterator)
                except StopIteration:
                    return 0
                except Exception as e:
                    print(f"Stream error: {e}")
                    return 0

                n = len(chunk)
                dest_len = len(b)

                if n > dest_len:
                    # Chunk is bigger than the buffer PyArrow provided
                    b[:] = chunk[:dest_len]
                    self.leftover = chunk[dest_len:]
                    return dest_len
                else:
                    # Chunk fits entirely
                    b[:n] = chunk
                    self.leftover = b""
                    return n

        # chunk_size=None means "yield as soon as data arrives from the network"
        content_iter = r.iter_content(chunk_size=None)

        import pyarrow.ipc as ipc

        try:
            raw_stream = StreamWrapper(content_iter)

            schema = None
            batch_idx = 0
            message_idx = 0

            while True:
                message = ipc.read_message(raw_stream)
                if message is None:
                    print("\n[IPC] End of stream\n")
                    break

                print(f"\nIPC Message - {message_idx}")
                print(f"  - Type: {message.type}")
                print(f"  - Metadata size: {message.metadata.size} bytes")

                body_size = message.body.size if message.body is not None else 0
                print(f"  - Body length: {body_size} bytes")

                if message.type == "schema":
                    schema = ipc.read_schema(message)

                    print(f"\n[{time.strftime('%H:%M:%S')}] Schema received!")
                    print("Schema:")
                    print(schema)
                    print("-" * 50)

                elif message.type == "record batch":
                    batch = ipc.read_record_batch(message, schema)

                    print(f"  - Rows: {batch.num_rows}, Columns: \"{batch.num_columns}\"")

                    print("\nColumns Details:")
                    for col_idx, column in enumerate(batch.columns):
                        field = schema[col_idx]
                        print(f"\n- Column {col_idx} ({field.name})")

                        arr = column

                        print(f"  - length: {len(arr)}")
                        print(f"  - null count: {arr.null_count}")
                        print(f"  - offset: {arr.offset}")

                        # Arrow arrays are views over raw IPC buffers (zero-copy)
                        buffers = arr.buffers()

                        print(f"    buffers ({len(buffers)} total):")
                        for buf_idx, buf in enumerate(buffers):
                            if buf is None:
                                print(f"    - buffer[{buf_idx}]: None")
                            else:
                                preview = buf.to_pybytes()[:16]
                                print(
                                    f"    - buffer[{buf_idx}]: size={buf.size} bytes, "
                                    f"address={hex(buf.address)}, first16={preview}"
                                )

                    print("\nMemory footprint:")
                    print(f"- batch.nbytes: {batch.nbytes} bytes\n")

                    print("-" * 50)
                    sys.stdout.flush()

                    batch_idx += 1

                message_idx += 1

        except Exception as e:
            print(f"\n{e}")


if __name__ == "__main__":
    time.sleep(2)
    consume()