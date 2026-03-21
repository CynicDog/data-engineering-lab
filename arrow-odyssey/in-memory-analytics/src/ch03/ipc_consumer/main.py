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

        try:
            # open_stream will immediately try to read the Schema
            with pa.ipc.open_stream(StreamWrapper(content_iter)) as reader:
                print(f"[{time.strftime('%H:%M:%S')}] Schema received!")

                for batch in reader:
                    print(f"[{time.strftime('%H:%M:%S')}] Received batch: {batch.num_rows} rows")
                    sys.stdout.flush()
        except Exception as e:
            print(f"Error during IPC streaming: {e}")


if __name__ == "__main__":
    time.sleep(2)
    consume()