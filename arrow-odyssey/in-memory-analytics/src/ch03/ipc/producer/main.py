import boto3
import pyarrow as pa
import pyarrow.csv
import io
from botocore import UNSIGNED
from botocore.config import Config
from fastapi import FastAPI
from fastapi.responses import StreamingResponse

app = FastAPI()

BUCKET = "dataforgood-fb-data"
KEY = "csv/month=2019-06/country=ZWE/type=children_under_five/ZWE_children_under_five.csv.gz"


def stream_arrow_ipc():
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    response = s3.get_object(Bucket=BUCKET, Key=KEY)

    # Use PyArrow's native decompression stream
    compressed_stream = pa.input_stream(response['Body'], compression='gzip')

    read_options = pyarrow.csv.ReadOptions(block_size=1024 * 1024)  # 1MB chunks
    parse_options = pyarrow.csv.ParseOptions(delimiter='\t')

    # open_csv works on the stream incrementally
    with pyarrow.csv.open_csv(compressed_stream, read_options=read_options, parse_options=parse_options) as reader:
        sink = io.BytesIO()

        # Initialize the IPC stream writer
        with pa.ipc.new_stream(sink, reader.schema) as writer:
            # Send the initial schema header immediately
            header_data = sink.getvalue()
            if header_data:
                yield header_data
                sink.seek(0)
                sink.truncate(0)

            # Stream the batches
            for batch in reader:
                writer.write_batch(batch)
                batch_data = sink.getvalue()
                if batch_data:
                    yield batch_data

                sink.seek(0)
                sink.truncate(0)

        # Finalize and send any remaining footer bytes
        footer_data = sink.getvalue()
        if footer_data:
            yield footer_data


@app.get("/data")
async def get_data():
    return StreamingResponse(stream_arrow_ipc(), media_type="application/octet-stream")