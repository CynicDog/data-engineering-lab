import os
from pathlib import Path


MTA_FEED_URL = (
    "https://api-endpoint.mta.info/"
    "Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw"
)

HEADERS = {
    "Accept": "application/x-protobuf",
}

POLL_INTERVAL_SECONDS = int(
    os.getenv("POLL_INTERVAL_SECONDS", "30")
)

RAW_BASE_PATH = Path(
    os.getenv("RAW_BASE_PATH", Path.cwd() / "data" / "raw")
)