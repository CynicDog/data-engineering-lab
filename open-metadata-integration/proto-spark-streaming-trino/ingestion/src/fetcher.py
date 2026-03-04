import urllib.request
import urllib.error
from proto.com.google.transit.realtime import (
    gtfs_realtime_pb2,
)

FeedMessage = gtfs_realtime_pb2.FeedMessage


def fetch_feed(url: str, headers: dict) -> FeedMessage:
    """
    Fetches the MTA GTFS-realtime feed using native urllib.
    """
    # Create the Request object to include custom headers (like API keys)
    req = urllib.request.Request(url, headers=headers)

    try:
        # Context manager ensures the connection is closed automatically
        with urllib.request.urlopen(req, timeout=10) as response:
            # Check for HTTP 200
            if response.status != 200:
                raise urllib.error.HTTPError(
                    url, response.status, f"HTTP Error {response.status}",
                    response.headers, None
                )

            content = response.read()

            feed = FeedMessage()
            feed.ParseFromString(content)
            return feed

    except urllib.error.URLError as e:
        print(f"[ERROR] Failed to reach the server: {e.reason}")
        raise
    except Exception as e:
        print(f"[ERROR] Unexpected error during fetch: {e}")
        raise