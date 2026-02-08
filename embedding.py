import os
import requests

# Trigger the local embedding/indexing service (empty POST body).
DEFAULT_INDEX_URL = os.getenv("EMBED_INDEX_URL", "http://localhost:8000/index")


def trigger_embedding(url: str = DEFAULT_INDEX_URL, timeout: int = 3000) -> str:
    """
    POST to the embedding/index endpoint with no body.
    Raises for HTTP errors; returns response text on success.
    """
    resp = requests.post(url, timeout=timeout)
    resp.raise_for_status()
    return resp.text


if __name__ == "__main__":
    print(trigger_embedding())

