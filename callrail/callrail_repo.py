from typing import Any, Callable
import os

import httpx

PAGE_SIZE = 250
BASE_URL = "https://api.callrail.com/v3/a/381525732"


def get_client() -> httpx.Client:
    return httpx.Client(
        base_url=BASE_URL,
        headers={"Authorization": f"Token token={os.getenv('CALLRAIL_API_KEY')}"},
        params={"per_page": PAGE_SIZE},
        timeout=None,
        limits=httpx.Limits(max_keepalive_connections=10, max_connections=10),
    )


def get(uri: str, res_fn: Callable[[dict[str, Any]], list[dict[str, Any]]]):
    def _get(params: dict[str, Any]):
        def _get(client: httpx.Client, page=1) -> list[dict[str, Any]]:
            r = client.get(uri, params={**params, "page": page})
            res = r.json()
            data = res_fn(res)
            return data if not data else data + _get(client, page + 1)

        with get_client() as client:
            return _get(client)

    return _get
