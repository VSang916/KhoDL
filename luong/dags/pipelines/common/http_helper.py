from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def new_session(
    headers: dict | None = None,
    retry_statuses=(429, 500, 502, 503, 504),
    methods=("GET",),
    total: int = 3,
    backoff: float = 1,
) -> requests.Session:
    """Session có retry + backoff cho lỗi mạng/lỗi tạm thời của server."""
    s = requests.Session()
    s.headers["User-Agent"] = DEFAULT_UA
    s.headers.update(headers or {})
    retry = Retry(total=total, backoff_factor=backoff, status_forcelist=list(retry_statuses), allowed_methods=list(methods))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s
