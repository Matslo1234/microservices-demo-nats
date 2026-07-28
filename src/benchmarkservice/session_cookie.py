# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

from __future__ import annotations

from http.cookies import SimpleCookie
from typing import Any


SESSION_COOKIE_NAME = "shop_session-id"


def issued_session_cookie(response: Any) -> str | None:
    """Return the signed frontend session cookie issued by a response."""
    raw = response.headers.get("Set-Cookie")
    if not raw:
        return None
    cookies = SimpleCookie()
    cookies.load(raw)
    session = cookies.get(SESSION_COOKIE_NAME)
    return session.value if session is not None else None
