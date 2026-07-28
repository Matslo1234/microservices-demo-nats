# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import unittest

from session_cookie import issued_session_cookie


class Response:
    def __init__(self, set_cookie: str | None):
        self.headers = {}
        if set_cookie is not None:
            self.headers["Set-Cookie"] = set_cookie


class SessionCookieTest(unittest.TestCase):
    def test_extracts_frontend_issued_signed_session(self) -> None:
        response = Response(
            "shop_session-id=7dbfbfba-12bb-4d9f-912a-416ace95d861.signature; "
            "Path=/; HttpOnly; SameSite=Lax"
        )

        self.assertEqual(
            "7dbfbfba-12bb-4d9f-912a-416ace95d861.signature",
            issued_session_cookie(response),
        )

    def test_ignores_unrelated_or_missing_cookie(self) -> None:
        self.assertIsNone(issued_session_cookie(Response(None)))
        self.assertIsNone(
            issued_session_cookie(Response("other=value; Path=/"))
        )


if __name__ == "__main__":
    unittest.main()
