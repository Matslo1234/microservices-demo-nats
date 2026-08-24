# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import types
import unittest
from contextlib import contextmanager
from unittest import mock

try:
    from locustfile import (
        NatsAdapter,
        StorefrontAdapter,
        http_status_failure,
        sse_reconnect_delay,
    )
except (KeyError, ModuleNotFoundError):
    NatsAdapter = None
    StorefrontAdapter = None
    http_status_failure = None
    sse_reconnect_delay = None


class FakeResponse:
    def __init__(self, status_code: int, headers: dict | None = None):
        self.status_code = status_code
        self.headers = headers or {}
        self.failure_message = None
        self.succeeded = False
        self._response = types.SimpleNamespace(release=lambda: None)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def failure(self, message: str) -> None:
        self.failure_message = message

    def success(self) -> None:
        self.succeeded = True


class FakeRequestEvents:
    def __init__(self):
        self.measurements = []
        self.fired = []

    @contextmanager
    def measure(self, request_type, name, context):
        measurement = {}
        self.measurements.append((request_type, name, context, measurement))
        yield measurement

    def fire(self, **values) -> None:
        self.fired.append(values)


@unittest.skipUnless(NatsAdapter is not None, "benchmark runtime is unavailable")
class LocustTimeoutClassificationTest(unittest.TestCase):
    def test_status_zero_overrides_the_fallback_outcome(self) -> None:
        error = http_status_failure(0, "STATUS_ERROR", "SSE")

        self.assertEqual("TIMEOUT", error.outcome)
        self.assertEqual("SSE timed out", error.detail)

        error = http_status_failure(502, "STATUS_ERROR", "SSE")
        self.assertEqual("STATUS_ERROR", error.outcome)
        self.assertEqual("SSE status 502", error.detail)

    def test_sse_status_zero_is_a_timeout(self) -> None:
        response = FakeResponse(0)
        adapter = object.__new__(NatsAdapter)
        adapter.client = types.SimpleNamespace(get=lambda *_args, **_kwargs: response)
        adapter.session_cookie = None

        stream, error, retry_after = adapter._open_order_event_stream(
            "/orders/order-1/events", "/orders/[id]/events"
        )

        self.assertIsNone(stream)
        self.assertIsNone(retry_after)
        self.assertEqual("TIMEOUT", error.outcome)
        self.assertEqual("TIMEOUT: SSE timed out", response.failure_message)

    def test_sse_retry_honors_retry_after(self) -> None:
        response = FakeResponse(503, {"Retry-After": "4"})
        adapter = object.__new__(NatsAdapter)
        adapter.client = types.SimpleNamespace(
            get=lambda *_args, **_kwargs: response
        )
        adapter.session_cookie = None

        stream, error, retry_after = adapter._open_order_event_stream(
            "/orders/order-1/events", "/orders/[id]/events"
        )

        self.assertIsNone(stream)
        self.assertIsNone(error)
        self.assertEqual(4.0, retry_after)
        self.assertTrue(response.succeeded)

    def test_sse_retry_uses_capped_exponential_backoff_with_jitter(
        self,
    ) -> None:
        source = types.SimpleNamespace(
            uniform=lambda _lower, upper: upper
        )

        self.assertAlmostEqual(1.2, sse_reconnect_delay(0, None, source))
        self.assertAlmostEqual(2.4, sse_reconnect_delay(1, None, source))
        self.assertAlmostEqual(4.8, sse_reconnect_delay(2, None, source))
        self.assertAlmostEqual(5.0, sse_reconnect_delay(3, None, source))
        self.assertAlmostEqual(4.0, sse_reconnect_delay(0, 4.0, source))

    def test_product_status_zero_becomes_a_timeout_precondition(self) -> None:
        response = FakeResponse(0)
        adapter = object.__new__(StorefrontAdapter)
        adapter.client = types.SimpleNamespace(get=lambda *_args, **_kwargs: response)
        adapter.random = types.SimpleNamespace(choice=lambda _values: "product-1")
        adapter.session_cookie = None

        error = adapter.add_item()

        self.assertEqual("TIMEOUT", error.outcome)
        self.assertEqual(
            "TIMEOUT: product request timed out", response.failure_message
        )

    def test_cart_status_zero_becomes_a_timeout_precondition(self) -> None:
        response = FakeResponse(0)
        adapter = object.__new__(NatsAdapter)
        adapter.client = types.SimpleNamespace(
            post=lambda *_args, **_kwargs: response
        )
        adapter.session_cookie = None

        error = adapter._submit_cart_add({"product_id": "product-1"})

        self.assertEqual("TIMEOUT", error.outcome)
        self.assertEqual(
            "TIMEOUT: cart request timed out", response.failure_message
        )

    def test_cart_operation_status_zero_becomes_a_timeout_precondition(
        self,
    ) -> None:
        accepted = FakeResponse(
            202, {"Location": "/operations/operation-1", "Retry-After": "1"}
        )
        timed_out = FakeResponse(0)
        adapter = object.__new__(NatsAdapter)
        adapter.client = types.SimpleNamespace(
            post=lambda *_args, **_kwargs: accepted,
            get=lambda *_args, **_kwargs: timed_out,
        )
        adapter.session_cookie = None

        with (
            mock.patch("locustfile.drain_deadline", return_value=float("inf")),
            mock.patch("locustfile.gevent.sleep"),
        ):
            error = adapter._submit_cart_add({"product_id": "product-1"})

        self.assertEqual("TIMEOUT", error.outcome)
        self.assertEqual(
            "TIMEOUT: cart operation request timed out",
            timed_out.failure_message,
        )

    def test_checkout_status_zero_is_recorded_as_timeout(self) -> None:
        response = FakeResponse(0)
        requests = FakeRequestEvents()
        adapter = object.__new__(NatsAdapter)
        adapter.client = types.SimpleNamespace(
            post=lambda *_args, **_kwargs: response
        )
        adapter.session_cookie = None
        adapter.user = types.SimpleNamespace(
            environment=types.SimpleNamespace(
                events=types.SimpleNamespace(request=requests)
            )
        )
        context = {
            "accepted": False,
            "application_type": "NATS",
            "outcome": "UNKNOWN",
            "phase": "steady",
            "workload": "closed",
        }

        returned, _, _, _, error = adapter._checkout_to_outcome(
            {}, context, 1.0, 1.0
        )

        self.assertEqual("TIMEOUT", error.outcome)
        self.assertEqual("TIMEOUT", returned["outcome"])
        self.assertEqual(
            "TIMEOUT: checkout request timed out", response.failure_message
        )
        acceptance = next(
            item for item in requests.measurements if item[1] == "checkout_acceptance"
        )
        self.assertEqual("TIMEOUT", acceptance[2]["outcome"])
        self.assertIs(error, acceptance[3]["exception"])

    def test_timeout_precondition_is_recorded_as_timeout(self) -> None:
        requests = FakeRequestEvents()
        adapter = object.__new__(StorefrontAdapter)
        adapter.user = types.SimpleNamespace(
            environment=types.SimpleNamespace(
                events=types.SimpleNamespace(request=requests)
            )
        )
        error = http_status_failure(0, "PRECONDITION_FAILED", "cart request")

        adapter.record_precondition_failure(failure=error)

        _, _, context, measurement = requests.measurements[0]
        self.assertEqual("TIMEOUT", context["outcome"])
        self.assertEqual("cart request timed out", context["failure_message"])
        self.assertIs(error, measurement["exception"])


if __name__ == "__main__":
    unittest.main()
