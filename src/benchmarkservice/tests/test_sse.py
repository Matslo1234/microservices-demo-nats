# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import unittest

from sse import (
    SSEProtocolError,
    is_event_stream_content_type,
    iter_received_orders,
    iter_sse_events,
    order_events_url,
)


class RawResponse:
    def __init__(self, lines: list[bytes]) -> None:
        self.lines = iter(lines)
        self.released = False

    def readline(self, separator: bytes) -> bytes:
        self.separator = separator
        return next(self.lines, b"")

    def release(self) -> None:
        self.released = True


class Response:
    def __init__(self, lines: list[bytes]) -> None:
        self._response = RawResponse(lines)


class SSETest(unittest.TestCase):
    def test_parser_ignores_heartbeats_and_joins_data_lines(self) -> None:
        events = list(
            iter_sse_events(
                [
                    b": keep-alive\n",
                    b"\n",
                    b"event: order\n",
                    b"id: version-2\n",
                    b"data: {\"order_id\":\"order-1\",\n",
                    b"data: \"status\":\"COMPLETED\"}\n",
                    b"\n",
                ]
            )
        )

        self.assertEqual(1, len(events))
        self.assertEqual("order", events[0].event)
        self.assertEqual("version-2", events[0].event_id)
        self.assertIn("\n", events[0].data)

    def test_parser_accepts_gevent_bytearray_lines(self) -> None:
        events = list(
            iter_sse_events(
                [
                    bytearray(b"event: order\n"),
                    bytearray(b"data: {}\n"),
                    bytearray(b"\n"),
                ]
            )
        )

        self.assertEqual("order", events[0].event)

    def test_received_order_records_receipt_clocks_and_releases_stream(self) -> None:
        response = Response(
            [
                b"event: order\n",
                b"id: 2026-08-19T12:00:00Z\n",
                b'data: {"order_id":"order-1","status":"REJECTED",'
                b'"safe_message":"payment failed"}\n',
                b"\n",
            ]
        )

        orders = list(
            iter_received_orders(
                response,
                monotonic_clock=lambda: 42.125,
                epoch_clock=lambda: 1_787_137_200.5,
            )
        )

        self.assertEqual("REJECTED", orders[0].order["status"])
        self.assertEqual(42.125, orders[0].received_monotonic)
        self.assertEqual("2026-08-19T12:00:00Z", orders[0].event_id)
        self.assertTrue(response._response.released)
        self.assertEqual(b"\n", response._response.separator)

    def test_invalid_order_json_is_rejected_and_stream_is_released(self) -> None:
        response = Response(
            [b"event: order\n", b"data: not-json\n", b"\n"]
        )

        with self.assertRaises(SSEProtocolError):
            list(iter_received_orders(response))

        self.assertTrue(response._response.released)

    def test_order_events_url_preserves_base_and_query(self) -> None:
        self.assertEqual(
            "https://store.example/base/orders/order-1/events?trace=1",
            order_events_url(
                "https://store.example/base/orders/order-1?trace=1#ignored"
            ),
        )
        self.assertEqual(
            "/orders/order-1/events", order_events_url("/orders/order-1")
        )

    def test_content_type_allows_parameters(self) -> None:
        self.assertTrue(
            is_event_stream_content_type("text/event-stream; charset=utf-8")
        )
        self.assertFalse(is_event_stream_content_type("application/json"))


if __name__ == "__main__":
    unittest.main()
