# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import unittest

from sse import ReceivedOrderEvent, iter_received_orders

try:
    from gevent.pywsgi import WSGIServer
    from locust.contrib.fasthttp import FastHttpSession
    from locust.event import EventHook
except ModuleNotFoundError:
    WSGIServer = None
    FastHttpSession = None
    EventHook = None

try:
    from locustfile import NatsAdapter
except (KeyError, ModuleNotFoundError):
    NatsAdapter = None


@unittest.skipUnless(FastHttpSession is not None, "Locust is not installed")
class LocustSSEIntegrationTest(unittest.TestCase):
    def test_fast_http_response_can_be_read_as_order_sse(self) -> None:
        def application(environment, start_response):
            start_response(
                "200 OK",
                [
                    ("Content-Type", "text/event-stream"),
                    ("Cache-Control", "no-cache"),
                ],
            )
            return [
                b"event: order\n"
                b"id: version-1\n"
                b'data: {"order_id":"order-1","status":"COMPLETED"}\n\n'
            ]

        server = WSGIServer(("127.0.0.1", 0), application, log=None)
        server.start()
        session = None
        try:
            session = FastHttpSession(
                base_url=f"http://127.0.0.1:{server.server_port}",
                request_event=EventHook(),
                user=None,
                network_timeout=2.0,
                connection_timeout=2.0,
            )
            with session.get(
                "/events", stream=True, catch_response=True
            ) as response:
                response.success()
            orders = list(iter_received_orders(response))

            self.assertEqual(1, len(orders))
            self.assertEqual("COMPLETED", orders[0].order["status"])
            self.assertEqual("version-1", orders[0].event_id)
        finally:
            client_pool = getattr(
                getattr(session, "client", None), "clientpool", None
            )
            close_pool = getattr(client_pool, "close", None)
            if callable(close_pool):
                close_pool()
            server.stop()
            server.close()


@unittest.skipUnless(NatsAdapter is not None, "benchmark runtime is unavailable")
class SettlementReceiptTest(unittest.TestCase):
    def test_failure_and_settlement_receipt_times_are_retained(self) -> None:
        adapter = object.__new__(NatsAdapter)
        context = {}

        notification = ReceivedOrderEvent(
            order={
                "order_id": "order-1",
                "status": "COMPLETED",
                "notification_status": "FAILED",
            },
            event_id="notification-event",
            received_monotonic=10.0,
            received_epoch=1_787_137_200.0,
        )
        cart_clear = ReceivedOrderEvent(
            order={
                "order_id": "order-1",
                "status": "COMPLETED",
                "notification_status": "FAILED",
                "cart_clear_status": "REJECTED",
                "cart_clear_failure_code": "VERSION_CONFLICT",
            },
            event_id="cart-clear-event",
            received_monotonic=11.0,
            received_epoch=1_787_137_201.0,
        )

        self.assertFalse(adapter._record_settlement_event(context, notification))
        self.assertTrue(adapter._record_settlement_event(context, cart_clear))

        self.assertEqual(
            "notification-event", context["notification_failure_event_id"]
        )
        self.assertEqual(
            context["notification_failure_received_at"],
            context["failure_received_at"],
        )
        self.assertEqual(
            "cart-clear-event", context["cart_clear_failure_event_id"]
        )
        self.assertEqual("VERSION_CONFLICT", context["cart_clear_failure_message"])
        self.assertEqual("cart-clear-event", context["settlement_event_id"])
        self.assertEqual(11.0, context["_settlement_received_monotonic"])


if __name__ == "__main__":
    unittest.main()
