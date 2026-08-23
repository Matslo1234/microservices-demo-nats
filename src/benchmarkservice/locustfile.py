# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

from __future__ import annotations

import datetime
import math
import os
import random
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import gevent
from locust import FastHttpUser, between, events, task
from locust.exception import StopUser

import runtime
from config import (
    SATURATION_START_RATE,
    SATURATION_STEP_RATE,
)
from runtime import (
    CONFIG,
    RECORDER,
    RESOURCE_SAMPLER,
    drain_deadline,
    phase_for_elapsed,
    phase_now,
    start_clock,
    submission_deadline,
)
from saturation import SaturationCoordinator
from session_cookie import SESSION_COOKIE_NAME, issued_session_cookie
from sse import (
    ReceivedOrderEvent,
    SSEProtocolError,
    close_stream_response,
    is_event_stream_content_type,
    iter_received_orders,
    order_events_url,
    utc_timestamp,
)
from timing import received_latency_ms


PRODUCTS = (
    "0PUK6V6EV0",
    "1YMWWN1N4O",
    "2ZYFJ3GM2N",
    "66VCHSJNUP",
    "6E92ZMYYFZ",
    "9SIQT8TOJO",
    "L9ECAV7KIM",
    "LS4PSXUNUM",
    "OLJCESPC7Z",
)
CURRENCIES = ("EUR", "USD", "JPY", "CAD", "GBP", "TRY")
TERMINAL_ORDER_STATUSES = {
    "COMPLETED",
    "CANCELLED",
    "REJECTED",
    "MANUAL_REVIEW",
}
TERMINAL_NOTIFICATION_STATUSES = {"SENT", "FAILED"}
TERMINAL_CART_CLEAR_STATUSES = {"SUCCEEDED", "REJECTED"}
SSE_RECONNECT_DELAY_SECONDS = 0.25
SSE_NETWORK_TIMEOUT_SECONDS = 20.0
SETTLEMENT_TRACKERS: set[gevent.Greenlet] = set()


class BusinessFailure(Exception):
    def __init__(
        self,
        outcome: str,
        detail: str = "",
        *,
        received_monotonic: float | None = None,
        received_epoch: float | None = None,
    ) -> None:
        suffix = f": {detail}" if detail else ""
        super().__init__(outcome + suffix)
        self.outcome = outcome
        self.detail = detail
        self.received_monotonic = received_monotonic
        self.received_epoch = received_epoch


class SSEDeadlineReached(Exception):
    pass


def retry_after_seconds(response: Any, default: float = 1.0) -> float:
    try:
        value = float(response.headers.get("Retry-After", default))
    except (AttributeError, TypeError, ValueError):
        value = default
    return min(5.0, max(0.05, value))


def failure_message(
    outcome: str,
    detail: str = "",
    *,
    received_monotonic: float | None = None,
    received_epoch: float | None = None,
) -> BusinessFailure:
    return BusinessFailure(
        outcome,
        detail,
        received_monotonic=received_monotonic,
        received_epoch=received_epoch,
    )


def http_status_failure(
    status_code: int,
    fallback_outcome: str,
    operation: str,
    *,
    received_monotonic: float | None = None,
    received_epoch: float | None = None,
) -> BusinessFailure:
    """Classify Locust's synthetic status 0 as a transport timeout."""
    if status_code == 0:
        return failure_message(
            "TIMEOUT",
            f"{operation} timed out",
            received_monotonic=received_monotonic,
            received_epoch=received_epoch,
        )
    return failure_message(
        fallback_outcome,
        f"{operation} status {status_code}",
        received_monotonic=received_monotonic,
        received_epoch=received_epoch,
    )


def receipt_context(
    context: dict[str, Any],
    prefix: str,
    received_epoch: float,
    event_id: str | None = None,
) -> None:
    context[f"{prefix}_received_at"] = utc_timestamp(received_epoch)
    context[f"{prefix}_received_epoch"] = round(received_epoch, 6)
    if event_id:
        context[f"{prefix}_event_id"] = event_id


def record_received_failure(
    context: dict[str, Any], error: BusinessFailure
) -> None:
    if error.received_epoch is None:
        return
    receipt_context(context, "failure", error.received_epoch)
    context["failure_message"] = error.detail or str(error)


def close_order_stream(
    stream: Iterator[ReceivedOrderEvent] | None,
) -> None:
    if stream is None:
        return
    close = getattr(stream, "close", None)
    if callable(close):
        close()


def next_order_event_before(
    stream: Iterator[ReceivedOrderEvent], deadline: float
) -> ReceivedOrderEvent:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise SSEDeadlineReached()
    with gevent.Timeout(remaining, SSEDeadlineReached()):
        return next(stream)


@contextmanager
def measure_business(
    user: FastHttpUser,
    name: str,
    context: dict[str, Any],
    started_monotonic: float,
    started_epoch: float,
) -> Any:
    measurement: dict[str, Any] = {
        "exception": None,
        "response_time": None,
    }
    try:
        yield measurement
    except Exception as error:
        measurement["exception"] = error
    finally:
        response_time = measurement["response_time"]
        if response_time is None:
            response_time = (time.monotonic() - started_monotonic) * 1000
        user.environment.events.request.fire(
            request_type="BUSINESS",
            name=name,
            response_time=response_time,
            response_length=0,
            response=None,
            context=context,
            exception=measurement["exception"],
            start_time=started_epoch,
            url=None,
        )


@events.test_start.add_listener
def on_test_start(environment: Any, **kwargs: Any) -> None:
    start_clock()
    random.seed(CONFIG.seed)
    RECORDER.open()
    RESOURCE_SAMPLER.start()


@events.request.add_listener
def on_request(
    request_type: str,
    name: str,
    response_time: float,
    exception: Exception | None,
    start_time: float | None = None,
    context: dict[str, Any] | None = None,
    **kwargs: Any,
) -> None:
    RECORDER.request(
        request_type,
        name,
        response_time,
        exception,
        start_time,
        context,
    )


@events.test_stop.add_listener
def on_test_stop(environment: Any, **kwargs: Any) -> None:
    gevent.joinall(list(SETTLEMENT_TRACKERS), timeout=3)
    for greenlet in list(SETTLEMENT_TRACKERS):
        greenlet.kill(block=True)
    RESOURCE_SAMPLER.stop()
    RECORDER.close()


class StorefrontAdapter:
    def __init__(
        self,
        user: FastHttpUser,
        seed: int,
        session_id: str | None = None,
    ) -> None:
        self.user = user
        self.client = user.client
        self.random = random.Random(seed)
        self.session_cookie = session_id

    def headers(
        self,
        accept: str | None = None,
        idempotency_key: bool = False,
    ) -> dict[str, str]:
        headers: dict[str, str] = {}
        if accept:
            headers["Accept"] = accept
        if self.session_cookie:
            headers["Cookie"] = (
                f"{SESSION_COOKIE_NAME}={self.session_cookie}"
            )
        if idempotency_key:
            headers["Idempotency-Key"] = str(uuid.uuid4())
        return headers

    def home(self) -> None:
        with self.client.get(
            "/", headers=self.headers(), catch_response=True
        ) as response:
            if response.status_code == 0:
                response.failure(
                    str(http_status_failure(0, "HTTP_FAILURE", "home request"))
                )

    def set_currency(self) -> None:
        with self.client.post(
            "/setCurrency",
            {"currency_code": self.random.choice(CURRENCIES)},
            headers=self.headers(),
            allow_redirects=False,
            catch_response=True,
            name="/setCurrency",
        ) as response:
            if response.status_code not in (302, 303):
                error = http_status_failure(
                    response.status_code, "HTTP_FAILURE", "currency request"
                )
                response.failure(str(error))

    def browse_product(self) -> None:
        with self.client.get(
            "/product/" + self.random.choice(PRODUCTS),
            headers=self.headers(),
            catch_response=True,
            name="/product/[id]",
        ) as response:
            if response.status_code == 0:
                response.failure(
                    str(
                        http_status_failure(
                            0, "HTTP_FAILURE", "product request"
                        )
                    )
                )

    def view_cart(self) -> None:
        with self.client.get(
            "/cart", headers=self.headers(), catch_response=True
        ) as response:
            if response.status_code == 0:
                response.failure(
                    str(http_status_failure(0, "HTTP_FAILURE", "cart request"))
                )

    def add_item(self) -> BusinessFailure | None:
        product = self.random.choice(PRODUCTS)
        with self.client.get(
            "/product/" + product,
            headers=self.headers(),
            catch_response=True,
            name="/product/[id]",
        ) as response:
            if response.status_code != 200:
                error = http_status_failure(
                    response.status_code,
                    "PRECONDITION_FAILED",
                    "product request",
                )
                response.failure(str(error))
                return error
            issued = issued_session_cookie(response)
            if issued is not None:
                self.session_cookie = issued
        return self._submit_cart_add(
            {
                "product_id": product,
                "quantity": self.random.randint(1, 10),
            }
        )

    def _submit_cart_add(
        self, data: dict[str, Any]
    ) -> BusinessFailure | None:
        raise NotImplementedError

    def checkout_data(self) -> dict[str, Any]:
        current_year = datetime.datetime.now().year + 1
        return {
            "email": f"benchmark-{uuid.uuid4().hex}@example.com",
            "street_address": "1600 Amphitheatre Parkway",
            "zip_code": "94043",
            "city": "Mountain View",
            "state": "CA",
            "country": "United States",
            "credit_card_number": "4111111111111111",
            "credit_card_expiration_month": self.random.randint(1, 12),
            "credit_card_expiration_year": self.random.randint(
                current_year, current_year + 4
            ),
            "credit_card_cvv": f"{self.random.randint(100, 999)}",
        }

    def base_context(
        self,
        scheduled_at: float | None,
        schedule_delay_ms: float | None,
        saturation_rung: int | None = None,
        target_requests_per_second: float | None = None,
    ) -> dict[str, Any]:
        phase = phase_now()
        if scheduled_at is not None:
            phase = phase_for_elapsed(
                max(0.0, scheduled_at - runtime.TEST_STARTED_EPOCH)
            )
        if saturation_rung is not None:
            phase = "steady"
        elif (
            CONFIG.workload == "saturation"
            and target_requests_per_second is not None
        ):
            phase = "warmup"
        context = {
            "application_type": CONFIG.application_type,
            "workload": CONFIG.workload,
            "phase": phase,
            "scheduled_at": scheduled_at,
            "schedule_delay_ms": round(schedule_delay_ms, 3)
            if schedule_delay_ms is not None
            else None,
            "accepted": False,
            "outcome": "UNKNOWN",
        }
        if CONFIG.workload == "saturation":
            context.update(
                {
                    "saturation_rung": saturation_rung,
                    "target_requests_per_second": (
                        target_requests_per_second
                    ),
                }
            )
        return context

    def record_precondition_failure(
        self,
        scheduled_at: float | None = None,
        schedule_delay_ms: float | None = None,
        saturation_rung: int | None = None,
        target_requests_per_second: float | None = None,
        failure: BusinessFailure | None = None,
    ) -> None:
        context = self.base_context(
            scheduled_at,
            schedule_delay_ms,
            saturation_rung,
            target_requests_per_second,
        )
        error = failure or failure_message(
            "PRECONDITION_FAILED", "could not prepare cart"
        )
        context["outcome"] = error.outcome
        context["failure_message"] = error.detail or str(error)
        with self.user.environment.events.request.measure(
            "BUSINESS", "checkout_to_outcome", context=context
        ) as measurement:
            measurement["exception"] = error

    def record_generator_saturation(
        self,
        scheduled_at: float,
        schedule_delay_ms: float,
        saturation_rung: int | None = None,
        target_requests_per_second: float | None = None,
    ) -> None:
        context = self.base_context(
            scheduled_at,
            schedule_delay_ms,
            saturation_rung,
            target_requests_per_second,
        )
        context["outcome"] = "GENERATOR_SATURATED"
        with self.user.environment.events.request.measure(
            "BUSINESS", "checkout_to_outcome", context=context
        ) as measurement:
            measurement["exception"] = failure_message(
                "GENERATOR_SATURATED",
                "open-loop concurrency limit reached",
            )

    def checkout(
        self,
        scheduled_at: float | None = None,
        schedule_delay_ms: float | None = None,
        track_settlement_inline: bool = False,
        saturation_rung: int | None = None,
        target_requests_per_second: float | None = None,
    ) -> None:
        raise NotImplementedError


class GrpcAdapter(StorefrontAdapter):
    def _submit_cart_add(
        self, data: dict[str, Any]
    ) -> BusinessFailure | None:
        with self.client.post(
            "/cart",
            data,
            headers=self.headers(),
            allow_redirects=False,
            catch_response=True,
            name="/cart",
        ) as response:
            if response.status_code not in (302, 303):
                error = http_status_failure(
                    response.status_code,
                    "PRECONDITION_FAILED",
                    "cart request",
                )
                response.failure(str(error))
                return error
            response.success()
            return None

    def checkout(
        self,
        scheduled_at: float | None = None,
        schedule_delay_ms: float | None = None,
        track_settlement_inline: bool = False,
        saturation_rung: int | None = None,
        target_requests_per_second: float | None = None,
    ) -> None:
        context = self.base_context(
            scheduled_at,
            schedule_delay_ms,
            saturation_rung,
            target_requests_per_second,
        )
        data = self.checkout_data()
        with self.user.environment.events.request.measure(
            "BUSINESS", "checkout_to_outcome", context=context
        ) as measurement:
            with self.client.post(
                "/cart/checkout",
                data,
                headers=self.headers("text/html"),
                allow_redirects=False,
                catch_response=True,
                name="/cart/checkout",
            ) as response:
                if response.status_code != 200:
                    error = http_status_failure(
                        response.status_code,
                        "HTTP_FAILURE",
                        "checkout request",
                    )
                    response.failure(str(error))
                    context["outcome"] = error.outcome
                    measurement["exception"] = error
                    return
                response.success()
                context["accepted"] = True
                context["outcome"] = "COMPLETED"


class NatsAdapter(StorefrontAdapter):
    def _submit_cart_add(
        self, data: dict[str, Any]
    ) -> BusinessFailure | None:
        with self.client.post(
            "/cart",
            data,
            headers=self.headers("application/json", idempotency_key=True),
            allow_redirects=False,
            catch_response=True,
            name="/cart",
        ) as response:
            if response.status_code in (302, 303):
                response.success()
                return None
            if response.status_code != 202:
                error = http_status_failure(
                    response.status_code,
                    "PRECONDITION_FAILED",
                    "cart request",
                )
                response.failure(str(error))
                return error
            location = response.headers.get("Location")
            delay = retry_after_seconds(response)
            if not location:
                response.failure("202 cart operation omitted Location")
                return failure_message(
                    "PRECONDITION_FAILED",
                    "cart operation Location is missing",
                )
            response.success()

        deadline = min(
            time.monotonic() + CONFIG.outcome_timeout_seconds,
            drain_deadline(),
        )
        while time.monotonic() < deadline:
            gevent.sleep(delay)
            with self.client.get(
                location,
                headers=self.headers("application/json"),
                catch_response=True,
                name="/operations/[id]",
            ) as status_response:
                delay = retry_after_seconds(status_response)
                if status_response.status_code in (404, 503):
                    status_response.success()
                    continue
                if status_response.status_code != 200:
                    error = http_status_failure(
                        status_response.status_code,
                        "PRECONDITION_FAILED",
                        "cart operation request",
                    )
                    status_response.failure(str(error))
                    return error
                try:
                    operation = status_response.json()
                except Exception:
                    status_response.failure("operation response is not JSON")
                    return failure_message(
                        "PRECONDITION_FAILED",
                        "cart operation response is not JSON",
                    )
                status_response.success()
                if operation.get("status") == "SUCCEEDED":
                    return None
                if operation.get("status") == "REJECTED":
                    return failure_message(
                        "PRECONDITION_FAILED", "cart operation was rejected"
                    )
        return failure_message(
            "PRECONDITION_FAILED", "cart operation did not complete"
        )

    def checkout(
        self,
        scheduled_at: float | None = None,
        schedule_delay_ms: float | None = None,
        track_settlement_inline: bool = False,
        saturation_rung: int | None = None,
        target_requests_per_second: float | None = None,
    ) -> None:
        base_context = self.base_context(
            scheduled_at,
            schedule_delay_ms,
            saturation_rung,
            target_requests_per_second,
        )
        settled_context = dict(base_context)
        data = self.checkout_data()
        checkout_started = time.monotonic()
        checkout_started_epoch = time.time()
        outcome_context, order, events_location, stream, outcome_error = (
            self._checkout_to_outcome(
                data,
                dict(base_context),
                checkout_started,
                checkout_started_epoch,
            )
        )
        settled_context.update(outcome_context)
        if (
            outcome_error is not None
            or order is None
            or events_location is None
        ):
            close_order_stream(stream)
            settled_context["settlement"] = "ORDER_NOT_COMPLETED"
            self._fire_settlement(
                settled_context,
                checkout_started,
                checkout_started_epoch,
                outcome_error
                or failure_message(
                    str(outcome_context.get("outcome", "UNKNOWN"))
                ),
            )
            return

        arguments = (
            events_location,
            order,
            stream,
            checkout_started,
            checkout_started_epoch,
            settled_context,
        )
        if track_settlement_inline:
            self._track_settlement(*arguments)
            return
        tracker = gevent.spawn(self._track_settlement, *arguments)
        SETTLEMENT_TRACKERS.add(tracker)
        tracker.link(lambda completed: SETTLEMENT_TRACKERS.discard(completed))

    def _track_settlement(
        self,
        events_location: str,
        order: ReceivedOrderEvent,
        stream: Iterator[ReceivedOrderEvent] | None,
        checkout_started: float,
        checkout_started_epoch: float,
        context: dict[str, Any],
    ) -> None:
        error: BusinessFailure | None = failure_message("INCOMPLETE")
        try:
            error = self._wait_for_settlement(
                events_location,
                order,
                stream,
                checkout_started,
                context,
            )
        finally:
            if "settlement" not in context:
                context["settlement"] = "INCOMPLETE"
            self._fire_settlement(
                context, checkout_started, checkout_started_epoch, error
            )

    def _fire_settlement(
        self,
        context: dict[str, Any],
        checkout_started: float,
        checkout_started_epoch: float,
        error: BusinessFailure | None,
    ) -> None:
        finished_monotonic = context.get("_settlement_received_monotonic")
        if not isinstance(finished_monotonic, (int, float)):
            finished_monotonic = getattr(error, "received_monotonic", None)
        if not isinstance(finished_monotonic, (int, float)):
            finished_monotonic = time.monotonic()
        self.user.environment.events.request.fire(
            request_type="BUSINESS",
            name="checkout_to_settled",
            response_time=received_latency_ms(
                checkout_started, float(finished_monotonic)
            ),
            response_length=0,
            response=None,
            context=context,
            exception=error,
            start_time=checkout_started_epoch,
            url=None,
        )

    def _checkout_to_outcome(
        self,
        data: dict[str, Any],
        context: dict[str, Any],
        checkout_started: float,
        checkout_started_epoch: float,
    ) -> tuple[
        dict[str, Any],
        ReceivedOrderEvent | None,
        str | None,
        Iterator[ReceivedOrderEvent] | None,
        BusinessFailure | None,
    ]:
        final_order: ReceivedOrderEvent | None = None
        location: str | None = None
        events_location: str | None = None
        stream: Iterator[ReceivedOrderEvent] | None = None
        error: BusinessFailure | None = None
        transaction_id = str(uuid.uuid4())
        context["transaction_id"] = transaction_id

        with measure_business(
            self.user,
            "checkout_to_outcome",
            context,
            checkout_started,
            checkout_started_epoch,
        ) as outcome_measurement:
            acceptance_context = {
                **context,
                "outcome": "UNKNOWN",
                "accepted": False,
            }
            with self.user.environment.events.request.measure(
                "BUSINESS", "checkout_acceptance", context=acceptance_context
            ) as acceptance_measurement:
                with self.client.post(
                    "/cart/checkout",
                    data,
                    headers=self.headers(
                        "application/json", idempotency_key=True
                    ),
                    allow_redirects=False,
                    catch_response=True,
                    name="/cart/checkout",
                ) as response:
                    if response.status_code != 202:
                        error = http_status_failure(
                            response.status_code,
                            "HTTP_FAILURE",
                            "checkout request",
                        )
                        response.failure(str(error))
                        context["outcome"] = error.outcome
                        acceptance_context["outcome"] = error.outcome
                        acceptance_measurement["exception"] = error
                    else:
                        location = response.headers.get("Location")
                        if not location:
                            response.failure(
                                "202 checkout response omitted Location"
                            )
                            context["outcome"] = "INVALID_RESPONSE"
                            acceptance_context["outcome"] = "INVALID_RESPONSE"
                            error = failure_message(
                                "INVALID_RESPONSE", "Location is missing"
                            )
                            acceptance_measurement["exception"] = error
                        else:
                            response.success()
                            order_id = response.headers.get("X-Order-ID")
                            context.update(
                                {
                                    "accepted": True,
                                    "order_id": order_id,
                                }
                            )
                            acceptance_context.update(
                                {
                                    "accepted": True,
                                    "outcome": "ACCEPTED",
                                    "order_id": order_id,
                                }
                            )
                            RECORDER.accepted(
                                transaction_id, str(context["phase"])
                            )

            if error is not None or location is None:
                outcome_measurement["exception"] = error
                return context, None, None, None, error

            try:
                events_location = order_events_url(location)
            except ValueError as location_error:
                context["outcome"] = "INVALID_RESPONSE"
                error = failure_message(
                    "INVALID_RESPONSE", str(location_error)
                )
                outcome_measurement["exception"] = error
                RECORDER.terminal(
                    transaction_id, "INVALID_RESPONSE", phase_now()
                )
                return context, None, None, None, error

            per_order_deadline = (
                checkout_started + CONFIG.outcome_timeout_seconds
            )
            deadline = min(per_order_deadline, drain_deadline())
            while time.monotonic() < deadline:
                if stream is None:
                    stream, error = self._open_order_event_stream(
                        events_location, "/orders/[id]/events"
                    )
                    if error is not None:
                        context["outcome"] = error.outcome
                        record_received_failure(context, error)
                        if error.received_monotonic is not None:
                            outcome_measurement["response_time"] = (
                                received_latency_ms(
                                    checkout_started,
                                    error.received_monotonic,
                                )
                            )
                        outcome_measurement["exception"] = error
                        RECORDER.terminal(
                            transaction_id, error.outcome, phase_now()
                        )
                        return context, None, events_location, None, error
                    if stream is None:
                        gevent.sleep(
                            min(
                                SSE_RECONNECT_DELAY_SECONDS,
                                max(0.0, deadline - time.monotonic()),
                            )
                        )
                        continue

                try:
                    received_order = next_order_event_before(stream, deadline)
                except SSEDeadlineReached:
                    break
                except SSEProtocolError as stream_error:
                    received_monotonic = time.monotonic()
                    received_epoch = time.time()
                    close_order_stream(stream)
                    stream = None
                    context["outcome"] = "INVALID_RESPONSE"
                    error = failure_message(
                        "INVALID_RESPONSE",
                        str(stream_error),
                        received_monotonic=received_monotonic,
                        received_epoch=received_epoch,
                    )
                    record_received_failure(context, error)
                    outcome_measurement["response_time"] = (
                        received_latency_ms(
                            checkout_started, received_monotonic
                        )
                    )
                    outcome_measurement["exception"] = error
                    RECORDER.terminal(
                        transaction_id, "INVALID_RESPONSE", phase_now()
                    )
                    return context, None, events_location, None, error
                except StopIteration:
                    close_order_stream(stream)
                    stream = None
                    gevent.sleep(
                        min(
                            SSE_RECONNECT_DELAY_SECONDS,
                            max(0.0, deadline - time.monotonic()),
                        )
                    )
                    continue
                except gevent.Timeout:
                    close_order_stream(stream)
                    stream = None
                    continue
                except Exception:
                    close_order_stream(stream)
                    stream = None
                    gevent.sleep(
                        min(
                            SSE_RECONNECT_DELAY_SECONDS,
                            max(0.0, deadline - time.monotonic()),
                        )
                    )
                    continue

                order = received_order.order
                expected_order_id = context.get("order_id")
                event_order_id = str(order.get("order_id", ""))
                if expected_order_id and event_order_id != expected_order_id:
                    received_monotonic = received_order.received_monotonic
                    received_epoch = received_order.received_epoch
                    close_order_stream(stream)
                    stream = None
                    context["outcome"] = "INVALID_RESPONSE"
                    error = failure_message(
                        "INVALID_RESPONSE",
                        "order event ID does not match accepted order",
                        received_monotonic=received_monotonic,
                        received_epoch=received_epoch,
                    )
                    record_received_failure(context, error)
                    outcome_measurement["response_time"] = (
                        received_latency_ms(
                            checkout_started, received_monotonic
                        )
                    )
                    outcome_measurement["exception"] = error
                    RECORDER.terminal(
                        transaction_id, "INVALID_RESPONSE", phase_now()
                    )
                    return context, None, events_location, None, error

                status = str(order.get("status", "UNKNOWN"))
                if status not in TERMINAL_ORDER_STATUSES:
                    continue

                outcome_measurement["response_time"] = received_latency_ms(
                    checkout_started, received_order.received_monotonic
                )
                context.update(
                    {
                        "outcome": status,
                        "order_id": event_order_id,
                        "failure_code": order.get("failure_code"),
                        "safe_message": order.get("safe_message"),
                        "outcome_at": order.get("outcome_at"),
                        "outcome_transport": "SSE",
                    }
                )
                receipt_context(
                    context,
                    "outcome",
                    received_order.received_epoch,
                    received_order.event_id,
                )
                RECORDER.terminal(transaction_id, status, phase_now())
                final_order = received_order
                if status != "COMPLETED":
                    detail = str(
                        order.get("safe_message")
                        or order.get("failure_code")
                        or ""
                    )
                    error = failure_message(
                        status,
                        detail,
                        received_monotonic=(
                            received_order.received_monotonic
                        ),
                        received_epoch=received_order.received_epoch,
                    )
                    record_received_failure(context, error)
                    outcome_measurement["exception"] = error
                    close_order_stream(stream)
                    stream = None
                return (
                    context,
                    final_order,
                    events_location,
                    stream,
                    error,
                )

            close_order_stream(stream)
            outcome = (
                "INCOMPLETE"
                if drain_deadline() <= per_order_deadline
                else "TIMEOUT"
            )
            context["outcome"] = outcome
            error = failure_message(outcome)
            outcome_measurement["exception"] = error
            RECORDER.terminal(transaction_id, outcome, phase_now())
            return context, None, events_location, None, error

    def _open_order_event_stream(
        self, events_location: str, name: str
    ) -> tuple[
        Iterator[ReceivedOrderEvent] | None,
        BusinessFailure | None,
    ]:
        response = self.client.get(
            events_location,
            headers=self.headers("text/event-stream"),
            allow_redirects=False,
            catch_response=True,
            stream=True,
            name=name,
        )
        received_monotonic = time.monotonic()
        received_epoch = time.time()
        with response as status_response:
            if status_response.status_code in (404, 503):
                status_response.success()
                close_stream_response(status_response)
                return None, None
            if status_response.status_code != 200:
                error = http_status_failure(
                    status_response.status_code,
                    "STATUS_ERROR",
                    "SSE",
                    received_monotonic=received_monotonic,
                    received_epoch=received_epoch,
                )
                status_response.failure(str(error))
                close_stream_response(status_response)
                return None, error
            content_type = status_response.headers.get("Content-Type", "")
            if not is_event_stream_content_type(content_type):
                status_response.failure(
                    "order event response is not text/event-stream"
                )
                close_stream_response(status_response)
                return None, failure_message(
                    "INVALID_RESPONSE",
                    f"SSE Content-Type is {content_type!r}",
                    received_monotonic=received_monotonic,
                    received_epoch=received_epoch,
                )
            status_response.success()
        return iter_received_orders(response), None

    def _wait_for_settlement(
        self,
        events_location: str,
        order: ReceivedOrderEvent,
        stream: Iterator[ReceivedOrderEvent] | None,
        checkout_started: float,
        context: dict[str, Any],
    ) -> BusinessFailure | None:
        per_order_deadline = (
            checkout_started + CONFIG.settlement_timeout_seconds
        )
        deadline = min(per_order_deadline, drain_deadline())
        try:
            while True:
                if self._record_settlement_event(context, order):
                    context["settlement"] = "SETTLED"
                    return None
                if time.monotonic() >= deadline:
                    settlement = (
                        "INCOMPLETE"
                        if drain_deadline() <= per_order_deadline
                        else "SETTLEMENT_TIMEOUT"
                    )
                    context["settlement"] = settlement
                    return failure_message(settlement)

                if stream is None:
                    stream, error = self._open_order_event_stream(
                        events_location,
                        "/orders/[id]/events [settlement]",
                    )
                    if error is not None:
                        context["settlement"] = error.outcome
                        record_received_failure(context, error)
                        return error
                    if stream is None:
                        gevent.sleep(
                            min(
                                SSE_RECONNECT_DELAY_SECONDS,
                                max(0.0, deadline - time.monotonic()),
                            )
                        )
                        continue

                try:
                    order = next_order_event_before(stream, deadline)
                except SSEDeadlineReached:
                    continue
                except SSEProtocolError as stream_error:
                    received_monotonic = time.monotonic()
                    received_epoch = time.time()
                    context["settlement"] = "INVALID_RESPONSE"
                    error = failure_message(
                        "INVALID_RESPONSE",
                        str(stream_error),
                        received_monotonic=received_monotonic,
                        received_epoch=received_epoch,
                    )
                    record_received_failure(context, error)
                    return error
                except StopIteration:
                    close_order_stream(stream)
                    stream = None
                    gevent.sleep(
                        min(
                            SSE_RECONNECT_DELAY_SECONDS,
                            max(0.0, deadline - time.monotonic()),
                        )
                    )
                except gevent.Timeout:
                    close_order_stream(stream)
                    stream = None
                except Exception:
                    close_order_stream(stream)
                    stream = None
                    gevent.sleep(
                        min(
                            SSE_RECONNECT_DELAY_SECONDS,
                            max(0.0, deadline - time.monotonic()),
                        )
                    )
        finally:
            close_order_stream(stream)

    def _record_settlement_event(
        self,
        context: dict[str, Any],
        received_order: ReceivedOrderEvent,
    ) -> bool:
        order = received_order.order
        notification_status = str(order.get("notification_status", ""))
        cart_clear_status = str(order.get("cart_clear_status", ""))
        context.update(
            {
                "notification_status": notification_status or None,
                "cart_clear_status": cart_clear_status or None,
                "cart_clear_failure_code": order.get(
                    "cart_clear_failure_code"
                ),
                "settlement_transport": "SSE",
            }
        )
        if (
            notification_status in TERMINAL_NOTIFICATION_STATUSES
            and "notification_received_at" not in context
        ):
            receipt_context(
                context,
                "notification",
                received_order.received_epoch,
                received_order.event_id,
            )
        if (
            notification_status == "FAILED"
            and "notification_failure_received_at" not in context
        ):
            receipt_context(
                context,
                "notification_failure",
                received_order.received_epoch,
                received_order.event_id,
            )
            context["notification_failure_message"] = (
                "order confirmation notification failed"
            )
            if "failure_received_at" not in context:
                receipt_context(
                    context,
                    "failure",
                    received_order.received_epoch,
                    received_order.event_id,
                )
                context["failure_message"] = context[
                    "notification_failure_message"
                ]
        if (
            cart_clear_status in TERMINAL_CART_CLEAR_STATUSES
            and "cart_clear_received_at" not in context
        ):
            receipt_context(
                context,
                "cart_clear",
                received_order.received_epoch,
                received_order.event_id,
            )
        if (
            cart_clear_status == "REJECTED"
            and "cart_clear_failure_received_at" not in context
        ):
            receipt_context(
                context,
                "cart_clear_failure",
                received_order.received_epoch,
                received_order.event_id,
            )
            context["cart_clear_failure_message"] = str(
                order.get("cart_clear_failure_code")
                or "cart clear rejected"
            )
            if "failure_received_at" not in context:
                receipt_context(
                    context,
                    "failure",
                    received_order.received_epoch,
                    received_order.event_id,
                )
                context["failure_message"] = context[
                    "cart_clear_failure_message"
                ]

        settled = (
            notification_status in TERMINAL_NOTIFICATION_STATUSES
            and cart_clear_status in TERMINAL_CART_CLEAR_STATUSES
        )
        if settled:
            receipt_context(
                context,
                "settlement",
                received_order.received_epoch,
                received_order.event_id,
            )
            context["_settlement_received_monotonic"] = (
                received_order.received_monotonic
            )
        return settled


def adapter_for(
    user: FastHttpUser, seed: int, session_id: str | None = None
) -> StorefrontAdapter:
    adapter_type = NatsAdapter if CONFIG.application_type == "NATS" else GrpcAdapter
    return adapter_type(user, seed, session_id)


class ClosedLoopUser(FastHttpUser):
    wait_time = between(1, 10)
    network_timeout = SSE_NETWORK_TIMEOUT_SECONDS
    connection_timeout = 5.0

    def on_start(self) -> None:
        seed = CONFIG.seed + id(self) % 1_000_003
        self.adapter = adapter_for(self, seed)
        self.adapter.home()

    def ensure_submission_window(self) -> None:
        if time.monotonic() >= submission_deadline():
            raise StopUser()

    @task(1)
    def home(self) -> None:
        self.ensure_submission_window()
        self.adapter.home()

    @task(2)
    def currency(self) -> None:
        self.ensure_submission_window()
        self.adapter.set_currency()

    @task(10)
    def product(self) -> None:
        self.ensure_submission_window()
        self.adapter.browse_product()

    @task(2)
    def add_to_cart(self) -> None:
        self.ensure_submission_window()
        self.adapter.add_item()

    @task(3)
    def cart(self) -> None:
        self.ensure_submission_window()
        self.adapter.view_cart()

    @task(1)
    def checkout(self) -> None:
        self.ensure_submission_window()
        preparation_error = self.adapter.add_item()
        if preparation_error is not None:
            self.adapter.record_precondition_failure(failure=preparation_error)
            return
        self.adapter.checkout()


class OpenLoopDriver(FastHttpUser):
    fixed_count = 1
    wait_time = lambda self: 0
    network_timeout = SSE_NETWORK_TIMEOUT_SECONDS
    connection_timeout = 5.0
    concurrency = max(
        20,
        min(
            5_000,
            math.ceil(
                CONFIG.arrival_rate
                * max(
                    CONFIG.outcome_timeout_seconds,
                    CONFIG.settlement_timeout_seconds,
                )
            ),
        ),
    )

    @task
    def schedule(self) -> None:
        active: set[gevent.Greenlet] = set()
        interval = 1.0 / CONFIG.arrival_rate
        index = 0
        while True:
            scheduled_elapsed = index * interval
            if scheduled_elapsed >= CONFIG.submission_seconds:
                break
            scheduled_monotonic = (
                submission_deadline()
                - CONFIG.submission_seconds
                + scheduled_elapsed
            )
            delay = scheduled_monotonic - time.monotonic()
            if delay > 0:
                gevent.sleep(delay)
            scheduled_at = runtime.TEST_STARTED_EPOCH + scheduled_elapsed
            schedule_delay_ms = max(
                0.0, (time.monotonic() - scheduled_monotonic) * 1000
            )
            if len(active) >= self.concurrency:
                adapter_for(
                    self,
                    CONFIG.seed + index + 1,
                ).record_generator_saturation(
                    scheduled_at, schedule_delay_ms
                )
                index += 1
                continue
            greenlet = gevent.spawn(
                self._transaction,
                index,
                scheduled_at,
                schedule_delay_ms,
            )
            active.add(greenlet)
            greenlet.link(lambda completed: active.discard(completed))
            index += 1

        remaining = max(0.0, drain_deadline() - time.monotonic())
        gevent.joinall(list(active), timeout=remaining)
        for greenlet in list(active):
            greenlet.kill(block=False)
        raise StopUser()

    def _transaction(
        self,
        index: int,
        scheduled_at: float,
        schedule_delay_ms: float,
    ) -> None:
        adapter = adapter_for(
            self,
            CONFIG.seed + index + 1,
            session_id=f"benchmark-{uuid.uuid4()}",
        )
        preparation_error = adapter.add_item()
        if preparation_error is not None:
            adapter.record_precondition_failure(
                scheduled_at,
                schedule_delay_ms,
                failure=preparation_error,
            )
            return
        adapter.checkout(
            scheduled_at,
            schedule_delay_ms,
            track_settlement_inline=True,
        )


class SaturationDriver(FastHttpUser):
    fixed_count = 1
    wait_time = lambda self: 0
    network_timeout = SSE_NETWORK_TIMEOUT_SECONDS
    connection_timeout = 5.0
    worker_count = max(
        1, int(os.environ.get("BENCHMARK_WORKER_COUNT", "1") or "1")
    )
    worker_index = int(
        os.environ.get("BENCHMARK_WORKER_INDEX", "") or "0"
    )
    concurrency = max(
        20,
        min(
            5_000,
            math.ceil(
                CONFIG.saturation_effective_max_rate
                / worker_count
                * max(
                    CONFIG.outcome_timeout_seconds,
                    CONFIG.settlement_timeout_seconds,
                )
            ),
        ),
    )

    @task
    def schedule(self) -> None:
        if getattr(self, "_saturation_started", False):
            # A Locust task exception normally causes the same task to run
            # again. Saturation is a one-shot schedule, so never start a new
            # ladder (and new submissions) after a failed attempt.
            self.environment.process_exit_code = 1
            runner = self.environment.runner
            if runner is not None:
                gevent.spawn_later(0, runner.quit)
            raise StopUser()
        self._saturation_started = True

        active: set[gevent.Greenlet] = set()
        coordinator: SaturationCoordinator | None = None
        try:
            coordinator = SaturationCoordinator(
                application_type=CONFIG.application_type,
                output_directory=runtime.OUTPUT_DIRECTORY,
                worker_index=self.worker_index,
                worker_count=self.worker_count,
            )
            if CONFIG.warmup_seconds:
                self._schedule_window(
                    active=active,
                    duration_seconds=CONFIG.warmup_seconds,
                    target_rate=SATURATION_START_RATE,
                    rung=None,
                )

            remaining = float(CONFIG.duration_seconds)
            rung = 0
            while remaining > 0:
                duration = min(
                    float(CONFIG.saturation_step_seconds), remaining
                )
                target_rate = min(
                    CONFIG.saturation_max_rate,
                    SATURATION_START_RATE + rung * SATURATION_STEP_RATE,
                )
                runtime.set_saturation_rung(rung, target_rate)
                completed_before = RECORDER.completed_count()
                pending_start = RESOURCE_SAMPLER.latest_pending()
                started_elapsed = runtime.elapsed_now()
                self._schedule_window(
                    active=active,
                    duration_seconds=duration,
                    target_rate=target_rate,
                    rung=rung,
                )
                ended_elapsed = runtime.elapsed_now()
                remaining -= duration
                maximum_rate_reached = (
                    target_rate >= CONFIG.saturation_max_rate
                )
                decision = coordinator.finish_rung(
                    rung=rung,
                    target_rate=target_rate,
                    started_elapsed_seconds=started_elapsed,
                    ended_elapsed_seconds=ended_elapsed,
                    completed_before=completed_before,
                    completed_after=RECORDER.completed_count(),
                    pending_start=pending_start,
                    pending_end=RESOURCE_SAMPLER.latest_pending(),
                    final_rung=remaining <= 0,
                    maximum_rate_reached=maximum_rate_reached,
                )
                if decision.get("stop"):
                    runtime.begin_early_drain()
                    break
                rung += 1

            remaining_drain = max(
                0.0, drain_deadline() - time.monotonic()
            )
            gevent.joinall(list(active), timeout=remaining_drain)
            for greenlet in list(active):
                greenlet.kill(block=False)
        except Exception:
            # The Job runner uses this override as its process result. Without
            # it, --exit-code-on-error=0 would make coordination failures look
            # like successful benchmark workers.
            self.environment.process_exit_code = 1
            raise
        finally:
            for greenlet in list(active):
                greenlet.kill(block=False)
            try:
                if coordinator is not None:
                    coordinator.close()
            except Exception:
                self.environment.process_exit_code = 1
                raise
            finally:
                # Always stop the runner. Otherwise Locust catches a task
                # failure and begins the complete saturation ladder again.
                runner = self.environment.runner
                if runner is not None:
                    gevent.spawn_later(0, runner.quit)
        raise StopUser()

    def _schedule_window(
        self,
        *,
        active: set[gevent.Greenlet],
        duration_seconds: float,
        target_rate: float,
        rung: int | None,
    ) -> None:
        window_started_monotonic = time.monotonic()
        window_started_epoch = time.time()
        scheduled_count = max(
            0, math.ceil(target_rate * duration_seconds - 1e-9)
        )
        for ordinal in range(
            self.worker_index, scheduled_count, self.worker_count
        ):
            scheduled_offset = ordinal / target_rate
            scheduled_monotonic = (
                window_started_monotonic + scheduled_offset
            )
            delay = scheduled_monotonic - time.monotonic()
            if delay > 0:
                gevent.sleep(delay)
            scheduled_at = window_started_epoch + scheduled_offset
            schedule_delay_ms = max(
                0.0, (time.monotonic() - scheduled_monotonic) * 1000
            )
            if len(active) >= self.concurrency:
                adapter_for(
                    self,
                    CONFIG.seed + (rung or 0) * 1_000_003 + ordinal + 1,
                ).record_generator_saturation(
                    scheduled_at,
                    schedule_delay_ms,
                    rung,
                    target_rate,
                )
                continue
            greenlet = gevent.spawn(
                self._transaction,
                ordinal,
                scheduled_at,
                schedule_delay_ms,
                rung,
                target_rate,
            )
            active.add(greenlet)
            greenlet.link(lambda completed: active.discard(completed))

        window_ends = window_started_monotonic + duration_seconds
        delay = window_ends - time.monotonic()
        if delay > 0:
            gevent.sleep(delay)

    def _transaction(
        self,
        ordinal: int,
        scheduled_at: float,
        schedule_delay_ms: float,
        rung: int | None,
        target_rate: float,
    ) -> None:
        adapter = adapter_for(
            self,
            CONFIG.seed + (rung or 0) * 1_000_003 + ordinal + 1,
            session_id=f"benchmark-{uuid.uuid4()}",
        )
        preparation_error = adapter.add_item()
        if preparation_error is not None:
            adapter.record_precondition_failure(
                scheduled_at,
                schedule_delay_ms,
                rung,
                target_rate,
                failure=preparation_error,
            )
            return
        adapter.checkout(
            scheduled_at,
            schedule_delay_ms,
            track_settlement_inline=True,
            saturation_rung=rung,
            target_requests_per_second=target_rate,
        )
