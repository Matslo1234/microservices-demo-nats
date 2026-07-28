# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

from __future__ import annotations

import datetime
import math
import random
import time
import uuid
from contextlib import contextmanager
from typing import Any

import gevent
from locust import FastHttpUser, between, events, task
from locust.exception import StopUser

import runtime
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
from session_cookie import SESSION_COOKIE_NAME, issued_session_cookie
from timing import outcome_latency_ms


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
SETTLEMENT_TRACKERS: set[gevent.Greenlet] = set()


class BusinessFailure(Exception):
    pass


def retry_after_seconds(response: Any, default: float = 1.0) -> float:
    try:
        value = float(response.headers.get("Retry-After", default))
    except (AttributeError, TypeError, ValueError):
        value = default
    return min(5.0, max(0.05, value))


def failure_message(outcome: str, detail: str = "") -> BusinessFailure:
    suffix = f": {detail}" if detail else ""
    return BusinessFailure(outcome + suffix)


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
        self.client.get("/", headers=self.headers())

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
                response.failure(
                    f"unexpected currency status {response.status_code}"
                )

    def browse_product(self) -> None:
        self.client.get(
            "/product/" + self.random.choice(PRODUCTS),
            headers=self.headers(),
            name="/product/[id]",
        )

    def view_cart(self) -> None:
        self.client.get("/cart", headers=self.headers())

    def add_item(self) -> bool:
        product = self.random.choice(PRODUCTS)
        response = self.client.get(
            "/product/" + product,
            headers=self.headers(),
            name="/product/[id]",
        )
        issued = issued_session_cookie(response)
        if issued is not None:
            self.session_cookie = issued
        if response.status_code != 200:
            return False
        return self._submit_cart_add(
            {
                "product_id": product,
                "quantity": self.random.randint(1, 10),
            }
        )

    def _submit_cart_add(self, data: dict[str, Any]) -> bool:
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
    ) -> dict[str, Any]:
        phase = phase_now()
        if scheduled_at is not None:
            phase = phase_for_elapsed(
                max(0.0, scheduled_at - runtime.TEST_STARTED_EPOCH)
            )
        return {
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

    def record_precondition_failure(
        self,
        scheduled_at: float | None = None,
        schedule_delay_ms: float | None = None,
    ) -> None:
        context = self.base_context(scheduled_at, schedule_delay_ms)
        context["outcome"] = "PRECONDITION_FAILED"
        with self.user.environment.events.request.measure(
            "BUSINESS", "checkout_to_outcome", context=context
        ) as measurement:
            measurement["exception"] = failure_message(
                "PRECONDITION_FAILED", "could not prepare cart"
            )

    def checkout(
        self,
        scheduled_at: float | None = None,
        schedule_delay_ms: float | None = None,
    ) -> None:
        raise NotImplementedError


class GrpcAdapter(StorefrontAdapter):
    def _submit_cart_add(self, data: dict[str, Any]) -> bool:
        with self.client.post(
            "/cart",
            data,
            headers=self.headers(),
            allow_redirects=False,
            catch_response=True,
            name="/cart",
        ) as response:
            if response.status_code not in (302, 303):
                response.failure(f"unexpected cart status {response.status_code}")
                return False
            response.success()
            return True

    def checkout(
        self,
        scheduled_at: float | None = None,
        schedule_delay_ms: float | None = None,
    ) -> None:
        context = self.base_context(scheduled_at, schedule_delay_ms)
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
                    response.failure(
                        f"unexpected checkout status {response.status_code}"
                    )
                    context["outcome"] = "HTTP_FAILURE"
                    measurement["exception"] = failure_message(
                        "HTTP_FAILURE", f"status {response.status_code}"
                    )
                    return
                response.success()
                context["accepted"] = True
                context["outcome"] = "COMPLETED"


class NatsAdapter(StorefrontAdapter):
    def _submit_cart_add(self, data: dict[str, Any]) -> bool:
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
                return True
            if response.status_code != 202:
                response.failure(f"unexpected cart status {response.status_code}")
                return False
            location = response.headers.get("Location")
            delay = retry_after_seconds(response)
            if not location:
                response.failure("202 cart operation omitted Location")
                return False
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
                    status_response.failure(
                        "unexpected operation status "
                        f"{status_response.status_code}"
                    )
                    return False
                try:
                    operation = status_response.json()
                except Exception:
                    status_response.failure("operation response is not JSON")
                    return False
                status_response.success()
                if operation.get("status") == "SUCCEEDED":
                    return True
                if operation.get("status") == "REJECTED":
                    return False
        return False

    def checkout(
        self,
        scheduled_at: float | None = None,
        schedule_delay_ms: float | None = None,
    ) -> None:
        base_context = self.base_context(scheduled_at, schedule_delay_ms)
        settled_context = dict(base_context)
        data = self.checkout_data()
        checkout_started = time.monotonic()
        checkout_started_epoch = time.time()
        outcome_context, order, location, outcome_error = (
            self._checkout_to_outcome(
                data,
                dict(base_context),
                checkout_started,
                checkout_started_epoch,
            )
        )
        settled_context.update(
            {
                "accepted": outcome_context.get("accepted", False),
                "outcome": outcome_context.get("outcome", "UNKNOWN"),
                "order_id": outcome_context.get("order_id"),
            }
        )
        if outcome_error is not None or order is None or location is None:
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

        tracker = gevent.spawn(
            self._track_settlement,
            location,
            order,
            checkout_started,
            checkout_started_epoch,
            settled_context,
        )
        SETTLEMENT_TRACKERS.add(tracker)
        tracker.link(lambda completed: SETTLEMENT_TRACKERS.discard(completed))

    def _track_settlement(
        self,
        location: str,
        order: dict[str, Any],
        checkout_started: float,
        checkout_started_epoch: float,
        context: dict[str, Any],
    ) -> None:
        error: BusinessFailure | None = failure_message("INCOMPLETE")
        try:
            error = self._wait_for_settlement(
                location, order, checkout_started, context
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
        self.user.environment.events.request.fire(
            request_type="BUSINESS",
            name="checkout_to_settled",
            response_time=(time.monotonic() - checkout_started) * 1000,
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
        dict[str, Any] | None,
        str | None,
        BusinessFailure | None,
    ]:
        final_order: dict[str, Any] | None = None
        location: str | None = None
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
                        response.failure(
                            "unexpected checkout status "
                            f"{response.status_code}"
                        )
                        context["outcome"] = "HTTP_FAILURE"
                        acceptance_context["outcome"] = "HTTP_FAILURE"
                        error = failure_message(
                            "HTTP_FAILURE", f"status {response.status_code}"
                        )
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
                            delay = retry_after_seconds(response)

            if error is not None or location is None:
                outcome_measurement["exception"] = error
                return context, None, None, error

            per_order_deadline = (
                checkout_started + CONFIG.outcome_timeout_seconds
            )
            deadline = min(per_order_deadline, drain_deadline())
            while time.monotonic() < deadline:
                gevent.sleep(delay)
                with self.client.get(
                    location,
                    headers=self.headers("application/json"),
                    catch_response=True,
                    name="/orders/[id]",
                ) as status_response:
                    delay = retry_after_seconds(status_response)
                    if status_response.status_code in (404, 503):
                        status_response.success()
                        continue
                    if status_response.status_code != 200:
                        status_response.failure(
                            "unexpected order status "
                            f"{status_response.status_code}"
                        )
                        context["outcome"] = "STATUS_ERROR"
                        error = failure_message(
                            "STATUS_ERROR",
                            f"status {status_response.status_code}",
                        )
                        outcome_measurement["exception"] = error
                        RECORDER.terminal(
                            transaction_id, "STATUS_ERROR", phase_now()
                        )
                        return context, None, location, error
                    try:
                        order = status_response.json()
                    except Exception:
                        status_response.failure("order response is not JSON")
                        context["outcome"] = "INVALID_RESPONSE"
                        error = failure_message(
                            "INVALID_RESPONSE", "order status is not JSON"
                        )
                        outcome_measurement["exception"] = error
                        RECORDER.terminal(
                            transaction_id, "INVALID_RESPONSE", phase_now()
                        )
                        return context, None, location, error
                    status_response.success()
                    status = str(order.get("status", "UNKNOWN"))
                    if status not in TERMINAL_ORDER_STATUSES:
                        continue
                    try:
                        response_time = outcome_latency_ms(
                            checkout_started_epoch, order.get("outcome_at")
                        )
                    except ValueError as timestamp_error:
                        context["outcome"] = "INVALID_RESPONSE"
                        error = failure_message(
                            "INVALID_RESPONSE", str(timestamp_error)
                        )
                        outcome_measurement["exception"] = error
                        RECORDER.terminal(
                            transaction_id, "INVALID_RESPONSE", phase_now()
                        )
                        return context, None, location, error
                    outcome_measurement["response_time"] = response_time
                    context.update(
                        {
                            "outcome": status,
                            "order_id": order.get("order_id")
                            or context.get("order_id"),
                            "failure_code": order.get("failure_code"),
                            "outcome_at": order.get("outcome_at"),
                        }
                    )
                    RECORDER.terminal(transaction_id, status, phase_now())
                    final_order = order
                    if status != "COMPLETED":
                        error = failure_message(
                            status, str(order.get("failure_code", ""))
                        )
                        outcome_measurement["exception"] = error
                    return context, final_order, location, error

            outcome = (
                "INCOMPLETE"
                if drain_deadline() <= per_order_deadline
                else "TIMEOUT"
            )
            context["outcome"] = outcome
            error = failure_message(outcome)
            outcome_measurement["exception"] = error
            RECORDER.terminal(transaction_id, outcome, phase_now())
            return context, None, location, error

    def _wait_for_settlement(
        self,
        location: str,
        order: dict[str, Any],
        checkout_started: float,
        context: dict[str, Any],
    ) -> BusinessFailure | None:
        delay = 1.0
        per_order_deadline = (
            checkout_started + CONFIG.settlement_timeout_seconds
        )
        deadline = min(per_order_deadline, drain_deadline())
        while True:
            notification_status = str(order.get("notification_status", ""))
            cart_clear_status = str(order.get("cart_clear_status", ""))
            context.update(
                {
                    "notification_status": notification_status or None,
                    "cart_clear_status": cart_clear_status or None,
                    "cart_clear_failure_code": order.get(
                        "cart_clear_failure_code"
                    ),
                }
            )
            if (
                notification_status in TERMINAL_NOTIFICATION_STATUSES
                and cart_clear_status in TERMINAL_CART_CLEAR_STATUSES
            ):
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
            gevent.sleep(delay)
            with self.client.get(
                location,
                headers=self.headers("application/json"),
                catch_response=True,
                name="/orders/[id] [settlement]",
            ) as status_response:
                delay = retry_after_seconds(status_response)
                if status_response.status_code in (404, 503):
                    status_response.success()
                    continue
                if status_response.status_code != 200:
                    status_response.failure(
                        "unexpected settlement status "
                        f"{status_response.status_code}"
                    )
                    context["settlement"] = "STATUS_ERROR"
                    return failure_message(
                        "STATUS_ERROR", f"status {status_response.status_code}"
                    )
                try:
                    order = status_response.json()
                except Exception:
                    status_response.failure("settlement response is not JSON")
                    context["settlement"] = "INVALID_RESPONSE"
                    return failure_message("INVALID_RESPONSE")
                status_response.success()


def adapter_for(
    user: FastHttpUser, seed: int, session_id: str | None = None
) -> StorefrontAdapter:
    adapter_type = NatsAdapter if CONFIG.application_type == "NATS" else GrpcAdapter
    return adapter_type(user, seed, session_id)


class ClosedLoopUser(FastHttpUser):
    wait_time = between(1, 10)
    network_timeout = 10.0
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
        if not self.adapter.add_item():
            self.adapter.record_precondition_failure()
            return
        self.adapter.checkout()


class OpenLoopDriver(FastHttpUser):
    fixed_count = 1
    wait_time = lambda self: 0
    network_timeout = 10.0
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
        if not adapter.add_item():
            adapter.record_precondition_failure(
                scheduled_at, schedule_delay_ms
            )
            return
        adapter.checkout(scheduled_at, schedule_delay_ms)
