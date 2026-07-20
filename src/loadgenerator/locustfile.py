#!/usr/bin/python
#
# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import random
import time
import uuid
from locust import FastHttpUser, TaskSet, between
from faker import Faker
import datetime
fake = Faker()

products = [
    '0PUK6V6EV0',
    '1YMWWN1N4O',
    '2ZYFJ3GM2N',
    '66VCHSJNUP',
    '6E92ZMYYFZ',
    '9SIQT8TOJO',
    'L9ECAV7KIM',
    'LS4PSXUNUM',
    'OLJCESPC7Z']

def index(l):
    l.client.get("/")

def setCurrency(l):
    currencies = ['EUR', 'USD', 'JPY', 'CAD', 'GBP', 'TRY']
    l.client.post("/setCurrency",
        {'currency_code': random.choice(currencies)})

def browseProduct(l):
    l.client.get("/product/" + random.choice(products))

def viewCart(l):
    l.client.get("/cart")

def productMeta(l):
    selected = random.sample(products, 2)
    l.client.get("/product-meta/" + ",".join(selected), name="/product-meta/[ids]")

def addToCart(l):
    product = random.choice(products)
    l.client.get("/product/" + product)
    submit_cart_operation(l, "/cart", {
        'product_id': product,
        'quantity': random.randint(1,10)})
    
def empty_cart(l):
    submit_cart_operation(l, '/cart/empty', {})

def submit_cart_operation(l, path, data):
    idempotency_key = str(uuid.uuid4())
    with l.client.post(path, data, headers={
            'Idempotency-Key': idempotency_key,
            'Accept': 'application/json'},
            allow_redirects=False, catch_response=True, name=path) as response:
        if response.status_code in (302, 303):
            response.success()
            return
        if response.status_code != 202:
            response.failure(f"unexpected cart operation status {response.status_code}")
            return
        location = response.headers.get('Location')
        if not location:
            response.failure("202 cart operation omitted Location")
            return
        response.success()

    # Exercise JetStream/HTTP idempotency under normal load. A repeated key may
    # observe either the bounded compatibility redirect or the same durable
    # operation resource, but it must never create a second operation identity.
    with l.client.post(path, data, headers={
            'Idempotency-Key': idempotency_key,
            'Accept': 'application/json'}, allow_redirects=False,
            catch_response=True, name=path + " [idempotent retry]") as retry:
        if retry.status_code in (302, 303):
            retry.success()
        elif retry.status_code != 202:
            retry.failure(f"unexpected cart retry status {retry.status_code}")
            return
        elif retry.headers.get('Location') != location:
            retry.failure("cart retry returned a different operation resource")
            return
        else:
            retry.success()

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        with l.client.get(location, headers={'Accept': 'application/json'}, catch_response=True,
                name="/operations/[id]") as status_response:
            if status_response.status_code in (404, 503):
                status_response.success()
            elif status_response.status_code != 200:
                status_response.failure(
                    f"unexpected operation status {status_response.status_code}")
                return
            else:
                operation = status_response.json()
                if operation.get('status') == 'SUCCEEDED':
                    status_response.success()
                    return
                if operation.get('status') == 'REJECTED':
                    status_response.failure(
                        f"cart operation rejected: {operation.get('failure_code', 'unknown')}")
                    return
                status_response.success()
        time.sleep(0.1)
    with l.client.get(location, headers={'Accept': 'application/json'}, catch_response=True,
            name="/operations/[id]") as status_response:
        status_response.failure("cart operation did not finish within 10 seconds")

def checkout(l):
    addToCart(l)
    current_year = datetime.datetime.now().year+1
    idempotency_key = str(uuid.uuid4())
    checkout_data = {
        'email': fake.email(),
        'street_address': fake.street_address(),
        'zip_code': fake.zipcode(),
        'city': fake.city(),
        'state': fake.state_abbr(),
        'country': fake.country(),
        'credit_card_number': fake.credit_card_number(card_type="visa"),
        'credit_card_expiration_month': random.randint(1, 12),
        'credit_card_expiration_year': random.randint(current_year, current_year + 70),
        'credit_card_cvv': f"{random.randint(100, 999)}",
    }
    with l.client.post("/cart/checkout", checkout_data,
            headers={'Idempotency-Key': idempotency_key, 'Accept': 'application/json'},
            allow_redirects=False, catch_response=True, name="/cart/checkout") as response:
        if response.status_code != 202:
            response.failure(f"unexpected checkout status {response.status_code}")
            return
        location = response.headers.get('Location')
        if not location:
            response.failure("202 checkout response omitted Location")
            return
        response.success()

    with l.client.post("/cart/checkout", checkout_data,
            headers={'Idempotency-Key': idempotency_key, 'Accept': 'application/json'},
            allow_redirects=False, catch_response=True,
            name="/cart/checkout [idempotent retry]") as retry:
        if retry.status_code != 202:
            retry.failure(f"unexpected checkout retry status {retry.status_code}")
            return
        if retry.headers.get('Location') != location:
            retry.failure("checkout retry returned a different order resource")
            return
        retry.success()

    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        with l.client.get(location, headers={'Accept': 'application/json'},
                catch_response=True, name="/orders/[id]") as status_response:
            if status_response.status_code in (404, 503):
                status_response.success()
            elif status_response.status_code != 200:
                status_response.failure(
                    f"unexpected order status {status_response.status_code}")
                return
            else:
                order = status_response.json()
                if order.get('status') == 'COMPLETED':
                    status_response.success()
                    return
                if order.get('status') in ('CANCELLED', 'REJECTED', 'MANUAL_REVIEW'):
                    status_response.failure(
                        f"order ended as {order.get('status')}: {order.get('failure_code', '')}")
                    return
                status_response.success()
        time.sleep(0.1)
    with l.client.get(location, headers={'Accept': 'application/json'},
            catch_response=True, name="/orders/[id]") as status_response:
        status_response.failure("order did not reach a terminal state within 30 seconds")
    
def logout(l):
    l.client.get('/logout')  


class UserBehavior(TaskSet):

    def on_start(self):
        index(self)

    tasks = {index: 1,
        setCurrency: 2,
        browseProduct: 10,
        productMeta: 2,
        addToCart: 2,
        viewCart: 3,
        checkout: 1}

class WebsiteUser(FastHttpUser):
    tasks = [UserBehavior]
    wait_time = between(1, 10)
