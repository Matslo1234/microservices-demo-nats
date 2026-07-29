#!/usr/bin/python
# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import unittest

from nats_events import _consume, _stop


class Message:

  def __init__(self):
    self.data = b""
    self.subject = "boutique.evt.test"
    self.acks = 0
    self.naks = 0

  async def ack(self):
    self.acks += 1

  async def nak(self, delay):
    del delay
    self.naks += 1


class Subscription:

  def __init__(self, message):
    self.message = message
    self.requests = []

  async def fetch(self, **request):
    self.requests.append(request)
    return [self.message]


class StreamingConsumerTests(unittest.IsolatedAsyncioTestCase):

  async def asyncSetUp(self):
    _stop.clear()

  async def asyncTearDown(self):
    _stop.clear()

  async def test_processes_each_message_as_soon_as_it_arrives(self):
    message = Message()
    subscription = Subscription(message)
    processed = []

    async def handler(received):
      processed.append(received)
      _stop.set()

    await _consume(subscription, handler)

    self.assertEqual([message], processed)
    self.assertEqual([{"batch": 1, "timeout": 1}], subscription.requests)
    self.assertEqual(1, message.acks)
    self.assertEqual(0, message.naks)


if __name__ == "__main__":
  unittest.main()
