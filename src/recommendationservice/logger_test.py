#!/usr/bin/python
# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import logging
import os
import unittest
import uuid
from unittest.mock import patch

from logger import getJSONLogger


class LoggerConfigurationTests(unittest.TestCase):

  def logger(self):
    logger = getJSONLogger(f"recommendationservice-test-{uuid.uuid4()}")
    self.addCleanup(logger.handlers.clear)
    return logger

  def test_defaults_to_info(self):
    with patch.dict(os.environ, {}, clear=True):
      logger = self.logger()

    self.assertEqual(logging.INFO, logger.level)
    self.assertFalse(logger.isEnabledFor(logging.DEBUG))

  def test_debug_logging_can_be_enabled(self):
    with patch.dict(os.environ, {"LOG_LEVEL": "debug"}, clear=True):
      logger = self.logger()

    self.assertEqual(logging.DEBUG, logger.level)
    self.assertTrue(logger.isEnabledFor(logging.DEBUG))


if __name__ == "__main__":
  unittest.main()
