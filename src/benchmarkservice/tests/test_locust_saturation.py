# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import types
import unittest
from unittest import mock

try:
    from locustfile import SaturationDriver
except (KeyError, ModuleNotFoundError):
    SaturationDriver = None


@unittest.skipUnless(SaturationDriver is not None, "Locust is not installed")
class SaturationDriverTest(unittest.TestCase):
    def test_coordination_failure_stops_runner_and_sets_failure_exit(self) -> None:
        runner = mock.Mock()
        driver = object.__new__(SaturationDriver)
        driver.environment = types.SimpleNamespace(
            process_exit_code=None,
            runner=runner,
        )

        with (
            mock.patch(
                "locustfile.SaturationCoordinator",
                side_effect=RuntimeError("coordination unavailable"),
            ),
            mock.patch("locustfile.gevent.spawn_later") as spawn_later,
            self.assertRaisesRegex(RuntimeError, "coordination unavailable"),
        ):
            driver.schedule()

        self.assertEqual(1, driver.environment.process_exit_code)
        spawn_later.assert_called_once_with(0, runner.quit)


if __name__ == "__main__":
    unittest.main()
