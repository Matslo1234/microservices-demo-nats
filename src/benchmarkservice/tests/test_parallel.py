import json
import tempfile
import unittest
from pathlib import Path

from parallel import (
    archive_directory,
    extract_archive,
    merge_worker_outputs,
)


class ParallelWorkerTest(unittest.TestCase):
    def test_merge_joins_business_and_rebases_outstanding_counts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            run = Path(temporary)
            workers = [run / "worker-0000", run / "worker-0001"]
            for index, worker in enumerate(workers):
                worker.mkdir()
                self._write_jsonl(
                    worker / "business.jsonl",
                    [
                        {
                            "timestamp": 2 - index,
                            "phase": "steady",
                            "name": "checkout_to_outcome",
                            "context": {"worker": index},
                        }
                    ],
                )
                self._write_jsonl(
                    worker / "outstanding.jsonl",
                    [
                        {
                            "timestamp": 1 + index * 0.1,
                            "event": "accepted",
                            "outstanding": 1,
                        },
                        {
                            "timestamp": 2 + index * 0.1,
                            "event": "completed",
                            "outstanding": 0,
                        },
                    ],
                )
                (worker / "runner.log").write_text(
                    f"log {index}\n", encoding="utf-8"
                )
                (worker / "locust_stats.csv").write_text(
                    f"worker\n{index}\n", encoding="utf-8"
                )
                (worker / "worker-status.json").write_text(
                    json.dumps(
                        {
                            "worker_index": index,
                            "state": "completed",
                            "exit_code": 0,
                        }
                    ),
                    encoding="utf-8",
                )
            (workers[0] / "resources.jsonl").write_text(
                '{"phase":"steady"}\n', encoding="utf-8"
            )

            statuses = merge_worker_outputs(run, workers)

            business = self._read_jsonl(run / "business.jsonl")
            outstanding = self._read_jsonl(run / "outstanding.jsonl")
            self.assertEqual([1, 2], [
                record["timestamp"] for record in business
            ])
            self.assertEqual([1, 2, 1, 0], [
                record["outstanding"] for record in outstanding
            ])
            self.assertEqual([0, 1], [
                status["worker_index"] for status in statuses
            ])
            self.assertTrue(
                (run / "locust_worker-0000_stats.csv").exists()
            )
            self.assertTrue(
                (run / "locust_worker-0001_stats.csv").exists()
            )
            self.assertEqual(
                '{"phase":"steady"}\n',
                (run / "resources.jsonl").read_text(encoding="utf-8"),
            )

    def test_worker_archive_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            destination = root / "destination"
            source.mkdir()
            (source / "result.json").write_text(
                '{"ok":true}', encoding="utf-8"
            )

            extract_archive(archive_directory(source), destination)

            self.assertEqual(
                '{"ok":true}',
                (destination / "result.json").read_text(encoding="utf-8"),
            )

    @staticmethod
    def _write_jsonl(path: Path, records: list[dict]) -> None:
        path.write_text(
            "".join(json.dumps(record) + "\n" for record in records),
            encoding="utf-8",
        )

    @staticmethod
    def _read_jsonl(path: Path) -> list[dict]:
        return [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
        ]


if __name__ == "__main__":
    unittest.main()
