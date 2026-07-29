#!/usr/bin/env python3
"""Regression tests for scripts/apply-latest.sh."""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "apply-latest.sh"


class ApplyLatestTest(unittest.TestCase):
    def test_uses_registry_digest_for_most_recently_pushed_tag(self) -> None:
        stale_digest = "1" * 64
        registry_digest = "b5ae14bc9920d3a3f5b2eb973005d98ca384a534d1f6f6753015f805df1a6af5"

        with tempfile.TemporaryDirectory() as temporary_directory:
            repository = Path(temporary_directory)
            scripts = repository / "scripts"
            release = repository / "release"
            fake_bin = repository / "bin"
            scripts.mkdir()
            release.mkdir()
            fake_bin.mkdir()

            copied_script = scripts / "apply-latest.sh"
            shutil.copy2(SCRIPT, copied_script)
            generator = scripts / "generate-release-manifests.py"
            generator.write_text(
                textwrap.dedent(
                    """\
                    DEFAULT_IMAGE_DIGESTS = {
                        "shippingservice": "0000000000000000000000000000000000000000000000000000000000000000",
                    }
                    """
                ),
                encoding="utf-8",
            )

            manifest = release / "manifest.yaml"
            manifest.write_text(
                "image: testuser/shippingservice:v0.4.0@sha256:" + ("0" * 64) + "\n",
                encoding="utf-8",
            )

            fake_curl = fake_bin / "curl"
            fake_curl.write_text(
                textwrap.dedent(
                    f"""\
                    #!/usr/bin/env bash
                    set -euo pipefail
                    url="${{!#}}"
                    case "${{url}}" in
                      https://hub.test/v2/*)
                        printf '%s\\n' '{{"next":null,"results":[{{"name":"v0.4.0","tag_status":"active","tag_last_pushed":"2026-07-28T12:00:00Z","digest":"sha256:{'2' * 64}"}},{{"name":"arbitrary-tag","tag_status":"active","tag_last_pushed":"2026-07-29T12:00:00Z","digest":"sha256:{stale_digest}"}}]}}'
                        ;;
                      https://auth.test/token)
                        printf '%s\\n' '{{"token":"test-token"}}'
                        ;;
                      https://registry.test/v2/testuser/shippingservice/manifests/arbitrary-tag)
                        printf 'HTTP/1.1 200 OK\\r\\nDocker-Content-Digest: sha256:{registry_digest}\\r\\n\\r\\n'
                        ;;
                      *)
                        echo "unexpected curl URL: ${{url}}" >&2
                        exit 1
                        ;;
                    esac
                    """
                ),
                encoding="utf-8",
            )
            fake_curl.chmod(0o755)

            environment = os.environ.copy()
            environment.update(
                {
                    "PATH": f"{fake_bin}:{environment['PATH']}",
                    "DOCKERHUB_USERNAME": "testuser",
                    "DOCKERHUB_API_BASE": "https://hub.test/v2",
                    "DOCKER_REGISTRY_BASE": "https://registry.test",
                    "DOCKER_REGISTRY_AUTH_BASE": "https://auth.test",
                    "DOCKER_REGISTRY_SERVICE": "registry.test",
                }
            )

            result = subprocess.run(
                [copied_script],
                cwd=repository,
                env=environment,
                check=True,
                capture_output=True,
                text=True,
            )

            self.assertIn(
                f"testuser/shippingservice:arbitrary-tag -> sha256:{registry_digest}",
                result.stdout,
            )
            self.assertEqual(
                f"image: testuser/shippingservice:v0.4.0@sha256:{registry_digest}\n",
                manifest.read_text(encoding="utf-8"),
            )
            self.assertNotIn(stale_digest, manifest.read_text(encoding="utf-8"))
            self.assertEqual(
                textwrap.dedent(
                    f"""\
                    DEFAULT_IMAGE_DIGESTS = {{
                        "shippingservice": "{registry_digest}",
                    }}
                    """
                ),
                generator.read_text(encoding="utf-8"),
            )


if __name__ == "__main__":
    unittest.main()
