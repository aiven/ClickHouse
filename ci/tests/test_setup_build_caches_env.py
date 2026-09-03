import os
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
os.makedirs("./ci/tmp", exist_ok=True)
sys.modules.setdefault("requests", types.ModuleType("requests"))

from ci.jobs.build_clickhouse import ensure_sccache_server, setup_build_caches_env
from ci.praktika.settings import Settings
from ci.praktika.utils import Shell


def _clear_sccache_env():
    for key in list(os.environ):
        if key.startswith("SCCACHE_") or key in (
            "AWS_ACCESS_KEY_ID",
            "CTCACHE_S3_BUCKET",
            "CTCACHE_S3_FOLDER",
            "CTCACHE_S3_READ_ONLY",
            "CTCACHE_DIR",
            "CTCACHE_LOG_LEVEL",
        ):
            os.environ.pop(key, None)


class TestSetupBuildCachesEnv(unittest.TestCase):
    def setUp(self):
        _clear_sccache_env()

    def tearDown(self):
        _clear_sccache_env()

    def test_local_run_without_bucket_is_disk_only(self):
        info = MagicMock()
        info.is_local_run = True
        info.pr_number = 0

        setup_build_caches_env(info)

        self.assertNotIn("SCCACHE_BUCKET", os.environ)
        self.assertNotIn("SCCACHE_S3_KEY_PREFIX", os.environ)
        self.assertNotIn("SCCACHE_S3_NO_CREDENTIALS", os.environ)
        self.assertNotIn("SCCACHE_S3_READ_ONLY", os.environ)
        self.assertIn("SCCACHE_DIR", os.environ)

    def test_explicit_bucket_is_preserved_and_writable(self):
        info = MagicMock()
        info.is_local_run = True
        info.pr_number = 123
        os.environ["SCCACHE_BUCKET"] = "my-bucket"
        os.environ["SCCACHE_S3_KEY_PREFIX"] = "clickhouse/26.8/"
        os.environ["SCCACHE_S3_ALLOW_WRITE"] = "1"

        setup_build_caches_env(info)

        self.assertEqual(os.environ["SCCACHE_BUCKET"], "my-bucket")
        self.assertEqual(os.environ["SCCACHE_S3_KEY_PREFIX"], "clickhouse/26.8/")
        self.assertNotIn("SCCACHE_S3_NO_CREDENTIALS", os.environ)
        self.assertNotIn("SCCACHE_S3_READ_ONLY", os.environ)

    def test_explicit_bucket_without_allow_write_respects_pr_readonly(self):
        info = MagicMock()
        info.is_local_run = True
        info.pr_number = 7
        os.environ["SCCACHE_BUCKET"] = "my-bucket"

        setup_build_caches_env(info)

        self.assertEqual(os.environ["SCCACHE_BUCKET"], "my-bucket")
        self.assertEqual(os.environ.get("SCCACHE_S3_READ_ONLY"), "true")
        self.assertNotIn("SCCACHE_S3_NO_CREDENTIALS", os.environ)

    def test_upstream_ci_uses_settings_bucket(self):
        info = MagicMock()
        info.is_local_run = False
        info.pr_number = 0

        setup_build_caches_env(info)

        self.assertEqual(os.environ["SCCACHE_BUCKET"], Settings.S3_ARTIFACT_PATH)
        self.assertEqual(os.environ["SCCACHE_S3_KEY_PREFIX"], "ccache/sccache")
        self.assertEqual(os.environ["CTCACHE_S3_BUCKET"], Settings.S3_ARTIFACT_PATH)


class TestEnsureSccacheServer(unittest.TestCase):
    def setUp(self):
        _clear_sccache_env()

    def tearDown(self):
        _clear_sccache_env()

    def test_s3_failure_falls_back_to_disk_only(self):
        os.environ["SCCACHE_BUCKET"] = "unreachable-bucket"
        os.environ["SCCACHE_S3_KEY_PREFIX"] = "clickhouse/26.8/"
        calls = []

        def fake_check(command, **_kwargs):
            calls.append(command)
            if command == "sccache --start-server":
                return len([c for c in calls if c == "sccache --start-server"]) > 1
            return True

        with patch.object(Shell, "check", side_effect=fake_check):
            self.assertTrue(ensure_sccache_server())

        self.assertNotIn("SCCACHE_BUCKET", os.environ)
        self.assertNotIn("SCCACHE_S3_KEY_PREFIX", os.environ)
        self.assertEqual(calls.count("sccache --start-server"), 2)
        self.assertIn("sccache --stop-server", calls)

    def test_successful_start_keeps_bucket(self):
        os.environ["SCCACHE_BUCKET"] = "my-bucket"

        with patch.object(Shell, "check", return_value=True) as check:
            self.assertTrue(ensure_sccache_server())
            check.assert_called_once_with(
                "sccache --start-server", retries=1, verbose=True
            )

        self.assertEqual(os.environ["SCCACHE_BUCKET"], "my-bucket")


if __name__ == "__main__":
    unittest.main()
