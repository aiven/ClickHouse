import os
import sys
import types
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
os.makedirs("./ci/tmp", exist_ok=True)
sys.modules.setdefault("requests", types.ModuleType("requests"))

from ci.praktika import runner as runner_module
from ci.praktika.job import Job


class FakeEnv:
    WORKFLOW_CONFIG = True
    JOB_NAME = ""

    def dump(self):
        pass


class FakeWorkflow:
    enable_exit_code_result = False


class FakeRunConfig:
    digest_dockers = {"clickhouse/test": "sha256:test"}

    @staticmethod
    def from_workflow_data():
        return FakeRunConfig()


class FakeResult:
    class Status:
        ERROR = "error"

    @staticmethod
    def experimental_file_name_static():
        return "./ci/tmp/nonexistent-praktika-experimental-result.json"

    @staticmethod
    def experimental_from_fs(_job_name):
        raise AssertionError("experimental result should not be read")

    @staticmethod
    def from_fs(_job_name):
        return FakeResult()

    def is_completed(self):
        return True

    def is_skipped(self):
        return False

    def add_ext_key_value(self, *_args, **_kwargs):
        return self

    def set_label(self, *_args, **_kwargs):
        return self

    def dump(self):
        pass


class FakeProcess:
    timeout_exceeded = False

    def __init__(self, command, **_kwargs):
        self.command = command

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _tb):
        return False

    def wait(self):
        return 0


class PraktikaRunnerDockerOwnershipTest(unittest.TestCase):
    def test_root_docker_jobs_disable_bytecode_and_restore_checkout_ownership(self):
        run_commands = []
        popen_commands = []

        def fake_popen(command, **kwargs):
            popen_commands.append(command)
            return FakeProcess(command, **kwargs)

        with (
            patch.object(runner_module._Environment, "get", staticmethod(lambda: FakeEnv())),
            patch.object(runner_module, "RunConfig", FakeRunConfig),
            patch.object(runner_module, "Result", FakeResult),
            patch.object(runner_module.Utils, "is_amd", staticmethod(lambda: True)),
            patch.object(runner_module.Utils, "is_arm", staticmethod(lambda: False)),
            patch.object(runner_module.Shell, "check", staticmethod(lambda *_args, **_kwargs: False)),
            patch.object(
                runner_module.Shell,
                "run",
                staticmethod(lambda command, *_args, **_kwargs: run_commands.append(command)),
            ),
            patch.object(runner_module.Docker, "pull_image", staticmethod(lambda *_a, **_kw: 0)),
            patch.object(runner_module, "TeePopen", fake_popen),
            patch.object(runner_module.os, "getuid", lambda: 1234),
            patch.object(runner_module.os, "getgid", lambda: 5678),
        ):
            job = Job.Config(
                name="root docker job",
                runs_on=["self-hosted"],
                command="python3 -c 'import ci.jobs.functional_tests'",
                run_in_docker="clickhouse/test+root+--privileged",
            )

            exit_code = runner_module.Runner()._run(workflow=FakeWorkflow(), job=job)

        self.assertEqual(exit_code, 0)
        # The job container must not write `__pycache__` into the mounted checkout:
        # under `+root` those files would be root-owned and survive the run.
        self.assertIn(" -e PYTHONDONTWRITEBYTECODE=1 ", popen_commands[0])
        chown_commands = [c for c in run_commands if "chown -R" in c]
        self.assertEqual(len(chown_commands), 1)
        # Ownership of the whole checkout is restored, not only of the temp dir,
        # so root-owned files cannot poison the next run on a persistent worker.
        self.assertIn("chown -R 1234:5678 ", chown_commands[0])
        self.assertFalse(chown_commands[0].rstrip().endswith("ci/tmp"))
        self.assertTrue(chown_commands[0].rstrip().endswith(os.getcwd()))


if __name__ == "__main__":
    unittest.main()
