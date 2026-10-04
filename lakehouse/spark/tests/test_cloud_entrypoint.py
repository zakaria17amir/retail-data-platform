import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest
from lakehouse_spark.cli import lakehouse_uri
from lakehouse_spark.cloud.entrypoint import parse_args

PACKAGE_SH = Path(__file__).resolve().parents[1] / "cloud" / "package.sh"


def test_lakehouse_uri_prefers_env_and_defaults_to_local_bucket() -> None:
    assert lakehouse_uri({"LAKEHOUSE_URI": "s3://retail-lakehouse/"}) == "s3://retail-lakehouse"
    assert lakehouse_uri({"LAKEHOUSE_BUCKET": "lake"}) == "s3a://lake"
    assert lakehouse_uri({}) == "s3a://lakehouse"


@pytest.mark.parametrize("command", ["silver", "export"])
def test_parse_args_command_run_id_and_root_from_env(command: str) -> None:
    args = parse_args([command, "--run-id", "20260927T0100Z-ab12"], {"LAKEHOUSE_URI": "s3://lh"})
    assert (args.command, args.run_id, args.root) == (command, "20260927T0100Z-ab12", "s3://lh")


def test_parse_args_root_flag_overrides_env() -> None:
    args = parse_args(["export", "--run-id", "r1", "--root", "s3://other/"], {})
    assert args.root == "s3://other"


@pytest.mark.parametrize(
    "argv",
    [
        ["quality", "--run-id", "r1"],  # GX stays local/Airflow
        ["export"],
        ["export", "--run-id", ""],
        ["export", "--run-id", "../silver"],
        ["export", "--run-id", "a/b"],
    ],
)
def test_parse_args_rejects(argv: list[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        parse_args(argv, {})
    assert exc.value.code == 2


@pytest.mark.skipif(shutil.which("sh") is None, reason="needs a POSIX sh")
def test_package_sh_builds_importable_py_files_zip(tmp_path: Path) -> None:
    out = tmp_path / "dist" / "lakehouse_spark.zip"
    env = {**os.environ, "PYTHON": sys.executable}
    subprocess.run(["sh", PACKAGE_SH.as_posix(), out.as_posix()], check=True, env=env)
    names = zipfile.ZipFile(out).namelist()
    assert "lakehouse_spark/cloud/entrypoint.py" in names
    assert "lakehouse_spark/silver/job.py" in names
    assert all(n.startswith("lakehouse_spark/") and n.endswith(".py") for n in names)
    probe = "import lakehouse_spark.cloud.entrypoint as m; print(m.__file__)"
    found = subprocess.run(
        [sys.executable, "-c", f"import sys; sys.path.insert(0, {str(out)!r}); {probe}"],
        check=True,
        capture_output=True,
        text=True,
        cwd=tmp_path,
    ).stdout
    assert "lakehouse_spark.zip" in found
