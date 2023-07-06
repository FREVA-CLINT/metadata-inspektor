"""Test for the slk module."""

import mock
import os
from pathlib import Path
import shutil


def test_load_module(slk_bin: Path) -> None:
    """Testing the module load command."""
    from metadata_inspector._slk import load_module, MODULES_CMD_ENV

    env = os.environ.copy()
    env[MODULES_CMD_ENV] = str(slk_bin)
    mock_env = load_module()
    assert mock_env == {}
    with mock.patch.dict(os.environ, env, clear=True):
        mock_env = load_module()
        assert "PATH" in mock_env
        assert shutil.which("slk", path=mock_env["PATH"]) is not None
        assert shutil.which("slk_helpers", path=mock_env["PATH"]) is not None
    with mock.patch.dict(os.environ, mock_env, clear=True):
        tmp_env = load_module()
        assert os.environ["PATH"] == mock_env["PATH"] == tmp_env["PATH"]
