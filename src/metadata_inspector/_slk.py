"""Module that interacts with slk.

Until we do have access to the rest api we'll have to rely on the command line
tools.
"""


from __future__ import annotations
import base64
from datetime import datetime, timedelta
from getpass import getuser
import json
from pathlib import Path
import os
import re
import shutil
import warnings
from subprocess import run, PIPE, SubprocessError, CalledProcessError

from hurry.filesize import alternative, size
import requests

MODULES_CMD_ENV = "MODULES_CMD"
SLK = "slk"

SESSION_PATH = Path("~").expanduser() / ".slk" / "config.json"


def load_module() -> dict[str, str]:
    """Load the slk module."""
    module_path = os.environ.get(
        MODULES_CMD_ENV, "/usr/share/Modules/libexec/modulecmd.tcl"
    )
    if shutil.which("slk") is not None:
        return os.environ.copy()
    _env: dict[str, str] = {}
    try:
        res = (
            run(
                [module_path, "python", "load", SLK],
                check=True,
                stdout=PIPE,
                stderr=PIPE,
            )
            .stdout.decode()
            .split("\n")
        )
    except (FileNotFoundError, CalledProcessError) as error:
        warnings.warn(f"Could not load {SLK}: {error}")
        return _env
    for line in res:
        if "=" in line:
            key, _, value = line.partition("=")
            new_key = (re.findall(r"\['([^']*)", key.strip()) or [""])[0]
            if new_key.strip():
                _env[new_key.strip()] = value.strip().replace("'", "")
    return _env


def get_file_size(input_path: str) -> str:
    """Extract the size of an object on the HSM store.

    Parameters
    ----------
    input_path: str
        The path to the hsm ojbect

    Returns
    -------
    str: A string representation of the size of the ojbect
    """
    command = ["slk_helpers", "size", input_path]
    try:
        res = run(
            command, env=load_module(), check=True, stdout=PIPE, stderr=PIPE
        )
    except SubprocessError as error:  # pragma: no cover
        warnings.warn(
            f"Error: could not get meta-data: {error}"
        )  # pragma: no cover
        return "unkown"  # pragma: no cover
    try:
        fsize = int(res.stdout.decode().strip())
    except TypeError:  # pragma: no cover
        return "unkown"  # pragma: no cover
    return size(fsize, system=alternative)


def get_slk_metadata(input_path: str) -> dict[str, dict[str, str]]:
    """Extract dataset metdata from path in the hsm.

    Parameters
    ----------
    input_path: str
        The hsm path the metdata is extracted from


    Returns
    -------
    str: string representation of the metdata
    """
    command = ["slk_helpers", "metadata", input_path]
    try:
        res = run(
            command, env=load_module(), check=True, stdout=PIPE, stderr=PIPE
        )
    except SubprocessError as error:  # pragma: no cover
        warnings.warn(
            f"Error: could not get meta-data: {error}"
        )  # pragma: no cover
        return {}  # pragma: no cover
    # This needs to be done because the output of the command is only nearly
    # yaml. That is the ":" for the first keys are missing:
    # For example:
    # document
    #      Version: foo
    # netcdf
    #     id: bar
    #     var_name: tas
    # Since yaml needs could not handle this we have to add the ':' to the
    # keys manually.
    data: dict[str, dict[str, str]] = {}
    data.setdefault("netcdf", {})
    for line in res.stdout.decode().split("\n"):
        if line.startswith("netcdf") or line.startswith("document"):
            main_key = line.strip()
            data[main_key] = {}
            continue
        if line and not line[0].strip():
            key, _, value = line.partition(":")
            current_key = key.strip()
            data[main_key][current_key] = value.strip()
        elif line:
            data[main_key][current_key] += line.strip()
    data["netcdf"]["file_size"] = get_file_size(input_path)
    return data


def get_expiration_date() -> datetime:
    """Get the expiration date of the session key."""
    session_path = Path("~").expanduser() / ".slk" / "config.json"
    now = datetime.now()
    for fmt in ("%a %b %d %H:%M:%S %Z %Y", "%a %b %d %H:%M:%S %Y"):
        try:
            with session_path.open() as f_obj:
                date = json.load(f_obj).get("expireDate", now.strftime(fmt))
            return datetime.strptime(date, fmt)
        except FileNotFoundError:  # pragma: no cover
            break  # pragma: no cover
        except ValueError:
            pass
    return now


def _login_via_request(passwd: str) -> None:
    data = {
        "data": {
            "attributes": {
                "domain": "ldap",
                "name": getuser(),
                "password": passwd,
            },
            "type": "authentication",
        }
    }
    headers = {"Content-type": "application/json"}
    fmt = "%a %b %d %H:%M:%S %Z %Y"
    exp_date = (datetime.now() + timedelta(days=20)).astimezone().strftime(fmt)
    url = "https://archive.dkrz.de/api/v2/authentication"
    res = requests.post(
        url, data=json.dumps(data), headers=headers, verify=False
    )
    key = (
        res.json().get("data", {}).get("attributes", {}).get("session_key", "")
    )
    if key:
        sec = {"user": getuser(), "sessionKey": key, "expireDate": exp_date}
        SESSION_PATH.parent.mkdir(exist_ok=True, parents=True)
        with SESSION_PATH.open("w") as f_obj:
            json.dump(sec, f_obj)
        SESSION_PATH.chmod(0o600)


def login() -> None:
    """Login to the system."""
    passwd = os.environ.get("LC_TELEPHONE", "")
    now = datetime.now()
    exp_date = get_expiration_date()
    diff = (exp_date - now).total_seconds()
    if passwd:
        passwd = base64.b64decode(passwd.encode()).decode()
        _login_via_request(passwd)
    elif diff <= 0:
        print("Your session has expired, login to slk")
        run(["slk", "login"], shell=False, check=True, env=load_module())
