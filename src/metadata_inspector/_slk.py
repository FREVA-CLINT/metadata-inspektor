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
import warnings
from subprocess import run, PIPE, SubprocessError

import requests

SLK_PATH = "/sw/spack-levante/slk-3.3.21-5xnsgp/bin"
JDK_PATH = "/sw/spack-levante/openjdk-17.0.0_35-k5o6dr/bin"
JAVA_HOME = "/sw/spack-levante/openjdk-17.0.0_35-k5o6dr"

SESSION_PATH = Path("~").expanduser() / ".slk" / "config.json"


def get_env() -> dict[str, str]:
    """Prepare the environment variables for slk."""

    env = os.environ.copy()
    env["PATH"] = f"{SLK_PATH}:{env['PATH']}"
    env["PATH"] = f"{JDK_PATH}:{env['PATH']}"
    env["JAVA_HOME"] = "{JAVA_HOME}"
    return env


def get_slk_metadata(input_path: str) -> str:
    """Extract dataset metdata from path in the hsm.

    Parameters
    ----------
    input_path: Path
        The hsm path the metdata is extracted from


    Returns
    -------
    str: string representation of the metdata
    """
    command = ["slk_helpers", "metadata", input_path]
    try:
        res = run(command, env=get_env(), check=True, stdout=PIPE, stderr=PIPE)
    except SubprocessError as error:
        warnings.warn(f"Error: could not get metdata: {error}")
        return ""
    lines: list[str] = []
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
    for line in [o.strip() for o in res.stdout.decode().split("\n")]:
        if line.strip().lower().startswith("keywords:"):
            lines.append(line)
    return "\n".join(lines)


def get_expiration_date() -> datetime:
    """Get the expiration date of the session key."""
    session_path = Path("~").expanduser() / ".slk" / "config.json"
    fmt = "%a %b %d %H:%M:%S %Z %Y"
    now = datetime.now().astimezone().strftime(fmt)
    try:
        with session_path.open() as f_obj:
            date = json.load(f_obj).get("expireDate", now)
    except FileNotFoundError:
        date = now
    return datetime.strptime(date, fmt)


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
        run(["slk", "login"], shell=False, check=True, env=get_env())


if __name__ == "__main__":
    login()
