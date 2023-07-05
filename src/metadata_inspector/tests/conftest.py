"""pytest definitions to run the unittests."""
from __future__ import annotations
import base64
from functools import partial
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Generator
import subprocess

import pytest
import mock
import numpy as np
import pandas as pd
import xarray as xr

meta_data = {
    "global": {
        "Conventions": "CF-1.4",
        "conventionsURL": "http://www.cfconventions.org",
        "title": "NUKLEUS eval run with ERA5 forcing on EURO-CORDEX 0.11",
        "project_id": "NUKLEUS-evaluation",
        "CORDEX_domain": "EUR-11",
        "driving_model_id": "ECMWF-ERA5",
        "driving_experiment_name": "evaluation",
        "experiment_id": "evaluation",
        "driving_experiment": "ECMWF-ERA5, reanalysis",
        "driving_model_ensemble_member": "r0i0p0",
        "experiment": (
            "NUKLEUS ERA5 evaluation run 1979-2020 with CLMcom-BTU-ICON-2-6-4"
        ),
        "model_id": "CLMcom-BTU-ICON-2-6-4_clm1",
        "institute_id": "CLMcom-BTU",
        "institution": (
            "Chair of Atmospheric Processes, "
            "BTU Cottbus-Senftenberg, Germany, "
            "in collaboration with the CLM-Community"
        ),
        "rcm_version_id": "v1",
        "contact": "klaus.keuler@b-tu.de / michael.woldt@b-tu.de",
        "nesting_levels": "1",
        "comment_nesting": "direct nesting in global forcing",
        "comment_2ndNest": "not used",
        "rcm_config_int2lm": "EUR-11_CLMcom-BTU-SPICE0-9-4_config",
        "source": "Climate Limited-area Modelling Community (CLM-Community)",
        "references": "http://cordex.clm-community.eu/",
        "product": "output",
        "frequency": "fx",
        "tracking_id": "4bf8768a-cf86-11ec-9fd6-0800383c5a29",
        "creation_date": "2022-05-09 12:53:44",
    },
    "dims": ["rlat", "rlon"],
    "data_vars": ["rotated_pole", "orog"],
    "rlat": {
        "standard_name": "grid_latitude",
        "long_name": "latitude in rotated pole grid",
        "units": "degrees",
        "axis": "Y",
        "size": "412",
        "start": "-23.375",
        "end": "21.834999084472656",
    },
    "rlon": {
        "standard_name": "grid_longitude",
        "long_name": "longitude in rotated pole grid",
        "units": "degrees",
        "axis": "X",
        "size": "424",
        "start": "-28.375",
        "end": "18.155000686645508",
    },
    "rotated_pole": {
        "long_name": "coordinates of the rotated North Pole",
        "grid_mapping_name": "rotated_latitude_longitude",
        "grid_north_pole_latitude": "39.25",
        "grid_north_pole_longitude": "-162.0",
        "dims": [],
    },
    "orog": {
        "standard_name": "surface_altitude",
        "long_name": "Surface Altitude",
        "units": "m",
        "grid_mapping": "rotated_pole",
        "dims": ["rlat", "rlon"],
    },
}


class SubProcess:
    def __init__(self, stdout: list[str]) -> None:
        self._stdout = "\n".join(stdout)

    @property
    def stdout(self) -> bytes:
        return self._stdout.encode()


class RequestMock:
    def __init__(
        self,
        url: str,
        data: dict[str, dict[str, str]] | None = None,
        headers: dict[str, str] | None = None,
        out: dict[str, dict[str, str]] | None = None,
        **kwargs: Any,
    ) -> None:
        self.url = url
        self.data = data
        self.headers = headers
        self.out = out

    def json(self) -> dict[str, dict[str, str]]:
        """Mock the json get value."""
        return self.out or {}

    @classmethod
    def post(cls, url: str, **kwargs: Any) -> RequestMock:
        """Mock the rest post method."""
        return cls(url, **kwargs)

    @classmethod
    def get(cls, url: str, **kwargs: Any) -> RequestMock:
        """Mock the rest get method."""
        return cls(url, **kwargs)


def run(command: list[str], **kwargs: Any) -> SubProcess:
    """Patch the subprocess.run command."""

    main_command, sub_cmd = command[0:2]
    if main_command.startswith("slk_helpers"):
        if sub_cmd == "metadata":
            cmd = "document\n"
            cmd += "   Keywords: " + json.dumps(meta_data)
            cmd += "\n   Version: ae7677769b0a757248659ddbbb83f224"
    return SubProcess([cmd])


def create_data(variable_name: str, size: int) -> xr.Dataset:
    """Create a netcdf dataset."""
    coords: dict[str, np.ndarray] = {}
    coords["x"] = np.linspace(-10, -5, size)
    coords["y"] = np.linspace(120, 125, size)
    lat, lon = np.meshgrid(coords["y"], coords["x"])
    lon_vec = xr.DataArray(lon, name="Lg", coords=coords, dims=("y", "x"))
    lat_vec = xr.DataArray(lat, name="Lt", coords=coords, dims=("y", "x"))
    coords["time"] = np.array(
        [
            np.datetime64("2020-01-01T00:00"),
            np.datetime64("2020-01-01T12:00"),
            np.datetime64("2020-01-02T00:00"),
            np.datetime64("2020-01-02T12:00"),
        ]
    )
    dims = (4, size, size)
    data_array = np.empty(dims)
    for time in range(dims[0]):
        data_array[time] = np.zeros((size, size))
    dset = xr.DataArray(
        data_array,
        dims=("time", "y", "x"),
        coords=coords,
        name=variable_name,
    )
    data_array = np.zeros(dims)
    return xr.Dataset(
        {variable_name: dset, "Lt": lon_vec, "Lg": lat_vec}
    ).set_coords(list(coords.keys()))


@pytest.fixture(scope="function")
def patch_file(session_path: Path) -> Generator[Path, None, None]:
    req = {"data": {"attributes": {"session_key": "secret"}}}
    post = partial(RequestMock.post, out=req)
    subprocess.run = run  # type: ignore
    env = os.environ.copy()
    env["LC_TELEPHONE"] = base64.b64encode("foo".encode()).decode()
    with mock.patch.dict(os.environ, env, clear=True):
        with mock.patch("metadata_inspector._slk.SESSION_PATH", session_path):
            with mock.patch("requests.post", post):
                with mock.patch("requests.get", RequestMock.get):
                    with mock.patch("subprocess.run", run):
                        yield session_path


@pytest.fixture(scope="session")
def data() -> Generator[xr.Dataset, None, None]:
    """Define a simple dataset with a blob in the middle."""
    dset = create_data("precip", 100)
    yield dset


@pytest.fixture(scope="session")
def zarr_file(data: xr.Dataset) -> Generator[Path, None, None]:
    """Save a zarr dataset to disk."""
    with TemporaryDirectory() as td:
        zarr_data = Path(td) / "precip.zarr"
        data.to_zarr(zarr_data, mode="w")
        yield zarr_data


@pytest.fixture(scope="session")
def netcdf_files(data: xr.Dataset) -> Generator[Path, None, None]:
    """Save data with a blob to file."""

    with TemporaryDirectory() as td:
        for time in (data.time[:2], data.time[2:]):
            time1 = pd.Timestamp(time.values[0]).strftime("%Y%m%d%H%M")
            time2 = pd.Timestamp(time.values[1]).strftime("%Y%m%d%H%M")
            out_file = (
                Path(td)
                / "the_project"
                / "test1"
                / "precip"
                / f"precip_{time1}-{time2}.nc"
            )
            out_file.parent.mkdir(exist_ok=True, parents=True)
            data.sel(time=time).to_netcdf(
                out_file, mode="w", engine="h5netcdf"
            )
        yield Path(td)


@pytest.fixture(scope="session")
def session_path() -> Generator[Path, None, None]:
    with TemporaryDirectory() as temp_dir:
        yield Path(temp_dir) / "slk.json"
