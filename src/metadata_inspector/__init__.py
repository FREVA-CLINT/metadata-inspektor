"""Metadata inspector."""
from __future__ import annotations
import argparse
from functools import partial
import os
from pathlib import Path
from subprocess import run, PIPE, SubprocessError
import warnings
import sys
from typing import TextIO

from cftime import num2date
from dask import array as dask_array
from hurry.filesize import alternative, size
import numpy as np
import xarray as xr
import yaml

from ._version import __version__

SLK_PATH = "/sw/spack-levante/slk-3.3.21-5xnsgp/bin"
JDK_PATH = "/sw/spack-levante/openjdk-17.0.0_35-k5o6dr/bin"
JAVA_HOME = "/sw/spack-levante/openjdk-17.0.0_35-k5o6dr"


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
    env = os.environ.copy()
    env["PATH"] = f"{SLK_PATH}:{env['PATH']}"
    env["PATH"] = f"{JDK_PATH}:{env['PATH']}"
    env["JAVA_HOME"] = "{JAVA_HOME}"
    command = ["slk_helpers", "metadata", input_path]
    try:
        res = run(command, env=env, check=True, stdout=PIPE, stderr=PIPE)
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


def _summarize_datavar(name: str, var: xr.DataArray, col_width: int) -> str:

    out = [xr.core.formatting.summarize_variable(name, var.variable, col_width)]
    if var.attrs:
        n_spaces = 0
        for k in out[0]:
            if k == " ":
                n_spaces += 1
            else:
                break
        out += [
            n_spaces * " " + line
            for line in xr.core.formatting.attrs_repr(var.attrs).split("\n")
        ]
    return "\n".join(out)


def parse_args() -> tuple[list[Path], bool]:
    """Construct command line argument parser."""

    argp = argparse.ArgumentParser
    app = argp(
        prog="metadata-inspector",
        description="""Inspect meta data of a weather/climate datasets""",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    app.add_argument(
        "input",
        metavar="input",
        type=Path,
        nargs="+",
        help="Input files that will be processed",
    )
    app.add_argument(
        "--html", action="store_true", help="Create html representation of the dataset."
    )
    app.add_argument(
        "--version",
        "-V",
        action="version",
        version="%(prog)s {version}".format(version=__version__),
    )
    args = app.parse_args()
    return args.input, args.html


def dataset_from_hsm(input_file: str) -> xr.Dataset:
    """Create a dataset view from attributes."""

    attrs: dict[str, dict[str, str]] = (
        yaml.safe_load(get_slk_metadata(input_file)) or {}
    ).get("Keywords", {})

    dset = xr.Dataset({}, attrs=attrs.pop("global"))
    for dim in attrs.pop("dims"):
        size = int(attrs[dim].pop("size"))
        start, end = float(attrs[dim].pop("start")), float(attrs[dim].pop("end"))
        vec = np.linspace(start, end, size)
        if dim == "time":
            vec = num2date(vec, attrs[dim]["units"], attrs[dim]["calendar"])
        dset[dim] = xr.DataArray(vec, name=dim, dims=(dim,), attrs=attrs[dim])
    for data_var in attrs.pop("data_vars"):
        dims = attrs[data_var].pop("dims")
        sizes = [dset[d].size for d in dims]
        dset[data_var] = xr.DataArray(
            dask_array.empty(sizes), name=data_var, dims=dims, attrs=attrs[data_var]
        )
    return dset


def _get_files(input_: list[Path]) -> tuple[list[str], list[str]]:
    """Get all files from given input"""

    files_fs: list[str] = []
    files_archive: list[str] = []
    extensions: tuple[str, ...] = (
        ".nc",
        ".nc4",
        ".grb",
        ".grib",
        ".zarr",
        ".h5",
        ".hdf5",
    )
    for inp_file in input_:
        inp = inp_file.expanduser().absolute()
        if inp.is_dir() and inp.exists():
            files_fs += [
                str(inp_file)
                for inp_file in inp.rglob("*")
                if inp_file.suffix in extensions
            ]
        elif inp.is_file() and inp.exists():
            files_fs.append(str(inp))
        elif inp.parent.exists() and inp.parent.is_dir():
            files_fs += [
                str(inp_file)
                for inp_file in inp.parent.rglob(inp.name)
                if inp_file.suffix in extensions
            ]
        elif inp.parts[1] == "arch":
            files_archive.append(str(inp))
    return sorted(files_fs), sorted(files_archive)


def _open_datasets(files_fs: list[str], files_hsm: list[str]) -> xr.Dataset:

    kwargs = dict(
        parallel=True,
        combine="by_coords",
    )
    dsets: list[xr.Dataset] = []
    if files_fs:
        dsets.append(xr.open_mfdataset(files_fs, **kwargs))
    if files_hsm:
        for inp_file in files_hsm:
            dsets.append(dataset_from_hsm(inp_file))
    return xr.merge(dsets)


def main(input_files: list[Path], html: bool = False) -> tuple[str, TextIO]:
    """Print the representation of a dataset.

    Parameters
    ----------

    input_files: list[Path]
        Collection of input files that are to be inspected
    html: bool, default: True
        If true a representation suitable for html is displayed.
    """

    kwargs = dict(parallel=True, combine="by_coords",)
    files_to_open = _get_files(input_files)
    if not files_to_open:
        return "No files found", sys.stderr
    try:
        dset = _open_datasets(files_fs, files_hsm)
    except Exception as error:
        error_header = (
            "No data found, file(s) might be corrupted. See err. message below:"
        )
        error_msg = str(error)
        if html:
            error_msg = error_msg.replace("\n", "<br>")
            msg = f"<h2>{error_header}</h2><br><p>{error_msg}</p>"
        else:
            msg = f"{error_header}\n{error_msg}"
        return msg, sys.stderr
    fsize = size(dset.nbytes, system=alternative)
    if html:
        out_str = xr.core.formatting_html.dataset_repr(dset)
    else:
        xr.core.options.OPTIONS["display_expand_data_vars"] = True
        xr.core.options.OPTIONS["display_expand_attrs"] = True
        xr.core.options.OPTIONS["display_expand_data"] = True
        xr.core.options.OPTIONS["display_max_rows"] = 100
        xr.core.formatting.data_vars_repr = partial(
            xr.core.formatting._mapping_repr,
            title="Data variables",
            summarizer=_summarize_datavar,
            expand_option_name="display_expand_data_vars",
        )
        out_str = xr.core.formatting.dataset_repr(dset)
    replace_str = (
        ("xarray.Dataset", f"Dataset (byte-size: {fsize})"),
        ("<svg class='icon xr-icon-file-text2'>", "<i class='fa fa-file-text-o'>"),
        ("<svg class='icon xr-icon-database'>", "<i class='fa fa-database'>"),
        ("</use></svg>", "</use></i>"),
        ("numpy.", ""),
        ("np.", ""),
        ("dask.", ""),
    )
    for entry, replace in replace_str:
        out_str = out_str.replace(entry, replace)

    return out_str.encode("utf-8").decode("latin-1", "replace"), sys.stdout


def cli() -> None:
    """Command line argument inteface."""
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore")
        try:
            msg, text_io = main(*parse_args())
        except Exception as error:
            msg, text_io = f"Error: {error}"
    print(msg, file=text_io, flush=True)


if __name__ == "__main__":
    cli()
