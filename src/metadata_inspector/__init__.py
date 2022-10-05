"""Metadata inspector."""
from __future__ import annotations
import argparse
from functools import partial
from pathlib import Path
import warnings
import sys
from typing import Optional, TextIO

from cftime import num2date
from dask import array as dask_array
from hurry.filesize import alternative, size
import numpy as np
import xarray as xr
import yaml

from ._version import __version__
from ._slk import get_slk_metadata, login


def _summarize_datavar(name: str, var: xr.DataArray, col_width: int) -> str:

    out = [
        xr.core.formatting.summarize_variable(name, var.variable, col_width)
    ]
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


def parse_args(args: Optional[list[str]] = None) -> tuple[list[Path], bool]:
    """Construct command line argument parser."""

    argp = argparse.ArgumentParser
    app = argp(
        prog="metadata-inspector",
        description=(
            "Inspect meta data of a weather/climate datasets "
            "with help of xarray"
        ),
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
        "--html",
        action="store_true",
        help="Create html representation of the dataset.",
    )
    app.add_argument(
        "--version",
        "-V",
        action="version",
        version="%(prog)s {version}".format(version=__version__),
    )
    parsed_args = app.parse_args(args)
    return parsed_args.input, parsed_args.html


def dataset_from_hsm(input_file: str) -> xr.Dataset:
    """Create a dataset view from attributes."""

    attrs: dict[str, dict[str, str]] = (
        yaml.safe_load(get_slk_metadata(input_file)) or {}
    ).get("Keywords", {})

    dset = xr.Dataset({}, attrs=attrs.pop("global"))
    for dim in attrs.pop("dims"):
        size = int(attrs[dim].pop("size"))
        start, end = float(attrs[dim].pop("start")), float(
            attrs[dim].pop("end")
        )
        vec = np.linspace(start, end, size)
        if dim == "time":
            vec = num2date(vec, attrs[dim]["units"], attrs[dim]["calendar"])
        dset[dim] = xr.DataArray(vec, name=dim, dims=(dim,), attrs=attrs[dim])
    for data_var in attrs.pop("data_vars"):
        dims = attrs[data_var].pop("dims")
        sizes = [dset[d].size for d in dims]
        dset[data_var] = xr.DataArray(
            dask_array.empty(sizes),
            name=data_var,
            dims=dims,
            attrs=attrs[data_var],
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
        ".grib2",
        ".grb2",
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
                if inp_file.suffix.lower() in extensions
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
        use_cftime=True,
    )
    dsets: list[xr.Dataset] = []
    if files_fs:
        dsets.append(xr.open_mfdataset(files_fs, **kwargs))
    if files_hsm:
        login()
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

    files_fs, files_hsm = _get_files(input_files)
    if not files_fs and not files_hsm:
        return "No files found", sys.stderr
    try:
        dset = _open_datasets(files_fs, files_hsm)
    except Exception as error:
        error_header = (
            "No data found, file(s) might be corrupted. "
            "See err. message below:"
        )
        error_msg = str(error)
        if html:
            error_msg = error_msg.replace("\n", "<br>")
            msg = (
                "<p><b>Error:</b>Could not open dataset for more details "
                "do not use the --html flag.</p>"
            )
            return msg, sys.stdout

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
        (
            "<svg class='icon xr-icon-file-text2'>",
            "<i class='fa fa-file-text-o'>",
        ),
        ("<svg class='icon xr-icon-database'>", "<i class='fa fa-database'>"),
        ("</use></svg>", "</use></i>"),
        ("numpy.", ""),
        ("np.", ""),
        ("dask.", ""),
    )
    for entry, replace in replace_str:
        out_str = out_str.replace(entry, replace)

    return out_str, sys.stdout


def cli(args: Optional[list[str]] = None) -> None:
    """Command line argument inteface."""
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore")
        try:
            msg, text_io = main(*parse_args(args))
        except Exception as error:
            msg, text_io = f"Error: {error}", sys.stderr
    print(msg, file=text_io, flush=True)
