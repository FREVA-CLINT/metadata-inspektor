"""Metadata inspector."""
from __future__ import annotations
import argparse
from pathlib import Path

from hurry.filesize import alternative, size
import xarray as xr

from ._version import __version__


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


def _data_vars_repr(dset: xr.Dataset) -> str:

    out = []
    for data_var in dset.data_vars:
        var_repr = dset[data_var].__repr__().split("\n")[1:]
        if var_repr:
            var_1 = var_repr[0].split(",")
            var_1[0] = f"<Variable: pr"
            var_1 = ",".join(var_1)
            var_repr[0] = var_1
            var_repr_n = "\n\t".join(var_repr)
            out.append(var_repr_n)
    return "\n".join(out)


def dataset_repr(dset: xr.Dataset) -> str:
    """Create a representation of a xarray dataset.

    Parameters
    ----------
    ds: xr.Dataset
        xarray Dataset

    Returns
    -------
    str: String representation of the xarray dataset.

    """
    summary = ["{}".format(type(dset).__name__)]

    col_width = xr.core.formatting._calculate_col_width(
        xr.core.formatting._get_col_items(dset.variables)
    )

    dims_start = xr.core.formatting.pretty_print("Dimensions:", col_width)
    summary.append("{}({})".format(dims_start, xr.core.formatting.dim_summary(dset)))

    if dset.coords:
        summary.append(xr.core.formatting.coords_repr(dset.coords, col_width=col_width))

    unindexed_dims_str = xr.core.formatting.unindexed_dims_repr(dset.dims, dset.coords)
    if unindexed_dims_str:
        summary.append(unindexed_dims_str)

    summary.append(_data_vars_repr(dset))

    if dset.attrs:
        summary.append(xr.core.formatting.attrs_repr(dset.attrs))
    return "\n".join(summary)


def main(input_files: list[Path], html: bool = False) -> None:
    """Print the representation of a dataset.

    Parameters
    ----------

    input_files: list[Path]
        Collection of input files that are to be inspected
    html: bool, default: True
        If true a representation suitable for html is displayed.
    """

    kwargs = dict(
        parallel=True,
        coords="minimal",
        data_vars="minimal",
        compat="override",
        combine="nested",
        concat_dim="time",
        chunks={"time": -1},
    )
    try:
        dset = xr.open_mfdataset(input_files, **kwargs)
    except Exception as error:
        error_header = (
            "No data found, file(s) might be corrupted. See err. message below:"
        )
        error_msg = error.__str__()
        if html:
            error_msg = error_msg.replace("\n", "<br>")
            msg = f"<h2>{error_header}</h2><br><p>{error_msg}</p>"
        else:
            msg = f"{error_header}\n{error_msg}"
        print(msg)
        return
    fsize = size(dset.nbytes, system=alternative)
    if html:
        out_str = xr.core.formatting_html.dataset_repr(dset)
    else:
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

    print(out_str)


def cli() -> None:
    """Command line argument inteface."""

    main(*parse_args())


if __name__ == "__main__":
    cli()
