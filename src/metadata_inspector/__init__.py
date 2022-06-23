"""Metadata inspector."""
from __future__ import annotations
import argparse
from functools import partial
from pathlib import Path
import warnings

from hurry.filesize import alternative, size
import xarray as xr

from ._version import __version__


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


def _get_files(input_: list[Path]) -> list[str]:
    """Get all files from given input"""

    files: list[str] = []
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
            files += [
                str(inp_file)
                for inp_file in inp.rglob("*")
                if inp_file.suffix in extensions
            ]
        elif inp.is_file() and inp.exists():
            files.append(str(inp))
        elif inp.parent.exists() and inp.parent.is_dir():
            files += [
                str(inp_file)
                for inp_file in inp.parent.rglob(inp.name)
                if inp_file.suffix in extensions
            ]
    return sorted(files)


def main(input_files: list[Path], html: bool = False) -> str:
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
        return "No files found"
    try:
        dset = xr.open_mfdataset(files_to_open, **kwargs)
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
        return msg
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

    return out_str.encode("utf-8")


def cli() -> None:
    """Command line argument inteface."""
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore")
        try:
            print(main(*parse_args()))
        except Exception as error:
            print(f"Error: {error}")


if __name__ == "__main__":
    cli()
