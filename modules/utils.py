"""Shared file, dataset, and coordinate helpers for LDAS workflows."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
import re
import shutil

import xarray as xr


_MONTHS = {
    month: index + 1
    for index, month in enumerate(
        [
            "jan", "feb", "mar", "apr", "may", "jun",
            "jul", "aug", "sep", "oct", "nov", "dec",
        ]
    )
}
_FORECAST_NAME_PATTERNS = (
    re.compile(r"^ldas_fcst_(\d{4})_([a-z]{3})(\d{2})\.nc$", re.I),
    re.compile(r"^ldas_fcst_(\d{4})(\d{2})(\d{2})\.nc$", re.I),
)


def _parse_date_from_name(name: str) -> datetime | None:
    """Parse an initialization date from a supported LDAS forecast filename."""
    for index, pattern in enumerate(_FORECAST_NAME_PATTERNS):
        match = pattern.match(name)
        if not match:
            continue

        if index == 0:
            year = int(match.group(1))
            month = _MONTHS.get(match.group(2).lower())
            day = int(match.group(3))
        else:
            year, month, day = map(int, match.groups())

        if month is None:
            return None
        try:
            return datetime(year, month, day)
        except ValueError:
            return None
    return None


def split_fcst_hcst(
    dir_path: str | Path,
    hdct_end_year: int = 2020,
    prefix: str = "ldas_fcst_",
    recursive: bool = False,
) -> tuple[str, list[str], datetime]:
    """Return the latest forecast and same-month hindcasts through a cutoff year."""
    base = Path(dir_path)
    if not base.is_dir():
        raise NotADirectoryError(f"Not a directory: {dir_path}")

    glob_pattern = "**/*.nc" if recursive else "*.nc"
    items: list[tuple[datetime, float, str, Path]] = []
    for path in base.glob(glob_pattern):
        if not path.is_file() or not path.name.startswith(prefix):
            continue
        initialization = _parse_date_from_name(path.name)
        if initialization is not None:
            items.append(
                (initialization, path.stat().st_mtime, path.name, path)
            )

    if not items:
        raise FileNotFoundError(
            f"No matching .nc files found in {dir_path} (prefix='{prefix}')"
        )

    items.sort(key=lambda item: (item[0], item[1], item[2]))
    forecast_date, _, _, forecast_path = items[-1]
    hindcasts = [
        path
        for initialization, _, _, path in items
        if initialization.year <= hdct_end_year
        and initialization.month == forecast_date.month
        and initialization.day == 1
    ]
    hindcasts.sort(key=lambda path: _parse_date_from_name(path.name))

    return str(forecast_path), [str(path) for path in hindcasts], forecast_date


def read_trim_forecast(file_path: str | Path, variable: str) -> xr.DataArray:
    """Open one forecast variable from a NetCDF dataset."""
    try:
        return xr.open_dataset(file_path)[variable]
    except KeyError:
        print(f"ERROR: Variable {variable} not found in dataset {file_path}")
        raise


def read_trim_hindcast(
    file_path: str | Path | Sequence[str | Path],
    variable: str,
) -> xr.DataArray:
    """Open one hindcast variable and rechunk its reduction dimensions."""
    try:
        hindcast = xr.open_mfdataset(file_path, join="outer")[variable]
    except KeyError:
        print(f"ERROR: Variable {variable} not found in hindcast dataset")
        raise

    chunk_dims = {
        dimension: -1
        for dimension in ("time", "ensemble")
        if dimension in hindcast.dims
    }
    return hindcast.chunk(chunk_dims) if chunk_dims else hindcast


def _find_variable(
    dataset: xr.Dataset | xr.DataArray,
    possible_names: Sequence[str],
) -> xr.DataArray:
    for name in possible_names:
        if name in dataset.coords:
            return dataset.coords[name]
        if isinstance(dataset, xr.Dataset) and name in dataset.variables:
            return dataset[name]
    raise AttributeError(
        f"None of the variable names {list(possible_names)} found in the dataset."
    )


def get_standard_coordinates(
    dataset: xr.Dataset | xr.DataArray,
    lon_names: Sequence[str] | None = None,
    lat_names: Sequence[str] | None = None,
    time_names: Sequence[str] | None = None,
) -> tuple[xr.DataArray, xr.DataArray, xr.DataArray]:
    """Return longitude, latitude, and time values under common aliases."""
    lon = _find_variable(dataset, lon_names or ("east_west", "lon", "longitude"))
    lat = _find_variable(dataset, lat_names or ("north_south", "lat", "latitude"))
    time = _find_variable(dataset, time_names or ("time", "month", "date"))
    return lon, lat, time


def _purge_path(path: str | Path) -> None:
    """Remove one file or directory tree if it exists."""
    target = Path(path)
    if target.is_dir():
        shutil.rmtree(target)
    elif target.exists():
        target.unlink()


def purge_old_init(directory: str | Path, current_init: str) -> None:
    """Remove non-JSON entries that do not belong to the current initialization."""
    for entry in Path(directory).iterdir():
        if entry.suffix == ".json":
            continue
        if current_init not in entry.name:
            _purge_path(entry)
            print(f"Deleted (old init): {entry}")
