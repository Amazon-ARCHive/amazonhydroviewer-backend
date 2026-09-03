"""Build wildfire-risk products from probabilistic LDAS forecasts."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import xarray as xr


RAINF_VARIABLE = "Rainf_tavg"
TEMPERATURE_VARIABLE = "Tair_f_tavg"
SOIL_MOISTURE_VARIABLE = "SoilMoist_inst"


def probability_store(
    probability_dir: str | Path,
    init_date: str,
    variable: str,
) -> Path:
    """Return the Zarr store produced by ``get_prob_fcst.mainloop``."""
    return Path(probability_dir) / f"{init_date}_tercile_prob_max_{variable}"


def _read_category(
    store: str | Path,
    variable: str,
    category: int,
) -> xr.DataArray:
    path = Path(store)
    if not path.exists():
        raise FileNotFoundError(f"Probabilistic forecast store not found: {path}")

    with xr.open_dataarray(path, engine="zarr") as probability:
        if probability.name not in (None, variable):
            raise ValueError(
                f"Expected variable {variable!r} in {path}, "
                f"found {probability.name!r}."
            )
        if "category" not in probability.dims:
            raise ValueError(f"Category dimension not found in {path}")
        return probability.isel(category=category, drop=True).load()


def build_fire_risk_soilm(
    soil_fcst_file: str | Path,
    *,
    soilmoist_var: str = SOIL_MOISTURE_VARIABLE,
    minimum_probability: float = 60.0,
    soil_profile_index: int = 1,
) -> xr.DataArray:
    """Return risk where below-normal soil moisture exceeds the threshold."""
    below_normal = _read_category(soil_fcst_file, soilmoist_var, category=0)

    profile_dim = next(
        (
            name
            for name in ("SoilMoist_profiles", "soil_moisture_profile", "depth")
            if name in below_normal.dims
        ),
        None,
    )
    if profile_dim is not None:
        if not 0 <= soil_profile_index < below_normal.sizes[profile_dim]:
            raise IndexError(
                f"Soil profile index {soil_profile_index} is outside "
                f"{profile_dim} (size {below_normal.sizes[profile_dim]})."
            )
        below_normal = below_normal.isel({profile_dim: soil_profile_index}, drop=True)

    risk = (below_normal > minimum_probability).rename("FireRisk_fcst_soilm")
    risk.attrs.update(
        description="Below-normal soil-moisture probability exceeds threshold",
        minimum_probability_percent=minimum_probability,
        source_variable=soilmoist_var,
    )
    return risk.drop_encoding()


def build_fire_risk_tp(
    fcst_file_dir: str | Path,
    *,
    init_date: str,
    variables: Sequence[str] = (RAINF_VARIABLE, TEMPERATURE_VARIABLE),
    minimum_probability: float = 60.0,
) -> xr.DataArray:
    """Return risk where below-normal rain and above-normal heat coincide."""
    required = {RAINF_VARIABLE, TEMPERATURE_VARIABLE}
    missing = required.difference(variables)
    if missing:
        raise ValueError(f"T/P fire risk requires variables: {sorted(missing)}")

    probability_dir = Path(fcst_file_dir)
    rain_below = _read_category(
        probability_store(probability_dir, init_date, RAINF_VARIABLE),
        RAINF_VARIABLE,
        category=0,
    )
    temperature_above = _read_category(
        probability_store(probability_dir, init_date, TEMPERATURE_VARIABLE),
        TEMPERATURE_VARIABLE,
        category=2,
    )
    rain_below, temperature_above = xr.align(
        rain_below, temperature_above, join="exact"
    )

    risk = (
        (rain_below > minimum_probability)
        & (temperature_above > minimum_probability)
    ).rename("FireRisk_fcst_tp")
    risk.attrs.update(
        description=(
            "Below-normal precipitation and above-normal temperature "
            "probabilities both exceed threshold"
        ),
        minimum_probability_percent=minimum_probability,
        source_variables=f"{RAINF_VARIABLE},{TEMPERATURE_VARIABLE}",
    )
    return risk.drop_encoding()
