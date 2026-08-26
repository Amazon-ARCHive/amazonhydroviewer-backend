"""This Script Illustrate Build Fire Risk Forecast From The Latest Initialization"""

from __future__ import annotations

import argparse
import numpy as np
import xarray as xr
import modules.utils as arch
import get_ldas_probabilistic_output as prob
from pathlib import Path

def parse_args()->argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fire-risk-method", type=str, required=True)
    parser.add_argument("--forecast-tmp-files", type=Path, required=True)
    parser.add_argument('--output-zarr', type=Path, required=True)
    return parser.parse_args()


def build_fire_risk_soilm(
        soilmoist_fcst : Path,
        soilmoist_var : str,
        w2f : bool = False,
) -> np.Array:
    """
    Soil-Moisture Based Fire Risk:
    Fire-prone areas identified where the probability of 
    below-normal soil moisture exceeds 60%, 
    indicating sustained dry fuel conditions.

    Args:
        soilmoist
    """
    try: 
        ds = xr.open_dataset(soilmoist_fcst, engine='zarr')
        da_soilm_bnormal = ds[soilmoist_var].isel(
            {'category' : 0,
            'SoilMoist_profiles' : 1}
        ).drop_vars('category')
        ds.close()
        fcst_fire_risk = (
            (da_soilm_bnormal > 60)
            .rename("FireRisk_fcst_soilm")
            .drop_encoding()
        )
        if w2f: 
            fcst_fire_risk.to_zarr()
            return 
        else:
            return fcst_fire_risk
    
    except Exception as e:
        raise e
    
    

def build_fire_risk_tp(
        vars : list[str],
        fcst_file_dir : Path,
        
) -> np.Array:
    """
    Meteorological (T/P) - Based Fire Risk: 
    Precipitation is below normal with >60% probability and
    temperature is above normal with >60% probability, 
    representing concurrent hot-dry atmospheric conditions 
    that elevate ignition likelihood.
    """
    
    for var in vars: 
        fcst_file = fcst_file_dir / f"{var}"
        ds = xr.open_mfdataset(fcst_file ,engine='zarr')
        if var == 'Rainf_tavg':
            da_rainf_bnormal = ds[var].isel({'category': 0}).drop_vars('category')
        elif var == 'Tair_f_tavg':
            da_temp_anormal = ds[var].isel({'category' : 2}).drop_vars('category')
        ds.close()

    fcst_fire_tp = (
        ((da_temp_anormal < 60) & (da_rainf_bnormal > 60))
        .rename("FireRisk_fcst_tp")
        .drop_encoding()
    )

    return fcst_fire_tp

def main() -> None:
    args = parse_args()
    if args.fire_risks_method == 'tp':
        fcst_fire_risk = build_fire_risk_tp()
    elif args.fire_risks_method == 'soilm':
        fcst_fire_risk = build_fire_risk_soilm()
    fcst_fire_risk.to_zarr(args.output_zarr)
    

if __name__ == "__main__":
    main()