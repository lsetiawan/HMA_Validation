#!/usr/bin/env python
import os
import sys
import glob
import re
import datetime

import pandas as pd
import numpy as np
import xarray as xr
from collections import OrderedDict
from dask.diagnostics import ProgressBar

__author__ = ['Anthony Arendt','Zheng Liu']



def get_xr_dataset_zarr(zstore,  twoDcoords=False, keepVars=['SMB'], keepDims=[],**kwargs):
    """
    Reads in High Mountain Asia MAR V3.5 Regional Climate Model Output from a zarr store. 
    Returns a "cleaned" xarray dataset.  
    :param zstore: path to the store containing the data. 
    :param keepVars: list of variables to keep
     **kwargs
        Arbitrary keyword arguments related to xarray open_zarr or other zarr operation.
    :return: xarray dataset
    """
    # some reformatting is necessary since MAR output does not follow CF conventions

    # first, optional selection of user-specified variables. This has to occur before the coordinate
    # manipulations below
	
    # Density of Water
    Ro_w = 1.e3
    
    # Necessary dimensions 
    needDims = ['TIME','X11_210','Y11_190']
    if 'SMB' in keepVars: needDims += ['SECTOR']
    keepDims += [tdim for tdim in needDims if tdim not in keepDims]
    
    ds   = xr.open_zarr(zstore, **kwargs)
    dzsn = ds['DZSN1']
    rosn = ds['ROSN1']
    swe  = (dzsn*rosn).sum('SNOLAY')/Ro_w
    
    tt   = ds.TIME
    t0   = tt[0]
    d_tt = ( tt - t0 ) / np.timedelta64(1,'D')
    if keepVars:
        try:
            products = [x for x in ds]
            deleted_vars = [y for y in products if y not in keepVars+['LAT','LON']]
            ds = ds.drop(deleted_vars)
            try:
                dims = ds.coords
                deleted_dims = [y for y in dims if y not in keepDims]
                ds = ds.drop(deleted_dims)
            except:
                print(keepDims)
                print("List of dimensions to keep does not match variable names in the dataset.")
                sys.exit("Exiting...")
        except:
            print("List of variables to keep does not match variable names in the dataset.")
            sys.exit("Exiting...")
    # add SWE to dataset
    ds   = ds.update({'SWE':swe})
    ds.update
    # rename the dimensions to be lat/long so that other himatpy utilities are consistent with this
    ds.rename({'LON':'long', 'LAT':'lat'}, inplace = True)
    ds.rename({'Y11_190':'Y', 'X11_210':'X','TIME':'time'}, inplace = True)
    ds.time.values = d_tt
    return ds