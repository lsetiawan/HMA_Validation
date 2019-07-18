import zarr
import s3fs
import os
import boto3
import xarray as xr
from glob import glob
from dask.diagnostics import ProgressBar
from netCDF4 import Dataset
import h5netcdf, h5py

SNAME    = 'MAR'
S3_root  = 's3://pangeo-data-upload-oregon/icesat2/HMA_Validation/'
MAR_path = os.path.join( S3_root , 'Data', SNAME )
ZAR_path = os.path.join( S3_root , 'Zarr', SNAME )
MAR_locl = os.path.join( os.getcwd() , 'Data' , SNAME ) 
ZAR_locl = os.path.join( os.getcwd() , 'Zarr' , SNAME ) 

fs       = s3fs.S3FileSystem()

fns = sorted(glob(MAR_locl+'/*.nc'))
nfns0 = len(fns)
fnlist = range(nfns0)


for ii in range( len(fnlist) ):
	
    ifn = fnlist[ii]
    fn  = fns[ifn]
    sfn = os.path.split(fn)[1]    
    print(ii,sfn)
    ds  = xr.open_dataset(fn,chunks={'TIME':31,'X11_210':50,'Y11_190':30})
    zpath    = os.path.join(ZAR_path,sfn+'.zarr')
    ds_store = s3fs.S3Map(root=zpath,s3=fs,check=True)
    compressor = zarr.Blosc(cname='zstd', clevel=3)
    encoding = {vname: {'compressor': compressor} for vname in ds.data_vars}
    delayed_obj = ds.to_zarr(store=ds_store,encoding=encoding,compute=False) 
    with ProgressBar():
        results = delayed_obj.compute()
    ds.close()
    