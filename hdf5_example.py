import os
import h5py
import numpy as np
import dask.array as da

h5file_path = 'myfile.hdf5'
if os.path.exists(h5file_path):
    os.unlink(h5file_path)

# create a continuous uniform distribution between 0.0 and 1.0
arr = np.random.random(size=(10000, 2000))

with h5py.File(h5file_path, 'w') as h5f:
    h5f.create_dataset('dataset_1', data=arr)

with h5py.File(h5file_path, 'r') as h5f:
    dset = h5f['dataset_1'][:]
    x = da.from_array(dset, chunks=(1000, 1000))
    result = x.mean().compute()
    print(result)  # should be pretty clonse to 0.5
