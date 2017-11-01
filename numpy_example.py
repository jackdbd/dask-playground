import numpy as np
import dask.array as da

x = np.arange(1000)
y = da.from_array(x, chunks=(100))

y.visualize('numpy_example.svg')
y.visualize('numpy_example.pdf')

result = y.mean().compute()
print(result)
