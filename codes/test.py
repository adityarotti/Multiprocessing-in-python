from multiprocessing import Pool, Manager
import numpy as np; 
import time

def f(arr):
    return len(arr)

t = time.time()
arr = np.arange(1000000)
print "construct array = ", time.time() - t;


pool = Pool(processes = 2)

t = time.time()
res = pool.apply_async(f, [arr,])
res.get()
print "multiprocessing overhead = ", time.time() - t;

