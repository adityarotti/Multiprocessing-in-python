from multiprocessing import Pool
import numpy as np
import time

def f(x):
	return x**2

t1=time.time()
x=np.arange(5e6)
p=Pool(processes=6)
result1=p.map(f,x)
p.close()
p.join()
print("Parallel code took:",time.time()-t1)
#print result1
#print x

t2=time.time()
result2=[]
for i in x:
	result2.append(f(i))
print("Serial code took:",time.time()-t2)
#print result2
